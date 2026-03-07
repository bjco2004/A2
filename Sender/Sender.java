//CP372 Assignment - Sender
//Caleb Gautreau and Bryce Co

import java.net.*;
import java.io.*;
import java.util.*;

public class Sender {
   public static void main(String[] args) throws Exception {
       // Parse and validate command-line arguments
       if (args.length < 5) {
           System.out.println("Usage: java Sender <rcv_ip> <rcv_data_port> <sender_ack_port> <input_file> <timeout_ms> [window_size]");
           return;
       }
       String rcvIP = args[0];
       int rcvPort = Integer.parseInt(args[1]);
       int ackPort = Integer.parseInt(args[2]);
       String fileName = args[3];
       int timeout = Integer.parseInt(args[4]);

       // Determine protocol: Stop-and-Wait (no window) or GBN (with window)
       boolean gbn = args.length == 6;
       int windowSize = gbn ? Integer.parseInt(args[5]) : 1;
       // Validate window size for GBN: must be multiple of 4 and <= 128
       if (gbn) {
           if (windowSize % 4 != 0) {
               System.out.println("ERROR: Window size must be a multiple of 4");
               return;
           }
           if (windowSize > 128) {
               System.out.println("ERROR: Window size must be <= 128");
               return;
           }
       }
       InetAddress receiverAddress = InetAddress.getByName(rcvIP);
       // Create UDP socket that listens on ackPort for incoming ACKs
       DatagramSocket socket = new DatagramSocket(ackPort);
       socket.setSoTimeout(timeout);
       // Start timing the entire transmission (SOT to EOT ACK)
       long startTime = System.currentTimeMillis();
     
      // PHASE 1: HANDSHAKE
       // Send Start-of-Transfer packet with sequence number 0
       DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, null);
       socket.send(new DatagramPacket(sot.toBytes(), 128, receiverAddress, rcvPort));

       System.out.println("SENT SOT seq=0");

       // Wait for receiver to ACK the SOT
       byte[] buf = new byte[128];
       DatagramPacket dp = new DatagramPacket(buf, buf.length);
       socket.receive(dp);

       DSPacket ack = new DSPacket(dp.getData());
       System.out.println("RECEIVED ACK seq=" + ack.getSeqNum());
       if (ack.getType() != DSPacket.TYPE_ACK) return; 

       // PHASE 2: READ FILE AND CREATE DATA PACKETS
       FileInputStream fis = new FileInputStream(fileName);

       List<DSPacket> packets = new ArrayList<>();

       // Start sequence numbers at 1 (0 is reserved for SOT)
       int seq = 1;

       // Read file in chunks of 124 bytes (max payload per DATA packet)
       byte[] dataBuf = new byte[124];
       int bytesRead;

       while ((bytesRead = fis.read(dataBuf)) != -1) {
           // Create DATA packet with current sequence number and payload
           byte[] payload = Arrays.copyOf(dataBuf, bytesRead);
           packets.add(new DSPacket(DSPacket.TYPE_DATA, seq, payload));

           // Increment sequence number with wrap-around at 128
           seq = (seq + 1) % 128;
       }
       fis.close();
       // Handle empty file case: send EOT immediately with seq=1
       if (packets.isEmpty()) {
           int eotSeq = 1;
           DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, eotSeq, null);
           socket.send(new DatagramPacket(eot.toBytes(), 128, receiverAddress, rcvPort));
           System.out.println("SENT EOT seq=" + eotSeq);
          
           socket.receive(dp);
           DSPacket finalAck = new DSPacket(dp.getData());
           System.out.println("RECEIVED ACK FOR EOT seq=" + finalAck.getSeqNum());
          
           long endTime = System.currentTimeMillis();
           double elapsedSeconds = (endTime - startTime) / 1000.0;
           System.out.printf("Total Transmission Time: %.2f seconds%n", elapsedSeconds);
          
           socket.close();
           return;
       }
       if (!gbn) {
           // STOP-AND-WAIT RDT 3.0 PROTOCOL
           for (int i = 0; i < packets.size(); i++) {
               DSPacket p = packets.get(i);
               int consecutiveTimeouts = 0;
               // Keep resending until ACK is received for this specific packet
               while (true) {
                   socket.send(new DatagramPacket(p.toBytes(), 128, receiverAddress, rcvPort));
                   System.out.println("SENT DATA seq=" + p.getSeqNum() + " length=" + p.getLength());
                   try {
                       socket.receive(dp);
                       DSPacket recvAck = new DSPacket(dp.getData());
                       // Only proceed if ACK matches this packet's sequence number
                       if (recvAck.getType() == DSPacket.TYPE_ACK &&
                           recvAck.getSeqNum() == p.getSeqNum()) {
                           System.out.println("RECEIVED ACK seq=" + recvAck.getSeqNum());
                           break; // Move to next packet
                       }
                   } catch (SocketTimeoutException e) {
                       // No ACK received within timeout period
                       consecutiveTimeouts++;
                       System.out.println("TIMEOUT - RESENDING seq=" + p.getSeqNum()
                               + " (timeout " + consecutiveTimeouts + " of 3)");
                       // Critical failure: 3 consecutive timeouts for same packet
                       if (consecutiveTimeouts >= 3) {
                           System.out.println("ERROR: 3 consecutive timeouts. Unable to transfer file.");
                           socket.close();
                           return;
                       }
                       // Loop back to resend this packet
                   }
               }
           }


       } else {
           // GO-BACK-N (GBN) PROTOCOL
           int base = 0;      // Index of oldest unacknowledged packet
           int next = 0;      // Index of next packet to send
           int consecutiveTimeouts = 0;
           while (base < packets.size()) {
               // Inner loop: send up to windowSize packets in one burst
               while (next < base + windowSize && next < packets.size()) {
                   // Calculate how many packets left to send in this batch
                   int remaining = Math.min(4, Math.min(base + windowSize - next, packets.size() - next));
                   // For GBN: apply packet permutation to every 4 consecutive packets
                   // This tests receiver's out-of-order delivery and buffering
                   if (remaining == 4) {
                       List<DSPacket> group = new ArrayList<>();
                       group.add(packets.get(next));
                       group.add(packets.get(next + 1));
                       group.add(packets.get(next + 2));
                       group.add(packets.get(next + 3));


                       // Permute: (i, i+1, i+2, i+3) → (i+2, i, i+3, i+1) (chaos factor)
                       group = ChaosEngine.permutePackets(group);


                       for (DSPacket p : group) {
                           socket.send(new DatagramPacket(p.toBytes(), 128, receiverAddress, rcvPort));
                           System.out.println("SENT DATA seq=" + p.getSeqNum() + " length=" + p.getLength());
                       }


                       next += 4;
                   } else {
                       // Fewer than 4 packets remaining: send in normal order
                       DSPacket p = packets.get(next);
                       socket.send(new DatagramPacket(p.toBytes(), 128, receiverAddress, rcvPort));
                       System.out.println("SENT DATA seq=" + p.getSeqNum() + " length=" + p.getLength());
                       next++;
                   }
               }


               // Wait for cumulative ACK from receiver
               try {
                   socket.receive(dp);
                   DSPacket recvAck = new DSPacket(dp.getData());


                   if (recvAck.getType() != DSPacket.TYPE_ACK) {
                       continue;
                   }


                   int ackSeq = recvAck.getSeqNum();
                   System.out.println("RECEIVED ACK seq=" + ackSeq);


                   // Cumulative ACK: advance base to acknowledge all packets up to ackSeq
                   while (base < packets.size()) {
                       int pktSeq = packets.get(base).getSeqNum();
                       int distance = (ackSeq - pktSeq + 128) % 128;
                       if (distance < 128 / 2 || pktSeq == ackSeq) {
                           if (pktSeq == ((ackSeq + 1) % 128)) {
                               break; // Stop advancing base
                           }
                           base++;
                           if (pktSeq == ackSeq) {
                               break; // Next packet is unacked
                           }
                       } else {
                           break;
                       }
                   }


                   // Reset timeout counter on successful ACK (progress made)
                   consecutiveTimeouts = 0;


               } catch (SocketTimeoutException e) {
                   // No ACK received within timeout period
                   consecutiveTimeouts++;
                   System.out.println("TIMEOUT → RESENDING WINDOW (timeout " + consecutiveTimeouts + " of 3)");


                   // Critical failure: 3 consecutive timeouts without progress
                   if (consecutiveTimeouts >= 3) {
                       System.out.println("ERROR: 3 consecutive timeouts. Unable to transfer file.");
                       socket.close();
                       return;
                   }
                   // Resend entire window from base (Go-Back-N strategy)
                   next = base;
               }
           }
       }
      
      // PHASE 3: TEARDOWN
       // Send End-of-Transfer packet with sequence = (last data seq + 1) % 128
       int eotSeq = seq;
       DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, eotSeq, null);
       socket.send(new DatagramPacket(eot.toBytes(), 128, receiverAddress, rcvPort));
       System.out.println("SENT EOT seq=" + eotSeq);
       
      // Wait for receiver to ACK the EOT
       socket.receive(dp);
       DSPacket finalAck = new DSPacket(dp.getData());
       System.out.println("RECEIVED ACK FOR EOT seq=" + finalAck.getSeqNum());
      
      // Calculate and print total transmission time in seconds
       long endTime = System.currentTimeMillis();
       double elapsedSeconds = (endTime - startTime);
       System.out.printf("Total Transmission Time: %.2f ms%n", elapsedSeconds);
       socket.close();
   }
}

