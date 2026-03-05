//CP372 Assignment - Reciever
//Caleb Gautreau and Bryce Co

import java.net.*;
import java.io.*;
import java.util.*;

public class Receiver {

    public static void main(String[] args) throws Exception {

        // Parse and validate command-line arguments
        if (args.length < 5) {
            System.out.println("Usage: java Receiver <sender_ip> <sender_ack_port> <rcv_data_port> <output_file> <RN> [window_size]");
            return;
        }
        
        // Read Arguments
        InetAddress senderAddress = InetAddress.getByName(args[0]); //What address to send Acks to
        int senderAckPort = Integer.parseInt(args[1]); //What port to send Acks to
        int rcvPort = Integer.parseInt(args[2]); // Listening Port
        String outFile = args[3]; //File to write to
        int RN = Integer.parseInt(args[4]); //Reliability number for chaos engine
        
        // Determine protocol: Stop-and-Wait (no window) or GBN (with window)
        // Window size - optional, defaults to 1 (Stop-and-Wait)
        boolean gbn = args.length == 6;
        int windowSize = gbn ? Integer.parseInt(args[5]) : 1;

        // Open UDP socket on rcv_data_port to listen for incoming DATA, SOT, EOT packets
        DatagramSocket socket = new DatagramSocket(rcvPort);

        // Open file for Writing received data
        FileOutputStream fos = new FileOutputStream(outFile);

        // Track next in-order sequence number expected from sender
        int expectedSeq = 1;
        
        // Track total ACKs sent (for chaos engine's ACK drop simulation)
        int ackCount = 0;

        // Buffer for out-of-order DATA packets (GBN feature)
        // Key = sequence number, Value = packet payload bytes
        Map<Integer, byte[]> buffer = new HashMap<>();

        byte[] buf = new byte[128];

        // PHASE 1 & 2: RECEIVE AND PROCESS PACKETS
        while (true) {

            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            socket.receive(dp);

            DSPacket packet = new DSPacket(dp.getData());

            int seq = packet.getSeqNum();

            System.out.println("RECEIVED PACKET type=" + packet.getType() + " seq=" + seq);

            // PHASE 1: HANDLE HANDSHAKE (SOT)
            if (packet.getType() == DSPacket.TYPE_SOT) {

                System.out.println("RECEIVED SOT seq=" + seq); 

                // Reply with ACK for SOT (always seq=0)
                DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, 0, null);

                ackCount++;

                // Chaos factor: may drop this ACK based on RN and ackCount
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    socket.send(new DatagramPacket(
                            ack.toBytes(),
                            128,
                            senderAddress,
                            senderAckPort));

                    System.out.println("SENT ACK seq=0");
                }

                continue;
            }

            // PHASE 2: HANDLE DATA PACKETS
            if (packet.getType() == DSPacket.TYPE_DATA) {

                System.out.println("RECEIVED DATA seq=" + seq + " length=" + packet.getLength());

                // Check if packet is within receive window
                // Window: [expectedSeq, expectedSeq + windowSize) modulo 128
                boolean inWindow = false;
                if (windowSize == 1) {
                    // Stop-and-Wait: only accept exact expectedSeq (strict in-order)
                    inWindow = (seq == expectedSeq);
                } else {
                    // GBN: check if seq is within receive window using modulo arithmetic
                    // seqOffset = distance from expectedSeq to seq (handles wrap-around)
                    int seqOffset = (seq - expectedSeq + 128) % 128;
                    inWindow = (seqOffset < windowSize);
                }

                // Discard packets outside window
                if (!inWindow) {
                    System.out.println("PACKET OUTSIDE WINDOW - DISCARDING");
                    // Resend cumulative ACK for last in-order packet
                    int ackSeq = (expectedSeq + 127) % 128;
                    DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, ackSeq, null);
                    ackCount++;
                    if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                        socket.send(new DatagramPacket(
                                ack.toBytes(),
                                128,
                                senderAddress,
                                senderAckPort));
                        System.out.println("SENT ACK seq=" + ackSeq);
                    }
                    continue;
                }

                // If packet matches expectedSeq, deliver it immediately
                if (seq == expectedSeq) {

                    System.out.println("DELIVERED seq=" + seq);

                    // Write payload to output file
                    fos.write(packet.getPayload());

                    // Advance expectedSeq with modulo-128 wrap-around
                    expectedSeq = (expectedSeq + 1) % 128;

                    // Deliver any buffered packets that are now in-order (GBN feature)
                    while (buffer.containsKey(expectedSeq)) {
                        fos.write(buffer.remove(expectedSeq));
                        expectedSeq = (expectedSeq + 1) % 128;
                    }

                } else if (!buffer.containsKey(seq)) {
                    // Out-of-order packet within window: buffer it for later delivery (GBN feature)
                    System.out.println("BUFFERED seq=" + seq);

                    buffer.put(seq, packet.getPayload());
                }

                // Send cumulative ACK: acknowledge all packets up to (expectedSeq - 1)
                // Cumulative ACK = (expectedSeq - 1) % 128 = (expectedSeq + 127) % 128
                int ackSeq = (expectedSeq + 127) % 128;

                DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, ackSeq, null);

                ackCount++;

                // Chaos factor: may drop this ACK based on RN and ackCount
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    socket.send(new DatagramPacket(
                            ack.toBytes(),
                            128,
                            senderAddress,
                            senderAckPort));

                    System.out.println("SENT ACK seq=" + ackSeq);
                }

                continue;
            }

            // PHASE 3: HANDLE TEARDOWN (EOT)
            if (packet.getType() == DSPacket.TYPE_EOT) {

                System.out.println("RECEIVED EOT seq=" + seq);

                // Reply with ACK for EOT (using same sequence number)
                DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, seq, null);

                ackCount++;

                // Chaos factor: may drop this ACK based on RN and ackCount
                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    socket.send(new DatagramPacket(
                            ack.toBytes(),
                            128,
                            senderAddress,
                            senderAckPort));

                    System.out.println("SENT ACK FOR EOT seq=" + seq);
                }

                // Exit main loop - transfer complete
                break;
            }
        }

        // Close file and socket
        fos.close();
        socket.close();
    }
}
