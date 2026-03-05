//CP372 Assignment - Reciever
//Caleb Gautreau and Bryce Co

import java.net.*;
import java.io.*;
import java.util.*;

public class Receiver {

    public static void main(String[] args) throws Exception {

        // Make sure argument length of 5 or else print error
        if (args.length < 5) {
            System.out.println("Usage: java Receiver <sender_ip> <sender_ack_port> <rcv_data_port> <output_file> <RN>");
            return;
        }
        
        // Read Arguments
        InetAddress senderAddress = InetAddress.getByName(args[0]); //What address to send Acks to
        int senderAckPort = Integer.parseInt(args[1]); //What port to send Acks to
        int rcvPort = Integer.parseInt(args[2]); // Listening Port
        String outFile = args[3]; //File to write to
        int RN = Integer.parseInt(args[4]); //Reliability number for chaos engine

        // Open socket on receive port
        DatagramSocket socket = new DatagramSocket(rcvPort);

        // Open file for Writing
        FileOutputStream fos = new FileOutputStream(outFile);

        int expectedSeq = 1;
        int ackCount = 0;

        Map<Integer, byte[]> buffer = new HashMap<>();

        byte[] buf = new byte[128];

        while (true) {

            DatagramPacket dp = new DatagramPacket(buf, buf.length);
            socket.receive(dp);

            DSPacket packet = new DSPacket(dp.getData());

            int seq = packet.getSeqNum();

            System.out.println("RECEIVED PACKET type=" + packet.getType() + " seq=" + seq);

            if (packet.getType() == DSPacket.TYPE_SOT) {

                System.out.println("RECEIVED SOT seq=" + seq); 

                DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, 0, null);

                ackCount++;

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

            if (packet.getType() == DSPacket.TYPE_DATA) {

                System.out.println("RECEIVED DATA seq=" + seq + " length=" + packet.getLength());

                if (seq == expectedSeq) {

                    System.out.println("DELIVERED seq=" + seq);

                    fos.write(packet.getPayload());

                    expectedSeq = (expectedSeq + 1) % 128;

                    while (buffer.containsKey(expectedSeq)) {
                        fos.write(buffer.remove(expectedSeq));
                        expectedSeq = (expectedSeq + 1) % 128;
                    }

                } else if (!buffer.containsKey(seq)) {

                    System.out.println("BUFFERED seq=" + seq);

                    buffer.put(seq, packet.getPayload());
                }

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

            if (packet.getType() == DSPacket.TYPE_EOT) {

                System.out.println("RECEIVED EOT seq=" + seq);

                DSPacket ack = new DSPacket(DSPacket.TYPE_ACK, seq, null);

                ackCount++;

                if (!ChaosEngine.shouldDrop(ackCount, RN)) {
                    socket.send(new DatagramPacket(
                            ack.toBytes(),
                            128,
                            senderAddress,
                            senderAckPort));

                    System.out.println("SENT ACK FOR EOT seq=" + seq);
                }

                break;
            }
        }

        fos.close();
        socket.close();
    }
}
