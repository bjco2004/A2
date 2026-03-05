import java.net.*;
import java.io.*;
import java.util.*;

public class Sender {

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.out.println("Usage: java Sender <rcv_ip> <rcv_data_port> <sender_ack_port> <input_file> <timeout_ms> [window_size]");
            return;
        }

        String rcvIP = args[0];
        int rcvPort = Integer.parseInt(args[1]);
        int ackPort = Integer.parseInt(args[2]);
        String fileName = args[3];
        int timeout = Integer.parseInt(args[4]);

        boolean gbn = args.length == 6;
        int windowSize = gbn ? Integer.parseInt(args[5]) : 1;

        InetAddress receiverAddress = InetAddress.getByName(rcvIP);

        DatagramSocket socket = new DatagramSocket(ackPort);
        socket.setSoTimeout(timeout);

        long startTime = System.currentTimeMillis();

        // SEND SOT
        DSPacket sot = new DSPacket(DSPacket.TYPE_SOT, 0, null);
        socket.send(new DatagramPacket(sot.toBytes(), 128, receiverAddress, rcvPort));

        System.out.println("SENT SOT seq=0");

        // WAIT FOR ACK 0
        byte[] buf = new byte[128];
        DatagramPacket dp = new DatagramPacket(buf, buf.length);
        socket.receive(dp);

        DSPacket ack = new DSPacket(dp.getData());
        System.out.println("RECEIVED ACK seq=" + ack.getSeqNum());
        if (ack.getType() != DSPacket.TYPE_ACK) return;

        FileInputStream fis = new FileInputStream(fileName);

        List<DSPacket> packets = new ArrayList<>();

        int seq = 1;

        byte[] dataBuf = new byte[124];
        int bytesRead;

        while ((bytesRead = fis.read(dataBuf)) != -1) {

            byte[] payload = Arrays.copyOf(dataBuf, bytesRead);
            packets.add(new DSPacket(DSPacket.TYPE_DATA, seq, payload));

            seq = (seq + 1) % 128;
        }

        fis.close();

        int base = 0;
        int next = 0;

        while (base < packets.size()) {

            while (next < base + windowSize && next < packets.size()) {

                List<DSPacket> group = new ArrayList<>();

                if (gbn && next + 3 < packets.size()) {

                    group.add(packets.get(next));
                    group.add(packets.get(next + 1));
                    group.add(packets.get(next + 2));
                    group.add(packets.get(next + 3));

                    group = ChaosEngine.permutePackets(group);

                    for (DSPacket p : group) {
                        socket.send(new DatagramPacket(p.toBytes(), 128, receiverAddress, rcvPort));
                        System.out.println("SENT DATA seq=" + p.getSeqNum() + " length=" + p.getLength());
                    }

                    next += 4;

                } else {

                    DSPacket p = packets.get(next);
                    socket.send(new DatagramPacket(p.toBytes(), 128, receiverAddress, rcvPort));
                    next++;
                }
            }

            try {

                socket.receive(dp);
                DSPacket recvAck = new DSPacket(dp.getData());

                int ackSeq = recvAck.getSeqNum();

                System.out.println("RECEIVED ACK seq=" + ackSeq);

                base = ackSeq;

            } catch (SocketTimeoutException e) {

                System.out.println("TIMEOUT → RESENDING WINDOW");

                next = base;
            }
        }

        int eotSeq = seq;
        DSPacket eot = new DSPacket(DSPacket.TYPE_EOT, eotSeq, null);

        socket.send(new DatagramPacket(eot.toBytes(), 128, receiverAddress, rcvPort));

        System.out.println("SENT EOT seq=" + eotSeq);

        socket.receive(dp);

        DSPacket finalAck = new DSPacket(dp.getData());
        System.out.println("RECEIVED ACK FOR EOT seq=" + finalAck.getSeqNum());

        long endTime = System.currentTimeMillis();

        System.out.println("Total Transmission Time: " +
                (endTime - startTime) + " ms");

        socket.close();
    }
}
