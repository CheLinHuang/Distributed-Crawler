import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class ListeningThread extends Thread {

    DatagramSocket serverSocket;
    DatagramSocket sendSocket;

    public ListeningThread() {
        try {
            serverSocket = new DatagramSocket(Daemon.packetPortNumber);
            sendSocket = new DatagramSocket();

        } catch (SocketException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


    public void updateMembershipList(String ID, String type, long counter) {
        // update the membership list according to the message content
        synchronized (Daemon.membershipList) {
            long[] values = Daemon.membershipList.get(ID);
            switch (type) {
                // heartbeat signal
                case "HB":
                    Daemon.membershipList.put(ID, new long[]{counter, System.currentTimeMillis()});
                    // if the node sending the heartbeat was detected as failure erringly
                    // automatically rejoin the node and send out the gossip message to inform other node
                    if (values == null) {
                        Protocol.sendGossip(ID, "Add", counter, 3, 4, sendSocket);
                        Daemon.writeLog("REJOIN", ID);
                        Daemon.hashValues.put(Hash.hashing(ID, 8), ID);
                        Daemon.updateNeighbors();
                        if (Daemon.neighborUpdated) Daemon.moveReplica(false);
                    }
                    break;
                // gossip message to add a new node
                case "Add":
                    if ((values == null) || (counter > values[0])) {
                        Daemon.membershipList.put(ID, new long[]{counter, System.currentTimeMillis()});
                        if (values == null) {
                            Daemon.hashValues.put(Hash.hashing(ID, 8), ID);
                            Daemon.updateNeighbors();
                            if (Daemon.neighborUpdated) Daemon.moveReplica(false);
                            Daemon.writeLog("ADD", ID);
                        }
                    }
                    break;
                // gossip message to add a new node
                case "Remove":
                    if ((values != null) && (counter > values[0])) {
                        Daemon.membershipList.remove(ID);
                        Daemon.hashValues.remove(Hash.hashing(ID, 8));
                        Daemon.updateNeighbors();
                        if (Daemon.neighborUpdated) Daemon.moveReplica(false);
                        Daemon.writeLog("REMOVE", ID);
                    }
                    break;
                // gossip message that some node leaves voluntarily
                case "Leave":
                    if (values != null) {
                        Daemon.membershipList.remove(ID);
                        Daemon.hashValues.remove(Hash.hashing(ID, 8), ID);
                        Daemon.updateNeighbors();
                        if (Daemon.neighborUpdated) Daemon.moveReplica(false);
                        Daemon.writeLog("REMOVE", ID);
                    }
            }
        }
    }

    @Override
    public void run() {

        try {

            byte[] receiveData = new byte[1024];

            while (true) {
                DatagramPacket receivePacket =
                        new DatagramPacket(receiveData, receiveData.length);

                serverSocket.receive(receivePacket);
                String message = new String(receivePacket.getData(),
                        0, receivePacket.getLength());

                String[] parseMsg = message.split("_");

                // flag = 0 --> heartbeat signal, flag = 1 --> gossip message
                String flag = parseMsg[0];

                switch (flag) {
                    // case 0 means heartbeat message
                    case "0":
                        updateMembershipList(parseMsg[1], "HB", Integer.parseInt(parseMsg[2]));
                        //Daemon.writeLog("MESSAGE", "HEARTBEAT");
                        break;
                    // case 1 means gossip message
                    case "1":
                        // If TTL > 1, relay the gossip message
                        int TTL = Integer.parseInt(parseMsg[4]);
                        if (TTL > 1) {
                            Protocol.sendGossip(parseMsg[1], parseMsg[2],
                                    Integer.parseInt(parseMsg[3]), --TTL, 4, sendSocket);
                        }
                        updateMembershipList(parseMsg[1], parseMsg[2], Integer.parseInt(parseMsg[3]));
                        //Daemon.writeLog("MESSAGE", "GOSSIP");
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
