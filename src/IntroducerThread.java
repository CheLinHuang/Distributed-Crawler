import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Map;

public class IntroducerThread extends Thread {

    @Override
    public void run() {

        DatagramSocket introducerSocket = null;

        // try until socket create correctly
        while (introducerSocket == null) {
            try {
                introducerSocket = new DatagramSocket(Daemon.joinPortNumber);
            } catch (Exception e) {
                //e.printStackTrace();
            }
        }

        byte[] receiveData = new byte[1024];
        byte[] sendData;

        // keep listening to the join request
        while (true) {

            try {

                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                introducerSocket.receive(receivePacket);

                // the new member sent it's ID
                String joinID = new String(receivePacket.getData(), 0, receivePacket.getLength());

                // extract new member's IP address
                InetAddress IPAddress = InetAddress.getByName(joinID.split("#")[1]);

                // assign status and add to the membership list
                long[] status = {0, System.currentTimeMillis()};
                Daemon.membershipList.put(joinID, status);
                Daemon.hashValues.put(Hash.hashing(joinID, 8), joinID);

                // build the whole membership list string
                StringBuilder sb = new StringBuilder();
                synchronized (Daemon.membershipList) {
                    for (Map.Entry<String, long[]> entry : Daemon.membershipList.entrySet()) {
                        sb.append(entry.getKey());
                        sb.append('/');
                        long[] memberStatus = entry.getValue();
                        sb.append(memberStatus[0]);
                        sb.append('%');
                    }
                }

                // send the whole membership list back
                sendData = sb.toString().getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, receivePacket.getPort());
                introducerSocket.send(sendPacket);

                // Gossip the new member join
                Protocol.sendGossip(joinID, "Add", 0, 3, 4, introducerSocket);

                // update my neighbor
                Daemon.updateNeighbors();
                if (Daemon.neighborUpdated) Daemon.moveReplica(false);

                Daemon.writeLog("ADD", joinID);

            } catch (Exception e) {
                //e.printStackTrace();
            }
        }
    }
}
