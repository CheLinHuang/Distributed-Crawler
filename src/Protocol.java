import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Protocol {

    public static void sendGossip(String ID, String action, long counter,
                                  int TTL, int numOfTarget, DatagramSocket sendSocket) {
        // create the gossip message to send
        byte[] sendData = ("1_" + ID + "_" + action + "_" + counter + "_" + TTL).getBytes();

        // randomly choose min(numOfTarget, sizeOfMembershipList) elements
        // from the membership list to send gossip message
        int size = Daemon.membershipList.size();
        List<Integer> targets = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            targets.add(i);
        }
        Collections.shuffle(targets);
        Object[] IDArray = Daemon.membershipList.keySet().toArray();
        for (int i = 0; i < Math.min(IDArray.length, numOfTarget); i++) {
            try {
                String target = ((String) IDArray[targets.get(i)]).split("#")[1];
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length,
                        InetAddress.getByName(target), Daemon.packetPortNumber);
                sendSocket.send(sendPacket);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void sendHeartBeat(String ID, long counter, DatagramSocket sendSocket) {
        try {
            // create the heartbeat message to send
            byte[] sendData = ("0_" + ID + "_" + counter).getBytes();

            // send the heartbeat signal to all its neighbor
            for (String neighbor : Daemon.neighbors) {

                DatagramPacket sendPacket =
                        new DatagramPacket(sendData, sendData.length,
                                InetAddress.getByName(neighbor.split("#")[1]), Daemon.packetPortNumber);
                sendSocket.send(sendPacket);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
