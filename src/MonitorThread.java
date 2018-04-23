import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.List;

public class MonitorThread extends Thread {

    @Override
    public void run() {

        DatagramSocket monitorSocket = null;

        // try until socket create correctly
        while (monitorSocket == null) {
            try {
                monitorSocket = new DatagramSocket();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        while (true) {

            // monitor the membership list every 500ms
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }

            boolean update = false;
            List<String> timeoutID = new ArrayList<>();
            List<Long> timeoutCounter = new ArrayList<>();

            synchronized (Daemon.membershipList) {
                synchronized (Daemon.neighbors) {
                    for (String key : Daemon.neighbors) {

                        // iterate through the neighbors, check if time until last time stamp is bigger than timeout
                        if (System.currentTimeMillis() - Daemon.membershipList.get(key)[1] > 2000) {

                            // if it's bigger than timeout, remove this node and record it
                            timeoutID.add(key);
                            timeoutCounter.add(Daemon.membershipList.get(key)[0]);
                            Daemon.membershipList.remove(key);
                            Daemon.hashValues.remove(Hash.hashing(key, 8));
                            Daemon.writeLog("FAILURE", key);
                            update = true;

                        } else {
                            //Daemon.writeLog("PASS", key);
                        }
                    }
                }
            }

            // If there is an update in membership list, send the gossip and update the neighbors
            if (update) {
                Daemon.updateNeighbors();
                if (Daemon.neighborUpdated) Daemon.moveReplica(false);
                for (int i = 0; i < timeoutID.size(); i++) {
                    Protocol.sendGossip(timeoutID.get(i), "Remove", timeoutCounter.get(i),
                            3, 4, monitorSocket);
                    Daemon.writeLog("REMOVE", timeoutID.get(i));
                }
            }
        }
    }
}
