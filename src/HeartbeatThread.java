import java.net.DatagramSocket;

public class HeartbeatThread extends Thread {

    // period is the period to send the heartbeat signal (in msec)
    private int period;
    private long counter;

    public HeartbeatThread(int period) {
        super("HeartbeatThread");
        this.period = period;
        this.counter = 1;
    }

    @Override
    public void run() {
        DatagramSocket sendSocket = null;

        try {
            sendSocket = new DatagramSocket();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        while (true) {
            try {

                // send the heartbeat every time period
                Thread.sleep(period);
                synchronized (Daemon.membershipList) {
                    Protocol.sendHeartBeat(Daemon.ID, counter++, sendSocket);
                    // update the counter and the timestamp
                    Daemon.membershipList.put(Daemon.ID, new long[]{counter, System.currentTimeMillis()});
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}