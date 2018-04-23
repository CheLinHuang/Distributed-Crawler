import java.net.ServerSocket;

public class InstaLoaderServer extends Thread {

    public void run() {
        boolean listening = true;

        new InstaLoader().start();

        // Keep listening for incoming file related request
        try (ServerSocket serverSocket = new ServerSocket(Daemon.instaPortNumber)) {

            // Accept socket connection and create new thread
            while (listening)
                new InstaLoaderThread(serverSocket.accept()).start();

        } catch (Exception e) {
            System.err.println("Could not listen to port " + Daemon.instaPortNumber);
            System.exit(-1);
        }
    }
}