import java.net.ServerSocket;

public class FileServer extends Thread {

    public void run() {
        boolean listening = true;

        // Keep listening for incoming file related request
        try (ServerSocket serverSocket = new ServerSocket(Daemon.filePortNumber)) {

            // Accept socket connection and create new thread
            while (listening)
                new FileServerThread(serverSocket.accept()).start();

        } catch (Exception e) {
            System.err.println("Could not listen to port " + Daemon.filePortNumber);
            System.exit(-1);
        }
    }
}
