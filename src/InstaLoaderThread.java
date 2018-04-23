import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;

public class InstaLoaderThread extends Thread {

    private Socket socket;

    public InstaLoaderThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try (
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream())
        ) {

            String operation = in.readUTF();

            switch (operation) {
                case "ADD": {
                    String hashtag = in.readUTF();
                    File f = new File("./" + hashtag + "/");
                    f.mkdir();
                    userCommand.putFile(new String[]{"put", "./" + hashtag + "/", hashtag + "/"});
                    f.delete();
                    /*
                    String key = Hash.getServer(Hash.hashing(hashtag, 8));
                    if (key.equals(Daemon.ID)) {
                        hashtags.add(hashtag);
                    } else {
                        Socket newSocket = new Socket(key.substring(key.indexOf("#") + 1), Daemon.instaPortNumber);
                        DataOutputStream newOut = new DataOutputStream(newSocket.getOutputStream());
                        DataInputStream newIn = new DataInputStream(newSocket.getInputStream());
                        newOut.writeUTF("ADD");
                        newOut.writeUTF(hashtag);
                        newIn.readUTF();
                    }*/

                    out.writeUTF("");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
