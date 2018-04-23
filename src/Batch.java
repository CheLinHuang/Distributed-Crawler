import java.io.DataOutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.List;

public class Batch {

    public static void main(String[] args) {

        Socket socket;
        DataOutputStream out;
        try {
            socket = new Socket("172.22.158.253", 6666);
            out = new DataOutputStream(socket.getOutputStream());
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        List<String> f = FilesOP.listFiles("/Users/tiesto1114/Desktop/CS525/Test/");

        System.out.println(f.size());

        if (f.size() != 0) {

            Collections.shuffle(f);
            String docID = "", caption, timestamp, geotag = "";
            String[] geotags;

            for (String s : f) {

                if (s.endsWith("UTC.txt")) {
                    caption = InstaLoader.extractCaption("/Users/tiesto1114/Desktop/CS525/Test/" + s);
                    System.out.println();
                    System.out.println("docID: " + s);
                    System.out.println("caption: " + caption);
                    System.out.println("timestamp: " + (timestamp = s.substring(0, 19)));
                    System.out.println("geotag: " + geotag);
                    System.out.println();
                    try {
                        out.writeUTF(s);
                        out.writeUTF(caption);
                        out.writeUTF(timestamp);
                        Thread.sleep(20);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            while (true) {

            }
        }
    }
}
