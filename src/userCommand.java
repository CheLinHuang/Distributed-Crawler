import java.io.*;
import java.net.Socket;

public class userCommand {

    public static void putFile(String[] cmdParts) {

        if (cmdParts.length != 3) {
            System.out.println("Unsupported command format!");
            System.out.println("To put a file into the SDFS");
            System.out.println("Please enter \"put srcFileName tgtFileName\"");
            return;
        }

        String srcFileName = cmdParts[1];
        String tgtFileName = cmdParts[2];
        String fileServer = Hash.getServer(Hash.hashing(tgtFileName)).split("#")[1];
        Daemon.writeLog("put file to", fileServer);

        // Open the local file
        File file = new File(srcFileName);
        if (!file.exists()) {
            Daemon.writeLog("Local file not exist", srcFileName);
            System.out.println("Local file not exist!");
        } else {
            try {
                // Connect to server
                Socket socket = new Socket(fileServer, Daemon.filePortNumber);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream in = new DataInputStream(socket.getInputStream());
                dos.writeUTF("put");
                dos.writeUTF(tgtFileName);
                String response = in.readUTF();
                Daemon.writeLog("Server response", response);

                Thread t = null;
                if (response.equals("Accept")) {
                    // Accept the put file request
                    t = FilesOP.sendFile(file, socket);
                } else if (response.equals("Confirm")) {

                    // Require confirmation to put file
                    System.out.println("Are you sure to send the file? (y/n)");
                    BufferedReader StdIn = new BufferedReader(new InputStreamReader(System.in));

                    // Require confirmation within 30 sec
                    long startTime = System.currentTimeMillis();
                    while (((System.currentTimeMillis() - startTime) < 30000) && !StdIn.ready()) {
                        try {
                            Thread.sleep(200);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    if (StdIn.ready()) {
                        boolean repeat = true;
                        while (repeat) {
                            String cmd = StdIn.readLine().toLowerCase();
                            switch (cmd) {
                                case "y":
                                    dos.writeUTF("Y");
                                    Daemon.writeLog("Force put within 1 min", tgtFileName);
                                    t = FilesOP.sendFile(file, socket);
                                    repeat = false;
                                    break;
                                case "n":
                                    dos.writeUTF("N");
                                    Daemon.writeLog("Reject put within 1 min", tgtFileName);
                                    repeat = false;
                                    // do nothing
                                    break;
                                default:
                                    System.out.println("Unsupported command!");
                                    System.out.println("Are you sure to send the file? (y/n)");
                            }
                        }
                    } else {
                        dos.writeUTF("N");
                        Daemon.writeLog("No response for confirmation", tgtFileName);
                        System.out.println("No response! Update aborted!");
                    }
                }

                // If SendFileThread exist, send file
                if (t != null) {
                    t.start();
                    t.join();
                }

                Daemon.writeLog("put complete", tgtFileName);
                socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void listFile(String[] cmdParts) {

        if (cmdParts.length != 2) {
            System.out.println("Unsupported command format!");
            System.out.println("To list a file on the SDFS");
            System.out.println("Please enter \"ls sdfsfilename\"");
            return;
        }

        String sdfsFileName = cmdParts[1];
        String fileServer = Hash.getServer(Hash.hashing(sdfsFileName)).split("#")[1];

        try {
            // Connect to server
            Socket socket = new Socket(fileServer, Daemon.filePortNumber);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("ls");
            out.writeUTF(sdfsFileName);

            socket.setSoTimeout(2000);
            String response = in.readUTF();
            if (response.equals("Empty")) {
                Daemon.writeLog("No such file!", sdfsFileName);
                System.out.println("No such file!");
            } else {
                String[] nodes = response.split("#");
                Daemon.writeLog("File on node:", "");
                System.out.println(sdfsFileName + " is stored in the following nodes:");
                for (String node : nodes) {
                    Daemon.writeLog("", node);
                    System.out.println(node);
                }
                System.out.println("==================================");
            }
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getFile(String[] cmdParts) {
        if (cmdParts.length != 3) {
            System.out.println("Unsupported command format!");
            System.out.println("To get a file from the SDFS");
            System.out.println("Please enter \"get sdfsfilename localfilename\"");
            return;
        }

        String sdfsfilename = cmdParts[1];
        String localfilename = cmdParts[2];
        String fileServer = Hash.getServer(Hash.hashing(sdfsfilename)).split("#")[1];
        Daemon.writeLog("Get file from", fileServer);
        try {
            // Connect to server
            Socket socket = new Socket(fileServer, Daemon.filePortNumber);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            DataInputStream in = new DataInputStream(socket.getInputStream());
            out.writeUTF("get");
            out.writeUTF(sdfsfilename);

            String response = in.readUTF();
            Daemon.writeLog("Server response", response);

            if (response.equals("File Exist")) {
                FilesOP.receiveFile(localfilename, socket);
//                out.writeUTF("Received");
                System.out.println("get the file successfully");
                Daemon.writeLog("get complete", sdfsfilename);
            } else {
                System.out.println("File not exist!");
            }

            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteFile(String[] cmdParts) {
        if (cmdParts.length != 2) {
            System.out.println("Unsupported command format!");
            System.out.println("To delete a file on the SDFS");
            System.out.println("Please enter \"delete sdfsfilename\"");
            return;
        }

        String sdfsfilename = cmdParts[1];
        String fileServer = Hash.getServer(Hash.hashing(sdfsfilename)).split("#")[1];

        try {
            Socket socket = new Socket(fileServer, Daemon.filePortNumber);
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            dos.writeUTF("delete");
            dos.writeUTF(sdfsfilename);
            System.out.println("delete the file successfully");
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
