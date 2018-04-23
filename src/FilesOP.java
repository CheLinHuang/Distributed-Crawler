import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class FilesOP {

    // Return all the files within given directory (includes sub-directory)
    public static List<String> listFiles(String dirName) {

        List<String> list = new ArrayList<>();
        File curDir = new File(dirName);
        listFilesHelper(curDir, "", list);

        return list;
    }

    // Helper method to get filenames
    private static void listFilesHelper(File file, String dirName, List<String> list) {

        File[] fileList = file.listFiles();
        if (fileList != null) {
            for (File f : fileList) {
                if (f.isDirectory()) {
                    list.add(dirName + f.getName());
                    listFilesHelper(f, dirName + f.getName() + "/", list);
                }
                if (f.isFile()) {
                    list.add(dirName + f.getName());
                }
            }
        }
    }

    public static List<String> listFolders(String dirName) {
        List<String> list = new ArrayList<>();
        File curDir = new File(dirName);
        File[] fileList = curDir.listFiles();
        if (fileList != null) {
            for (File f : fileList) {
                if (f.isDirectory())
                    list.add(f.getName());
            }
        }
        return list;
    }

    // Delete the given file
    public static boolean deleteFile(String fileName) {
        File file = new File(fileName);
        return file.delete();
    }

    // Return a thread for sending file purpose
    public static Thread sendFile(File file, Socket socket) {
        return new SendFileThread(file, socket);
    }

    public static void receiveFile(String filename, Socket socket) {

        try {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            long fileSize = in.readLong();
            Daemon.writeLog("file size", Long.toString(fileSize));

            if (fileSize == 0) {
                File file = new File(filename);
                file.mkdir();
            } else {
                BufferedOutputStream fileOutputStream = new BufferedOutputStream(new FileOutputStream(filename));
                byte[] buffer = new byte[Daemon.bufferSize];
                int bytes;
                while (fileSize > 0 && (bytes = in.read(buffer, 0, (int) Math.min(Daemon.bufferSize, fileSize))) != -1) {
                    fileOutputStream.write(buffer, 0, bytes);
                    fileSize -= bytes;
                }
                fileOutputStream.close();
            }
            out.writeUTF("Received");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Thread class for sending file purpose
    static class SendFileThread extends Thread {
        File file;
        Socket socket;

        public SendFileThread(File file, Socket socket) {
            this.file = file;
            this.socket = socket;
        }

        @Override
        public void run() {

            try (
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream sktResponse = new DataInputStream(socket.getInputStream())
            ) {
                if (file.isDirectory()) {
                    dos.writeLong(0);
                } else {

                    DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
                    //Sending file size to the server
                    dos.writeLong(file.length());

                    //Sending file data to the server
                    int read;
                    // Read file buffer
                    byte[] buffer = new byte[2048];

                    while ((read = dis.read(buffer)) > 0)
                        dos.write(buffer, 0, read);
                    dis.close();
                }

                // wait for the server to response
                String res = sktResponse.readUTF();
                if (res.equals("Received")) {
                    System.out.println("Put the file successfully");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
