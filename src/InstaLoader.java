import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InstaLoader extends Thread {

    public static final String HDFS_PATH = "hdfs://sp18-cs525-g11-01.cs.illinois.edu:9000/";

    public static List<String> extractHashtag(String caption) {
        List<String> result = new ArrayList<>();
        int offset = 0;
        while ((offset = caption.indexOf("#", offset)) != -1) {
            int j = offset + 1;
            while (j < caption.length() && caption.charAt(j) != ' ' && caption.charAt(j) != '#') {
                j++;
            }
            result.add(caption.substring(offset + 1, j).toLowerCase());
            offset = j;
        }
        return result;
    }

    static String[] extractGeotag(String fileName) {
        String[] result = {"", ""};
        try (
                BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))
        ) {
            result[0] = bufferedReader.readLine();
            String[] words = bufferedReader.readLine().split("=");
            result[1] = words[words.length - 1];

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    static String extractCaption(String fileName) {
        StringBuilder stringBuilder = new StringBuilder();
        try (
                BufferedReader bufferedReader = new BufferedReader(new FileReader(fileName))
        ) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append(" ");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return stringBuilder.toString();
    }

    @Override
    public void run() {

        try {
            Thread.sleep(6);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        long time0 = System.currentTimeMillis();
        try (
                Socket socket = new Socket("10.193.58.171", 19898);
                DataOutputStream out = new DataOutputStream(socket.getOutputStream())
        ) {

            while (true) {


                for (String hashtag : FilesOP.listFolders("../SDFS/")) {

                    if (!Hash.getServer(Hash.hashing(hashtag)).equals(Daemon.ID)) {
                        System.out.println("Hashtag " + hashtag + " not belong this server");
                        continue;
                    }

                    List<String> fileList = FilesOP.listFiles("../SDFS/" + hashtag);
                    String prevEndTime = "";
                    boolean newTag = false;

                    if (fileList.size() == 0) {
                        newTag = true;
                    } else {
                        Collections.sort(fileList);
                        prevEndTime = fileList.get((fileList.size() - 1)).substring(0, 23);
                    }


                    Runtime rt = Runtime.getRuntime();
                    Process pr = rt.exec(new String[]{"instaloader", "-G", hashtag, "-c", "50"});
                    BufferedReader prInput = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                    String prInputLine;
                    while ((prInputLine = prInput.readLine()) != null) {

                        try {
                            String timetag = prInputLine.split("#")[2].split("/")[1].split("\\.")[0];
                            // System.out.println(timetag + "  " + prInputLine);
                            if (!newTag && prevEndTime.compareTo(timetag) > 0) {
                                System.out.println("Prevtime " + prevEndTime);
                                System.out.println("Timetag " + timetag);
                                pr.destroy();
                                break;
                            }
                        } catch (Exception e) {
                        }
                    }

                    Daemon.writeLog("Finish scraping", hashtag);


                    List<String> f = FilesOP.listFiles(hashtag);
                    int count = 0;
                    if (f.size() != 0) {

                        Collections.sort(f);
                        String endTime = f.get((f.size() - 1)).substring(0, 19);
                        String docID = "", caption = "", timestamp = "X", geotag = "", pic = "";

                        for (String s : f) {

                            if (!s.startsWith(timestamp)) {
                                timestamp = s.substring(0, 19);
                                docID = hashtag + "@" + timestamp;
                            }
                            if (s.endsWith("location.txt")) {
                                geotag = extractGeotag(s + hashtag + "/" + s)[1].replace(',', '@');
                            }
                            if (s.endsWith("UTC.jpg")) {
                                pic = s;
                            }
                            if (s.endsWith("UTC.txt")) {
                                caption = extractCaption(hashtag + "/" + s);
                                count++;
                                System.out.println();
                                System.out.println("docID: " + docID);
                                System.out.println("caption: " + caption);
                                System.out.println("timestamp: " + timestamp);
                                System.out.println("geotag: " + geotag);
                                System.out.println();

                                if (!geotag.equals("") && pic.endsWith(".jpg") && caption.contains(hashtag)) {
                                    try {
                                        out.writeUTF(docID);
                                        out.writeUTF(caption);
                                        out.writeUTF(timestamp);
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }

                                    HDFSOP.sendFile("/" + hashtag + "/" + s, "/realtime/" + hashtag + "/" + timestamp + "@" + geotag + "@.txt");
                                    HDFSOP.sendFile("/" + hashtag + "/" + pic, "/pic/" + docID + ".jpg");
                                }
                                geotag = "";
                                pic = "";
                            }

                            // TODO: HDFS and HBase operation

                            if (!s.startsWith(endTime)) {
                                FilesOP.deleteFile(hashtag + "/" + s);
                            } else if (s.endsWith(".txt")) {
                                userCommand.putFile(new String[]{"put", hashtag, hashtag});
                                userCommand.putFile(new String[]{"put", hashtag + "/" + s, hashtag + "/" + s});
                            }
                        }
                        System.out.println((System.currentTimeMillis() - time0) / 1000);
                        System.out.println(count);
                        out.writeUTF(Long.toString((System.currentTimeMillis() - time0) / 1000));
                        time0 = System.currentTimeMillis();
                        out.writeUTF(Integer.toString(count));

                        for (String s : fileList) {
                            if (!s.startsWith(endTime)) {
                                userCommand.deleteFile(new String[]{"delete", hashtag + "/" + s});
                            }
                        }
                    }

                }
            }
        } catch (IOException ee) {
            ee.printStackTrace();
        }
    }
}
