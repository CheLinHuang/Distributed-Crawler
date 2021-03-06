import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class HDFSOP {

    private static final String HDFS_PATH = "hdfs://sp18-cs525-g11-01.cs.illinois.edu:9000/";
    private static Configuration conf = new Configuration();
    private static FileSystem fileSystem = null;

    private static void initialize() {
        try {
            conf.set("fs.defaultFS", HDFS_PATH);
            fileSystem = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> listFiles(String hdfsPath) {
        if (fileSystem == null) {
            initialize();
        }

        List<String> list = new ArrayList<>();

        try {
            RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(
                    new Path(HDFS_PATH + hdfsPath), true);
            while (itr.hasNext()) {
                list.add(itr.next().getPath().toString().substring(46));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fileSystem = null;
        }
        return list;
    }

    public static boolean sendFile(String localPath, String hdfsPath) {
        if (fileSystem == null) {
            initialize();
        }

        try (
                FSDataOutputStream out = fileSystem.create(new Path(HDFS_PATH + hdfsPath));
                InputStream in = new BufferedInputStream(new FileInputStream(new File(localPath)))
        ) {
            byte[] b = new byte[1024];
            int numBytes;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            deleteFile(hdfsPath);
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean getFile(String localPath, String hdfsPath) {
        if (fileSystem == null) {
            initialize();
        }

        try (
                FSDataInputStream in = fileSystem.open(new Path(HDFS_PATH + hdfsPath));
                OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(localPath)))
        ) {
            byte[] b = new byte[1024];
            int numBytes;
            while ((numBytes = in.read(b)) > 0) {
                out.write(b, 0, numBytes);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            fileSystem = null;
            return false;
        }
        return true;
    }

    public static void deleteFile(String hdfsPath) {
        if (fileSystem == null) {
            initialize();
        }

        try {
            fileSystem.delete(new Path(HDFS_PATH + hdfsPath), true);
        } catch (IOException e) {
            e.printStackTrace();
            fileSystem = null;
        }
    }

    public static void deleteFile(List<String> hdfsPath) {
        for (String s : hdfsPath)
            deleteFile(s);
    }

    public static String readFile(String hdfsPath) {
        if (fileSystem == null) {
            initialize();
        }

        String s = "";

        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            IOUtils.copyBytes(fileSystem.open(new Path(HDFS_PATH + hdfsPath)), stream, 4096, true);
            s = new String(stream.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return s;
    }

    // realtime: realtime data or test data
    // spam: include spam or not
    // sortByTime: sort by time or not
    // return list of string array {docID, post content, timestamp, latitude, longitude}
    public static List<String[]> getData(boolean realtime, boolean spam, boolean sortByTime) {
        String folder;
        if (!realtime) {
            if (spam) {
                folder = "/data";
            } else {
                folder = "/data/event";
            }
        } else {
            folder = "/realtime";
        }

        List<String> list = listFiles(folder);

        if (sortByTime) {
            list.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return o1.substring(o1.lastIndexOf('/')).compareTo(o2.substring(o2.lastIndexOf('/')));
                }
            });
        } else {
            Collections.shuffle(list);
        }

        List<String[]> result = new ArrayList<>(list.size());

        for (String s : list) {
            int index = s.lastIndexOf('/');
            String id = s.substring(index + 1);
            String hashTag = s.substring(s.lastIndexOf('/', index - 1) + 1, index);
            String[] ids = id.split("@");
            result.add(new String[]{hashTag + "@" + ids[0], readFile(s), ids[0], ids[1], ids[2]});
        }

        return result;
    }

    public static String getServer(double lat, double lon) {
        String[] servers = readFile("/serverList.txt").split("\n");
        double minDist = Double.MAX_VALUE;
        String server = "";
        for (String s : servers) {
            String[] info = s.split(":");
            String[] geotag = info[0].split(",");
            double dist = distance(lat, new Double(geotag[0]), lon, new Double(geotag[1]));
            System.out.println(dist);
            if (dist < minDist) {
                minDist = dist;
                server = info[1];
            }
        }
        return server;
    }

    private static double distance(double lat1, double lat2, double lon1, double lon2) {

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c; // convert to km
    }
}
