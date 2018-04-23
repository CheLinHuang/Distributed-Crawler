import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.ArrayList;
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
                list.add(itr.next().getPath().toString());
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
}
