import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Daemon {

    static final List<String> neighbors = new ArrayList<>();
    static final Map<String, long[]> membershipList = new HashMap<>();
    static final TreeMap<Integer, String> hashValues = new TreeMap<>();
    final static int bufferSize = 512;
    static String ID;
    static Integer myHashValue;
    static int joinPortNumber;
    static int packetPortNumber;
    static int filePortNumber;
    static int instaPortNumber;
    static boolean neighborUpdated = false;
    private static PrintWriter fileOutput;
    private String[] hostNames;

    public Daemon(String configPath) {

        // check if the configPath is valid
        if (!(new File(configPath).isFile())) {
            System.err.println("No such file!");
            System.exit(1);
        }

        Properties config = new Properties();

        try (InputStream configInput = new FileInputStream(configPath)) {

            // load the configuration
            config.load(configInput);
            hostNames = config.getProperty("hostNames").split(":");
            joinPortNumber = Integer.parseInt(config.getProperty("joinPortNumber"));
            packetPortNumber = Integer.parseInt(config.getProperty("packetPortNumber"));
            filePortNumber = Integer.parseInt(config.getProperty("filePortNumber"));
            instaPortNumber = Integer.parseInt(config.getProperty("instaPortNumber"));
            String logPath = config.getProperty("logPath");

            // assign daemon process an ID: the IP address
            ID = "#" + InetAddress.getLocalHost().getHostName();
            myHashValue = Hash.hashing(ID, 8);
            // assign appropriate log file path
            File outputDir = new File(logPath);
            if (!outputDir.exists())
                outputDir.mkdirs();
            fileOutput = new PrintWriter(new BufferedWriter(new FileWriter(logPath + "result.log")));

        } catch (IOException e) {
            System.out.println("Exception time " + LocalDateTime.now().toString());
            e.printStackTrace();
        }
    }

    public static void updateNeighbors() {

        synchronized (membershipList) {
            synchronized (neighbors) {
                List<String> oldNeighbors = new ArrayList<>(neighbors);

                neighbors.clear();

                Integer currentHash = myHashValue;
                // get the predecessors
                for (int i = 0; i < 2; i++) {
                    currentHash = hashValues.lowerKey(currentHash);
                    // since we are maintaining a virtual ring, if lower key is null,
                    // it means that we are at the end of the list
                    if (currentHash == null) {
                        currentHash = hashValues.lastKey();
                    }
                    if (!currentHash.equals(myHashValue) && !neighbors.contains(hashValues.get(currentHash))) {
                        try {
                            neighbors.add(hashValues.get(currentHash));
                        } catch (Exception e) {
                            //System.out.println("Exception time " + LocalDateTime.now().toString());
                            e.printStackTrace();
                        }
                    }
                }
                // get the successors
                currentHash = myHashValue;
                for (int i = 0; i < 2; i++) {
                    currentHash = hashValues.higherKey(currentHash);
                    if (currentHash == null) {
                        currentHash = hashValues.firstKey();
                    }

                    if (!currentHash.equals(myHashValue) && !neighbors.contains(hashValues.get(currentHash))) {
                        try {
                            neighbors.add(hashValues.get(currentHash));
                        } catch (Exception e) {
                            //System.out.println("Exception time " + LocalDateTime.now().toString());
                            e.printStackTrace();
                        }
                    }
                }

                // check if the neighbors are changed
                // if yes, the files stored on this node might need to be moved
                // to maintain the consistency of the Chord-like ring
                neighborUpdated = false;
                if (oldNeighbors.size() != neighbors.size()) {
                    neighborUpdated = true;
                } else {
                    for (int i = 0; i < oldNeighbors.size(); i++) {
                        if (!oldNeighbors.get(i).equals(neighbors.get(i))) {
                            neighborUpdated = true;
                            break;
                        }
                    }
                }

                // for debugging

                System.out.println("print neighbors......");
                for (String neighbor : neighbors) {
                    System.out.println(neighbor);
                }

                // Update timestamp for non-neighbor
                for (String neighbor : neighbors) {
                    long[] memberStatus = {membershipList.get(neighbor)[0], System.currentTimeMillis()};
                    membershipList.put(neighbor, memberStatus);

                }
            }
        }
    }

    public synchronized static void moveReplica(boolean isNewNode) {

        if (isNewNode) {
            // For newly-added node, it is possible that
            int j = neighbors.size() - 1;
            while (j >= Math.max(0, neighbors.size() - 2)) {
                String tgtHostName = neighbors.get(j--).split("#")[1];
                try {
                    Socket socket = new Socket(tgtHostName, filePortNumber);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    DataInputStream in = new DataInputStream(socket.getInputStream());

                    out.writeUTF("get replica");
                    out.writeUTF(ID);
                    // still shows EOF exception
                    String sdfsFileName;

                    while (!(sdfsFileName = in.readUTF()).equals("Empty")) {

                        FilesOP.receiveFile("../SDFS/" + sdfsFileName, socket);
                        System.out.println("Re-replication: receive " + sdfsFileName + " from " + tgtHostName);
                    }

                } catch (Exception e) {
                    System.out.println("Exception time " + LocalDateTime.now().toString());
                    e.printStackTrace();
                }
            }
        } else {
            // For old node, check if it needs to create new replications in its successors
            for (String file : FilesOP.listFiles("../SDFS/")) {
                String targetID = Hash.getServer(Hash.hashing(file));

                if (targetID.equals(ID)) {
                    System.out.println("Re-replication: send " + file + " to successors");
                    // replicate the file to the two successors
                    List<Thread> threads = new ArrayList<>();
                    int j = neighbors.size() - 1;
                    while (j >= Math.max(0, neighbors.size() - 2)) {
                        String tgtHostName = neighbors.get(j--).split("#")[1];
                        try {
                            Socket socket = new Socket(tgtHostName, filePortNumber);
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            out.writeUTF("fail replica");
                            out.writeUTF(file);
                            String returnMsg = in.readUTF();

                            // for the case that the neighbor is also failed subsequently
                            // returnMsg will be null
                            if (returnMsg != null && returnMsg.equals("Ready to receive")) {
                                threads.add(FilesOP.sendFile(new File("../SDFS/" + file), socket));
                                threads.get(threads.size() - 1).start();
                            }
                        } catch (Exception e) {
                            System.out.println("Exception time " + LocalDateTime.now().toString());
                            e.printStackTrace();
                        }
                    }
                    for (Thread t : threads) {
                        try {
                            t.join();
                        } catch (Exception e) {
                            System.out.println("Exception time " + LocalDateTime.now().toString());
                            e.printStackTrace();
                        }
                    }
                } else {
                    // consider the case that a new node is added,
                    // and the target node for this file is no longer in the predecessors of this node,
                    // this means that this file is longer needed in this node
                    int j = 0;
                    boolean delete = true;
                    while (j < Math.min(neighbors.size(), 2)) {
                        if (neighbors.get(j).equals(targetID)) {
                            delete = false;
                            break;
                        }
                        j++;
                    }
                    if (delete) {
                        if (FilesOP.deleteFile("../SDFS/" + file)) {
                            System.out.println("Re-replication: delete " + file);
                        }
                    }
                }
            }
        }
    }

    public static void displayPrompt() {
        System.out.println("===============================");
        System.out.println("Please input the commands:.....");
        System.out.println("Enter \"join\" to join to group......");
        System.out.println("Enter \"member\" to show the membership list");
        System.out.println("Enter \"id\" to show self's ID");
        System.out.println("Enter \"leave\" to leave the group");
        System.out.println("Enter \"put localfilename sdfsfilename\" to put a file in this SDFS");
        System.out.println("Enter \"get sdfsfilename localfilename\" to fetch a sdfsfile to local system");
        System.out.println("Enter \"delete sdfsfilename\" to delete the sdfsfile");
        System.out.println("Enter \"ls sdfsfilename\" to show all the nodes which store the file");
        System.out.println("Enter \"store\" to list all the sdfsfiles stored locally");
        System.out.println("===============================");
    }

    public static void writeLog(String action, String nodeID) {

        // write logs about action happened to the nodeID into log
        fileOutput.println(LocalDateTime.now().toString() + " \"" + action + "\" " + nodeID);
        /*if (!action.equals("FAILURE") || !action.equals("MESSAGE") || !action.equals("JOIN")) {
            fileOutput.println("Updated Membership List:");
            for (String key : membershipList.keySet()) {
                fileOutput.println(key);
            }
            fileOutput.println("======================");
        }*/
        fileOutput.flush();
    }

    public static void main(String[] args) {

        boolean isIntroducer = false;
        String configPath;

        // parse the input arguments
        if (args.length == 0 || args.length > 2) {
            System.err.println("Please enter the argument as the following format: <configFilePath> <-i>(optional)");
            System.exit(1);
        }
        if (args.length == 1) {
            configPath = args[0];
        } else {
            configPath = args[0];
            if (args[1].equals("-i")) {
                isIntroducer = true;
                System.out.println("Set this node as an introducer!");
            } else {
                System.err.println("Could not recognize the input argument");
                System.err.println("Please enter the argument as the following format: <configFilePath> <-i>(optional)");
                System.exit(1);
            }
        }

        Daemon daemon = new Daemon(configPath);
        displayPrompt();

        try (BufferedReader StdIn = new BufferedReader(new InputStreamReader(System.in))) {

            // prompt input handling
            String cmd;
            while ((cmd = StdIn.readLine()) != null) {

                String[] cmdParts = cmd.trim().split(" +");

                switch (cmdParts[0]) {
                    case "join":
                        // to deal with the case that users enter "JOIN" command multiple times
                        if (membershipList.size() == 0) {
                            ExecutorService mPool = Executors.newFixedThreadPool(5 + ((isIntroducer) ? 1 : 0));
                            mPool.execute(new FileServer());
                            daemon.joinGroup(isIntroducer);
                            if (isIntroducer) {
                                mPool.execute(new IntroducerThread());
                            }
                            mPool.execute(new ListeningThread());
                            mPool.execute(new HeartbeatThread(100));
                            mPool.execute(new MonitorThread());
                            mPool.execute(new InstaLoaderServer());
                        }
                        System.out.println("join successfully");
                        break;

                    case "member":
                        System.out.println("===============================");
                        System.out.println("Membership List:");
                        int size = hashValues.size();
                        Integer[] keySet = new Integer[size];
                        hashValues.navigableKeySet().toArray(keySet);

                        for (Integer key : keySet) {
                            System.out.println(hashValues.get(key));
                        }
                        System.out.println("===============================");
                        break;

                    case "id":
                        System.out.println("ID: " + ID);
                        break;

                    case "leave":
                        if (membershipList.size() != 0) {
                            Protocol.sendGossip(ID, "Leave", membershipList.get(ID)[0] + 10,
                                    3, 4, new DatagramSocket());
                        }
                        fileOutput.println(LocalDateTime.now().toString() + " \"LEAVE!!\" " + ID);
                        fileOutput.close();
                        System.exit(0);

                    case "put":
                        writeLog(cmd, "");
                        userCommand.putFile(cmdParts);
                        break;
                    case "get":
                        writeLog(cmd, "");
                        userCommand.getFile(cmdParts);
                        break;
                    case "delete":
                        writeLog(cmd, "");
                        userCommand.deleteFile(cmdParts);
                        break;
                    case "ls":
                        writeLog(cmd, "");
                        userCommand.listFile(cmdParts);
                        break;
                    case "store":
                        writeLog(cmd, "");
                        System.out.println("SDFS files stored at this node are: ");
                        for (String s : FilesOP.listFiles("../SDFS/"))
                            System.out.println(s);
                        System.out.println("===============================");
                        break;
                    default:
                        System.out.println("Unsupported command!");
                        displayPrompt();
                }
            }

        } catch (IOException e) {
            System.out.println("Exception time " + LocalDateTime.now().toString());
            e.printStackTrace();
        }
    }

    public void joinGroup(boolean isIntroducer) {

        // As required, wipes all the files stored in the SDFS system on this node
        //for (String s : FilesOP.listFiles("../SDFS/"))
        //    FilesOP.deleteFile("../SDFS/" + s);

        DatagramSocket clientSocket = null;

        // try until socket create correctly
        while (clientSocket == null) {
            try {
                clientSocket = new DatagramSocket();
            } catch (Exception e) {
                System.out.println("Exception time " + LocalDateTime.now().toString());
                e.printStackTrace();
            }
        }

        byte[] sendData = ID.getBytes();

        // send join request to each introducer
        for (String hostName : hostNames) {
            try {
                InetAddress IPAddress = InetAddress.getByName(hostName);
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, joinPortNumber);
                clientSocket.send(sendPacket);
            } catch (Exception e) {
                //System.out.println("Exception time " + LocalDateTime.now().toString());
                e.printStackTrace();
            }
        }

        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        // wait for the server's response
        try {
            clientSocket.setSoTimeout(2000);
            clientSocket.receive(receivePacket);
            String response = new String(receivePacket.getData(), 0, receivePacket.getLength());

            // process the membership list that the first introducer response and ignore the rest
            String[] members = response.split("%");
            for (String member : members) {
                String[] memberDetail = member.split("/");
                long[] memberStatus = {Long.parseLong(memberDetail[1]), System.currentTimeMillis()};
                membershipList.put(memberDetail[0], memberStatus);
                hashValues.put(Hash.hashing(memberDetail[0], 8), memberDetail[0]);
            }

            writeLog("JOIN!", ID);
            updateNeighbors();
            // when the node is newly join the cluster,
            // always execute moveReplica
            moveReplica(true);

        } catch (SocketTimeoutException e) {

            if (!isIntroducer) {
                System.err.println("All introducers are down!! Cannot join the group.");
                System.exit(1);
            } else {
                // This node might be the first introducer,
                // keep executing the rest of codes
                System.out.println("You might be first introducer!");

                // put the process itself to the membership list
                long[] memberStatus = {0, System.currentTimeMillis()};
                membershipList.put(ID, memberStatus);
                hashValues.put(Hash.hashing(ID, 8), ID);
                writeLog("JOIN", ID);
            }

        } catch (Exception e) {
            System.out.println("Exception time " + LocalDateTime.now().toString());
            e.printStackTrace();
        }
    }
}
