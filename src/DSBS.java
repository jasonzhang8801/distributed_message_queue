import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by jasonzhang on 5/29/17.
 */

public class DSBS {
    // DSBS's ip and port number
    public static String ipAddr = null;
    public static int portNum = -1;
    // DSBS's topic information
    public static ConcurrentHashMap<String, List<PartitionEntry>> infoMap = new ConcurrentHashMap<>();
    // DSBS's data storage
    public static ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Record>>> dataMap = new ConcurrentHashMap<>();
    // DSBS's cluster information
    public static List<String[]> brokerList = new ArrayList<>();

    public static void main(String[] args) {
        // start server
        (new Thread(new DSBSServer())).start();

        // print host ip and port
        while (DSBS.ipAddr == null || DSBS.portNum == -1) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println(DSBS.ipAddr + " at port number: " + DSBS.portNum);

        // start console client to handle user input
        DSBSClient client = new DSBSClient();
        client.setUp();
    }
}

/**
 * server
 */
class DSBSServer implements Runnable {
    // DSBS server's ip and port
    private String ipAddr = null;
    private int portNum = -1;

    @Override
    public void run() {

        try (ServerSocket serverSocket = new ServerSocket(0);) {
            // assign ip and port number
            ipAddr = InetAddress.getLocalHost().getHostAddress();
            portNum = serverSocket.getLocalPort();

            if (ipAddr != null && portNum > 0) {
                DSBS.ipAddr = ipAddr;
                DSBS.portNum = portNum;

                // add local host's network info to brokerList
                DSBS.brokerList.add(new String[]{ipAddr, Integer.toString(portNum)});

            } else {
                System.out.println("Error: no valid IP or port number");
            }

            // listen the request
            while (true) {
                (new Thread(new DSBSServerWorker(serverSocket.accept()))).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/**
 * server worker
 */
class DSBSServerWorker implements Runnable {

    private Socket clientSocket = null;

    DSBSServerWorker(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (ObjectOutputStream out = new ObjectOutputStream(clientSocket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(clientSocket.getInputStream());
        ) {
            // create received package
            Package revPkg = null;

            try {
                if ((revPkg = (Package) in.readObject()) != null) {

                    TYPE pkgType = revPkg._type;

                    switch (pkgType) {
                        case B2BADD: {
                            // add the broker cluster
                            System.out.println("Server: received package with type \"B2BADD\"");

                            // retrieve the brokerList
                            B2BAdd pkg = (B2BAdd) revPkg;
                            DSBS.brokerList = pkg._brokerList;
                            pkg._brokerList = null;
                            pkg._ack = true;

                            // send ACK back
                            out.writeObject(pkg);
                            System.out.println("Server: sent ACK back");

                            break;
                        }
                        case T2B: {
                            // create topic into the broker cluster
                            System.out.println("Server: received package with type \"T2B\"");

                            // retrieve topic and partition number
                            T2B pkg = (T2B) revPkg;
                            String topic = pkg._topic;
                            int numOfPart = pkg._partition;

                            // check package
                            if (topic == null || numOfPart <= 0) {
                                System.out.println("System error: invalid topic or partition number");
                                return;
                            }

                            // save the topic information
                            if (!DSBS.infoMap.containsKey(topic)) {
                                List<PartitionEntry> listOfPartitionEntry = new ArrayList<>();

                                // randomly assign the partitions to brokers
                                Random rand = new Random();
                                for (int i = 0; i < numOfPart; i++) {
                                    int brokerIdx = rand.nextInt(DSBS.brokerList.size());
                                    System.out.println("topic " + topic + " with partition " + i + " at broker " + brokerIdx);

                                    // create a partitionEntry
                                    PartitionEntry partitionEntry = new PartitionEntry();
                                    partitionEntry._brokerID = brokerIdx;
                                    partitionEntry._partitionNum = i;
                                    partitionEntry._offsetMap = new Hashtable<>();

                                    listOfPartitionEntry.add(partitionEntry);
                                }

                                DSBS.infoMap.put(topic, listOfPartitionEntry);

                            } else {
                                System.out.println("Error: not support duplicated topic");
                                return;
                            }

                            // update other broker's infoMap
                            for (int i = 0; i < DSBS.brokerList.size(); i++) {

                                String ipAddr = DSBS.brokerList.get(i)[0];
                                int portNum = Integer.parseInt(DSBS.brokerList.get(i)[1]);

                                // connect remote broker
                                try (Socket socket = new Socket(ipAddr, portNum)) {
                                    try (ObjectOutputStream objOut = new ObjectOutputStream(socket.getOutputStream());
                                         ObjectInputStream objIn = new ObjectInputStream(socket.getInputStream())) {

                                        // construct send package
                                        B2BInfo sendPkg_B2BInfo = new B2BInfo(TYPE.B2BINFO, DSBS.infoMap);

                                        // send
                                        objOut.writeObject(sendPkg_B2BInfo);

                                        // construct rev package
                                        Package revPkg_B2BInfo;

                                        if ((revPkg_B2BInfo = (Package) objIn.readObject()) != null) {
                                            if (revPkg_B2BInfo._type == TYPE.B2BINFO && revPkg_B2BInfo._ack) {
                                                System.out.println("Server: update infoMap at broker with IP " + ipAddr);
                                            } else {
                                                System.out.println("Error: failed to update infoMap at broker with IP "
                                                        + ipAddr);
                                            }
                                        }
                                    }
                                }
                            }

                            // send ACK
                            revPkg._ack = true;
                            out.writeObject(revPkg);
                            break;
                        }
                        case B2BINFO: {
                            // update broker's infoMap
                            System.out.println("Server: received package with command \"B2BINFO\"");

                            B2BInfo pkg = (B2BInfo) revPkg;
                            DSBS.infoMap.putAll(pkg._infoMap);
                            pkg._infoMap = null;
                            pkg._ack = true;

                            // shouldn't init dataMap
                            // each broker is only responsible for its own partition
                            // let consumer or producer to init dataMap

                            // init dataMap
                            // otherwise, consumer with multi-thread will cause error
                            for (String topic : DSBS.infoMap.keySet()) {
                                List<PartitionEntry> listOfPartitionEntry = DSBS.infoMap.get(topic);

                                // init dataMap
                                ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();

                                for (PartitionEntry partitionEntry : listOfPartitionEntry) {
                                    int brokerId = partitionEntry._brokerID;

                                    // check who is responsible for partition
                                    if (DSBS.ipAddr.equals(DSBS.brokerList.get(brokerId)[0]) &&
                                            DSBS.portNum == Integer.parseInt(DSBS.brokerList.get(brokerId)[1])) {

                                        int partitionNum = partitionEntry._partitionNum;
                                        entryMap.put(partitionNum, new ArrayList<>());
                                        System.out.println("Server: initialized partition " + partitionNum + " with topic " + topic);
                                    }
                                }

                                DSBS.dataMap.put(topic, entryMap);
                            }

                            // send ACK
                            out.writeObject(pkg);
                            System.out.println("Server: sent ACK back");

                            break;
                        }
                        case P2BUP: {
                            // forward the partition information to producer
                            System.out.println("Server: received package with command \"P2BUP\"");

                            // retrieve topic
                            P2BUp pkg = (P2BUp) revPkg;
                            String topic = pkg._topic;

                            // check if the topic exists
                            if (DSBS.infoMap.containsKey(topic)) {
                                List<String[]> partitionList = new ArrayList<>();

                                List<PartitionEntry> listOfPartitionEntry = DSBS.infoMap.get(topic);
                                for (int i = 0; i < listOfPartitionEntry.size(); i++) {
                                    PartitionEntry partitionEntry = listOfPartitionEntry.get(i);

                                    int brokerIdx = partitionEntry._brokerID;
                                    int partitionNum = partitionEntry._partitionNum;
                                    String ipAddr = DSBS.brokerList.get(brokerIdx)[0];
                                    String portNum = DSBS.brokerList.get(brokerIdx)[1];

                                    partitionList.add(new String[]{ipAddr, portNum, Integer.toString(partitionNum)});
                                }

                                // construct send message
                                pkg._partitionList = partitionList;
                                pkg._ack = true;

                                // send ACK
                                out.writeObject(pkg);
                                System.out.println("Server: sent ACK back");

                            } else {
                                System.out.println("Error: please use TopicClient to create topic first");

                                // construct send message
                                pkg._partitionList = null;
                                pkg._ack = false;

                                // send NACK
                                out.writeObject(pkg);
                                System.out.println("Server: sent NACK back");
                            }

                            break;
                        }
                        case P2BDATA: {
                            // receive record from producer
                            System.out.println("Server: received package with command \"P2BDATA\"");

                            // process package
                            P2BData pkg = (P2BData) revPkg;
                            (new Thread(new DSBSP2BDataWorker(pkg))).start();

                            // receive data stream
                            while (true) {
                                Package revPkg1 = (Package) in.readObject();

                                if (revPkg1._type == TYPE.EOS) {
                                    revPkg1._ack = true;
                                    out.writeObject(revPkg1);

                                    // TEST ONLY
                                    // print out the length of queue
                                    System.out.println("Server: topic " + pkg._topic
                                            + " partition " + pkg._partitionNum
                                            + " queue size " + DSBS.dataMap.get(pkg._topic).get(pkg._partitionNum).size());
                                    break;
                                }
                                else if (revPkg1._type == TYPE.P2BDATA) {
                                    P2BData pkg1 =(P2BData) revPkg1;
                                    (new Thread(new DSBSP2BDataWorker(pkg1))).start();
                                } else {
                                    System.out.println("Error: invalid package type");
                                }
                            }

                            break;
                        }
                        case C2BUP: {
                            // forward the partition information to consumer
                            System.out.println("Server: received package with command \"C2BUP\"");

                            // retrieve topic
                            C2BUp pkg = (C2BUp) revPkg;
                            String topic = pkg._topic;

                            // check if the topic exists
                            if (DSBS.infoMap.containsKey(topic)) {
                                List<String[]> offsetList = new ArrayList<>();

                                List<PartitionEntry> listOfPartitionEntry = DSBS.infoMap.get(topic);
                                for (int i = 0; i < listOfPartitionEntry.size(); i++) {
                                    PartitionEntry partitionEntry = listOfPartitionEntry.get(i);

                                    int brokerIdx = partitionEntry._brokerID;
                                    int partitionNum = partitionEntry._partitionNum;
                                    String ipAddr = DSBS.brokerList.get(brokerIdx)[0];
                                    String portNum = DSBS.brokerList.get(brokerIdx)[1];

                                    // In DSBS, broker is stateful
                                    // each broker doesn't have all offset information
                                    offsetList.add(new String[]{ipAddr, portNum, Integer.toString(partitionNum), "-1"});
                                }

                                // send ACK
                                pkg._offsetList = offsetList;
                                pkg._ack = true;

                                out.writeObject(pkg);
                                System.out.println("Server: sent ACK back");

                            } else {
                                System.out.println("Error: no such topic");
                                // construct send message
                                pkg._offsetList = null;
                                pkg._ack = false;

                                // send NACK
                                out.writeObject(pkg);
                                System.out.println("Server: sent NACK back");
                            }

                            break;
                        }
                        case C2BDATA: {
                            // forward the data stream to consumer
                            // assumption: offset is the index which consumer should start to consume inclusively
                            System.out.println("Server: received package with command \"C2BDATA\"");

                            // retrieve topic and partition information
                            C2BData pkg = (C2BData) revPkg;

                            // TEST ONLY
                            // FAKE DATA
//                            int size = 100;
//                            generateFakeData(pkg._topic, pkg._partitionNum, size);
//                            System.out.println("Faking " + size + " records ...");

                            DSBSC2BDataHandler(pkg, out);

//                            // TEST ONLY
//                            long inTime = 0;
//                            long outTime = 0;

                            Package revPkg1;
                            while ((revPkg1 = (Package) in.readObject()) != null) {

//                                // TEST ONLY
//                                inTime = System.currentTimeMillis();
//                                System.out.println("TEST: network travel time " + (inTime - outTime) + " ms");

                                System.out.println("Server: received package with command \"C2BDATA\"");

                                if (revPkg1._type == TYPE.EOS) {
                                    System.out.println("Server: reached the end of data stream");
                                    break;
                                }

                                DSBSC2BDataHandler((C2BData)revPkg1, out);

//                                outTime = System.currentTimeMillis();
//                                System.out.println("TEST: data processing time " + (outTime - inTime) + " ms");
                            }
                            String topic = pkg._topic;
                            ConcurrentHashMap<Integer, List<Record>> entryMap = DSBS.dataMap.get(topic);
                            Set<Integer> keySet = entryMap.keySet();
                            for (int key : keySet) {
                                entryMap.get(key).clear();
                            }
                            System.out.println("All partitions on topic "+topic+" cleared");

                            break;
                        }
                        default: {
                            System.out.println("Server: invalid package command");
                            break;
                        }
                    }
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Fake data
     * @param topic
     * @param size
     */
    private static void generateFakeData(String topic, int partitionNum, int size) {
        List<Record> records = new ArrayList<>();
        for(int i=0; i< size;i++) {
            Record r = new Record(topic, "this some fake ass data hahahaha!");
            records.add(r);
        }

        ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();
        entryMap.put(partitionNum, records);
        DSBS.dataMap.put(topic, entryMap);
    }

    /**
     * handle the data request from consumer
     * @param pkg
     * @param out
     */
    private void DSBSC2BDataHandler(C2BData pkg, ObjectOutputStream out) {

        String topic = pkg._topic;
        int partitionNum = pkg._partitionNum;
        int groupId = pkg._groupID;
        int batchSize = pkg._batchSize;
        int offset = pkg._offset; // offset = -1

        // TEST ONLY
//        System.out.println("Received offset: " + offset);

        // check the batch size
        if (batchSize <= 0) {
            System.out.println("Error: invalid batch size");
            return;
        }

        // offsetMap for given group, topic, partition
        Map<Integer, Integer> offsetMap = null;

        // retrieve offset
        for (PartitionEntry partitionEntry : DSBS.infoMap.get(topic)) {
            if (partitionEntry._partitionNum == partitionNum) {

                offsetMap = partitionEntry._offsetMap;

                // check if the group exists
                if (!partitionEntry._offsetMap.containsKey(groupId)) {
                    partitionEntry._offsetMap.put(groupId, 0);
                }
                offset = partitionEntry._offsetMap.get(groupId);

                // TEST ONLY
                System.out.println("offset: " + offset);

                break;
            }
        }

//        // check if topic exists
//        int newOffset;
//        if (!DSBS.dataMap.containsKey(topic)) {
//            // init dataMap
//            ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();
//            // create synchronized list
//            // avoid race condition
////            entryMap.put(partitionNum, Collections.synchronizedList(new ArrayList<>()));
////            entryMap.put(partitionNum, new CopyOnWriteArrayList<>());
//            entryMap.put(partitionNum, new ArrayList<>());
//
//            DSBS.dataMap.put(topic, entryMap);
//
//            // send NACK
//            newOffset = offset;
//            pkg._offset = newOffset;
//            pkg._ack = false;
//
//            try {
//                out.writeObject(pkg);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            System.out.println("Server: create new queue for partition");
//            System.out.println("Server: sent NACK due to empty queue");
//
//            // no need to commit new offset
//
//        }

        // check if topic exists
        int newOffset;
        if (DSBS.dataMap.containsKey(topic)){
            // check if partition exists
            if (!DSBS.dataMap.get(topic).containsKey(partitionNum)) {
                System.out.println("System error: no valid partition number");
                return;
            }

            // partition for given topic
            List<Record> listOfRecord = DSBS.dataMap.get(topic).get(partitionNum);
            int size = listOfRecord.size();

            // assign the list of record with required batch size
            if (offset < size && offset + batchSize <= size) {
                List<Record> copy = new ArrayList<>(listOfRecord);
                pkg._data = new ArrayList<>(copy.subList(offset, offset + batchSize));
                newOffset = offset + batchSize;
                copy = null;

            } else if (offset < size && offset + batchSize > size) {
                List<Record> copy = new ArrayList<>(listOfRecord);
                pkg._data = new ArrayList<>(copy.subList(offset, size));
                newOffset = size;
                copy = null;

            } else {
                // send NACK back
                newOffset = size;
                pkg._offset = newOffset;
                pkg._ack = false;

                try {
                    out.writeObject(pkg);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println("Server: sent NACK due to empty queue");

                // commit the offset
                if (offsetMap != null) {
                    offsetMap.put(groupId, newOffset);
                }

                return;
            }

            // send data back
            pkg._offset = newOffset;
            pkg._ack = true;

            try {
                out.writeObject(pkg);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Server: sent a list of record back to consumer with topic \"" + topic + "\"");
        } else {
            System.out.println("System error: no valid topic");
            return;
        }

        // commit the offset
        if (offsetMap != null) {
            offsetMap.put(groupId, newOffset);
        }
    }
}

/**
 * console client to read stdin from the user
 */
class DSBSClient {

    public void setUp() {
        // read user input
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in));) {
            String state = "idle";
            DSBSParserEntry parserEntry = null;

            while (true) {
                if (state == null) {
                    System.out.println("System error: state shouldn't be null");
                    return;
                }

                switch (state.toLowerCase()) {
                    case "idle": {
                        // read user input
                        System.out.print("broker> ");
                        String stdIn = br.readLine();

                        // check the input
                        Pattern pattern = Pattern.compile("^\\s*$");
                        Matcher matcher = pattern.matcher(stdIn);
                        if (matcher.find()) {
                            System.out.println("Error: please type the valid command");
                            break;
                        }

                        // parse user's input
                        parserEntry = DSBSUtility.parser(stdIn);

                        //
                        if (parserEntry == null) {
                            System.out.println("System error: invalid parsing result, try again");
                            break;
                        }

                        // change the state
                        state = parserEntry.commandName;
                        break;
                    }
                    case "add": {
                        // construct the list of broker with ip and port
                        List<String> listOfIpAddr = parserEntry.listOfIpAddr;
                        List<String> listOfPortNum = parserEntry.listOfPortNum;

                        // check number of host
                        if (listOfIpAddr.size() <= 0 ||
                                listOfPortNum.size() <= 0 ||
                                listOfIpAddr.size() != listOfPortNum.size()) {
                            System.out.println("System error: the number of ip and port should be matched and positive");

                            state = "idle";
                            break;
                        }

                        // merge brokerList
                        for (int i = 0; i < listOfIpAddr.size(); i++) {
                            DSBS.brokerList.add(new String[]{listOfIpAddr.get(i), listOfPortNum.get(i)});
                        }

                        for (int i = 0; i < listOfIpAddr.size(); i++) {
                            String remoteIpAddr = listOfIpAddr.get(i);
                            int remotePortNum = Integer.parseInt(listOfPortNum.get(i));

                            try (Socket socket = new Socket(remoteIpAddr, remotePortNum)) {
                                try (ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                                     ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

                                    // construct the B2BAdd package
                                    B2BAdd sendPkg = new B2BAdd(TYPE.B2BADD, DSBS.brokerList);

                                    // send out to remote broker
                                    out.writeObject(sendPkg);
                                    System.out.println("Client: send merged brokerList to remote host with IP " +
                                            remoteIpAddr);

                                    // construct the received package
                                    Package revPkg = null;

                                    try {
                                        if ((revPkg = (Package) in.readObject()) != null) {
                                            if (revPkg._type == TYPE.B2BADD && revPkg._ack) {
                                                System.out.println("Client: add remote broker with IP " + remoteIpAddr);
                                            } else {
                                                System.out.println("Error: failed to add remote broker with IP " + remoteIpAddr);
                                            }
                                        }
                                    } catch (ClassNotFoundException e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                System.out.println("Client: invalid IP or port number");
                                System.exit(0);
                            }
                        }

                        state = "idle";
                        break;
                    }
                    default: {
                        break;
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class DSBSParserEntry {
    // command name
    String commandName = null;

    // add command
    List<String> listOfIpAddr = null;
    List<String> listOfPortNum = null;
}

/**
 * Handler for record stream from producer
 */
class DSBSP2BDataWorker implements Runnable {

    P2BData pkg;

    public DSBSP2BDataWorker(P2BData pkg) {
        this.pkg = pkg;
    }

    @Override
    public void run() {
        // retrieve data
        String topic = pkg._topic;
        List<Record> data = pkg._data;
        int partitionNum = pkg._partitionNum;

        // check if the topic exists
        if (DSBS.dataMap.containsKey(topic)) {
            ConcurrentHashMap<Integer, List<Record>> entryMap = DSBS.dataMap.get(topic);

            if (entryMap.containsKey(partitionNum)) {
                List<Record> tmpData = entryMap.get(partitionNum);
                tmpData.addAll(data);
                entryMap.put(partitionNum, tmpData);
            }
            else {
//                entryMap.put(partitionNum, data);
                System.out.println("System error: no valid partition");
                return;
            }
            DSBS.dataMap.put(topic, entryMap);
        }
        else {
//            ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();
//            entryMap.put(partitionNum, data);
//            DSBS.dataMap.put(topic, entryMap);
            System.out.println("System error: no valid topic");
            return;
        }
    }
}

abstract class DSBSUtility {

    public static DSBSParserEntry parser(String in) {
        DSBSParserEntry parserEntry = new DSBSParserEntry();

        // check input
        if (in == null || in.length() == 0) return null;

        // trim white space
        String trimmed = in.trim();

        // regular express
        Pattern pattern = null;
        Matcher matcher = null;

        // check the command
        pattern = Pattern.compile("^(add)");
        matcher = pattern.matcher(trimmed);

        if (!matcher.find()) {
            // no valid command
            System.out.println("Error: no valid command");
            return null;
        } else {
            // check if the parentheses are valid
            if (!DSBSUtility.isValidParenthesis(trimmed)) {
                System.out.println("Error: invalid parentheses");
                return null;
            }

            // retrieve command name
            String commandName = matcher.group(1);
            parserEntry.commandName = commandName;

            String withoutCommandSubstr = trimmed.substring(matcher.end(1)).trim();

            switch (commandName.toLowerCase()) {
                case "add": {
                    // init the remote network information list
                    parserEntry.listOfIpAddr = new ArrayList<>();
                    parserEntry.listOfPortNum = new ArrayList<>();

                    // retrieve the valid network information
                    // e.g. "123.456.78.90, 1234"
                    int start_pos = 0;
                    for (int i = 0; i < withoutCommandSubstr.length(); i++) {
                        char c = withoutCommandSubstr.charAt(i);

                        if (c == '(') {
                            start_pos = i;
                        } else if (c == ')') {
                            String content = withoutCommandSubstr.substring(start_pos + 1, i);

                            String remoteIpAddr = null;
                            String remotePortNum = null;

                            // validate the remote host ip
                            pattern = Pattern.compile("(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})");
                            matcher = pattern.matcher(content);

                            if (matcher.find()) {
                                remoteIpAddr = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host ip");
                                System.out.println("Help: please review the following IP address convention");
                                System.out.println("123.123.12.12");
                                return null;
                            }

                            // validate the remote host port
                            pattern = Pattern.compile(",\\s*(\\d{4,5}$)");
                            matcher = pattern.matcher(content);

                            if (matcher.find() && (Integer.parseInt(matcher.group(1)) >= 1024 && Integer.parseInt(matcher.group(1)) <= 65535)) {
                                remotePortNum = matcher.group(1);
                            } else {
                                System.out.println("Error: invalid host port");
                                System.out.println("Help: valid port number should be between 1024 and 49151, inclusive");
                                System.out.println("Please type command \"help\" to get more details");
                                return null;
                            }

                            // NEED TO CHECK THE SEPARATOR
//                            // validate the separator, comma
//                            pattern = Pattern.compile(".+,\\s*.+,\\s*.+");
//                            matcher = pattern.matcher(content);
//                            if (!matcher.find())

                            // add ip and port number
                            parserEntry.listOfIpAddr.add(remoteIpAddr);
                            parserEntry.listOfPortNum.add(remotePortNum);
                        }
                    }
                    break;
                }
                default: {
                    System.out.println("System error: invalid command");
                    break;
                }
            }
        }

        return parserEntry;
    }

    private static boolean isValidParenthesis(String in) {
        // check input
        if (in == null || in.length() == 0) return false;

        // check if there is at least one parenthesis
        Pattern pattern = Pattern.compile("[\\(\\)]");
        Matcher matcher = pattern.matcher(in);
        if (!matcher.find()) return false;

        // create a stack to store the parenthesis
        Deque<Character> stack = new ArrayDeque<>();

        for (int i = 0; i < in.length(); i++) {
            char c = in.charAt(i);

            // push the '(' into the stack
            if (c == '(') {
                stack.push(in.charAt(i));
            }
            // pop the ')' from the stack
            else if (c == ')') {
                if (stack.isEmpty()) {
                    return false;
                }
                stack.pop();
            }
        }

        if (!stack.isEmpty()) return false;
        return true;
    }
}
