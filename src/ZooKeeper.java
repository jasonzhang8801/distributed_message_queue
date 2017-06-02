import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by YueLiu on 5/27/17.
 */
public class ZooKeeper {
    static String _ip;
    static int _port;
    static List<String[]> _brokerList;         // {IP, port, partitionNum}
    static Map<String, List<PartitionEntry>> topicTable;
    static ServerSocket srvSock;

    public ZooKeeper () {
        _ip = null;
        _port = -1;
        _brokerList = new ArrayList<>();
        topicTable = new ConcurrentHashMap<>();
        srvSock = null;
    }

    public static void main(String[] args) {
        ZooKeeper zk = new ZooKeeper();

        try {
            srvSock = new ServerSocket(0);
            _port = srvSock.getLocalPort();
            _ip = InetAddress.getLocalHost().getHostAddress().toString();
            System.out.println("ZooKeeper is starting up...");
            System.out.println("ZooKeeper "+_ip+":"+_port+" is up running.");
            Scanner scanner = new Scanner(System.in);

            addType(scanner);
            while (true) {
                String cmd = scanner.next();
                if (cmd.toLowerCase().equals("yes")) {
                    addType(scanner);
                }
                else if (cmd.toLowerCase().equals("no")) {
                    break;
                }
                else {
                    System.out.println("Invalid input, please type again.");
                }
            }


            while (true) {
                Socket sock = srvSock.accept();
                ZooKeeperWorker worker = new ZooKeeperWorker(sock);
                Thread zkWorkerT = new Thread(worker);
                zkWorkerT.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void config(String[] array) {
        String ip = array[0].trim();
        int port = Integer.parseInt(array[1].trim());
        ZK2BAdd outPack = new ZK2BAdd(TYPE.ZK2BADD, _ip, _port);
        try (Socket fwdSock = new Socket(ip, port)) {

            ObjectOutputStream fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
            fwdOut.writeObject(outPack);
            ObjectInputStream fwdIn = new ObjectInputStream(fwdSock.getInputStream());

            ZK2BAdd inPack;
            while ((inPack = (ZK2BAdd) fwdIn.readObject()) != null) { }
            if (inPack != null && inPack._ack) {
                ZooKeeper._brokerList.add(new String[] {ip, Integer.toString(port)});
                System.out.println("Broker: "+ip+":"+port+" added.");
            }
            //fwdSock.close();
            System.out.println("Do you wish to add another broker? yes/no");

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void addType(Scanner scanner) {
        System.out.println("Please add Broker by specifying the ip and port. " +
                "Syntax: ip:port (e.g. 127.0.0.1:5000");

        String cmd = scanner.next();
        String[] array = cmd.split(":");
        config(array);
    }

}


class ZooKeeperWorker implements Runnable {

    private Socket sock;
    ObjectInputStream in;
    ObjectOutputStream out;


    public ZooKeeperWorker(Socket sock) {
        this.sock = sock;
    }

    @Override
    public void run() {
        try {
            in = new ObjectInputStream(sock.getInputStream());
            out = new ObjectOutputStream(sock.getOutputStream());
            Package pack1 = (Package) in.readObject();
            TYPE type = pack1._type;
            switch (type) {

                default: {
                    System.out.println("Error. Unknown package type.");
                }

                case T2ZK: {
                    T2ZK pack2 = (T2ZK) pack1;
                    String topic = pack2._topic;
                    int numOfPart = pack2._partition;
                    if (ZooKeeper._brokerList.size() != 0) {
                        assignTopic(topic, numOfPart, pack2);
                        out.writeObject(pack2);
                    }
                    else {
                        System.out.println("Broker information not available. Please add broker first " +
                                "and resend topic information.");
                        pack2._ack = false;
                        pack2._partition = -1;
                        pack2._topic = null;
                        out.writeObject(pack2);
                    }

                    List<PartitionEntry> partitionEntryList = ZooKeeper.topicTable.get(topic);

                    for (int i = 0; i < partitionEntryList.size(); i++) {
                        PartitionEntry partitionEntry = partitionEntryList.get(i);
                        int brokerID = partitionEntry._brokerID;
                        String brokerIp = ZooKeeper._brokerList.get(brokerID)[0];
                        int brokerPort = Integer.parseInt(ZooKeeper._brokerList.get(brokerID)[1]);

                        try (Socket fwdSock = new Socket(brokerIp, brokerPort)) {
                            ZK2BTopic outPack = new ZK2BTopic(TYPE.ZK2BTOPIC, topic, partitionEntry);
                            ObjectOutputStream fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                            ObjectInputStream fwdIn = new ObjectInputStream(fwdSock.getInputStream());
                            fwdOut.writeObject(outPack);

                            ZK2BTopic inPack;
                            while ((inPack = (ZK2BTopic) fwdIn.readObject()) != null) {
                            }
                            if (inPack != null && inPack._ack) {
                                System.out.println("Topic " + topic + " is registered on broker " + brokerIp + ":" + brokerPort+".");
                            }
                        }

                    }

                    break;
                }

                case P2BUP: {

                    P2BUp pack2 = (P2BUp) pack1;
                    String topic = pack2._topic;
                    List<PartitionEntry> partitionEntryList = ZooKeeper.topicTable.get(topic);
                    List<String[]> partitionList = new ArrayList<>();
                    for (int i = 0; i < partitionEntryList.size(); i++) {
                        PartitionEntry partitionEntry = partitionEntryList.get(i);
                        int brokerID = partitionEntry._brokerID;
                        String brokerIp = ZooKeeper._brokerList.get(brokerID)[0];
                        String brokerPort = ZooKeeper._brokerList.get(brokerID)[1];
                        String partitionNum = Integer.toString(partitionEntry._partitionNum);
                        partitionList.add(new String[] {brokerIp, brokerPort, partitionNum});
                    }
                    pack2._partitionList = partitionList;
                    pack2._ack = true;
                    out.writeObject(pack2);
                }

                case C2BUP: {

                    C2BUp pack2 = (C2BUp) pack1;
                    int groupID = pack2._groupID;
                    String topic = pack2._topic;
                    List<PartitionEntry> partitionEntryList = ZooKeeper.topicTable.get(topic);
                    List<String[]> offsetList = new ArrayList<>();

                    for (int i = 0; i < partitionEntryList.size(); i++) {
                        PartitionEntry partitionEntry = partitionEntryList.get(i);
                        int brokerID = partitionEntry._brokerID;
                        String brokerIp = ZooKeeper._brokerList.get(brokerID)[0];
                        String brokerPort = ZooKeeper._brokerList.get(brokerID)[1];
                        String partitionNum = Integer.toString(partitionEntry._partitionNum);
                        Map<Integer, Integer> offsetMap = partitionEntry._offsetMap;
                        int offset;

                        if (offsetMap.containsKey(groupID)) {     //get the offset for this groupID on partition[i]
                            offset = offsetMap.get(groupID);
                        }
                        else {                   //if no offset for this group, register this group and set offset = 0
                            offset = 0;
                            offsetMap.put(groupID, 0);
                        }
                        offsetList.add(new String[] {brokerIp, brokerPort, partitionNum, Integer.toString(offset)});
                    }
                    pack2._offsetList = offsetList;
                    pack2._ack = true;
                    out.writeObject(pack2);
                }

                case B2ZKOFFSET: {
                    B2ZKOffset pack2 = (B2ZKOffset) pack1;
                    String topic = pack2._topic;
                    int groupID = pack2._groupdID;
                    int partitionNum = pack2._partitionNum;
                    int offset = pack2._offset;

                    PartitionEntry partitionEntry = getPartition(ZooKeeper.topicTable.get(topic), partitionNum);
                    if (partitionEntry == null) {
                        System.out.println("Error. No such partition number on this topic.");
                    }
                    else {
                        Map<Integer, Integer> offsetMap = partitionEntry._offsetMap;
                        if (offsetMap.containsKey(groupID)) {
                            offsetMap.put(groupID, offset);
                        }
                        else {
                            System.out.println("Error. No such consumer group on this topic with this partition number.");
                        }
                    }

                }


            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void assignTopic(String topic, int numOfPart, T2ZK pack) {
        if (ZooKeeper.topicTable.containsKey(topic)) {
            System.out.println("Error, duplicate topic. Please register another topic");
            pack._partition = -1;
            pack._topic = null;
            pack._ack = false;
        }
        else {
            List<String[]> brokerList = ZooKeeper._brokerList;
            List<PartitionEntry> partitonEntryList = new ArrayList<>();
            Random rand = new Random();
            for (int i = 0; i < numOfPart; i++) {
                PartitionEntry partitionEntry = new PartitionEntry();
                partitionEntry._brokerID = rand.nextInt(brokerList.size());
                partitionEntry._partitionNum = i;
                partitionEntry._offsetMap = new ConcurrentHashMap<>();
                partitonEntryList.add(partitionEntry);
            }
            ZooKeeper.topicTable.put(topic, partitonEntryList);
            pack._partition = -1;
            pack._topic = null;
            pack._ack = true;
        }
    }

    public static PartitionEntry getPartition(List<PartitionEntry> partitionEntryList, int partitionNum) {

        PartitionEntry partitionEntry = null;
        for (int i = 0; i < partitionEntryList.size(); i++) {
            if (partitionNum == partitionEntryList.get(i)._partitionNum) {
                return partitionEntryList.get(i);
            }
        }

        return partitionEntry;
    }
}

class PartitionEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    int _brokerID;
    int _partitionNum;
    Map<Integer, Integer> _offsetMap;   // consumerGroupID -> offset

}
