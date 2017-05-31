import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by benlolz on 5/29/17.
 */
public class Broker {
    static int port;
    static String ip;
    static int zkPort;
    static String zkIp;
    static ServerSocket srvSock;
    static ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Record>>> topicMap;


    public Broker() {
        port = -1;
        ip = null;
        zkPort = -1;
        zkIp = null;
        srvSock = null;
        topicMap = new ConcurrentHashMap<>();
    }

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.run();
    }


    public void run() {
        try {
            srvSock = new ServerSocket(0);
            port = srvSock.getLocalPort();
            ip = InetAddress.getLocalHost().getHostAddress().toString();
            System.out.println("BrokerServer is starting up...");

            while (true) {
                Socket sock = srvSock.accept();
                BrokerWorker worker = new BrokerWorker(sock);
                Thread brokerWorkerT = new Thread(worker);
                brokerWorkerT.start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


class BrokerWorker implements Runnable {
    private Socket sock;
    ObjectInputStream in;
    ObjectOutputStream out;
    ObjectOutputStream fwdOut;
    ObjectInputStream fwdIn;

    public BrokerWorker(Socket sock) {
        this.sock = sock;
    }

    @Override
    public void run() {
        try {
            in = new ObjectInputStream(sock.getInputStream());
            Package pack1 = (Package) in.readObject();
            if (pack1._type == TYPE.P2BUP) {
                P2BUp pack2 = (P2BUp) pack1;
                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                fwdOut.writeObject(pack2);

                fwdIn = new ObjectInputStream(fwdSock.getInputStream());
                pack2 = null;
                while (pack2 == null) {
                    pack2 = (P2BUp) in.readObject();
                }
                fwdSock.close();

                pack2._ack = true;
                out = new ObjectOutputStream(sock.getOutputStream());
                out.writeObject(pack2);
                sock.close();
            }
            else if (pack1._type == TYPE.P2BDATA) {
                P2BData pack2 =(P2BData) pack1;
                BrokerP2BDataProcessor processor1 = new BrokerP2BDataProcessor(pack2);
                Thread t1 = new Thread(processor1);
                t1.start();

                while (true) {
                    Package pack3 = (Package) in.readObject();
                    if (pack3._type == TYPE.EOS) {
                        EOS pack4 = (EOS) pack3;
                        pack4._ack = true;
                        out = new ObjectOutputStream(sock.getOutputStream());
                        out.writeObject(pack4);
                        sock.close();
                        break;
                    }
                    else {
                        P2BData pack4 =(P2BData) pack3;
                        BrokerP2BDataProcessor processor2 = new BrokerP2BDataProcessor(pack2);
                        Thread t2 = new Thread(processor2);
                        t2.start();
                    }
                }
            }
            else if (pack1._type == TYPE.ZK2BADD) {
                ZK2BAdd pack2 = (ZK2BAdd) pack1;
                Broker.zkIp = pack2._zkIP;
                Broker.zkPort = pack2._zkPort;
                pack2._zkIP = null;
                pack2._zkPort = -1;
                pack2._ack = true;
                out = new ObjectOutputStream(sock.getOutputStream());
                out.writeObject(pack2);
                sock.close();
            }
            else if (pack1._type == TYPE.ZK2BTOPIC) {
                ZK2BTopic pack2 = (ZK2BTopic) pack1;
                String topic = pack2._topic;
                PartitionEntry partitionEntry = pack2._partitionEntry;
                if (Broker.topicMap.containsKey(topic)) {
                    ConcurrentHashMap<Integer, List<Record>> entryMap = Broker.topicMap.get(topic);
                    if (!entryMap.containsKey(partitionEntry._partitionNum)) {
                        entryMap.put(partitionEntry._partitionNum, new ArrayList<Record>());
                    }
                    Broker.topicMap.put(topic, entryMap);
                }
                else {
                    ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();
                    entryMap.put(partitionEntry._partitionNum, new ArrayList<Record>());
                    Broker.topicMap.put(topic, entryMap);
                }
                pack2._partitionEntry = null;
                pack2._topic = null;
                pack2._ack = true;
                out = new ObjectOutputStream(sock.getOutputStream());
                out.writeObject(pack2);
                sock.close();
            }
            else if (pack1._type == TYPE.C2BUP) {
                C2BUp pack2 = (C2BUp) pack1;
                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                fwdOut.writeObject(pack2);

                fwdIn = new ObjectInputStream(fwdSock.getInputStream());
                pack2 = null;
                while (pack2 == null) {
                    pack2 = (C2BUp) in.readObject();
                }
                fwdSock.close();
                pack2._ack = true;
                out = new ObjectOutputStream(sock.getOutputStream());
                out.writeObject(pack2);
                sock.close();
            }
            else if (pack1._type == TYPE.C2BDATA) {
                C2BData pack2 = (C2BData) pack1;
                String topic = pack2._topic;
                int partitionNum = pack2._partitionNum;
                int offset = pack2._partitionNum;
                int batchSize = pack2._batchSize;
                while (true) {
                    if (Broker.topicMap.containsKey(topic)) {
                        ConcurrentHashMap<Integer, List<Record>> entryMap = Broker.topicMap.get(topic);
                        if (entryMap.containsKey(partitionNum)) {
                            List<Record> dataList = entryMap.get(partitionNum);
                            int size = dataList.size();
                            if (offset < size && offset +  batchSize<= size) {
                                List<Record> subList = new ArrayList<>(dataList.subList(offset, offset+batchSize));
                                pack2._data = subList;
                                pack2._offset = offset+batchSize;
                                pack2._partitionNum = -1;
                                pack2._topic = null;
                                out = new ObjectOutputStream(sock.getOutputStream());
                                out.writeObject(pack2);
                                B2ZKOffset pack3 = new B2ZKOffset(TYPE.B2ZKOFFSET, pack2._topic, pack2._groupID, pack2._partitionNum, pack2._offset);
                                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                                fwdOut.writeObject(pack3);
                                fwdSock.close();
                            }
                            else if (offset < size && offset + batchSize > size) {
                                List<Record> subList = new ArrayList<>(dataList.subList(offset, size));
                                pack2._data = subList;
                                pack2._offset = size;
                                pack2._partitionNum = -1;
                                pack2._topic = null;
                                out = new ObjectOutputStream(sock.getOutputStream());
                                out.writeObject(pack2);
                                B2ZKOffset pack3 = new B2ZKOffset(TYPE.B2ZKOFFSET, pack2._topic, pack2._groupID, pack2._partitionNum, pack2._offset);
                                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                                fwdOut.writeObject(pack3);
                                fwdSock.close();
                            }
                            else {
                                EOS pack3 = new EOS(TYPE.EOS);
                                out = new ObjectOutputStream(sock.getOutputStream());
                                out.writeObject(pack3);
                                sock.close();
                            }
                        }
                        else {
                            System.out.println("Broker doesn't own the partition on topic: "+topic);
                        }
                    }
                    else {
                        System.out.println("Broker doesn't have any data on topic: "+topic);
                    }
                }

            }


        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}

class BrokerP2BDataProcessor implements Runnable{

    P2BData pack;

    public BrokerP2BDataProcessor(P2BData pack) {
        this.pack = pack;
    }
    @Override
    public void run() {
        String topic = pack._topic;
        List<Record> data = pack._data;
        int partitionNum = pack._partitionNum;
        if (Broker.topicMap.containsKey(topic)) {
            ConcurrentHashMap<Integer, List<Record>> entryMap = Broker.topicMap.get(topic);
            if (entryMap.containsKey(partitionNum)) {
                List<Record> tmpData = entryMap.get(partitionNum);
                tmpData.addAll(data);
                entryMap.put(partitionNum, tmpData);
            }
            else {
                entryMap.put(partitionNum, data);
            }
            Broker.topicMap.put(topic, entryMap);
        }
        else {
            ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();
            entryMap.put(partitionNum, data);
            Broker.topicMap.put(topic, entryMap);
        }
    }
}
