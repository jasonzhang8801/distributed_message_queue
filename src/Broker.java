import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
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
            System.out.println("Broker ip: " + ip + ", port: " + port);

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


    public BrokerWorker(Socket sock) {
        this.sock = sock;
    }

    @Override
    public void run() {


        try {
            ObjectInputStream in;
            ObjectOutputStream out;
            ObjectOutputStream fwdOut;
            ObjectInputStream fwdIn;
            in = new ObjectInputStream(sock.getInputStream());
            out = new ObjectOutputStream(sock.getOutputStream());


            Package pack1 = (Package) in.readObject();
            if (pack1._type == TYPE.P2BUP) {
                //System.out.println("Broker received P2BUp");
                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                fwdIn = new ObjectInputStream(fwdSock.getInputStream());

                P2BUp pack2 = (P2BUp) pack1;
                fwdOut.writeObject(pack2);
                //System.out.println("Broker forwarded P2BUp to ZK");

                pack2 = (P2BUp) fwdIn.readObject();

                fwdIn.close();
                fwdOut.close();
                fwdSock.close();
                pack2._ack = true;
                out.writeObject(pack2);
                //System.out.println("Broker sent back P2BUp");

            }
            else if (pack1._type == TYPE.P2BDATA) {
                P2BData pack2 =(P2BData) pack1;
                BrokerP2BDataProcessor processor1 = new BrokerP2BDataProcessor(pack2);
                Thread t1 = new Thread(processor1);
                t1.start();
                String topic = pack2._topic;
                int num = pack2._partitionNum;

                while (true) {
                    Package pack3 = (Package) in.readObject();
                    if (pack3._type == TYPE.EOS) {
                        EOS pack4 = (EOS) pack3;
                        pack4._ack = true;
                        out.writeObject(pack4);

                        System.out.println("Broker received number of records on one partition : "
                                +Broker.topicMap.get(topic).get(num).size());
                        break;
                    }
                    else {
                        P2BData pack4 =(P2BData) pack3;
                        BrokerP2BDataProcessor processor2 = new BrokerP2BDataProcessor(pack4);
                        Thread t2 = new Thread(processor2);
                        t2.start();
                    }
                }
            }
            else if (pack1._type == TYPE.ZK2BADD) {
                ZK2BAdd pack2 = (ZK2BAdd) pack1;
                Broker.zkIp = pack2._zkIP;
                Broker.zkPort = pack2._zkPort;
                pack2._ack = true;
                out.writeObject(pack2);
                System.out.println("Received ZK2BADD request from ZooKeeper. Broker is added to the Kafka cluster.");

            }
            else if (pack1._type == TYPE.ZK2BTOPIC) {
                ZK2BTopic pack2 = (ZK2BTopic) pack1;
                String topic = pack2._topic;
                PartitionEntry partitionEntry = pack2._partitionEntry;
                if (Broker.topicMap.containsKey(topic)) {
                    ConcurrentHashMap<Integer, List<Record>> entryMap = Broker.topicMap.get(topic);
                    if (!entryMap.containsKey(partitionEntry._partitionNum)) {
                        entryMap.put(partitionEntry._partitionNum, Collections.synchronizedList(new ArrayList<Record>()));
                    }
                    Broker.topicMap.put(topic, entryMap);
                }
                else {
                    ConcurrentHashMap<Integer, List<Record>> entryMap = new ConcurrentHashMap<>();
                    entryMap.put(partitionEntry._partitionNum, Collections.synchronizedList(new ArrayList<Record>()));
                    Broker.topicMap.put(topic, entryMap);
                }
                pack2._partitionEntry = null;
                pack2._ack = true;
                System.out.println("Received partition "+partitionEntry._partitionNum+" on topic "+topic+ " from ZooKeeper");
                out.writeObject(pack2);

            }
            else if (pack1._type == TYPE.C2BUP) {

                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                fwdIn = new ObjectInputStream(fwdSock.getInputStream());

                C2BUp pack2 = (C2BUp) pack1;
                fwdOut.writeObject(pack2);  //forward to ZooKeeper

                pack2 = (C2BUp) fwdIn.readObject();

                fwdIn.close();
                fwdOut.close();
                fwdSock.close();

                pack2._ack = true;
                out.writeObject(pack2);     //reply back to Consumer of partition Info

            }
            else if (pack1._type == TYPE.C2BDATA) {
                C2BData pack2 = (C2BData) pack1;
                String topic = pack2._topic;
                int partitionNum = pack2._partitionNum;
                int groupID = pack2._groupID;
                int prevOffset = pack2._offset;     //record the previous offset


                processC2BData(pack2, out);

                       //prepare data according to the incoming offset with C2BData pack
                // and send it back to consumer.

                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());


                Package pack3;
                while (true) {
                    pack3 = (Package) in.readObject();
                    if (pack3._type == TYPE.C2BDATA) {
                        pack3 = (C2BData) pack3;
                        B2ZKOffset pack4 = new B2ZKOffset(TYPE.B2ZKOFFSET, topic, groupID, partitionNum, prevOffset);

                        fwdOut.writeObject(pack4);          //Commit previous offset to ZooKeeper

                        prevOffset = ((C2BData) pack3)._offset;
                        processC2BData((C2BData)pack3, out);
//                        if (!processC2BData((C2BData)pack3, out)) {
//
//                            //break;
//                        }
                    }
                    else if (pack3._type == TYPE.EOS) {

                        fwdOut.close();
                        fwdSock.close();
                        break;
                    }
                }
            }

            in.close();
            out.close();
            sock.close();


        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void processC2BData(C2BData pack, ObjectOutputStream out) {

        String topic = pack._topic;
        int partitionNum = pack._partitionNum;
        int offset = pack._offset;
        int batchSize = pack._batchSize;



        if (Broker.topicMap.containsKey(topic)) {
            ConcurrentHashMap<Integer, List<Record>> entryMap = Broker.topicMap.get(topic);
            if (entryMap.containsKey(partitionNum)) {
                List<Record> dataList = entryMap.get(partitionNum);
                int size = dataList.size();
//                System.out.println("partition size = " + size);
//                System.out.println("offset = " + offset);
//                System.out.println("batchsize = " + batchSize);
                try {
                    if (offset < size && offset + batchSize <= size) {

                        List<Record> subList = new ArrayList<>(dataList.subList(offset, offset + batchSize));
                        pack._data = subList;
                        pack._offset = offset + batchSize;
                        pack._ack = true;
                        out.writeObject(pack);
//                        System.out.println("Broker send a batch of List<Record> with size: "+ subList.size()
//                                +" from partition " + partitionNum);
                        //return true;

                    } else if (offset < size && offset + batchSize > size) {

                        List<Record> subList = new ArrayList<>(dataList.subList(offset, size));



                        pack._data = subList;
                        pack._offset = size;
                        pack._ack = true;
                        out.writeObject(pack);
//                        System.out.println("Broker send a batch of List<Record> with size: "+ subList.size()
//                                +" from partition " + partitionNum);
                        //return true;

                    } else {            //If incoming offset == queue.size(), reply with EOS package instead of C2BData
                        //and close connection

                        pack._data = new ArrayList<>();
                        pack._offset = size;
                        pack._ack = false;
                        out.writeObject(pack);
                        System.out.println("End of partition...");
                        //return true;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                System.out.println("Broker doesn't own the partition on topic: " + topic);
                //return false;
            }
        } else {
            System.out.println("Broker doesn't have any data on topic: " + topic);
            //return false;
        }
        //return false;
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

