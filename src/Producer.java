import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by YueLiu on 5/27/17.
 */

public class Producer {
    private static String _topic;
    private static int _bufferSize = 100000;
    private static int _batchSize = 20;
    private static int _recordSize = 4;
    private static List<String[]> _destList;        // {partitionIP, partitionPort, partitionNum}
    private static List<ProducerWorker> _workerList = new ArrayList<>();
    private static volatile boolean _finish;

//    private static Server _server;

    public static void main(String[] args) {
//        serverStart();
        configure();
        connectBroker();
        produce();
    }

    // server start
//    private static void serverStart() {
//        _server = new Server();
//        new Thread(_server).start();
//    }


    // configure topic, bufferSize, batchSize and recordSize
    private static void configure() {
        System.out.println("Producer boot");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input topic name:");
        _topic = scanner.next();
        System.out.println("Please input batch size:");
        _batchSize = scanner.nextInt();
        System.out.println("Please input buffer size:");
        _bufferSize = scanner.nextInt();
        System.out.println("Please input record size(byte):");
        _recordSize = scanner.nextInt();
    }


    // connect to broker and get partition list
    private static void connectBroker() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input ZooKeeper/DSBS IP and port:");
        String IP = scanner.next();
        int port = scanner.nextInt();

        try (Socket socket = new Socket(IP, port)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            P2BUp p = new P2BUp(TYPE.P2BUP, _topic);
            out.writeObject(p);
            p = (P2BUp) in.readObject();
            _destList = p._partitionList;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // produce data
    private static void produce() {
        // generate record template
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < _recordSize / 2; i++) {
            sb.append("r");
        }
        Record record = new Record(_topic, sb.toString());

        // create and start ProducerWorker
        int partition = _destList.size();
        for (String[] a_destList : _destList) {
            _workerList.add(new ProducerWorker(a_destList[0], Integer.parseInt(a_destList[1]), Integer.parseInt(a_destList[2])));
        }
        List<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < partition; i++) {
            threadList.add(new Thread(_workerList.get(i)));
            threadList.get(i).start();
        }

        // produce record and send to ProducerWorker randomly
        long start = System.currentTimeMillis();
        Random random = new Random();
        for (int i = 0; i < _bufferSize; i++) {
            _workerList.get(random.nextInt(partition))._queue.offer(record);
        }

        // Producer finish produce record, produceWorker will send P2BEOS
        _finish = true;

        // produceWorker join
        for (int i = 0; i < partition; i++) {
            try {
                threadList.get(i).join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // calculate throughput
        long end = System.currentTimeMillis();
        System.out.println(start + " " + end);
        long throughput = _bufferSize / (end - start) ;

        System.out.println("Buffer Size = " + _bufferSize +  " Batch Size = " + _batchSize + " Record Size = " + _recordSize + " Throughput = " + throughput + "/ms");
    }


    private static class ProducerWorker implements Runnable {
        private Socket _socket;
        private int _partitionNum;
        private Queue<Record> _queue;

        private ProducerWorker(String IP, int port, int partitionNum) {
            try {
                _socket = new Socket(IP, port);
            } catch (IOException e) {
                e.printStackTrace();
            }

            _partitionNum = partitionNum;
            _queue = new ConcurrentLinkedDeque<>();
        }

        @Override
        public void run() {
            try (ObjectOutputStream out = new ObjectOutputStream(_socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(_socket.getInputStream())){

                // send P2BData
                while (!_finish || !_queue.isEmpty()) {
                    List<Record> data = new ArrayList<>();
                    while (!_queue.isEmpty() && data.size() < _batchSize) {
                        data.add(_queue.poll());
                    }
                    out.writeObject(new P2BData(TYPE.P2BDATA, _topic, _partitionNum, data));
                }

                // send P2BEOS
                out.writeObject(new EOS(TYPE.EOS));

                // wait for broker's response
                in.readObject();

                // close socket
                _socket.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }


//    private static class Server implements Runnable {
//
//        private static ServerSocket _serverSocket;
//
//        private Server() {
//            try {
//                _serverSocket = new ServerSocket(0);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    new Thread(new ServerWorker(_serverSocket.accept())).start();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//    }
//
//    private static class ServerWorker implements Runnable {
//        private Socket _socket;
//
//        private ServerWorker(Socket socket) {
//            _socket = socket;
//        }
//
//        @Override
//        public void run() {
//
//        }
//    }
}