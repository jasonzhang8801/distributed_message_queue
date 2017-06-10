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
    private static String _topic = "abc";
    private static int _bufferSize = 31000;
    private static int _batchSize = 20;
    private static int _recordSize;
    private static List<String[]> _destList;        // {partitionIP, partitionPort, partitionNum}
    private static List<ProducerWorker> _workerList = new ArrayList<>();
    private static volatile boolean _finish;

    public static void main(String[] args) {
        configure();
        connectBroker();
        produce();
    }


    // configure topic, bufferSize, batchSize and recordSize
    private static void configure() {
        System.out.println("Producer boot");
        Scanner scanner = new Scanner(System.in);
     //   System.out.println("Please input topic name:");
     //   _topic = scanner.next();
     //   System.out.println("Please input batch size:");
     //   _batchSize = scanner.nextInt();
     //   System.out.println("Please input buffer size:");
     //   _bufferSize = scanner.nextInt();
        System.out.println("Please input record size(byte):");
        _recordSize = scanner.nextInt();
    }


    // connect to broker and get partition list
    private static void connectBroker() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input Broker/DSBS IP and port:");
        String IP = scanner.next();
        int port = scanner.nextInt();

        System.out.println("P2BUP...");
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
        System.out.println("P2BUP finish");
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
        System.out.println("P2BData...");
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
        System.out.println("P2BData finish");

        // calculate throughput
        long end = System.currentTimeMillis();
        long throughput = (_bufferSize * 1000) / (end - start);
        System.out.println("start = "+ start);
        System.out.println("end = "+ end);
        System.out.println("\nProducer finished, the result is:");
        System.out.println("Buffer Size = " + _bufferSize + " Batch Size = " + _batchSize + " Record Size = " + _recordSize + " Throughput = " + throughput + " record(s)/s");
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
            System.out.println("ProducerWorker" + _partitionNum + "...");
            int count = 0;
            try (ObjectOutputStream out = new ObjectOutputStream(_socket.getOutputStream());
                 ObjectInputStream in = new ObjectInputStream(_socket.getInputStream())) {

                // send P2BData
                while (!_finish || !_queue.isEmpty()) {
                    List<Record> data = new ArrayList<>();
                    while (!_queue.isEmpty() && data.size() < _batchSize) {
                        data.add(_queue.poll());
                    }
                    count++;
                    out.writeObject(new P2BData(TYPE.P2BDATA, _topic, _partitionNum, data));
                }

                System.out.println("send #package = " + count);
                // send P2BEOS
                out.writeObject(new EOS(TYPE.EOS));

                // wait for broker's response
                in.readObject();

                // close socket
                _socket.close();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            System.out.println("ProducerWorker" + _partitionNum + "finish");
        }
    }
}