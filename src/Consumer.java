/*
 * Author: Yiqiao Li
 *
 * Description: ConsumerGroup.java is responsible for consuming
 * data from Kafka brokers. It will initially connect one of the
 * broker machines in Kafka cluster to acquire partition information
 * on the requested topic, then create and assign consumers threads
 * to fetch data from each brokers respectively in a concurrent fashion.
 *
 * How to use: when running this java program, type in:
 * 	>java ConsumerGroup brokerIPAddress port topic
 * Example: >java ConsumerGroup 127.0.0.1 12345 livestream
 *
 */

import java.net.*;
import java.util.*;
import java.io.*;

public class Consumer {


    public static final int BATCH_SIZE = 32;
    public static final int GROUP_ID = 0;

    public static long[] startTimes;
    public static long[] endTimes;

    public static void main(String[] args) throws UnknownHostException, IOException {
        if (args.length != 3) {
            System.out.println("Type in parameters: ip port topic");
            System.exit(0);;
        }
        String server_addr = args[0];
        int port = Integer.parseInt(args[1]);
        String topic = args[2];
        System.out.println("Requesting connection to broker: " +
                server_addr + " at port: " + port + " - on topic: \"" + topic + "\"");
        //initialize a C2BUp package to send to a broker
        List<String[]> offsetList = new ArrayList<String[]>();
        C2BUp infoPackage = new C2BUp(TYPE.C2BUP, GROUP_ID, topic, offsetList);
        //connect to a broker inside the Kafka cluster
        //TODO error handling
        Socket socket = new Socket(server_addr, port);
        System.out.println("Socket connection established");
        ObjectOutputStream outStream;
        ObjectInputStream inStream;
        try {
            outStream = new ObjectOutputStream(socket.getOutputStream());
            outStream.writeObject(infoPackage);
            System.out.println("initial request package sent");
            inStream = new ObjectInputStream(socket.getInputStream());
            infoPackage = (C2BUp)inStream.readObject();
            System.out.println("return package received");
            offsetList = infoPackage._offsetList;
            socket.close();
            //assign consumer workers to their tasks according to
            //the returned partition info
 
            int partitionNum = offsetList.size();
            ConsumerWorker[] workers = new ConsumerWorker[partitionNum];
            startTimes = new long[partitionNum];
            endTimes = new long[partitionNum];

            //initialize CONSUMER_COUNT number of consumers
            for (int i=0; i<workers.length ; i++) {
                workers[i] = new
                        ConsumerWorker(
                        "worker" + i, topic, GROUP_ID, BATCH_SIZE, offsetList.get(i), i);
            }
            //start all consumer worker threads to start consuming data
            for (int i=0; i<workers.length ; i++) {
                workers[i].start();
            }

            for (int i=0; i<workers.length ; i++) {
                workers[i].join();
            }

            long startTime = findStartTime(startTimes);
            long endTime = findEndTime(endTimes);
            long throughput = (ConsumerWorker.RECORD_CNT_LMT * partitionNum * 1000)/ (endTime - startTime);
            System.out.println("start timestamp = "+ startTime);
            System.out.println("end timestamp = "+ endTime);

            System.out.println("\nConsumer finished, the result is:");
            System.out.println("Batch size = " + Consumer.BATCH_SIZE + " Throughput = " + throughput + " record/s");


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static long findStartTime(long[] inputTime) {
        long result = Long.MAX_VALUE;
        for (long t : inputTime) {
            result = Math.min(result, t);
        }
        return result;
    }

    private static long findEndTime(long[] inputTime) {
        long result = Long.MIN_VALUE;
        for (long t : inputTime) {
            result = Math.max(result, t);
        }
        return result;
    }
}
