import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class ConsumerWorker implements Runnable {
	private static final int RECORD_CNT_LMT = 100;

    private Thread _thread;
    private String _threadName;
    private String _topic;
    private int _groupID;
    private int _batchsize;
    public String[] partitionInfo;
    
    
    ConsumerWorker(String threadName, String topic, int groupID, int batchsize, String[] partition) {
	    	_threadName = threadName;
	    	_topic = topic;
	    	_batchsize = batchsize;
	    	partitionInfo = partition;
	    	System.out.println("Worker: " + threadName + " created");
    }
    
    public void run() {
    		consume();
    }
    
    public void start() {
	    	if (_thread == null) {
	    		_thread = new Thread(this, _threadName);
	    		_thread.start();
	    		System.out.println(_threadName + " started");
	    	}
    }
    
    private void consume() {
    		if (partitionInfo.length != 4) {
    			System.out.println("Faulty partition info on " + _threadName);
    			return;
    		}
    		System.out.println("consumption started");
    		String broker_addr = partitionInfo[0];
    		int port = Integer.parseInt(partitionInfo[1]);
    		int partNum = Integer.parseInt(partitionInfo[2]);
    		int offset = Integer.parseInt(partitionInfo[3]);
    		C2BData datapackage = new
    			C2BData(TYPE.C2BDATA, _topic, partNum, offset,
    				_groupID, _batchsize, new ArrayList<Record>());
        try {
        		Socket socket = new Socket(broker_addr, port);
            ObjectOutputStream outStream = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream inStream = new ObjectInputStream(socket.getInputStream());
            System.out.println("Socket connection established");
            outStream.writeObject(datapackage);


            System.out.println("initial consumption request package sent");
			int recordcount = 0;
            while (true) {
				Package p = (Package)inStream.readObject();
                datapackage = (C2BData)p;
                printDataBatch(datapackage);
				recordcount += datapackage._data.size();
				if (recordcount >= RECORD_CNT_LMT) break;
				outStream.writeObject(datapackage); // hotfix

            }
			outStream.writeObject(new EOS(TYPE.EOS));
            System.out.println(_threadName + " Data consumption complete");

			outStream.close();
			inStream.close();
            socket.close();

    		} catch (Exception e) {
    			e.printStackTrace();
    		}

    }
    
    private void printPartitionInfo() {
        for (int i = 0; i < partitionInfo.length; i++) {
        		System.out.print(partitionInfo[i] + " ");
        }
        System.out.print("\n");
    }
    
    private void printDataBatch(C2BData datapackage) {
    		if (datapackage._data.size() == 0) {
    			System.out.println("data package empty");
    			return;
    		}
    		System.out.println();
    		for (int i=0; i<datapackage._data.size(); i++) {
    			System.out.println(i + " " + datapackage._data.get(i));
    		}
    		System.out.println();
    }
}
