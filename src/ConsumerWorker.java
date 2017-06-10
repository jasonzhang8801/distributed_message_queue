import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;

public class ConsumerWorker implements Runnable {
	public static final int RECORD_CNT_LMT = 10000;

    private Thread _thread;
    private String _threadName;
    private String _topic;
    private int _groupID;
    private int _batchsize;
    private int _ID;
    public String[] partitionInfo;

	// yue
	int _recordcount;


    ConsumerWorker(String threadName, String topic, int groupID, int batchsize, String[] partition, int ID) {
	    	_threadName = threadName;
	    	_topic = topic;
	    	_batchsize = batchsize;
	    	partitionInfo = partition;
	    	_ID = ID;
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

    public void join() throws InterruptedException {
    	_thread.join();
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

			// yue
			Package p = null;
			while (true) {
				p = (Package)inStream.readObject();
				datapackage = (C2BData)p;

				if (datapackage._ack == true) {
					Consumer.startTimes[_ID] = System.currentTimeMillis();
					outStream.writeObject(datapackage);
					System.out.println("start time" + _ID + " " + Consumer.startTimes[_ID]);
					break;
				}

				outStream.writeObject(datapackage);
			}
			long inTime=0;
			long outTime=0;
            while (true) {
				p = (Package)inStream.readObject();
				inTime = System.currentTimeMillis();
				//System.out.println("Network travel time: " + (inTime - outTime) + " ms");
                datapackage = (C2BData)p;
//                printDataBatch(datapackage);
				_recordcount += datapackage._data.size();
				if (_recordcount >= RECORD_CNT_LMT) break;
				outStream.writeObject(datapackage); // hotfix
				outTime = System.currentTimeMillis();
				//System.out.println("Package processing time: " + (outTime - inTime) + " ms");
            }
			outStream.writeObject(new EOS(TYPE.EOS));
            System.out.println(_threadName + " Data consumption complete");

			outStream.close();
			inStream.close();
            socket.close();

            // yue
			Consumer.endTimes[_ID] = System.currentTimeMillis();
			System.out.println("end time" + _ID + " " + Consumer.endTimes[_ID]);
    		} catch (Exception e) {
    			e.printStackTrace();
				//System.out.println();
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
