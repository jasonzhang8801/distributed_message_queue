import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by jasonzhang on 5/29/17.
 */

public class DSBS {
    // DSBS's ip and port number
    public static String ipAddr = null;
    public static int portNum = -1;

    // DSBS's data storage
    public static ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Record>>> dataMap = null;

    // DSBS's cluster information
    public static List<String[]> brokerList = null;

    /**
     * create the information map to store the topic with the partitions
     */
    public static void createInfoMap() {}


    public static void main(String[] args) {

    }


}

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
            Package revPackage = null;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
