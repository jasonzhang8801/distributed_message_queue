import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by benlolz on 5/29/17.
 */
public class Broker {
    static int port;
    static String ip;
    ServerSocket srvSock;
    ConcurrentHashMap<String, ConcurrentHashMap<Integer, List<Record>>> topicMap;


    public Broker() {
        port = -1;
        ip = null;
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
    FileOutputStream fileOut;
    ObjectOutputStream obOut;

    public BrokerWorker(Socket sock) {
        this.sock = sock;
    }

    @Override
    public void run() {
        try {
            ObjectInputStream in = new ObjectInputStream(sock.getInputStream());
            Package pack = (Package) in.readObject();
            if (pack._type == TYPE.P2BUP) {
                pack = (P2BUp) pack;
            }

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
