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
    static int zkPort;
    static String zkIp;
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
            Package pack = (Package) in.readObject();
            if (pack._type == TYPE.P2BUP) {
                pack = (P2BUp) pack;
                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                fwdOut.writeObject(pack);

                fwdIn = new ObjectInputStream(fwdSock.getInputStream());
                pack = null;
                while (pack == null) {
                    pack = (P2BUp) in.readObject();
                }
                fwdSock.close();
                out = new ObjectOutputStream(sock.getOutputStream());
                out.writeObject(pack);
                sock.close();
            }
            else if (pack._type == TYPE.C2BUP) {
                pack = (C2BUp) pack;
                Socket fwdSock = new Socket(Broker.zkIp, Broker.zkPort);
                fwdOut = new ObjectOutputStream(fwdSock.getOutputStream());
                fwdOut.writeObject(pack);

                fwdIn = new ObjectInputStream(fwdSock.getInputStream());
                pack = null;
                while (pack == null) {
                    pack = (C2BUp) in.readObject();
                }
                fwdSock.close();
                out = new ObjectOutputStream(sock.getOutputStream());
                out.writeObject(pack);
                sock.close();
            }


        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
