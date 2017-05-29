import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by benlolz on 5/29/17.
 */
public class Broker {
    static String port;
    ServerSocket srvSock;

    public static void main(String[] args) {
        Broker broker = new Broker();
        broker.run();
    }

    @Override
    public void run() {
        try {
            srvSock = new ServerSocket(0);
            Broker.port = Integer.toString(srvSock.getLocalPort());
            System.out.println("Server is starting up...");

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
    Socket sock;
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
                P2BUp package = (P2BUp) pack;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
