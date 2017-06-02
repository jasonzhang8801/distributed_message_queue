import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.Queue;

/**
 * Created by Kawayipk on 6/1/17.
 */
public class MyBroker {
    public static void main(String[] args) {
        Server server = new Server();
        new Thread(server).start();
        System.out.println("MyBroker exit");

    }

    private static class Server implements Runnable {

        private static ServerSocket _serverSocket;

        private Server() {
            try {
                _serverSocket = new ServerSocket(0);
                System.out.println(InetAddress.getLocalHost().getHostAddress() + " " + _serverSocket.getLocalPort());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    new Thread(new ServerWorker(_serverSocket.accept())).start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private static class ServerWorker implements Runnable {
            private Socket _socket;
            private Queue<Package> _queue;

            private ServerWorker(Socket socket) {
                _socket = socket;
                _queue = new LinkedList<>();
            }

            @Override
            public void run() {
                P2BData p = null;
                try(ObjectInputStream in = new ObjectInputStream(_socket.getInputStream());
                    ObjectOutputStream out = new ObjectOutputStream(_socket.getOutputStream())) {

                    while(((Package)in.readObject())._type == TYPE.P2BDATA) {
                        _queue.offer(p);
                    }

                    p._ack = true;
                    out.writeObject(p);

                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }

                System.out.println(_queue);

            }
        }
    }

}
