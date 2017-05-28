import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by YueLiu on 5/27/17.
 */
public class TopicClient {

    public static void main(String[] args) {
        System.out.println("Topic producer boot");
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please input topic name:");
        String topic = scanner.next();
        System.out.println("Please input topic partition:");
        int partition = scanner.nextInt();
        System.out.println("Please input ZooKeeper/DSBS's IP and port");
        String IP = scanner.next();
        int port = scanner.nextInt();

        sendTopic(topic, partition, IP, port);

        System.out.println("Topic is Sent successfully, exit");

    }

    // send topic and partition to ZooKeeper/DSBS
    private static void sendTopic(String topic, int partition, String IP, int port) {
        try (Socket socket = new Socket(IP, port)) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());

            out.writeObject(new T2ZK(TYPE.T2ZK, topic, partition));
            out.flush();
            in.readObject();

            in.close();
            out.close();
            socket.close();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }
}
