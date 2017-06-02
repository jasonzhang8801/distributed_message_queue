import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by YueLiu on 5/27/17.
 */
public class TopicClient {

    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println("input format is {topic_name, partition_num, zookeeper/broker, ip, port}, please try again");
            return;
        }

        sendTopic(args[0], Integer.parseInt(args[1]), args[2].equals("zookeeper"), args[3], Integer.parseInt(args[4]));
        System.out.println("Topic is Sent successfully, exit");
    }

    // send topic and partition to ZooKeeper/DSBS
    private static void sendTopic(String topic, int partition, boolean isZK, String IP, int port) {
        try (Socket socket = new Socket(IP, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            if (isZK) {
                out.writeObject(new T2ZK(TYPE.T2ZK, topic, partition));
            } else {
                out.writeObject(new T2B(TYPE.T2B, topic, partition));

            }

            in.readObject();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }
}
