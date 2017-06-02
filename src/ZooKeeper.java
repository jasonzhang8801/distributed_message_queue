import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Created by YueLiu on 5/27/17.
 */
public class ZooKeeper {
    String _ip;
    int _port;
    List<String[]> _brokerList;         // {IP, port, partitionNum}
    Map<String, List<PartitionEntry>> topicTable;
}

class PartitionEntry implements Serializable{
    private static final long serialVersionUID = 1L;

    int _brokerID;
    int _partitionNum;
    Map<Integer, Integer> _offsetMap;   // consumerGroupID -> offset
}
