import java.util.List;
import java.util.Map;

/**
 * Created by YueLiu on 5/27/17.
 */
class ZooKeeper {
    String _ip;
    int _port;
    List<String[]> _brokerList;         // {IP, port, partitionNum}
    Map<String, List<PartitionEntry>> topicTable;
}


class PartitionEntry {
    int _brokerID;
    int _partitionNum;
    Map<Integer, Integer> _offsetMap;   // consumerGroupID -> offset
}
