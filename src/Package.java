import java.util.List;

/**
 * Created by YueLiu on 5/27/17.
 */
abstract class Package {
    TYPE _type;
    boolean _ack;

    Package(TYPE type) {
        _type = type;
    }
}

class ZK2BAdd extends Package {
    public ZK2BAdd(TYPE type) {
        super(type);
    }
}

class T2ZK extends Package {
    String _topic;
    int _partition;

    T2ZK(TYPE type, String topic, int partition) {
        super(type);
        _topic = topic;
        _partition = partition;
    }
}

class ZK2BTopic extends Package {
    String _topic;
    PartitionEntry _partitionEntry;

    ZK2BTopic(TYPE type, String topic, PartitionEntry partitionEntry) {
        super(type);
        _topic = topic;
        _partitionEntry = partitionEntry;
    }
}

class P2BUp extends Package {
    String _topic;
    List<String[]> _partitionList;      // {IP, port, partitionNum}

    P2BUp(TYPE type, String topic) {
        super(type);
        _topic = topic;
    }
}

class P2BData extends Package {
    String _topic;
    int _partitionNum;
    List<Record> _data;

    P2BData(TYPE type, String topic, int partitionNum, List<Record> data) {
        super(type);
        _topic = topic;
        _partitionNum = partitionNum;
        _data = data;
    }
}

class C2BUp extends Package {
    int _groupID;
    String _topic;
    List<String[]> _offsetList;     // {IP, port, partitionNum, offset}

    C2BUp(TYPE type, int groupID, String topic, List<String[]> offsetList) {
        super(type);
        _groupID = groupID;
        _topic = topic;
        _offsetList = offsetList;
    }
}

class C2BDATA extends Package {
    String _topic;
    int _partitionNum;
    int _offset;
    List<Record> _data;

    C2BDATA(TYPE type, String topic, int partitionNum, int offset, List<Record> data) {
        super(type);
        _topic = topic;
        _partitionNum = partitionNum;
        _offset = offset;
        _data = data;
    }
}

class B2ZKOffset extends Package {
    String _topic;
    int _groupdID;
    int _partitionNum;
    int _offset;

    B2ZKOffset(TYPE type, String topic, int _groupdID, int _partitionNum, int _offset) {
        super(type);
        _topic = topic;
        _groupdID = _groupdID;
        _partitionNum = _partitionNum;
        _offset = _offset;
    }
}

class B2PEOS extends Package {
    B2PEOS(TYPE type) {
        super(type);
    }
}

enum TYPE {
    ZK2BADD, T2ZK, ZK2BTOPIC, P2BUP, P2BDATA, C2BUP, C2BDATA, B2ZKOFFSET, B2PEOS,
}