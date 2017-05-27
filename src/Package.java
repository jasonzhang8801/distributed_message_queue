import java.util.List;

enum TYPE {
    PACKAGE, ZK2BADD, T2ZK, ZK2BTOPIC, P2BUP, P2BDATA, C2BUP, C2BDATA, B2ZKOFFSET, P2BEOS,

}

/**
 * Created by YueLiu on 5/27/17.
 */
abstract class Package {
    boolean _ack;
    TYPE _type;

    public Package(TYPE type) {
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

    public T2ZK(TYPE type, String topic, int partition) {
        super(type);
        _topic = topic;
        _partition = partition;
    }
}

class ZK2BTopic extends Package {
    String _topic;
    PartitionEntry _partitionEntry;

    public ZK2BTopic(TYPE type, String topic, PartitionEntry partitionEntry) {
        super(type);
        _topic = topic;
        _partitionEntry = partitionEntry;
    }
}

class P2BUp extends Package {
    String _topic;
    List<String[]> _partitionList;

    public P2BUp(TYPE type, String topic, List<String[]> partitionList) {
        super(type);
        _topic = topic;
        _partitionList = partitionList;
    }
}

class P2BData extends Package {
    String _topic;
    int _partitionNum;
    List<Record> _data;

    public P2BData(TYPE type, String topic, int partitionNum, List<Record> data) {
        super(type);
        _topic = topic;
        _partitionNum = partitionNum;
        _data = data;
    }
}

class C2BUp extends Package {
    int _groupID;
    String _topic;
    List<String[]> _offsetList;

    public C2BUp(TYPE type, int groupID, String topic, List<String[]> offsetList) {
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

    public C2BDATA(TYPE type, String topic, int partitionNum, int offset, List<Record> data) {
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

    public B2ZKOffset(TYPE type, String topic, int _groupdID, int _partitionNum, int _offset) {
        super(type);
        _topic = topic;
        _groupdID = _groupdID;
        _partitionNum = _partitionNum;
        _offset = _offset;
    }
}

class P2BEOS extends Package {
    public P2BEOS(TYPE type) {
        super(type);
    }
}

