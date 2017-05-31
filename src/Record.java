/**
 * Created by YueLiu on 5/27/17.
 */
class Record {
    String _topic;
    String _value;

    Record(String topic, String value) {
        _topic = topic;
        _value = value;
    }
}

class EOFRecord {
    String _topic;
    String _value;

    EOFRecord(String topic, String value) {
        _topic = topic;
        _value = value;
    }
}
