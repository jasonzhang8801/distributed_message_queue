/**
 * Created by YueLiu on 5/27/17.
 */
class Record {
    String _topic;
    String _value;

    public Record(String topic, String value) {
        _topic = topic;
        _value = value;
    }
}

class EOFRecord extends Record{
    public EOFRecord(String topic, String value) {
        super(topic, value);
    }
}
