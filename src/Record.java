import java.io.Serializable;

/**
 * Created by YueLiu on 5/27/17.
 */
class Record implements Serializable {
    String _topic;
    String _value;

    Record(String topic, String value) {
        _topic = topic;
        _value = value;
    }
}

