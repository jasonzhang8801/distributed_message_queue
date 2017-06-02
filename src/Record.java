import java.io.Serializable;

/**
 * Created by YueLiu on 5/27/17.
 */
class Record implements Serializable{
    private static final long serialVersionUID = 1L;

    String _topic;
    String _value;

    Record(String topic, String value) {
        _topic = topic;
        _value = value;
    }

    @Override
    public String toString() {
        return _topic + " " + _value + " ";
    }
}

