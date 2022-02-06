package vn.datnguyen.recommender.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.esotericsoftware.kryo.io.Input;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public class AvroEventScheme implements Scheme {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    private final static String VALUE_FIELD = customProperties.getProp("VALUE_FIELD");

    public List<Object> deserialize(ByteBuffer bytes) {
        AvroEventSerializer serializer = new AvroEventSerializer();
        AvroEvent event = serializer.read(null, new Input(bytes.array()), AvroEvent.class);
        
        List<Object> values = new ArrayList<>();
        values.add(0, event);
        return values;
    }

    public Fields getOutputFields() {
        return new Fields(new String[]{VALUE_FIELD});
    }
}
