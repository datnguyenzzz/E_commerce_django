package vn.datnguyen.recommender.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

import org.apache.kafka.common.header.internals.RecordHeader;

public class RecordHeaderSerializer extends FieldSerializer<RecordHeader> {
    public RecordHeaderSerializer(Kryo kryo) {
        super(kryo, RecordHeader.class);
    }

    @Override
    public void write(Kryo kryo, Output output, RecordHeader object) {
        output.writeString(object.key());
        byte[] V = object.value(); 
        int sz = V.length;
        output.writeInt(sz);
        output.writeBytes(V);
    }

    @Override
    public RecordHeader read(Kryo kryo, Input input, Class<RecordHeader> type) {
        try {
            String K = input.readString(); 
            int sz = input.readInt();
            byte[] V = input.readBytes(sz);
            return new RecordHeader(K,V);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
