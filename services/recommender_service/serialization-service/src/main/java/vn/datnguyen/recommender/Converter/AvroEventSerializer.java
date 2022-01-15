package vn.datnguyen.recommender.Converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import vn.datnguyen.recommender.Serialization.AvroEvent;

public class AvroEventSerializer implements Serializer<AvroEvent> {

    private final DatumWriter<AvroEvent> eventWriter = new SpecificDatumWriter<>(AvroEvent.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {} 

    @Override
    public byte[] serialize(final String s, final AvroEvent event) {

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            final BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
            eventWriter.write(event, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("Unable serialize AvroEvent");
        }
    }

    @Override
    public void close() {}
}
