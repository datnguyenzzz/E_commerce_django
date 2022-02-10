package vn.datnguyen.recommender.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;

public class AvroEventSerializer extends Serializer<AvroEvent> {

    private final Logger logger = LoggerFactory.getLogger(AvroEventSerializer.class);
    private Schema SCHEMA = AvroEvent.getClassSchema();

    public void write(Kryo kryo, Output output, AvroEvent object) {
        DatumWriter<AvroEvent> writer = new SpecificDatumWriter<>(SCHEMA);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        try {
            writer.write(object, encoder);
            encoder.flush();
            out.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        byte[] outBytes = out.toByteArray();
        output.writeInt(outBytes.length, true);
        output.write(outBytes);
    }

    public AvroEvent read(Kryo kryo, Input input, Class<AvroEvent> type) {
        AvroEvent event = null;
        byte[] value = input.getBuffer();
        SpecificDatumReader<AvroEvent> reader = new SpecificDatumReader<>(SCHEMA);

        try {
            event = reader.read(null, DecoderFactory.get().binaryDecoder(value, null));
        }
        catch (IOException e) {
            logger.error(e.getMessage());
        }
        return event;
    }
}
