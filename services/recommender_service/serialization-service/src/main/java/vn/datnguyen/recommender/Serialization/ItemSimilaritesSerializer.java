package vn.datnguyen.recommender.Serialization;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import vn.datnguyen.recommender.AvroClasses.RecommendItemSimilaritesResult;

public class ItemSimilaritesSerializer implements Serializer<RecommendItemSimilaritesResult> {

    private final DatumWriter<RecommendItemSimilaritesResult> eventWriter = new SpecificDatumWriter<>(RecommendItemSimilaritesResult.class);

    @Override
    public void configure(Map<String, ?> map, boolean b) {} 

    @Override
    public byte[] serialize(final String s, final RecommendItemSimilaritesResult event) {

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