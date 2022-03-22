package vn.datnguyen.recommender.Serialization;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import vn.datnguyen.recommender.AvroClasses.RecommendItemSimilaritesResult;

public class ItemSimilaritesDeserializer implements Deserializer<RecommendItemSimilaritesResult> {

    private final DatumReader<RecommendItemSimilaritesResult> eventReader = new SpecificDatumReader<>(RecommendItemSimilaritesResult.class);
    
    @Override
    public void configure(final Map<String, ?> map, final boolean b) {}

    @Override
    public RecommendItemSimilaritesResult deserialize(final String s, final byte[] bytes) {

        final Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);

        try {
            return eventReader.read(null, decoder);
        } catch (IOException e) {
            throw new SerializationException("Unable to deserializer event " + e.getMessage());
        }
    }

    @Override
    public void close() {}
}
