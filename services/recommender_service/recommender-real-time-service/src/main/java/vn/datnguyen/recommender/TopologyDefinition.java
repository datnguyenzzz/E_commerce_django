package vn.datnguyen.recommender;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Topologies.CollaborativeFiltering;
import vn.datnguyen.recommender.Topologies.ContentBased;
import vn.datnguyen.recommender.utils.CustomProperties;

import java.nio.charset.StandardCharsets;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.FieldSerializer;

@SuppressWarnings("unused")
public class TopologyDefinition {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    // ack worker 
    private static final String NUM_ACK_WORKERS = customProperties.getProp("NUM_ACK_WORKERS"); 
    //PARALLISM
    private static final String CF_TOPOLOGY_WORKERS = customProperties.getProp("CF_TOPOLOGY_WORKERS");
    private static final String CB_TOPOLOGY_WORKERS = customProperties.getProp("CB_TOPOLOGY_WORKERS");
    private final static String TOPO_CF = customProperties.getProp("TOPO_CF");
    private final static String TOPO_CB = customProperties.getProp("TOPO_CB");

    private static CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering();
    private static ContentBased contentBased = new ContentBased();

    public static class RecordHeaderSerializer extends FieldSerializer<RecordHeader> {
        public RecordHeaderSerializer(Kryo kryo) {
            super(kryo, RecordHeader.class);
        }

        @Override
        public void write(Kryo kryo, Output output, RecordHeader object) {
            output.writeString(object.key());
            String value = new String(object.value(), StandardCharsets.UTF_8);
            output.writeString(value);
        }

        @Override
        public RecordHeader read(Kryo kryo, Input input, Class<RecordHeader> type) {
            try {
                String K = input.readString(); 
                byte[] V = input.readString().getBytes();
                return new RecordHeader(K,V);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static Config getCFConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumAckers(Integer.parseInt(NUM_ACK_WORKERS));
        config.setMessageTimeoutSecs(36000);
        config.setNumWorkers(Integer.parseInt(CF_TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        return config;
    }

    private static Config getCBConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumAckers(Integer.parseInt(NUM_ACK_WORKERS));
        config.setMessageTimeoutSecs(36000);
        config.setNumWorkers(Integer.parseInt(CB_TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        config.registerSerialization(RecordHeader.class, RecordHeaderSerializer.class);
        config.registerSerialization(RecordHeaders.class);
        return config;
    }

    private static void createTopology() throws Exception {
        TopologyBuilder colaborativeFilertingTopologyBuilder = collaborativeFiltering.initTopology();
        TopologyBuilder contentBasedTopologyBuilder = contentBased.initTopology();

        Config tpCFConfig = getCFConfig();
        //StormSubmitter.submitTopology(TOPO_CF, tpCFConfig, colaborativeFilertingTopologyBuilder.createTopology());

        Config tpCBConfig = getCBConfig();
        StormSubmitter.submitTopology(TOPO_CB, tpCBConfig, contentBasedTopologyBuilder.createTopology());
    }

    public static void main( String[] args ) throws Exception {
        createTopology();
    }
}
