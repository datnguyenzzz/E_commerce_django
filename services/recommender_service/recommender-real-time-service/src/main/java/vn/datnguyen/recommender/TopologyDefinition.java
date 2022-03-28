package vn.datnguyen.recommender;

import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.Item;
import vn.datnguyen.recommender.AvroClasses.RecommendItemSimilaritesResult;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Topologies.CollaborativeFiltering;
import vn.datnguyen.recommender.Topologies.ContentBasedCommand;
import vn.datnguyen.recommender.Topologies.ContentBasedQuery;
import vn.datnguyen.recommender.utils.CustomProperties;
import vn.datnguyen.recommender.utils.RecordHeaderSerializer;

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
    private static final String CB_COMMAND_TOPOLOGY_WORKERS = customProperties.getProp("CB_COMMAND_TOPOLOGY_WORKERS");
    private static final String CB_QUERY_TOPOLOGY_WORKERS = customProperties.getProp("CB_QUERY_TOPOLOGY_WORKERS");
    private final static String TOPO_CF = customProperties.getProp("TOPO_CF");
    private final static String TOPO_CB_COMMAND = customProperties.getProp("TOPO_CB_COMMAND");
    private final static String TOPO_CB_QUERY = customProperties.getProp("TOPO_CB_QUERY");

    private static CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering();
    private static ContentBasedQuery contentBasedQuery = new ContentBasedQuery();
    private static ContentBasedCommand contentBasedCommand = new ContentBasedCommand();

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

    private static Config getCBQueryConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumAckers(Integer.parseInt(NUM_ACK_WORKERS));
        config.setMessageTimeoutSecs(36000);
        config.setNumWorkers(Integer.parseInt(CB_COMMAND_TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        config.registerSerialization(Item.class);
        config.registerSerialization(RecommendItemSimilaritesResult.class);
        config.registerSerialization(RecordHeader.class, RecordHeaderSerializer.class);
        config.registerSerialization(RecordHeaders.class);
        return config;
    }

    private static Config getCBCommandConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumAckers(Integer.parseInt(NUM_ACK_WORKERS));
        config.setMessageTimeoutSecs(36000);
        config.setNumWorkers(Integer.parseInt(CB_QUERY_TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        config.registerSerialization(RecordHeader.class, RecordHeaderSerializer.class);
        config.registerSerialization(RecordHeaders.class);
        return config;
    }

    private static void createTopology() throws Exception {
        Config tpCFConfig = getCFConfig();
        TopologyBuilder colaborativeFilertingTopologyBuilder = collaborativeFiltering.initTopology();
        //StormSubmitter.submitTopology(TOPO_CF, tpCFConfig, colaborativeFilertingTopologyBuilder.createTopology());

        Config tpCBCommandConfig = getCBCommandConfig();
        TopologyBuilder contentBasedCommandTopologyBuilder = contentBasedCommand.initTopology();
        StormSubmitter.submitTopology(TOPO_CB_COMMAND, tpCBCommandConfig, contentBasedCommandTopologyBuilder.createTopology());

        Config tpCBQueryConfig = getCBQueryConfig();
        TopologyBuilder contentBasedQueryTopologyBuilder = contentBasedQuery.initTopology();
        StormSubmitter.submitTopology(TOPO_CB_QUERY, tpCBQueryConfig, contentBasedQueryTopologyBuilder.createTopology());
    }

    public static void main( String[] args ) throws Exception {
        createTopology();
    }
}
