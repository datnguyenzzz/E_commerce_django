package vn.datnguyen.recommender;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Topologies.CollaborativeFiltering;
import vn.datnguyen.recommender.Topologies.ContentBased;
import vn.datnguyen.recommender.utils.CustomProperties;

public class TopologyDefinition {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //PARALLISM
    private static final String TOPOLOGY_WORKERS = customProperties.getProp("TOPOLOGY_WORKERS");
    private final static String TOPO_CF = customProperties.getProp("TOPO_CF");
    private final static String TOPO_CB = customProperties.getProp("TOPO_CB");

    private static CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering();
    private static ContentBased contentBased = new ContentBased();

    private static Config getCFConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setMessageTimeoutSecs(30);
        config.setNumWorkers(Integer.parseInt(TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        return config;
    }

    private static Config getCBConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setMessageTimeoutSecs(30);
        config.setNumWorkers(Integer.parseInt(TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        return config;
    }

    private static void createTopology() throws Exception {
        TopologyBuilder colaborativeFilertingTopologyBuilder = collaborativeFiltering.initTopology();
        TopologyBuilder contentBasedTopologyBuilder = contentBased.initTopology();

        Config tpCFConfig = getCFConfig();
        StormSubmitter.submitTopology(TOPO_CF, tpCFConfig, colaborativeFilertingTopologyBuilder.createTopology());

        Config tpCBConfig = getCBConfig();
        StormSubmitter.submitTopology(TOPO_CB, tpCBConfig, contentBasedTopologyBuilder.createTopology());
    }

    public static void main( String[] args ) throws Exception {
        createTopology();
    }
}
