package vn.datnguyen.recommender;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Bolt.BoltFactory;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Topologies.CollaborativeFiltering;
import vn.datnguyen.recommender.utils.CustomProperties;

public class TopologyDefinition {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //PARALLISM
    private static final String TOPOLOGY_WORKERS = customProperties.getProp("TOPOLOGY_WORKERS");
    //---
    private static final String DISPATCHER_BOLT_THREADS = customProperties.getProp("DISPATCHER_BOLT_THREADS");
    private static final String UPDATE_RING_BOLT_THREADS = customProperties.getProp("UPDATE_RING_BOLT_THREADS");
    //private static final String LOGGER_BOLT_THREADS = customProperties.getProp("LOGGER_BOLT_THREADS");
    //private static final String DUPLICATE_FILTER_BOLT_THREADS = customProperties.getProp("DUPLICATE_FILTER_BOLT_THREADS");
    //STREAM
    private final static String CONTENT_BASED_STREAM = customProperties.getProp("CONTENT_BASED_STREAM");
    private final static String UPDATE_DATA_FROM_CENTRE_STREAM = customProperties.getProp("UPDATE_DATA_FROM_CENTRE_STREAM");
    //IDs
    private final static String WEIGHT_APPLIER_BOLT = customProperties.getProp("WEIGHT_APPLIER_BOLT");
    //--
    private static final String DISPATCHER_BOLT = customProperties.getProp("DISPATCHER_BOLT");
    private static final String UPDATE_RING_BOLT = customProperties.getProp("UPDATE_RING_BOLT");
    //
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    //--
    private static final String DISPATCHER_BOLT_TASKS = customProperties.getProp("DISPATCHER_BOLT_TASKS");
    private static final String UPDATE_RING_BOLT_TASKS = customProperties.getProp("UPDATE_RING_BOLT_TASKS");
    //private final static String LOGGER_BOLT = customProperties.getProp("LOGGER_BOLT");
    //private final static String DUPLICATE_FILTER_BOLT = customProperties.getProp("DUPLICATE_FILTER_BOLT");
    private final static String TOPO_ID = customProperties.getProp("TOPO_ID");

    private static BoltFactory boltFactory = new BoltFactory();
    private static CollaborativeFiltering collaborativeFiltering = new CollaborativeFiltering();

    private static Config getConfig() {
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
        // content based
        colaborativeFilertingTopologyBuilder.setBolt(DISPATCHER_BOLT, boltFactory.createDispatcherBolt(), Integer.parseInt(DISPATCHER_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(DISPATCHER_BOLT_TASKS))
            .fieldsGrouping(WEIGHT_APPLIER_BOLT, CONTENT_BASED_STREAM, new Fields(CENTRE_ID_FIELD));

        colaborativeFilertingTopologyBuilder.setBolt(UPDATE_RING_BOLT, boltFactory.createUpdateBoundedRingBolt(), Integer.parseInt(UPDATE_RING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(UPDATE_RING_BOLT_TASKS))
            .fieldsGrouping(DISPATCHER_BOLT, UPDATE_DATA_FROM_CENTRE_STREAM, new Fields(CENTRE_ID_FIELD, RING_ID_FIELD));

        Config tpConfig = getConfig();
        StormSubmitter.submitTopology(TOPO_ID, tpConfig, colaborativeFilertingTopologyBuilder.createTopology());
    }

    public static void main( String[] args ) throws Exception {
        createTopology();
    }
}
