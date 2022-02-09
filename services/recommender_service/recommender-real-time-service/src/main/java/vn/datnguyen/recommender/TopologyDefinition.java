package vn.datnguyen.recommender;


import org.apache.maven.model.Repository;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Bolt.BoltFactory;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.Spout.SpoutCreator;
import vn.datnguyen.recommender.utils.CustomProperties;

public class TopologyDefinition {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //PARALLISM
    private static final String KAFKA_SPOUT_THREAD = customProperties.getProp("KAFKA_SPOUT_THREAD");
    private static final String TOPOLOGY_WORKERS = customProperties.getProp("TOPOLOGY_WORKERS");
    private static final String LOGGER_BOLT_THREADS = customProperties.getProp("LOGGER_BOLT_THREADS");
    private static final String WEIGHT_APPLIER_BOLT_THREADS = customProperties.getProp("WEIGHT_APPLIER_BOLT_THREADS");
    //private static final String DUPLICATE_FILTER_BOLT_THREADS = customProperties.getProp("DUPLICATE_FILTER_BOLT_THREADS");
    //STREAM
    private final static String EVENTSOURCE_STREAM = customProperties.getProp("EVENTSOURCE_STREAM");
    //IDs
    private final static String KAFKA_SPOUT = customProperties.getProp("KAFKA_SPOUT");
    private final static String WEIGHT_APPLIER_BOLT = customProperties.getProp("WEIGHT_APPLIER_BOLT");
    private final static String LOGGER_BOLT = customProperties.getProp("LOGGER_BOLT");
    //private final static String DUPLICATE_FILTER_BOLT = customProperties.getProp("DUPLICATE_FILTER_BOLT");
    private final static String TOPO_ID = customProperties.getProp("TOPO_ID");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");

    private static SpoutCreator spoutCreator = new SpoutCreator();
    private static BoltFactory boltFactory = new BoltFactory();
    private static RepositoryFactory repositoryFactory;

    private static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(Integer.parseInt(TOPOLOGY_WORKERS));
        config.registerSerialization(AvroEvent.class);
        config.registerSerialization(Event.class);
        return config;
    }

    private static void createTopology() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(KAFKA_SPOUT, spoutCreator.kafkaAvroEventSpout(), Integer.parseInt(KAFKA_SPOUT_THREAD));

        topologyBuilder.setBolt(WEIGHT_APPLIER_BOLT, boltFactory.createWeightApplierBolt(), Integer.parseInt(WEIGHT_APPLIER_BOLT_THREADS))
            .shuffleGrouping(KAFKA_SPOUT, EVENTSOURCE_STREAM);

        topologyBuilder.setBolt(LOGGER_BOLT, boltFactory.creatLoggerBolt(), Integer.parseInt(LOGGER_BOLT_THREADS))
            .shuffleGrouping(WEIGHT_APPLIER_BOLT);

        Config tpConfig = getConfig();
        StormSubmitter.submitTopology(TOPO_ID, tpConfig, topologyBuilder.createTopology());
    }

    public static void main( String[] args ) throws Exception {

        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);

        repositoryFactory = new RepositoryFactory(connector.getSession());

        createTopology();
    }
}
