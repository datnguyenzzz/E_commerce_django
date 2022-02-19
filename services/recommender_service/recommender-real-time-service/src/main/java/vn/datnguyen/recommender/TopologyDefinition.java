package vn.datnguyen.recommender;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.Bolt.BoltFactory;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Spout.SpoutCreator;
import vn.datnguyen.recommender.utils.CustomProperties;

public class TopologyDefinition {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //PARALLISM
    private static final String KAFKA_SPOUT_THREAD = customProperties.getProp("KAFKA_SPOUT_THREAD");
    private static final String TOPOLOGY_WORKERS = customProperties.getProp("TOPOLOGY_WORKERS");
    private static final String CLIENT_RATING_BOLT_THREADS = customProperties.getProp("CLIENT_RATING_BOLT_THREADS");
    private final static String NEW_RECORD_BOLT_THREADS = customProperties.getProp("NEW_RECORD_BOLT_THREADS");
    private static final String WEIGHT_APPLIER_BOLT_THREADS = customProperties.getProp("WEIGHT_APPLIER_BOLT_THREADS");
    private static final String ITEM_COUNT_THREADS = customProperties.getProp("ITEM_COUNT_THREADS");
    private static final String CO_RATING_BOLT_THREADS = customProperties.getProp("CO_RATING_BOLT_THREADS");
    private static final String PAIR_COUNT_BOLT_THREADS = customProperties.getProp("PAIR_COUNT_BOLT_THREADS");
    private static final String SIMILARITIES_BOLT_THREADS = customProperties.getProp("SIMILARITIES_BOLT_THREADS");
    //private static final String LOGGER_BOLT_THREADS = customProperties.getProp("LOGGER_BOLT_THREADS");
    //private static final String DUPLICATE_FILTER_BOLT_THREADS = customProperties.getProp("DUPLICATE_FILTER_BOLT_THREADS");
    //STREAM
    private final static String EVENTSOURCE_STREAM = customProperties.getProp("EVENTSOURCE_STREAM");
    //IDs
    private final static String KAFKA_SPOUT = customProperties.getProp("KAFKA_SPOUT");
    private final static String WEIGHT_APPLIER_BOLT = customProperties.getProp("WEIGHT_APPLIER_BOLT");
    private final static String NEW_RECORD_BOLT = customProperties.getProp("NEW_RECORD_BOLT");
    private final static String CLIENT_RATING_BOLT = customProperties.getProp("CLIENT_RATING_BOLT");
    private static final String ITEM_COUNT_BOLT = customProperties.getProp("ITEM_COUNT_BOLT");
    private static final String CO_RATING_BOLT = customProperties.getProp("CO_RATING_BOLT");
    private static final String PAIR_COUNT_BOLT = customProperties.getProp("PAIR_COUNT_BOLT");
    private static final String SIMILARITIES_BOLT = customProperties.getProp("SIMILARITIES_BOLT");
    //
    private final static String CLIENT_ID_FIELD = customProperties.getProp("CLIENT_ID_FIELD");
    private final static String ITEM_1_ID_FIELD = customProperties.getProp("ITEM_1_ID_FIELD");
    private final static String ITEM_2_ID_FIELD = customProperties.getProp("ITEM_2_ID_FIELD");
    private final static String ITEM_ID_FIELD = customProperties.getProp("ITEM_ID_FIELD");
    //Tasks size 
    private final static String SPOUT_TASKS = customProperties.getProp("SPOUT_TASKS");
    private final static String WEIGHT_APPLIER_BOLT_TASKS = customProperties.getProp("WEIGHT_APPLIER_BOLT_TASKS");
    private final static String NEW_RECORD_BOLT_TASKS = customProperties.getProp("NEW_RECORD_BOLT_TASKS");
    private final static String CLIENT_RATING_BOLT_TASKS = customProperties.getProp("CLIENT_RATING_BOLT_TASKS");
    private final static String ITEM_COUNT_BOLT_TASKS = customProperties.getProp("ITEM_COUNT_BOLT_TASKS");
    private final static String CO_RATING_BOLT_TASKS = customProperties.getProp("CO_RATING_BOLT_TASKS");
    private final static String PAIR_COUNT_BOLT_TASKS = customProperties.getProp("PAIR_COUNT_BOLT_TASKS");
    private final static String SIMILARITIES_BOLT_TASKS = customProperties.getProp("SIMILARITIES_BOLT_TASKS");
    //private final static String LOGGER_BOLT = customProperties.getProp("LOGGER_BOLT");
    //private final static String DUPLICATE_FILTER_BOLT = customProperties.getProp("DUPLICATE_FILTER_BOLT");
    private final static String TOPO_ID = customProperties.getProp("TOPO_ID");

    private static SpoutCreator spoutCreator = new SpoutCreator();
    private static BoltFactory boltFactory = new BoltFactory();

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

        topologyBuilder.setSpout(KAFKA_SPOUT, spoutCreator.kafkaAvroEventSpout(), Integer.parseInt(KAFKA_SPOUT_THREAD))
            .setNumTasks(Integer.parseInt(SPOUT_TASKS));

        topologyBuilder.setBolt(WEIGHT_APPLIER_BOLT, boltFactory.createWeightApplierBolt(), Integer.parseInt(WEIGHT_APPLIER_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(WEIGHT_APPLIER_BOLT_TASKS))
            .shuffleGrouping(KAFKA_SPOUT, EVENTSOURCE_STREAM);
        
        topologyBuilder.setBolt(NEW_RECORD_BOLT, boltFactory.createNewRecordBolt(), Integer.parseInt(NEW_RECORD_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(NEW_RECORD_BOLT_TASKS))
            .fieldsGrouping(WEIGHT_APPLIER_BOLT, new Fields(ITEM_ID_FIELD));

        topologyBuilder.setBolt(CLIENT_RATING_BOLT, boltFactory.createClientRatingBolt(), Integer.parseInt(CLIENT_RATING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(CLIENT_RATING_BOLT_TASKS))
            .fieldsGrouping(NEW_RECORD_BOLT, new Fields(CLIENT_ID_FIELD));

        topologyBuilder.setBolt(ITEM_COUNT_BOLT, boltFactory.createItemCountBolt(), Integer.parseInt(ITEM_COUNT_THREADS))
            .setNumTasks(Integer.parseInt(ITEM_COUNT_BOLT_TASKS))
            .fieldsGrouping(CLIENT_RATING_BOLT, new Fields(ITEM_ID_FIELD));

        topologyBuilder.setBolt(CO_RATING_BOLT, boltFactory.createCoRatingBolt(), Integer.parseInt(CO_RATING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(CO_RATING_BOLT_TASKS))
            .fieldsGrouping(CLIENT_RATING_BOLT, new Fields(ITEM_ID_FIELD));

        topologyBuilder.setBolt(PAIR_COUNT_BOLT, boltFactory.createPairCountBolt(), Integer.parseInt(PAIR_COUNT_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(PAIR_COUNT_BOLT_TASKS))
            .fieldsGrouping(CO_RATING_BOLT, new Fields(ITEM_1_ID_FIELD, ITEM_2_ID_FIELD));

        topologyBuilder.setBolt(SIMILARITIES_BOLT, boltFactory.createSimilaritiesBolt(), Integer.parseInt(SIMILARITIES_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(SIMILARITIES_BOLT_TASKS))
            .fieldsGrouping(PAIR_COUNT_BOLT, new Fields(ITEM_1_ID_FIELD, ITEM_2_ID_FIELD))
            .fieldsGrouping(ITEM_COUNT_BOLT, new Fields(ITEM_ID_FIELD));

        Config tpConfig = getConfig();
        StormSubmitter.submitTopology(TOPO_ID, tpConfig, topologyBuilder.createTopology());
    }

    public static void main( String[] args ) throws Exception {
        createTopology();
    }
}
