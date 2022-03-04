package vn.datnguyen.recommender.Topologies;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.Bolt.BoltFactory;
import vn.datnguyen.recommender.Spout.CBSpoutCreator;
import vn.datnguyen.recommender.utils.CustomProperties;

public class ContentBased {
    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //---
    private static final String DISPATCHER_BOLT_THREADS = customProperties.getProp("DISPATCHER_BOLT_THREADS");
    private static final String UPDATE_RING_BOLT_THREADS = customProperties.getProp("UPDATE_RING_BOLT_THREADS");
    //private static final String LOGGER_BOLT_THREADS = customProperties.getProp("LOGGER_BOLT_THREADS");
    //private static final String DUPLICATE_FILTER_BOLT_THREADS = customProperties.getProp("DUPLICATE_FILTER_BOLT_THREADS");
    //STREAM
    private final static String CONTENT_BASED_STREAM = customProperties.getProp("CONTENT_BASED_STREAM");
    private final static String UPDATE_DATA_FROM_CENTRE_STREAM = customProperties.getProp("UPDATE_DATA_FROM_CENTRE_STREAM");
    //IDs
    //--
    private static final String DISPATCHER_BOLT = customProperties.getProp("DISPATCHER_BOLT");
    private static final String UPDATE_RING_BOLT = customProperties.getProp("UPDATE_RING_BOLT");
    private final static String EVENT_FILTERING_BOLT = customProperties.getProp("EVENT_FILTERING_BOLT");
    //
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    //--
    private static final String DISPATCHER_BOLT_TASKS = customProperties.getProp("DISPATCHER_BOLT_TASKS");
    private static final String UPDATE_RING_BOLT_TASKS = customProperties.getProp("UPDATE_RING_BOLT_TASKS");
    //PARALLISM
    private static final String KAFKA_SPOUT_THREAD = customProperties.getProp("KAFKA_SPOUT_THREAD");
    private static final String EVENT_FILTERING_BOLT_THREADS = customProperties.getProp("EVENT_FILTERING_BOLT_THREADS");
    //STREAM
    private final static String EVENTSOURCE_STREAM = customProperties.getProp("EVENTSOURCE_STREAM");
    //IDs
    private final static String KAFKA_SPOUT = customProperties.getProp("KAFKA_SPOUT");
    //--
    //Tasks size 
    private final static String SPOUT_TASKS = customProperties.getProp("SPOUT_TASKS");
    private final static String EVENT_FILTERING_BOLT_TASKS = customProperties.getProp("EVENT_FILTERING_BOLT_TASKS");
    //--
    //private final static String LOGGER_BOLT = customProperties.getProp("LOGGER_BOLT");
    //private final static String DUPLICATE_FILTER_BOLT = customProperties.getProp("DUPLICATE_FILTER_BOLT");
    private static CBSpoutCreator spoutCreator = new CBSpoutCreator();
    private static BoltFactory boltFactory = new BoltFactory();

    public ContentBased () {}

    public TopologyBuilder initTopology() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(KAFKA_SPOUT, spoutCreator.kafkaAvroEventSpout(), Integer.parseInt(KAFKA_SPOUT_THREAD))
            .setNumTasks(Integer.parseInt(SPOUT_TASKS));

        topologyBuilder.setBolt(EVENT_FILTERING_BOLT, boltFactory.createEventFilteringBolt(), Integer.parseInt(EVENT_FILTERING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(EVENT_FILTERING_BOLT_TASKS))
            .shuffleGrouping(KAFKA_SPOUT, EVENTSOURCE_STREAM);
        
        // content based
        topologyBuilder.setBolt(DISPATCHER_BOLT, boltFactory.createDispatcherBolt(), Integer.parseInt(DISPATCHER_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(DISPATCHER_BOLT_TASKS))
            .fieldsGrouping(EVENT_FILTERING_BOLT, CONTENT_BASED_STREAM, new Fields(CENTRE_ID_FIELD));

        topologyBuilder.setBolt(UPDATE_RING_BOLT, boltFactory.createUpdateBoundedRingBolt(), Integer.parseInt(UPDATE_RING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(UPDATE_RING_BOLT_TASKS))
            .fieldsGrouping(DISPATCHER_BOLT, UPDATE_DATA_FROM_CENTRE_STREAM, new Fields(CENTRE_ID_FIELD, RING_ID_FIELD));

        return topologyBuilder;

    }

}
