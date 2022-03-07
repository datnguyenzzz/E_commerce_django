package vn.datnguyen.recommender.Topologies;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.Bolt.BoltFactory;
import vn.datnguyen.recommender.Spout.CBSpoutCreator;
import vn.datnguyen.recommender.utils.CustomProperties;

public class ContentBased {
    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM
    private final static String EVENTSOURCE_STREAM = customProperties.getProp("EVENTSOURCE_STREAM");
    private final static String CONTENT_BASED_STREAM = customProperties.getProp("CONTENT_BASED_STREAM");
    private final static String UPDATE_DATA_FROM_CENTRE_STREAM = customProperties.getProp("UPDATE_DATA_FROM_CENTRE_STREAM");
    private final static String CONTENT_BASED_RECOMMEND_FOR_CLIENT = customProperties.getProp("CONTENT_BASED_RECOMMEND_FOR_CLIENT");
    private final static String INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM = customProperties.getProp("INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM");
    private final static String AGGREGATE_BOUNDED_RINGS_STREAM = customProperties.getProp("AGGREGATE_BOUNDED_RINGS_STREAM");
    private final static String CONTENT_BASED_RECOMMEND_FOR_ITEM = customProperties.getProp("CONTENT_BASED_RECOMMEND_FOR_ITEM");
    //IDs
    //--
    private static final String DISPATCHER_BOLT = customProperties.getProp("DISPATCHER_BOLT");
    private static final String UPDATE_RING_BOLT = customProperties.getProp("UPDATE_RING_BOLT");
    private final static String EVENT_FILTERING_BOLT = customProperties.getProp("EVENT_FILTERING_BOLT");
    private static final String RECOMMEND_FOR_ITEM_BOLT = customProperties.getProp("RECOMMEND_FOR_ITEM_BOLT");
    private static final String RING_AGGREGRATION_BOLT = customProperties.getProp("RING_AGGREGRATION_BOLT");
    //
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String RING_ID_FIELD = customProperties.getProp("RING_ID_FIELD");
    private final static String EVENT_COORD_FIELD = customProperties.getProp("EVENT_COORD_FIELD");
    //TASKS
    private final static String KAFKA_SPOUT_CB_TASKS = customProperties.getProp("SPOUT_TASKS");
    private final static String EVENT_FILTERING_BOLT_TASKS = customProperties.getProp("EVENT_FILTERING_BOLT_TASKS");
    private static final String DISPATCHER_BOLT_TASKS = customProperties.getProp("DISPATCHER_BOLT_TASKS");
    private static final String UPDATE_RING_BOLT_TASKS = customProperties.getProp("UPDATE_RING_BOLT_TASKS");
    private static final String RECOMMEND_FOR_ITEM_BOLT_TASKS = customProperties.getProp("RECOMMEND_FOR_ITEM_BOLT_TASKS");
    private static final String RING_AGGREGRATION_BOLT_TASKS = customProperties.getProp("RING_AGGREGRATION_BOLT_TASKS");
    //PARALLISM EXECUTORS
    private static final String KAFKA_SPOUT_CB_THREADS = customProperties.getProp("KAFKA_SPOUT_CB_THREADS");
    private static final String EVENT_FILTERING_BOLT_THREADS = customProperties.getProp("EVENT_FILTERING_BOLT_THREADS");
    private static final String DISPATCHER_BOLT_THREADS = customProperties.getProp("DISPATCHER_BOLT_THREADS");
    private static final String UPDATE_RING_BOLT_THREADS = customProperties.getProp("UPDATE_RING_BOLT_THREADS");
    private static final String RECOMMEND_FOR_ITEM_BOLT_THREADS = customProperties.getProp("RECOMMEND_FOR_ITEM_BOLT_THREADS");
    private static final String RING_AGGREGRATION_BOLT_THREADS = customProperties.getProp("RING_AGGREGRATION_BOLT_THREADS");
    //IDs
    private final static String KAFKA_SPOUT_CB = customProperties.getProp("KAFKA_SPOUT_CB");
    //--
    private static CBSpoutCreator spoutCreator = new CBSpoutCreator();
    private static BoltFactory boltFactory = new BoltFactory();

    public ContentBased () {}

    public TopologyBuilder initTopology() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(KAFKA_SPOUT_CB, spoutCreator.kafkaAvroEventSpout(), Integer.parseInt(KAFKA_SPOUT_CB_THREADS))
            .setNumTasks(Integer.parseInt(KAFKA_SPOUT_CB_TASKS));

        topologyBuilder.setBolt(EVENT_FILTERING_BOLT, boltFactory.createEventFilteringBolt(), Integer.parseInt(EVENT_FILTERING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(EVENT_FILTERING_BOLT_TASKS))
            .shuffleGrouping(KAFKA_SPOUT_CB, EVENTSOURCE_STREAM);
        
        // content based
        topologyBuilder.setBolt(DISPATCHER_BOLT, boltFactory.createDispatcherBolt(), Integer.parseInt(DISPATCHER_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(DISPATCHER_BOLT_TASKS))
            .fieldsGrouping(EVENT_FILTERING_BOLT, CONTENT_BASED_STREAM, new Fields(CENTRE_ID_FIELD));

        topologyBuilder.setBolt(UPDATE_RING_BOLT, boltFactory.createUpdateBoundedRingBolt(), Integer.parseInt(UPDATE_RING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(UPDATE_RING_BOLT_TASKS))
            .fieldsGrouping(DISPATCHER_BOLT, UPDATE_DATA_FROM_CENTRE_STREAM, new Fields(CENTRE_ID_FIELD, RING_ID_FIELD));

        topologyBuilder.setBolt(RECOMMEND_FOR_ITEM_BOLT, boltFactory.createRecommendForItemContentBased(), Integer.parseInt(RECOMMEND_FOR_ITEM_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(RECOMMEND_FOR_ITEM_BOLT_TASKS))
            .shuffleGrouping(EVENT_FILTERING_BOLT, CONTENT_BASED_RECOMMEND_FOR_ITEM);

        topologyBuilder.setBolt(RING_AGGREGRATION_BOLT,boltFactory.createRingAggregationBolt() , Integer.parseInt(RING_AGGREGRATION_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(RING_AGGREGRATION_BOLT_TASKS))
            .fieldsGrouping(RECOMMEND_FOR_ITEM_BOLT, AGGREGATE_BOUNDED_RINGS_STREAM, new Fields(EVENT_COORD_FIELD));

        return topologyBuilder;

    }

}
