package vn.datnguyen.recommender.Topologies;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.Processors.BoltFactory;
import vn.datnguyen.recommender.Processors.SpoutFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class ContentBasedQuery {
    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM
    private final static String EVENTSOURCE_STREAM_QUERY = customProperties.getProp("EVENTSOURCE_STREAM_QUERY");
    //private final static String CONTENT_BASED_RECOMMEND_FOR_CLIENT = customProperties.getProp("CONTENT_BASED_RECOMMEND_FOR_CLIENT");
    private final static String INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM = customProperties.getProp("INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM");
    private final static String AGGREGATE_BOUNDED_RINGS_STREAM = customProperties.getProp("AGGREGATE_BOUNDED_RINGS_STREAM");
    private final static String INDIVIDUAL_KNN_ALGORITHM_STREAM = customProperties.getProp("INDIVIDUAL_KNN_ALGORITHM_STREAM");
    private final static String CONTENT_BASED_RECOMMEND_FOR_ITEM = customProperties.getProp("CONTENT_BASED_RECOMMEND_FOR_ITEM");
    //IDs
    //--
    private static final String RECOMMEND_FOR_ITEM_BOLT = customProperties.getProp("RECOMMEND_FOR_ITEM_BOLT");
    private static final String RING_AGGREGRATION_BOLT = customProperties.getProp("RING_AGGREGRATION_BOLT");
    private static final String KNN_BOLT = customProperties.getProp("KNN_BOLT");
    private static final String KAFKA_PRODUCER_BOLT = customProperties.getProp("KAFKA_PRODUCER_BOLT");
    private static final String EVENT_FILTERING_FOR_QUERY_BOLT = customProperties.getProp("EVENT_FILTERING_FOR_QUERY_BOLT");
    //
    private final static String EVENT_ID_FIELD = customProperties.getProp("EVENT_ID_FIELD");
    //TASKS
    private final static String KAFKA_SPOUT_CB_TASKS = customProperties.getProp("SPOUT_TASKS");
    private final static String KAFKA_SPOUT_QUERY_RECOMMEND = customProperties.getProp("KAFKA_SPOUT_QUERY_RECOMMEND");
    private static final String RECOMMEND_FOR_ITEM_BOLT_TASKS = customProperties.getProp("RECOMMEND_FOR_ITEM_BOLT_TASKS");
    private static final String RING_AGGREGRATION_BOLT_TASKS = customProperties.getProp("RING_AGGREGRATION_BOLT_TASKS");
    private static final String KNN_BOLT_TASKS = customProperties.getProp("KNN_BOLT_TASKS");
    private static final String KAFKA_PRODUCER_BOLT_TASKS = customProperties.getProp("KAFKA_PRODUCER_BOLT_TASKS");
    private static final String EVENT_FILTERING_FOR_QUERY_BOLT_TASKS = customProperties.getProp("EVENT_FILTERING_FOR_QUERY_BOLT_TASKS");
    //PARALLISM EXECUTORS
    private static final String KAFKA_SPOUT_CB_THREADS = customProperties.getProp("KAFKA_SPOUT_CB_THREADS");
    private static final String RECOMMEND_FOR_ITEM_BOLT_THREADS = customProperties.getProp("RECOMMEND_FOR_ITEM_BOLT_THREADS");
    private static final String RING_AGGREGRATION_BOLT_THREADS = customProperties.getProp("RING_AGGREGRATION_BOLT_THREADS");
    private static final String KNN_BOLT_THREADS = customProperties.getProp("KNN_BOLT_THREADS");
    private static final String KAFKA_PRODUCER_BOLT_THREADS = customProperties.getProp("KAFKA_PRODUCER_BOLT_THREADS");
    private static final String EVENT_FILTERING_FOR_QUERY_BOLT_THREADS = customProperties.getProp("EVENT_FILTERING_FOR_QUERY_BOLT_THREADS");
    //IDs
    //--
    private static SpoutFactory spoutFactory = new SpoutFactory();
    private static BoltFactory boltFactory = new BoltFactory();

    public ContentBasedQuery () {}

    public TopologyBuilder initTopology() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(KAFKA_SPOUT_QUERY_RECOMMEND, spoutFactory.createQueryRecommendSpout(), Integer.parseInt(KAFKA_SPOUT_CB_THREADS))
            .setNumTasks(Integer.parseInt(KAFKA_SPOUT_CB_TASKS));

        topologyBuilder.setBolt(KAFKA_PRODUCER_BOLT, boltFactory.createKafkaProducerBolt(), Integer.parseInt(KAFKA_PRODUCER_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(KAFKA_PRODUCER_BOLT_TASKS))
            .shuffleGrouping(RING_AGGREGRATION_BOLT);

        topologyBuilder.setBolt(EVENT_FILTERING_FOR_QUERY_BOLT, boltFactory.createEventFilteringForQueryBolt(), Integer.parseInt(EVENT_FILTERING_FOR_QUERY_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(EVENT_FILTERING_FOR_QUERY_BOLT_TASKS))
            .shuffleGrouping(KAFKA_SPOUT_QUERY_RECOMMEND, EVENTSOURCE_STREAM_QUERY);
        
        // query
        topologyBuilder.setBolt(RECOMMEND_FOR_ITEM_BOLT, boltFactory.createRecommendForItemContentBased(), Integer.parseInt(RECOMMEND_FOR_ITEM_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(RECOMMEND_FOR_ITEM_BOLT_TASKS))
            .shuffleGrouping(EVENT_FILTERING_FOR_QUERY_BOLT, CONTENT_BASED_RECOMMEND_FOR_ITEM);
        
        topologyBuilder.setBolt(KNN_BOLT, boltFactory.createKnnBolt(), Integer.parseInt(KNN_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(KNN_BOLT_TASKS))
            .shuffleGrouping(RECOMMEND_FOR_ITEM_BOLT, INDIVIDUAL_BOUNDED_RING_HANDLER_STREAM);

        topologyBuilder.setBolt(RING_AGGREGRATION_BOLT,boltFactory.createRingAggregationBolt() , Integer.parseInt(RING_AGGREGRATION_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(RING_AGGREGRATION_BOLT_TASKS))
            .fieldsGrouping(RECOMMEND_FOR_ITEM_BOLT, AGGREGATE_BOUNDED_RINGS_STREAM, new Fields(EVENT_ID_FIELD))
            .fieldsGrouping(KNN_BOLT, INDIVIDUAL_KNN_ALGORITHM_STREAM, new Fields(EVENT_ID_FIELD));
        
        return topologyBuilder;

    }

}
