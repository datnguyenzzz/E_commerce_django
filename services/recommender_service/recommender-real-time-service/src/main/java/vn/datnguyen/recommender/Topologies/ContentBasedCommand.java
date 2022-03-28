package vn.datnguyen.recommender.Topologies;

import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import vn.datnguyen.recommender.Processors.BoltFactory;
import vn.datnguyen.recommender.Processors.SpoutFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class ContentBasedCommand {
    private static final CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM
    private final static String EVENTSOURCE_STREAM_COMMAND = customProperties.getProp("EVENTSOURCE_STREAM_COMMAND");
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
    //TASKS
    private final static String KAFKA_SPOUT_CB_TASKS = customProperties.getProp("SPOUT_TASKS");
    private final static String EVENT_FILTERING_BOLT_TASKS = customProperties.getProp("EVENT_FILTERING_BOLT_TASKS");
    private static final String DISPATCHER_BOLT_TASKS = customProperties.getProp("DISPATCHER_BOLT_TASKS");
    private static final String UPDATE_RING_BOLT_TASKS = customProperties.getProp("UPDATE_RING_BOLT_TASKS");
    //PARALLISM EXECUTORS
    private static final String KAFKA_SPOUT_CB_THREADS = customProperties.getProp("KAFKA_SPOUT_CB_THREADS");
    private static final String EVENT_FILTERING_BOLT_THREADS = customProperties.getProp("EVENT_FILTERING_BOLT_THREADS");
    private static final String DISPATCHER_BOLT_THREADS = customProperties.getProp("DISPATCHER_BOLT_THREADS");
    private static final String UPDATE_RING_BOLT_THREADS = customProperties.getProp("UPDATE_RING_BOLT_THREADS");
    //IDs
    private final static String KAFKA_SPOUT_CB = customProperties.getProp("KAFKA_SPOUT_CB");
    //--
    private static SpoutFactory spoutFactory = new SpoutFactory();
    private static BoltFactory boltFactory = new BoltFactory();

    public ContentBasedCommand () {}

    public TopologyBuilder initTopology() throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout(KAFKA_SPOUT_CB, spoutFactory.createCBSpout(), Integer.parseInt(KAFKA_SPOUT_CB_THREADS))
            .setNumTasks(Integer.parseInt(KAFKA_SPOUT_CB_TASKS));

        topologyBuilder.setBolt(EVENT_FILTERING_BOLT, boltFactory.createEventFilteringBolt(), Integer.parseInt(EVENT_FILTERING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(EVENT_FILTERING_BOLT_TASKS))
            .shuffleGrouping(KAFKA_SPOUT_CB, EVENTSOURCE_STREAM_COMMAND);
        
        // content based update state
        topologyBuilder.setBolt(DISPATCHER_BOLT, boltFactory.createDispatcherBolt(), Integer.parseInt(DISPATCHER_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(DISPATCHER_BOLT_TASKS))
            .fieldsGrouping(EVENT_FILTERING_BOLT, CONTENT_BASED_STREAM, new Fields(CENTRE_ID_FIELD));

        topologyBuilder.setBolt(UPDATE_RING_BOLT, boltFactory.createUpdateBoundedRingBolt(), Integer.parseInt(UPDATE_RING_BOLT_THREADS))
            .setNumTasks(Integer.parseInt(UPDATE_RING_BOLT_TASKS))
            .fieldsGrouping(DISPATCHER_BOLT, UPDATE_DATA_FROM_CENTRE_STREAM, new Fields(CENTRE_ID_FIELD, RING_ID_FIELD));

        return topologyBuilder;

    }

}
