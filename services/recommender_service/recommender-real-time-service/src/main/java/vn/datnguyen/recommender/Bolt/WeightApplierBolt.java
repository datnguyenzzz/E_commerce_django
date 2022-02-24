package vn.datnguyen.recommender.Bolt;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.FailedException;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.CassandraConnector;
import vn.datnguyen.recommender.AvroClasses.AvroAddItem;
import vn.datnguyen.recommender.AvroClasses.AvroAddToCartBehavior;
import vn.datnguyen.recommender.AvroClasses.AvroBuyBehavior;
import vn.datnguyen.recommender.AvroClasses.AvroDeleteItem;
import vn.datnguyen.recommender.AvroClasses.AvroDeleteRating;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroPublishRating;
import vn.datnguyen.recommender.AvroClasses.AvroQueryRating;
import vn.datnguyen.recommender.AvroClasses.AvroUpdateRating;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.IndexesCoordRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.AvroEventScheme;
import vn.datnguyen.recommender.utils.CustomProperties;

public class WeightApplierBolt extends BaseRichBolt {
    
    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();

    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM 
    private final static String ITEM_BASED_STREAM = customProperties.getProp("ITEM_BASED_STREAM");
    private final static String CONTENT_BASED_STREAM = customProperties.getProp("CONTENT_BASED_STREAM");
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String ITEM_ID_FIELD = customProperties.getProp("ITEM_ID_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    //INCOME EVENT
    private final static String avroPublishRatingEvent = customProperties.getProp("avroPublishRatingEvent");
    private final static String avroUpdateRatingEvent = customProperties.getProp("avroUpdateRatingEvent");
    private final static String avroDeleteRatingEvent = customProperties.getProp("avroDeleteRatingEvent");
    private final static String avroQueryRatingEvent = customProperties.getProp("avroQueryRatingEvent");
    private final static String avroBuyBehaviorEvent = customProperties.getProp("avroBuyBehaviorEvent");
    private final static String avroAddToCartBehaviorEvent = customProperties.getProp("avroAddToCartBehaviorEvent");
    private final static String avroAddItemEvent = customProperties.getProp("avroAddItemEvent");
    private final static String avroDeleteItemEvent = customProperties.getProp("avroDeleteItemEvent");
    //WEIGHT VALUEs
    private final static String DELETE_RATING_EVENT_WEIGHT = customProperties.getProp("DELETE_RATING_EVENT_WEIGHT");
    private final static String QUERY_RATING_EVENT_WEIGHT = customProperties.getProp("QUERY_RATING_EVENT_WEIGHT");
    private final static String BUY_EVENT_WEIGHT = customProperties.getProp("BUY_EVENT_WEIGHT");
    private final static String ADD_TO_CART_WEIGHT = customProperties.getProp("ADD_TO_CART_WEIGHT");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");

    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private OutputCollector collector;
    private AvroEventScheme avroEventScheme = new AvroEventScheme();
    private String eventId, timestamp, eventType, clientId, itemId;
    private int weight;
    private List<Integer> coord;

    private RepositoryFactory repositoryFactory;
    private IndexesCoordRepository indexesCoordRepository;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    private static ByteBuffer str_to_bb(String msg){
        try{
          return encoder.encode(CharBuffer.wrap(msg));
        }catch(Exception e){e.printStackTrace();}
        return null;
      }

    private void applyWeight(AvroEvent event) {
        this.eventId = event.getEventId();
        this.eventType = event.getEventType();
        this.timestamp = event.getTimestamp();

        if (eventType.equals(avroPublishRatingEvent)) {
            AvroPublishRating payload = (AvroPublishRating) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = payload.getScore();
        }
        else if (eventType.equals(avroUpdateRatingEvent)) {
            AvroUpdateRating payload = (AvroUpdateRating) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = payload.getScore();
        }
        else if (eventType.equals(avroDeleteRatingEvent)) {
            AvroDeleteRating payload = (AvroDeleteRating) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = Integer.parseInt(DELETE_RATING_EVENT_WEIGHT);
        }
        else if (eventType.equals(avroQueryRatingEvent)) {
            AvroQueryRating payload = (AvroQueryRating) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = Integer.parseInt(QUERY_RATING_EVENT_WEIGHT);
        }
        else if (eventType.equals(avroBuyBehaviorEvent)) {
            AvroBuyBehavior payload = (AvroBuyBehavior) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = Integer.parseInt(BUY_EVENT_WEIGHT);
        }
        else if (eventType.equals(avroAddToCartBehaviorEvent)) {
            AvroAddToCartBehavior payload = (AvroAddToCartBehavior) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = Integer.parseInt(ADD_TO_CART_WEIGHT);
        }
        else if (eventType.equals(avroAddItemEvent)) {
            AvroAddItem payload = (AvroAddItem) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();

            this.coord = new ArrayList<Integer>();
            this.coord.add(payload.getProperties1());
            this.coord.add(payload.getProperties2());
            this.coord.add(payload.getProperties3());
        } 
        else if (eventType.equals(avroDeleteItemEvent)) {
            AvroDeleteItem payload = (AvroDeleteItem) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            
            this.coord = new ArrayList<Integer>();
            this.coord.add(payload.getProperties1());
            this.coord.add(payload.getProperties2());
            this.coord.add(payload.getProperties3());
        } 
        else {
            logger.error("NOT EXIST EVENT");
            throw new FailedException("NOT EXIST EVENT");
        }
    }

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
    }

    private void createIndexCoordTable() {
        SimpleStatement createIndexesCoordStatement = indexesCoordRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(createIndexesCoordStatement, KEYSPACE_FIELD);
    }
   
    private void testedCentreCoord() {
        /**
         * Tested Centre coord
         */
        List<Integer> coord = new ArrayList<Integer>();
        coord.add(0); coord.add(0); coord.add(0);
        SimpleStatement initCoord = this.indexesCoordRepository.insertNewIndex(0, coord);
        this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);

        coord.set(0, 10); 
        initCoord = this.indexesCoordRepository.insertNewIndex(0, coord);
        this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);

        coord.set(0, -10); 
        initCoord = this.indexesCoordRepository.insertNewIndex(0, coord);
        this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);
    }

    private int findCentreId(List<Integer> itemProp) {
        return 0;
    }
    
    @Override
    public void execute(Tuple tuple) {
        String avroEventStr = (String) tuple.getValueByField(EVENT_FIELD);
        AvroEvent event = (AvroEvent) avroEventScheme.deserialize(str_to_bb(avroEventStr)).get(0);
        logger.info("********* APPLY WEIGHT BOLT **********" + event);

        if (event.getEventId() != null && event.getEventId() != "") {
            applyWeight(event);

            Event ouputEvent = new Event(this.eventId, this.timestamp, this.eventType, this.clientId, this.itemId, this.weight);

            if (this.eventType.equals(avroAddItemEvent) || this.eventType.equals(avroDeleteItemEvent)) {
                launchCassandraKeyspace();
                this.indexesCoordRepository = repositoryFactory.getIndexesCoordRepository();
                createIndexCoordTable();

                //tested centre cooord 
                testedCentreCoord();

                int centreId = findCentreId(this.coord);
                Values values = new Values(ouputEvent, null, centreId);
                collector.emit(CONTENT_BASED_STREAM, values);
            } else {
                Values values = new Values(ouputEvent, this.itemId, null);
                collector.emit(ITEM_BASED_STREAM, values);
            }
        }

        collector.ack(tuple);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_FIELD, ITEM_ID_FIELD, CENTRE_ID_FIELD));
    }
}
