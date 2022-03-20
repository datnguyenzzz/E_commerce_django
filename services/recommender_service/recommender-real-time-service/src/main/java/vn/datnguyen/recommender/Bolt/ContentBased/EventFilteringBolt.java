package vn.datnguyen.recommender.Bolt.ContentBased;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
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
import vn.datnguyen.recommender.AvroClasses.AvroDeleteItem;
import vn.datnguyen.recommender.AvroClasses.AvroEvent;
import vn.datnguyen.recommender.AvroClasses.AvroRecommendForItem;
import vn.datnguyen.recommender.AvroClasses.AvroRecommendForUser;
import vn.datnguyen.recommender.Bolt.LoggerBolt;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.IndexesCoordRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.AvroEventScheme;
import vn.datnguyen.recommender.utils.CustomProperties;

public class EventFilteringBolt extends BaseRichBolt {
    
    public static Charset charset = Charset.forName("UTF-8");
    public static CharsetEncoder encoder = charset.newEncoder();
    public static CharsetDecoder decoder = charset.newDecoder();

    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //STREAM 
    private final static String CONTENT_BASED_STREAM = customProperties.getProp("CONTENT_BASED_STREAM");
    //private final static String CONTENT_BASED_RECOMMEND_FOR_CLIENT = customProperties.getProp("CONTENT_BASED_RECOMMEND_FOR_CLIENT");
    private final static String CONTENT_BASED_RECOMMEND_FOR_ITEM = customProperties.getProp("CONTENT_BASED_RECOMMEND_FOR_ITEM");
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String CENTRE_ID_FIELD = customProperties.getProp("CENTRE_ID_FIELD");
    private final static String CENTRE_COORD_STRING_LIST = customProperties.getProp("CENTRE_COORD_STRING_LIST");
    private final static String KAFKA_MESSAGE_HEADER_FIELD = customProperties.getProp("KAFKA_MESSAGE_HEADER_FIELD");
    //INCOME EVENT
    private final static String avroAddItemEvent = customProperties.getProp("avroAddItemEvent");
    private final static String avroDeleteItemEvent = customProperties.getProp("avroDeleteItemEvent");
    private final static String avroRecommendForUserEvent = customProperties.getProp("avroRecommendForUserEvent");
    private final static String avroRecommendForItemEvent = customProperties.getProp("avroRecommendForItemEvent");
    //WEIGHT VALUEs
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    //
    private final static String CENTRE_ID = "centre_id";
    private final static String CENTRE_COORD = "centre_coord";

    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
    private OutputCollector collector;
    private AvroEventScheme avroEventScheme = new AvroEventScheme();
    private String eventId, timestamp, eventType, clientId, itemId;
    private int weight, limit;
    private List<Integer> coord = null;

    private RepositoryFactory repositoryFactory;
    private IndexesCoordRepository indexesCoordRepository;
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();
        this.indexesCoordRepository = repositoryFactory.getIndexesCoordRepository();
        createIndexCoordTable();

        //initial Centre coord 
        //they're suppose to be K-cluster of training dataset
        //testedCentreCoord();
        //
        prepareCentreList();
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

        if (eventType.equals(avroAddItemEvent)) {
            AvroAddItem payload = (AvroAddItem) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = 0;

            this.coord = new ArrayList<Integer>();
            this.coord.add(payload.getProperties1());
            this.coord.add(payload.getProperties2());
            this.coord.add(payload.getProperties3());
        } 
        else if (eventType.equals(avroDeleteItemEvent)) {
            AvroDeleteItem payload = (AvroDeleteItem) event.getData();
            this.clientId = payload.getClientId();
            this.itemId = payload.getItemId();
            this.weight = 0;
        } 
        else if (eventType.equals(avroRecommendForItemEvent)) {
            //do somthing
            AvroRecommendForItem payload = (AvroRecommendForItem) event.getData();
            this.itemId = payload.getItemId();
            this.limit = payload.getLimit();
            
            this.coord = new ArrayList<Integer>();
            this.coord.add(payload.getProperties1());
            this.coord.add(payload.getProperties2());
            this.coord.add(payload.getProperties3());
        }
        else if (eventType.equals(avroRecommendForUserEvent)) {
            //do something
            AvroRecommendForUser payload = (AvroRecommendForUser) event.getData();
            this.clientId = payload.getClientId();
            this.limit = payload.getLimit();
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
    
    /*
    private void testedCentreCoord() {

        List<Integer> centreCoord = new ArrayList<Integer>();
        centreCoord.add(0); centreCoord.add(0); centreCoord.add(0);
        int rowId = 0;
        SimpleStatement initCoord = this.indexesCoordRepository.insertNewIndex(rowId++, centreCoord);
        this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);

        centreCoord.set(0, 5); 
        initCoord = this.indexesCoordRepository.insertNewIndex(rowId++, centreCoord);
        this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);

        centreCoord.set(0, -5); 
        initCoord = this.indexesCoordRepository.insertNewIndex(rowId++, centreCoord);
        this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);
    }*/

    private List<List<Integer> > extractData(String dataString) {
        String data = dataString.replace("[", "").replace("]","");
        List<List<Integer> > result = new ArrayList<>();

        List<String> splitedCoord = Arrays.asList(data.split(","));

        for (String coord: splitedCoord) {
            List<String> tmp = Arrays.asList(coord.split(" ")); 
            List<Integer> itmp = new ArrayList<>();
            for (String x: tmp) {
                itmp.add(Integer.parseInt(x));
            }
            result.add(itmp);
        }

        return result;
    }

    private void prepareCentreList() {
        List<List<Integer> > centreList = extractData(CENTRE_COORD_STRING_LIST);

        SimpleStatement initCoord;
        for (int i=0; i<centreList.size(); i++) {
            int row = i;
            initCoord = this.indexesCoordRepository.insertNewIndex(row, centreList.get(row));
            this.repositoryFactory.executeStatement(initCoord, KEYSPACE_FIELD);
        }

    }

    private long distance(List<Integer> a, List<Integer> b) {
        long s = 0; 
        for (int i = 0; i<a.size(); i++) {
            s += (a.get(i) - b.get(i)) * (a.get(i) - b.get(i));
        }

        return s;
    }

    private int findCentreId(List<Integer> itemProp) {
        SimpleStatement findAllCentreStatement = this.indexesCoordRepository.selectAllCentre();
        List<Row> findAllCentre = this.repositoryFactory.executeStatement(findAllCentreStatement, KEYSPACE_FIELD).all();

        long minDist = Long.MAX_VALUE;
        int centreId=0;

        for (Row r: findAllCentre) {
            int id = (int) this.repositoryFactory.getFromRow(r, CENTRE_ID);
            List<Integer> centreCoord = this.repositoryFactory.getListIntegerFromRow(r, CENTRE_COORD);

            if (minDist > distance(centreCoord, itemProp)) {
                minDist = distance(centreCoord, itemProp); 
                centreId = id;
            }

        }
        return centreId;
    }

    
    private Map<String, String> readMessageHeader(Tuple input) {
        Map<String, String> headerMap = new HashMap<>();

        RecordHeaders messageHeaders = (RecordHeaders) input.getValueByField(KAFKA_MESSAGE_HEADER_FIELD);
        Iterator<Header> headerIterater = messageHeaders.iterator();
        while (headerIterater.hasNext()) {
            Header header = (Header) headerIterater.next();
            String headerKey = header.key();
            byte[] headerBytes = header.value();
            String headerValue = new String(headerBytes);
            headerMap.put(headerKey, headerValue);
        }
        return headerMap;
    }
    
    @Override
    public void execute(Tuple tuple) {
        String avroEventStr = (String) tuple.getValueByField(EVENT_FIELD);
        AvroEvent event = (AvroEvent) avroEventScheme.deserialize(str_to_bb(avroEventStr)).get(0);
        logger.info("********* EventFilteringBolt **********" + event);

        Map<String, String> messageHeader = readMessageHeader(tuple);
        logger.info("********* EventFilteringBolt **********: Message header = " + messageHeader);

        Tuple anchor = tuple;

        if (event.getEventId() != null && event.getEventId() != "") {
            applyWeight(event);

            Event ouputEvent = new Event(this.eventId, this.timestamp, this.eventType, this.clientId, this.itemId, this.weight, this.limit, this.coord);

            if (this.eventType.equals(avroAddItemEvent) || this.eventType.equals(avroDeleteItemEvent)) {
                int centreId = findCentreId(this.coord);
                collector.emit(CONTENT_BASED_STREAM, anchor, new Values(ouputEvent,centreId));
            } 
            else if (this.eventType.equals(avroRecommendForItemEvent)) {
                collector.emit(CONTENT_BASED_RECOMMEND_FOR_ITEM, anchor, new Values(ouputEvent));
            }
            else if (this.eventType.equals(avroRecommendForUserEvent)) {
                //collector.emit(CONTENT_BASED_RECOMMEND_FOR_CLIENT, new Values(ouputEvent));
                //collector.ack(tuple);
            }
            else {
                //Values values = new Values(ouputEvent, this.itemId);
                //collector.emit(ITEM_BASED_STREAM, values);
                //do nothing
            }
            collector.ack(tuple);
        } else {
            collector.fail(tuple);
        }
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(CONTENT_BASED_STREAM, new Fields(EVENT_FIELD, CENTRE_ID_FIELD));
        declarer.declareStream(CONTENT_BASED_RECOMMEND_FOR_ITEM, new Fields(EVENT_FIELD));
        //declarer.declareStream(CONTENT_BASED_RECOMMEND_FOR_CLIENT, new Fields(EVENT_FIELD));
    }
}
