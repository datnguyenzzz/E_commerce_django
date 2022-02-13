package vn.datnguyen.recommender.Bolt;

import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vn.datnguyen.recommender.CassandraConnector;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Models.ItemCount;
import vn.datnguyen.recommender.Repository.ItemCountRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

/**
 * ItemCount[p] = sum(ClienRating[u,p])
 * 
 * R_new[u,p] > R[u,p] -> ItemCount[p] += delta_R
 */
public class ItemCountBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(ItemCountBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String OLD_RATING = customProperties.getProp("OLD_RATING");
    private final static String NEW_ITEM_COUNT = customProperties.getProp("NEW_ITEM_COUNT");
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");

    private RepositoryFactory repositoryFactory;
    private ItemCountRepository itemCountRepository;
    
    private OutputCollector collector;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));

        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY keyspace in **** ItemCountBolt ****");
    }

    private void createTableIfNotExists() {
        SimpleStatement rowCreationStatement = this.itemCountRepository.createRowIfNotExists();
        ResultSet rowCreationResult = this.repositoryFactory.executeStatement(rowCreationStatement, KEYSPACE_FIELD);
        logger.info("*** ItemCountBolt ****: " + "row creation status " + rowCreationResult.all());
    }
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();
        this.itemCountRepository = this.repositoryFactory.getItemCountRepository();
        createTableIfNotExists();
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        int oldRating = (int) input.getValueByField(OLD_RATING);
        int deltaRating = incomeEvent.getWeight() - oldRating;

        logger.info("********* ItemCountBolt **********" + incomeEvent + " with old value = " + oldRating);

        SimpleStatement findOneStatement = this.itemCountRepository.findByItemId(incomeEvent.getItemId());
        ResultSet findOneResult = this.repositoryFactory.executeStatement(findOneStatement, KEYSPACE_FIELD);

        int rowFound = findOneResult.getAvailableWithoutFetching();

        logger.info("********* ItemCountBolt **********" + " rows found size = " + rowFound);

        if (rowFound == 0) {
            ItemCount itemCount = new ItemCount(incomeEvent.getItemId(), incomeEvent.getWeight());
            SimpleStatement insertNewScoreStatement = this.itemCountRepository.insertNewScore(itemCount);
            this.repositoryFactory.executeStatement(insertNewScoreStatement, KEYSPACE_FIELD);
            logger.info("***** ItemCountBolt *******: inserted new score for itemId = " + incomeEvent.getItemId());
            // emit to similarity
            Values values = new Values(incomeEvent, incomeEvent.getWeight());
            collector.emit(values);
        } else {
            int currItemCount = this.itemCountRepository.convertToPojo(findOneResult.one()).getScore();
            int newItemCountScore = currItemCount + deltaRating;

            logger.info("********* ItemCountBolt **********" + " current item count score = " + currItemCount);
            SimpleStatement updateScoreStatement = this.itemCountRepository.updateScore(
                incomeEvent.getItemId(), newItemCountScore);
            
            this.repositoryFactory.executeStatement(updateScoreStatement, KEYSPACE_FIELD);
            logger.info("***** ItemCountBolt *******: updated score for itemId = " + incomeEvent.getItemId() + " with new score = " + newItemCountScore);
            // emit to similarity
            Values values = new Values(incomeEvent, newItemCountScore);
            collector.emit(values);
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_FIELD, NEW_ITEM_COUNT));
    }
}
