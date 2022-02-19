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
import vn.datnguyen.recommender.Models.ClientRating;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Repository.ClientRatingRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.utils.CustomProperties;

public class NewRecordBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(NewRecordBolt.class);
    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String CLIENT_ID_FIELD = customProperties.getProp("CLIENT_ID_FIELD");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");
    
    private OutputCollector collector;
    private RepositoryFactory repositoryFactory;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY keyspace in **** NewRecordBolt ****");
    }
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
        launchCassandraKeyspace();
    }

    private void initClientRatingTable(String clientId, String itemId) {
        ClientRatingRepository clientRatingRepository = this.repositoryFactory.getClientRatingRepository();
        //Table Creation
        SimpleStatement rowCreationStatement = clientRatingRepository.createRowIfNotExists();
        ResultSet result = this.repositoryFactory.executeStatement(rowCreationStatement, KEYSPACE_FIELD);
        logger.info("*** NewRecordBolt ****: ClientRating " + "row creation status " + result.all());

        SimpleStatement indexCreationStatement = clientRatingRepository.createIndexOnItemId();
        result = this.repositoryFactory.executeStatement(indexCreationStatement, KEYSPACE_FIELD);
        logger.info("*** NewRecordBolt ****: ClientRating " + "index creation status " + result.all());
        // Rows init
        SimpleStatement findOneStatement = clientRatingRepository.findByClientIdAndItemId(
            clientId, itemId);
        ResultSet findOneResult = this.repositoryFactory.executeStatement(findOneStatement, KEYSPACE_FIELD);
        int rowFound = findOneResult.getAvailableWithoutFetching();

        if (rowFound == 0) {
            ClientRating clientRating = new ClientRating(clientId, itemId, 0);
            SimpleStatement insertNewStatement = clientRatingRepository.insertClientRating(clientRating);
            
            this.repositoryFactory.executeStatement(insertNewStatement, KEYSPACE_FIELD);
            
            logger.info("******* ClientRatingBolt ******** Insert new client rating: ");
        }
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String clientId = incomeEvent.getClientId();
        String itemId = incomeEvent.getItemId();

        logger.info("********* NewRecordBolt **********" + incomeEvent);

        initClientRatingTable(clientId, itemId);

        Values values = new Values(incomeEvent, clientId);
        collector.emit(values);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_FIELD, CLIENT_ID_FIELD));
    }
}
