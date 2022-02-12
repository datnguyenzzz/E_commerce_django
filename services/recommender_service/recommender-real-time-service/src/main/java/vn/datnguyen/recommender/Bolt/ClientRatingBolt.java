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
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.Repository.ClientRatingRepository;
import vn.datnguyen.recommender.utils.CustomProperties;

public class ClientRatingBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(ClientRatingBolt.class);

    private final static CustomProperties customProperties = CustomProperties.getInstance();
    //VALUE FIELDS
    private final static String DELTA_RATING = customProperties.getProp("DELTA_RATING");
    private final static String EVENT_FIELD = customProperties.getProp("EVENT_FIELD");
    private final static String KEYSPACE_FIELD = customProperties.getProp("KEYSPACE_FIELD");
    private final static String NUM_NODE_REPLICAS_FIELD = customProperties.getProp("NUM_NODE_REPLICAS_FIELD");
    //CASSANDRA PROPS
    private final static String CASS_NODE = customProperties.getProp("CASS_NODE");
    private final static String CASS_PORT = customProperties.getProp("CASS_PORT");
    private final static String CASS_DATA_CENTER = customProperties.getProp("CASS_DATA_CENTER");

    private RepositoryFactory repositoryFactory;
    private ClientRatingRepository clientRatingRepository;
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

    public void createTableIfNotExists() {
        SimpleStatement rowCreationStatement = this.clientRatingRepository.createRowIfNotExists();
        ResultSet result = this.repositoryFactory.executeStatement(rowCreationStatement, KEYSPACE_FIELD);
        logger.info("*** ClientRatingBolt ****: " + "row creation status " + result.all());

        SimpleStatement indexCreationStatement = this.clientRatingRepository.createIndexOnItemId();
        result = this.repositoryFactory.executeStatement(indexCreationStatement, KEYSPACE_FIELD);
        logger.info("*** ClientRatingBolt ****: " + "index creation status " + result.all());
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;
        launchCassandraKeyspace();

        this.clientRatingRepository = repositoryFactory.getClientRatingRepository();
        createTableIfNotExists();
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);

        ClientRating clientRating = new ClientRating(incomeEvent.getClientId(), incomeEvent.getItemId(), incomeEvent.getWeight());

        SimpleStatement findOneStatement = this.clientRatingRepository.findByClientIdAndItemId(
            clientRating.getClientId(), clientRating.getItemId());

        ResultSet findOneResult = this.repositoryFactory.executeStatement(findOneStatement, KEYSPACE_FIELD);

        logger.info(" ***** find on result: ***** " + findOneResult.all());

        if (findOneResult.all().size() == 0) {
            SimpleStatement insertNewStatement = this.clientRatingRepository.insertClientRating(
                clientRating);
            
            ResultSet insertNewResult = this.repositoryFactory.executeStatement(
                insertNewStatement, KEYSPACE_FIELD);
            
            logger.info("Insert new client rating: " + insertNewResult.all());

            Values value = new Values(incomeEvent, clientRating.getRating());
            collector.emit(value);
        } else {
            if (findOneResult.all().size() > 1) {
                logger.warn(" ******* ClientRatingBolt ******** " + " found more than 1 result ..... " + findOneResult.all().get(0));
            }
            int currRating = ((ClientRating) findOneResult.one()).getRating();
            logger.info("***** current rating result: *****: " + currRating);

            if (clientRating.getRating() > currRating) {

                SimpleStatement updateIfGreaterStatement = this.clientRatingRepository.updateIfGreaterClientRating(clientRating);
                ResultSet updateIfGreaterResult = this.repositoryFactory.executeStatement(
                    updateIfGreaterStatement, KEYSPACE_FIELD);

                logger.info("Update current client rating: " + updateIfGreaterResult.all());

                logger.info("****** Client Rating Bolt *****:" + "emit only triggered update event");
                Values value = new Values(incomeEvent, clientRating.getRating() - currRating);
                collector.emit(value);
            }
        }

        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_FIELD, DELTA_RATING));
    }
}
