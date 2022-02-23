package vn.datnguyen.recommender.Bolt;

import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
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
import vn.datnguyen.recommender.Repository.ItemCountRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.Repository.SimilaritiesRepository;
import vn.datnguyen.recommender.utils.CustomProperties;

public class NewRecordBolt extends BaseRichBolt {
    
    private final Logger logger = LoggerFactory.getLogger(LoggerBolt.class);
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
    //
    private final static String ITEM_1_ID = "item_1_id";
    private static final String SCORE = "score";
    
    private OutputCollector collector;
    private RepositoryFactory repositoryFactory;

    private void launchCassandraKeyspace() {
        CassandraConnector connector = new CassandraConnector();
        connector.connect(CASS_NODE, Integer.parseInt(CASS_PORT), CASS_DATA_CENTER);
        CqlSession session = connector.getSession();

        this.repositoryFactory = new RepositoryFactory(session);
        KeyspaceRepository keyspaceRepository = this.repositoryFactory.getKeyspaceRepository();
        keyspaceRepository.createAndUseKeyspace(KEYSPACE_FIELD, Integer.parseInt(NUM_NODE_REPLICAS_FIELD));
        logger.info("CREATE AND USE KEYSPACE SUCCESSFULLY keyspace in **** ItemCountBolt ****");
    }
    
    @Override
    public void prepare(Map<String, Object> map, TopologyContext TopologyContext, OutputCollector collector) {
        this.collector = collector;

        launchCassandraKeyspace();
    }

    private void initSimilaritiesTable(String itemId) {
        SimilaritiesRepository similaritiesRepository = this.repositoryFactory.getSimilaritiesRepository();
        ItemCountRepository itemCountRepository = this.repositoryFactory.getItemCountRepository();
        // init table
        SimpleStatement createTable = itemCountRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(createTable, KEYSPACE_FIELD);
        logger.info("************ ItemCountBolt *************: create table successfully ");

        createTable = similaritiesRepository.createTableIfNotExists();
        this.repositoryFactory.executeStatement(createTable, KEYSPACE_FIELD);
        logger.info("************ SimilaritiesBolt *************: create table successfully ");
        //
        SimpleStatement findByItem1IdStatement = similaritiesRepository.findByItem1Id(itemId);
        ResultSet findByItem1IdResult = this.repositoryFactory.executeStatement(findByItem1IdStatement, KEYSPACE_FIELD);
        List<Row> findByItem1Id = findByItem1IdResult.all();

        BatchStatementBuilder allBatch = BatchStatement.builder(BatchType.LOGGED);

        if (findByItem1Id.size() == 0) {
            logger.info("************ SimilaritiesBolt *************: initSimilaritiesTableWhenItemCountUpdate itemId = " + itemId);

            SimpleStatement findAllStatement = similaritiesRepository.findAllItemId();
            ResultSet findAllResult = this.repositoryFactory.executeStatement(findAllStatement, KEYSPACE_FIELD);
            List<Row> findAll = findAllResult.all();

            // Init score
            SimpleStatement insertScore = similaritiesRepository.initScore(itemId, itemId, 1.0, 1.0);
            allBatch.addStatement(insertScore);

            for (Row r: findAll) {
                String anotherItemId = (String) this.repositoryFactory.getFromRow(r, ITEM_1_ID);

                //find itemCountValue 
                int itemCountScore = 1;
                SimpleStatement findItemCountAItem = itemCountRepository.findByItemId(anotherItemId);
                ResultSet itemCountResult = this.repositoryFactory.executeStatement(findItemCountAItem, KEYSPACE_FIELD);

                if (itemCountResult.getAvailableWithoutFetching() > 0) {
                    Row itemCountRow = this.repositoryFactory.executeStatement(findItemCountAItem, KEYSPACE_FIELD).one();
                    itemCountScore = (int) this.repositoryFactory.getFromRow(itemCountRow, SCORE);
                }

                insertScore = similaritiesRepository.initScore(itemId, anotherItemId, Math.sqrt(itemCountScore), 1.0);
                allBatch.addStatement(insertScore);

                insertScore = similaritiesRepository.initScore(anotherItemId, itemId, Math.sqrt(itemCountScore), 1.0);
                allBatch.addStatement(insertScore);
            }

            this.repositoryFactory.executeStatement(allBatch.build(), KEYSPACE_FIELD);
        }
    }
    
    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String clientId = incomeEvent.getClientId();
        String itemId = incomeEvent.getItemId();

        initSimilaritiesTable(itemId);

        Values values = new Values(incomeEvent, clientId);
        collector.emit(values);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_FIELD, CLIENT_ID_FIELD));
    }
}
