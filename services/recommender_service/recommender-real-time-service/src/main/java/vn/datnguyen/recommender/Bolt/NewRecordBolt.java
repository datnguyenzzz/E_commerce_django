package vn.datnguyen.recommender.Bolt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
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
import vn.datnguyen.recommender.Models.ClientRating;
import vn.datnguyen.recommender.Models.Event;
import vn.datnguyen.recommender.Models.ItemCount;
import vn.datnguyen.recommender.Repository.ClientRatingRepository;
import vn.datnguyen.recommender.Repository.CoRatingRepository;
import vn.datnguyen.recommender.Repository.ItemCountRepository;
import vn.datnguyen.recommender.Repository.KeyspaceRepository;
import vn.datnguyen.recommender.Repository.PairCountRepository;
import vn.datnguyen.recommender.Repository.RepositoryFactory;
import vn.datnguyen.recommender.Repository.SimilaritiesRepository;
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
    //
    private static final String ITEM_1_ID = "item_1_id";
    private static final String RATING_ITEM_1 = "rating_item_1";
    
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

    private CompletionStage<AsyncResultSet> initClientRatingTable(String clientId, String itemId) {
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

            return this.repositoryFactory.asyncExecuteStatement(insertNewStatement, KEYSPACE_FIELD);
        }

        return null;
    }
    
    private CompletionStage<AsyncResultSet> initItemCountTable(String itemId) {
        ItemCountRepository itemCountRepository = this.repositoryFactory.getItemCountRepository();
        //
        SimpleStatement rowCreationStatement = itemCountRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(rowCreationStatement, KEYSPACE_FIELD);
        logger.info("*** NewRecordBolt ****:  ItemCountTAble " + "row creation status ");
        //
        SimpleStatement findOneStatement = itemCountRepository.findByItemId(itemId);
        ResultSet findOneResult = this.repositoryFactory.executeStatement(findOneStatement, KEYSPACE_FIELD);

        int rowFound = findOneResult.getAvailableWithoutFetching();

        if (rowFound == 0) {
            ItemCount itemCount = new ItemCount(itemId, 0);
            SimpleStatement insertNewScoreStatement = itemCountRepository.insertNewScore(itemCount);
            logger.info("***** ItemCountBolt *******: inserted new score for itemId = " + itemId);
            return this.repositoryFactory.asyncExecuteStatement(insertNewScoreStatement, KEYSPACE_FIELD);
        }

        return null;
    }

    private CompletionStage<AsyncResultSet> initCoRatingTable(String clientId, String itemId) {
        CoRatingRepository coRatingRepository = repositoryFactory.getCoRatingRepository();

        SimpleStatement createTableStatement = coRatingRepository.createRowIfNotExists();
        this.repositoryFactory.executeStatement(createTableStatement, KEYSPACE_FIELD);
        logger.info("*** NewRecordBolt ****: CoRatingBolt " + "row created ");

        SimpleStatement createIndexStatement = coRatingRepository.createIndexOnItemId();
        this.repositoryFactory.executeStatement(createIndexStatement, KEYSPACE_FIELD);
        logger.info("*** NewRecordBolt ****: CoRatingBolt " + "index item id created ");

        createIndexStatement = coRatingRepository.createIndexOnClientId();
        this.repositoryFactory.executeStatement(createIndexStatement, KEYSPACE_FIELD);
        logger.info("*** NewRecordBolt ****: CoRatingBolt " + "index client id created ");
        //
        SimpleStatement findByItem1Statement = coRatingRepository.findByItem1IdAndClientId(itemId, clientId);
        ResultSet findByItem1Result = this.repositoryFactory.executeStatement(findByItem1Statement, KEYSPACE_FIELD);

        int rowFound = findByItem1Result.getAvailableWithoutFetching();
        if (rowFound == 0) {
            SimpleStatement findSeItemIdStatement = coRatingRepository.findSetItemIdByClientId(clientId);
            ResultSet findSetItemIdResult = this.repositoryFactory.executeStatement(findSeItemIdStatement, KEYSPACE_FIELD);
            List<Row> findSetItemId = findSetItemIdResult.all();

            BatchStatementBuilder executeWhenItemNotFound = BatchStatement.builder(BatchType.LOGGED);

            SimpleStatement insertNewItemId = coRatingRepository.insertNewItemScore(itemId, itemId, clientId);
            SimpleStatement updateItemIdScoreStatement = coRatingRepository.updateItemScore(itemId, itemId, clientId, 0, 0);
            SimpleStatement updateItem1IdRatingStatement = coRatingRepository.updateItem1Rating(itemId, itemId, clientId, 0);
            SimpleStatement updateItem2IdRatingStatement = coRatingRepository.updateItem2Rating(itemId, itemId, clientId, 0);

            executeWhenItemNotFound.addStatement(insertNewItemId)
                .addStatement(updateItemIdScoreStatement)
                .addStatement(updateItem1IdRatingStatement)
                .addStatement(updateItem2IdRatingStatement);

            for (Row r: findSetItemId) {
                String otherItemId = (String) this.repositoryFactory.getFromRow(r, ITEM_1_ID);
                int otherItemRating = (int) this.repositoryFactory.getFromRow(r, RATING_ITEM_1);

                insertNewItemId = coRatingRepository.insertNewItemScore(itemId, otherItemId, clientId);
                updateItemIdScoreStatement = coRatingRepository.updateItemScore(itemId, otherItemId, clientId, 0, 0);
                updateItem1IdRatingStatement = coRatingRepository.updateItem1Rating(itemId, otherItemId, clientId, 0);
                updateItem2IdRatingStatement = coRatingRepository.updateItem2Rating(itemId, otherItemId, clientId, otherItemRating);

                executeWhenItemNotFound.addStatement(insertNewItemId)
                    .addStatement(updateItemIdScoreStatement)
                    .addStatement(updateItem1IdRatingStatement)
                    .addStatement(updateItem2IdRatingStatement);     

                // 
                insertNewItemId = coRatingRepository.insertNewItemScore(otherItemId, itemId, clientId);
                updateItemIdScoreStatement = coRatingRepository.updateItemScore(otherItemId, itemId, clientId, 0, 0);
                updateItem1IdRatingStatement = coRatingRepository.updateItem1Rating(otherItemId, itemId, clientId, otherItemRating);
                updateItem2IdRatingStatement = coRatingRepository.updateItem2Rating(otherItemId, itemId, clientId, 0);

                executeWhenItemNotFound.addStatement(insertNewItemId)
                    .addStatement(updateItemIdScoreStatement)
                    .addStatement(updateItem1IdRatingStatement)
                    .addStatement(updateItem2IdRatingStatement);
            }

            BatchStatement allBatch = executeWhenItemNotFound.build();
            logger.info("********* NewRecordBolt **********: CoRatingBolt Attempt to execute " + allBatch.size() + " queries in batch");
            return this.repositoryFactory.asyncExecuteStatement(allBatch, KEYSPACE_FIELD);
        }

        return null;
    }

    private CompletionStage<AsyncResultSet> initPairCountTable() {
        PairCountRepository pairCountRepository = repositoryFactory.getPairCountRepository();
        //
        SimpleStatement createTableStatement = pairCountRepository.createRowIfNotExists();
        logger.info("********* PairCountBolt **********: created table");
        return this.repositoryFactory.asyncExecuteStatement(createTableStatement, KEYSPACE_FIELD);
    }

    private CompletionStage<AsyncResultSet> initSimilaritiesTable(String itemId, int weight) {
        SimilaritiesRepository similaritiesRepository = repositoryFactory.getSimilaritiesRepository();
        //
        SimpleStatement createTable = similaritiesRepository.createTableIfNotExists();
        this.repositoryFactory.executeStatement(createTable, KEYSPACE_FIELD);
        logger.info("************ SimilaritiesBolt *************: create table successfully ");
        // 
        SimpleStatement findByItem1IdStatement = similaritiesRepository.findByItem1Id(itemId);
        ResultSet findByItem1IdResult = this.repositoryFactory.executeStatement(findByItem1IdStatement, KEYSPACE_FIELD);
        List<Row> findByItem1Id = findByItem1IdResult.all();

        BatchStatementBuilder allBatch = BatchStatement.builder(BatchType.LOGGED);

        if (findByItem1Id.size() == 0) {
            SimpleStatement findAllStatement = similaritiesRepository.findAllItemId();
            ResultSet findAllResult = this.repositoryFactory.executeStatement(findAllStatement, KEYSPACE_FIELD);
            List<Row> findAll = findAllResult.all();

            SimpleStatement insertScore = similaritiesRepository.initScore(itemId, itemId, 1.0 / Math.sqrt(weight));
            allBatch.addStatement(insertScore);

            for (Row r: findAll) {
                String anotherItemId = (String) this.repositoryFactory.getFromRow(r, ITEM_1_ID);

                insertScore = similaritiesRepository.initScore(itemId, anotherItemId, 1.0);
                allBatch.addStatement(insertScore);

                insertScore = similaritiesRepository.initScore(anotherItemId, itemId, 1.0);
                allBatch.addStatement(insertScore);
            }

            return this.repositoryFactory.asyncExecuteStatement(allBatch.build(), KEYSPACE_FIELD);
        }

        return null;
    }

    @Override
    public void execute(Tuple input) {
        Event incomeEvent = (Event) input.getValueByField(EVENT_FIELD);
        String clientId = incomeEvent.getClientId();
        String itemId = incomeEvent.getItemId();
        int weight = incomeEvent.getWeight();

        logger.info("********* NewRecordBolt **********" + incomeEvent);

        //need async
        CompletableFuture<Void> clientRating = (CompletableFuture<Void>)initClientRatingTable(clientId, itemId)
            .thenAccept((t) -> {
                logger.info("******* NewRecordBolt ******** FINISH clientRating: ");
            });
        CompletableFuture<Void> itemCount = (CompletableFuture<Void>)initItemCountTable(itemId)
            .thenAccept((t) -> {
                logger.info("******* NewRecordBolt ******** FINISH itemCount: ");
            });
        CompletableFuture<Void> coRating = (CompletableFuture<Void>)initCoRatingTable(clientId, itemId)
            .thenAccept((t) -> {
                logger.info("******* NewRecordBolt ******** FINISH coRating: ");
            });
        CompletableFuture<Void> pairCount = (CompletableFuture<Void>)initPairCountTable()
            .thenAccept((t) -> {
                logger.info("******* NewRecordBolt ******** FINISH pairCount: ");
            });
        CompletableFuture<Void> similarities = (CompletableFuture<Void>)initSimilaritiesTable(itemId, weight)
            .thenAccept((t) -> {
                logger.info("******* NewRecordBolt ******** FINISH similarities: ");
            });

        CompletableFuture.allOf(clientRating, itemCount, coRating, pairCount, similarities).join();

        Values values = new Values(incomeEvent, clientId);
        collector.emit(values);
        collector.ack(input);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(EVENT_FIELD, CLIENT_ID_FIELD));
    }
}
