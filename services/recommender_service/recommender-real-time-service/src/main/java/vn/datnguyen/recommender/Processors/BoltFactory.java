package vn.datnguyen.recommender.Processors;


import java.util.Properties;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import vn.datnguyen.recommender.Processors.ColaborativeFiltering.ClientRatingBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.CoRatingBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.DuplicateFilterBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.ItemCountBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.NewRecordBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.PairCountBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.SimilaritiesBolt;
import vn.datnguyen.recommender.Processors.ColaborativeFiltering.WeightApplierBolt;
import vn.datnguyen.recommender.Processors.ContentBased.DispatcherBolt;
import vn.datnguyen.recommender.Processors.ContentBased.EventFilteringBolt;
import vn.datnguyen.recommender.Processors.ContentBased.EventFilteringForQueryBolt;
import vn.datnguyen.recommender.Processors.ContentBased.KafkaProducerBolt;
import vn.datnguyen.recommender.Processors.ContentBased.KnnBolt;
import vn.datnguyen.recommender.Processors.ContentBased.RecommendForItemContentBased;
import vn.datnguyen.recommender.Processors.ContentBased.RingAggregationBolt;
import vn.datnguyen.recommender.Processors.ContentBased.UpdateBoundedRingBolt;
import vn.datnguyen.recommender.utils.CustomProperties;

public class BoltFactory {

    private final static CustomProperties customProperties = CustomProperties.getInstance();

    //Redis configs
    private final static String REDIS_HOST = customProperties.getProp("REDIS_HOST");
    private final static String REDIS_PORT = customProperties.getProp("REDIS_PORT");
    //
    private final static String BOOTSTRAP_SERVER = customProperties.getProp("BOOTSTRAP_SERVER");

    public BoltFactory() {}

    public LoggerBolt creatLoggerBolt() {
        return new LoggerBolt();
    }

    public WeightApplierBolt createWeightApplierBolt() {
        return new WeightApplierBolt();
    }

    public DuplicateFilterBolt createDuplicateFilterBolt() {
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
            .setHost(REDIS_HOST)
            .setPort(Integer.parseInt(REDIS_PORT))
            .build();
        return new DuplicateFilterBolt(poolConfig);
    }

    public ClientRatingBolt createClientRatingBolt() {
        return new ClientRatingBolt();
    }

    public ItemCountBolt createItemCountBolt() {
        return new ItemCountBolt();
    }

    public CoRatingBolt createCoRatingBolt() {
        return new CoRatingBolt();
    }

    public PairCountBolt createPairCountBolt() {
        return new PairCountBolt();
    }

    public SimilaritiesBolt createSimilaritiesBolt() {
        return new SimilaritiesBolt();
    }

    public NewRecordBolt createNewRecordBolt() {
        return new NewRecordBolt();
    }

    public DispatcherBolt createDispatcherBolt() {
        return new DispatcherBolt();
    }

    public UpdateBoundedRingBolt createUpdateBoundedRingBolt() {
        return new UpdateBoundedRingBolt();
    }
    
    public EventFilteringBolt createEventFilteringBolt() {
        return new EventFilteringBolt();
    }

    public EventFilteringForQueryBolt createEventFilteringForQueryBolt() {
        return new EventFilteringForQueryBolt();
    }

    public RecommendForItemContentBased createRecommendForItemContentBased() {
        return new RecommendForItemContentBased();
    }

    public RingAggregationBolt createRingAggregationBolt() {
        return new RingAggregationBolt();
    }

    public KnnBolt createKnnBolt() {
        return new KnnBolt();
    }

    public KafkaProducerBolt<Object,Object> createKafkaProducerBolt() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put("acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "vn.datnguyen.recommender.Serialization.ItemSimilaritesSerializer");

        KafkaProducerBolt<Object,Object> bolt = new KafkaProducerBolt<>()
            .withProducerProperties(props);
        
        return bolt;
    }
}
