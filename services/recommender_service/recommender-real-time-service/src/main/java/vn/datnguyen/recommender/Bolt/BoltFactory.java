package vn.datnguyen.recommender.Bolt;


import org.apache.storm.redis.common.config.JedisPoolConfig;

import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.ClientRatingBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.CoRatingBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.DuplicateFilterBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.ItemCountBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.NewRecordBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.PairCountBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.SimilaritiesBolt;
import vn.datnguyen.recommender.Bolt.ColaborativeFiltering.WeightApplierBolt;
import vn.datnguyen.recommender.Bolt.ContentBased.DispatcherBolt;
import vn.datnguyen.recommender.Bolt.ContentBased.EventFilteringBolt;
import vn.datnguyen.recommender.Bolt.ContentBased.KnnBolt;
import vn.datnguyen.recommender.Bolt.ContentBased.RecommendForItemContentBased;
import vn.datnguyen.recommender.Bolt.ContentBased.RingAggregationBolt;
import vn.datnguyen.recommender.Bolt.ContentBased.UpdateBoundedRingBolt;
import vn.datnguyen.recommender.utils.CustomProperties;

public class BoltFactory {

    private final static CustomProperties customProperties = CustomProperties.getInstance();

    //Redis configs
    private final static String REDIS_HOST = customProperties.getProp("REDIS_HOST");
    private final static String REDIS_PORT = customProperties.getProp("REDIS_PORT");
    //pair count

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

    public RecommendForItemContentBased createRecommendForItemContentBased() {
        return new RecommendForItemContentBased();
    }

    public RingAggregationBolt createRingAggregationBolt() {
        return new RingAggregationBolt();
    }

    public KnnBolt createKnnBolt() {
        return new KnnBolt();
    }
}
