package vn.datnguyen.recommender.Bolt;

import org.apache.storm.redis.common.config.JedisPoolConfig;

import vn.datnguyen.recommender.utils.CustomProperties;

public class BoltFactory {

    private final static CustomProperties customProperties = CustomProperties.getInstance();

    //Redis configs
    private final static String REDIS_HOST = customProperties.getProp("REDIS_HOST");
    private final static String REDIS_PORT = customProperties.getProp("REDIS_PORT");
    
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
}
