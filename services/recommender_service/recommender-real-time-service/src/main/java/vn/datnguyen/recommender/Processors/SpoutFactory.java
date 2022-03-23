package vn.datnguyen.recommender.Processors;

import org.apache.storm.kafka.spout.KafkaSpout;

import vn.datnguyen.recommender.Processors.ContentBased.CBSpoutCreator;
import vn.datnguyen.recommender.Processors.ContentBased.QueryRecommendationSpoutCreator;

public class SpoutFactory {
    public SpoutFactory() {}

    public KafkaSpout<?,?> createCBSpout() {
        CBSpoutCreator spoutCreator = new CBSpoutCreator();
        return spoutCreator.kafkaAvroEventSpout();
    }

    public KafkaSpout<?,?> createQueryRecommendSpout() {
        QueryRecommendationSpoutCreator spoutCreator = new QueryRecommendationSpoutCreator();
        return spoutCreator.kafkaAvroEventSpout();
    }
}
