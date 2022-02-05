package vn.datnguyen.recommender;


import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import vn.datnguyen.recommender.Bolt.LoggerBolt;
import vn.datnguyen.recommender.Spout.SpoutCreator;
import vn.datnguyen.recommender.utils.CustomProperties;

public class TopologyDefinition {

    private static final CustomProperties customProperties = CustomProperties.getInstance();
    private static final String KAFKA_SPOUT_THREAD = customProperties.getProp("KAFKA_SPOUT_THREAD");
    private static final String TOPOLOGY_WORKERS = customProperties.getProp("TOPOLOGY_WORKERS");
    private static final String LOGGER_BOLT_THREADS = customProperties.getProp("LOGGER_BOLT_THREADS");

    private static SpoutCreator spoutCreator = new SpoutCreator();
    private static LoggerBolt loggerBolt = new LoggerBolt();

    private static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(Integer.parseInt(TOPOLOGY_WORKERS)); 
        return config;
    }

    public static void main( String[] args ) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafka-spout", spoutCreator.kafkaSpout(), Integer.parseInt(KAFKA_SPOUT_THREAD));

        topologyBuilder.setBolt("logger-bolt", loggerBolt, Integer.parseInt(LOGGER_BOLT_THREADS))
            .shuffleGrouping("kafka-spout");

        StormSubmitter.submitTopology("Recommender-Realtime-Topology", getConfig(), topologyBuilder.createTopology());
    }
}
