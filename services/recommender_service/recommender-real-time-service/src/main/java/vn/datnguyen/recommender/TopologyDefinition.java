package vn.datnguyen.recommender;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import vn.datnguyen.recommender.Spout.SpoutCreator;
import vn.datnguyen.recommender.utils.CustomProperties;

public class TopologyDefinition {

    private static final CustomProperties customProperties = new CustomProperties();
    private static final String KAFKA_SPOUT_THREAD = customProperties.getProp("customProperties");
    private static final String TOPOLOGY_WORKERS = customProperties.getProp("TOPOLOGY_WORKERS");
    private static SpoutCreator spoutCreator = new SpoutCreator();

    public static class ExampleBolt extends BaseRichBolt {

        private OutputCollector collector;
    
        @Override
        public void prepare(Map map, TopologyContext TopologyContext, OutputCollector collector) {
            this.collector = collector;
        }
    
        @Override
        public void execute(Tuple tuple) {
        }
    
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("test-output-bolt"));
        }
    
    }

    private static Config getConfig() {
        Config config = new Config();
        config.setDebug(true);
        config.setNumWorkers(Integer.parseInt(TOPOLOGY_WORKERS)); 
        return config;
    }

    public static void main( String[] args ) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("kafka-spout", spoutCreator.kafkaSpout(), Integer.parseInt(KAFKA_SPOUT_THREAD));

        topologyBuilder.setBolt("bolt-test-1", new ExampleBolt(), 1)
            .shuffleGrouping("mew-test-spout");
        
        topologyBuilder.setBolt("bolt-test-2", new ExampleBolt(), 1)
            .shuffleGrouping("bolt-test-1");

        StormSubmitter.submitTopology("Recommender-Realtime-Topology", getConfig(), topologyBuilder.createTopology());

    }
}
