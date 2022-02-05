package vn.datnguyen.recommender;

import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class TopologyDefinition {
    public static class ExampleBolt extends BaseRichBolt {

        private OutputCollector collector;
    
        @Override
        public void prepare(Map map, TopologyContext TopologyContext, OutputCollector collector) {
            this.collector = collector;
        }
    
        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getString(0); 
    
            StringBuilder sb = new StringBuilder();
            sb.append(word).append(">_<");
    
            collector.emit(tuple, new Values(sb.toString()));
            collector.ack(tuple);
        }
    
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("test-output-bolt"));
        }
    
    }
    public static void main( String[] args ) throws Exception {
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("mew-test-spout", new TestWordSpout(), 1);

        topologyBuilder.setBolt("bolt-test-1", new ExampleBolt(), 3)
            .shuffleGrouping("mew-test-spout");
        
        topologyBuilder.setBolt("bolt-test-2", new ExampleBolt(), 2)
            .shuffleGrouping("bolt-test-1");

        Config config = new Config();
        config.setDebug(true);
        //3 supervisor - 5 worker each
        config.setNumWorkers(5);

        StormSubmitter.submitTopology("Recommender-Realtime-Topology", config, topologyBuilder.createTopology());

    }
}
