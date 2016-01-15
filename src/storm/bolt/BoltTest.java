package storm.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.bloomfilter.BloomFilter;

public class BoltTest implements IRichBolt {
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	public void execute(Tuple input) {		
		System.out.println("BoltTest execute");
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		String Object = input.getStringByField("Object");
		System.out.println("tuple: (" + Subject + "," + Predicate + "," + Object + ")");
		/*
		 * values.get(0): subject
		 * values.get(1): predicate
		 * values.get(2): object 
		 */
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {		
	}
}
