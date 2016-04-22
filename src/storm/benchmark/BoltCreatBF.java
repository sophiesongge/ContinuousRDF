package storm.benchmark;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltCreatBF implements IRichBolt {
	private OutputCollector collector;

	Map<String, BloomFilter<Object>> bloomFilters;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		//this.bloomfilters = new HashMap<String, BloomFilter>();
		this.bloomFilters = new HashMap<String, BloomFilter<Object>>();
	}

	public void execute(Tuple input) {
		
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		String Object = input.getStringByField("Object");
		
		if (!bloomFilters.containsKey(Predicate)) {
			BloomFilter< Object> bf= new BloomFilter<Object>(0.01, 15);
			bf.add(Subject);
			bloomFilters.put(Predicate, bf);
		} else {
			BloomFilter< Object> bf= bloomFilters.get(Predicate);
			bf.add(Subject);
			bloomFilters.put(Predicate, bf);
		}
		//output, send to next stage.
		collector.emit(new Values(bloomFilters));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		for (Map.Entry<String, BloomFilter<Object>> entry : bloomFilters.entrySet()) {
			System.out.println("Bloom Filter with Predicate = "+ entry.getKey() + " has values = " + entry.getValue().count());
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
