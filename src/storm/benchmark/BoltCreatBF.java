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
	String[] query;
	
	public BoltCreatBF(String[] input) {
		query = input;
	}

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
				
		System.out.println("Execute createBF for input (" + Subject + "," + Predicate + "," + Object + ")");
		
		if(queryMatch(input)){
			if (!bloomFilters.containsKey(Predicate)) {
				BloomFilter< Object> bf= new BloomFilter<Object>(0.01, 15);
				bf.add(Subject);
				bloomFilters.put(Predicate, bf);
			} else {
				BloomFilter< Object> bf= bloomFilters.get(Predicate);
				bf.add(Subject);
				bloomFilters.put(Predicate, bf);
			}
		}
		//output, send to next stage.
		collector.emit(new Values(bloomFilters));
	}
	
	private boolean queryMatch(Tuple tuple){
		
		if(!query[0].equalsIgnoreCase("*") && !query[0].equalsIgnoreCase(tuple.getStringByField("Subject"))){
			return false;
		}
		if(!query[1].equalsIgnoreCase("*") && !query[1].equalsIgnoreCase(tuple.getStringByField("Predicate"))){
			return false;
		}
		if(!query[2].equalsIgnoreCase("*") && !query[2].equalsIgnoreCase(tuple.getStringByField("Object"))){
			return false;
		}
		return true;
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		for (Map.Entry<String, BloomFilter<Object>> entry : bloomFilters.entrySet()) {
			//for benchmark, create new bolt, only need to change predicate
			System.out.println("Bloom Filter with Predicate = ("+ query[0] + ","+ query[1] + ","+ query[2] + ") has values = " + entry.getValue().count());
		}
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
