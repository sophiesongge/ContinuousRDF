package storm.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.rdf.RDFTriple;
import storm.bloomfilter.BloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltProber implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter bf;

	//Map<String, BloomFilter<Object>> bloomFilters;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.bf = new BloomFilter(0.001, 20);
		//this.bloomfilters = new HashMap<String, BloomFilter>();
		//this.bloomFilters = new HashMap<String, BloomFilter<Object>>();
	}

	public void execute(Tuple input) {
		
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		//String Object = input.getStringByField("Object");
		if(Predicate != "paper"){
			bf.add(Subject);
			collector.emit((List<Object>) bf);
		}
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		
		for(int i=0; i<bf.size(); i++){
			System.out.print(bf.getBit(i)+" ");
		}
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
