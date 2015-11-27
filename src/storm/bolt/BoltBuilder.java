package storm.bolt;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.bloomfilter.BloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltBuilder implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter<String> bf;
	private int id;
	
	/**
	 * initialization
	 */
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {	
		//initialize the emitter
		this.collector = collector;
		//initialize an empty Bloom Filter with fp=0.001 and maximum_element=20 
		this.bf = new BloomFilter(0.01, 20);
		this.id = context.getThisTaskId();
		//this.bloomfilters = new HashMap<String, BloomFilter>();
		//this.bloomFilters = new HashMap<String, BloomFilter<Object>>();
	}
	
	/**
	 * The main method of Bolt, it will be called when the bolt receives a new tuple
	 * It will add the subject to the triple received into the Bloom Filter 
	 */
	public void execute(Tuple input) {
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		String Object = input.getStringByField("Object");
		bf.add(Subject);
		if(Predicate.equals("Paper")){
			collector.emit(new Values("TaskID: "+id, Subject));
		}else{
			collector.emit(new Values("TaskID: "+id, "The predicate processed by this task: "+Predicate));
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
