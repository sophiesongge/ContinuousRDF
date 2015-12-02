package storm.bolt;

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
	private BloomFilter<String> bf1;
	private BloomFilter<String> bf2;
	private int id;
	
	/**
	 * initialization
	 */
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {	
		//initialize the emitter
		this.collector = collector;
		//initialize an empty Bloom Filter with fp=0.001 and maximum_element=20 
		this.bf1 = new BloomFilter(0.01, 10);
		this.bf2 = new BloomFilter(0.01, 10);
		this.id = context.getThisTaskId();
	}
	
	/**
	 * The main method of Bolt, it will be called when the bolt receives a new tuple
	 * It will add the subject to the triple received into the Bloom Filter 
	 */
	public void execute(Tuple input) {
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		String Object = input.getStringByField("Object");
		if(Predicate.equals("Diplome")){
			if(Object.equals("Ph.D")){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
			}
		}else if(Predicate.equals("Work")){
			if(Object.equals("INRIA")){
				bf1.add(Subject);
				//collector.emit(new Values("BuilderTaskID_1_"+id,bf1));
				//collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			}
			collector.emit(new Values("BuilderTaskID_1_"+id, bf1));
		}else{
			if(Object.equals("kNN")){
				bf2.add(Subject);
				//collector.emit(new Values("BuilderTaskID_2_"+id,bf2));
				//collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
			}
			collector.emit(new Values("BuilderTaskID_2_"+id, bf2));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ID","Content"));
	}

	public void cleanup() {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
