package storm.benchmarks;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.login.Configuration;

import storm.bloomfilter.BloomFilter;
import storm.config.TopologyConfiguration;
import storm.rdf.Query;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BenchmarkBoltBuilder implements IRichBolt {

	private OutputCollector collector;
	private static BloomFilter<String> bf;

	private int id;	

	String gPredicate;
	String gObject;

	private int GenerationSize = TopologyConfiguration.GENERATION_SIZE;

	public BenchmarkBoltBuilder(String p, String o) {
		gPredicate=p;
		gObject=o;
	}

	/**
	 * initialization
	 */
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {	
		//initialize the emitter
		this.collector = collector;
		//initialize an empty Bloom Filter with fp=0.001 and maximum_element= Generation Size
		this.bf = new BloomFilter(0.01, GenerationSize);

		this.id = context.getThisTaskId();
	}

	private static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	/**
	 * The main method of Bolt, it will be called when the bolt receives a new tuple
	 * It will add the subject to the triple received into the Bloom Filter 
	 */
	public void execute(Tuple input) {

		
		//String tripleID = input.getStringByField("id");
		//if(tripleID.equals("process")) {
		if(isTickTuple(input)) {
			ProcessGeneration();
		}
		else {
			String Subject = input.getStringByField("Subject");
			String Predicate = input.getStringByField("Predicate");
			String Object = input.getStringByField("Object");

			bf.add(Subject);
			if(Predicate.contains(gPredicate)){

				if(gObject.equals("ANY")){
					bf.add(Subject);
				}
				else if(Object.contains(gObject)){
					bf.add(Subject);
				}
			}
		}

	}

	void ProcessGeneration() {

		BloomFilter<String> bfToSend=new BloomFilter(bf);
		collector.emit(new Values("bf1"+id,bfToSend));
		bf.clear();
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","bf"));
	}

	public void cleanup() {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
