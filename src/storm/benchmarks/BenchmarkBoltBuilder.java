package storm.benchmarks;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.bloomfilter.BloomFilter;
import storm.rdf.Query;
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

	private int maxGenerationSize=30;
	private int currentGenerationSize=0;

	String gPredicate;
	String gObject;

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
		//initialize an empty Bloom Filter with fp=0.001 and maximum_element=20 
		this.bf = new BloomFilter(0.01, 10);

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

		if(Predicate.equals(gPredicate)){
			
			if(gObject.equals("ANY")){
				bf.add(Subject);
			}
			else if(Object.equals(gObject)){
				bf.add(Subject);
			}
		}

		currentGenerationSize++;
		if(currentGenerationSize==maxGenerationSize)
		{

			BloomFilter<String> bfToSend=new BloomFilter(bf);
			collector.emit(new Values("bf_"+gPredicate,bfToSend));
			bf.clear();

			currentGenerationSize=0;
		}

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
