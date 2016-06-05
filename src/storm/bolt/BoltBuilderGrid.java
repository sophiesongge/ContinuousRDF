package storm.bolt;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.base.Sys;

import storm.bloomfilter.BloomFilter;
import storm.rdf.Query;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltBuilderGrid implements IRichBolt {
	
	private OutputCollector collector;
	private static BloomFilter bf1;
	private static BloomFilter bf2;

	private int id;	
	
	private int GenerationSize=30;
	private int currentGenerationSize=0;
	
	String gPredicate;
	String gObject;
	
	public BoltBuilderGrid(String p, String o) {
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
		
		if(Predicate.equals(gPredicate) && Predicate.equals("Paper")){
			if(Object.equals(gObject)){
				bf1.add(Subject);
			}
		}
		else if(Predicate.equals(gPredicate) && Predicate.equals("Work")){
			if(Object.equals(gObject)){
				bf2.add(Subject);
			}
		}
		
		currentGenerationSize++;
		if(currentGenerationSize==GenerationSize)
		{
			if(gPredicate.equals("Paper")) {
				BloomFilter<String> bf1ToSend=new BloomFilter(bf1);
				collector.emit(new Values("bf1" + this.id,bf1ToSend));
				//collector.emit(new Values("bf1",bf1ToSend));
				bf1.clear();
			}
			else if(gPredicate.equals("Work")) {
				BloomFilter<String> bf2ToSend=new BloomFilter(bf2);
				collector.emit(new Values("bf2" + this.id,bf2ToSend));
				//collector.emit(new Values("bf2",bf2ToSend));
				bf2.clear();
			}

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
