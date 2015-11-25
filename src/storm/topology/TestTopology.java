package storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.bolt.BoltBuilder;
import storm.bolt.BoltProber;
import storm.spout.RDFSpout;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;



public class TestTopology{
	
public static BufferedReader reader;
	
	public static void main(String[] args) throws Exception{
		
		
		String filePath="./data/rdfdata.txt";
		File file = new File(filePath);
		reader = null;
		try{
			reader = new BufferedReader(new FileReader(file));
			stormCall();
				
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			if(reader != null){
				try{
					reader.close();
				}catch(IOException e1){
					//Do nothing
				}
			}
		}
		
		
	}
	
	public static void stormCall() throws Exception
	{
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * Spout to read data from file then it emits tuple as (Subject, Predicate, Object)
		 * Bolts to create bloom filters using fieldsGrouping on Predicate. 
		 * For now we are creating 3 bloomfilters for each predicate.
		*/
		builder.setSpout("spout_getdata", new RDFSpout(),1);
		builder.setBolt("bolt_builder", new BoltBuilder(),3).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		//builder.setBolt("bolt_prober", new BoltProber(), 3).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		
		
		/*
		 * This bolt is optional, but I have shifted code of this file to spout
		 * now spout is also doing formatting of data.
		 * builder.setBolt("bolt_formatter", new BoltsFormatter(),2).shuffleGrouping("spout_getdata");
		 */
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("RDFStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		cluster.shutdown();
		
	}
	

}
