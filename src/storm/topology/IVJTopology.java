package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.bolt.BoltBuilder;
import storm.bolt.BoltProber;
import storm.spout.TestSpout;



public class IVJTopology{
	
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
		 * "bolt_builder" will create Bloom Filters by fields grouping by "Predicate"
		 * "bolt_prober" will probe Bloom Filters
		*/
		builder.setSpout("spout_getdata", new TestSpout(),1);
		builder.setBolt("bolt_builder", new BoltBuilder(),4).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		builder.setBolt("bolt_prober", new BoltProber(),1).shuffleGrouping("bolt_builder");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("RDFContinuous", config, builder.createTopology());
		Thread.sleep(10000);
		
		cluster.shutdown();
		
	}
	

}
