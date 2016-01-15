package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.bolt.BoltBuilder;
import storm.bolt.BoltBuilderWithThreeBF;
import storm.bolt.BoltProber;
import storm.bolt.BoltProberWithThreeBF;
import storm.rdf.Query;
import storm.spout.RDFSpoutWithThreeBF;
import storm.spout.TestSpout;

public class TopologyWithThreeBF{
	public static Query query;
	
public static BufferedReader reader;

private static Scanner user_input;
	
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
		user_input = new Scanner( System.in );
		System.out.println("Value for V1?");
		String v1 = user_input.next();
		System.out.println("Value for V2? (put in ANY for any possible value)");
		String v2 = user_input.next();
		System.out.println("Value for V3? (put in ANY for any possible value)");
		String v3 = user_input.next();
		query = new Query(v1,v2,v3);
		
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * Spout to read data from file then it emits tuple as (Subject, Predicate, Object)
		 * "bolt_builder" will create Bloom Filters by fields grouping by "Predicate"
		 * "bolt_prober" will probe Bloom Filters
		*/
		builder.setSpout("spout_getdata", new RDFSpoutWithThreeBF(),1);
		builder.setBolt("bolt_builder", new BoltBuilderWithThreeBF(),1).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		builder.setBolt("bolt_prober", new BoltProberWithThreeBF(),1).shuffleGrouping("bolt_builder");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("RDFContinuous", config, builder.createTopology());
		Thread.sleep(30000);
		
		cluster.shutdown();
		
	}
	

}
