package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Scanner;

//import org.apache.jena.base.Sys;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.BoltBuilderGrid;
import storm.bolt.BoltProberGrid;
import storm.grouping.PredicateGrouping;

import storm.spout.RDFSpoutGrid;

public class TopologyGrid {
	
public static BufferedReader reader;


private static Scanner user_input;

	
	public static void main(String[] args) throws Exception{
		
		stormCall(args);

	}
	
	
	public static void stormCall(String[] args) throws Exception

	{
		System.out.println("hello");
		Config config = new Config();
		config.setDebug(true);
		//Logger LOG = LoggerFactory.getLogger(RDFSpoutCountBase.class);
		
		TopologyBuilder builder = new TopologyBuilder();
		

		
		
		//-----------------------
		/*
		builder.setSpout("spout_work", new RDFSpoutGrid("Work"),1);
		builder.setSpout("spout_diplome", new RDFSpoutGrid("Diplome"),1);
		builder.setSpout("spout_paper", new RDFSpoutGrid("Paper"),1);
		
		builder.setBolt("bolt_builder1", new  BoltBuilderGrid("Paper", "kNN"),1).shuffleGrouping("spout_paper");
		builder.setBolt("bolt_builder2", new  BoltBuilderGrid("Work", "INRIA"),1).shuffleGrouping("spout_work");
		builder.setBolt("bolt_prober", new  BoltProberGrid("2V","Diplome","ANY"),1).shuffleGrouping("spout_diplome").shuffleGrouping("bolt_builder1").shuffleGrouping("bolt_builder2");
		*/
		//-----------------------
		
		builder.setSpout("spout_work", new RDFSpoutGrid("Work"),1);
		builder.setSpout("spout_diplome", new RDFSpoutGrid("Diplome"),1);
		builder.setSpout("spout_paper", new RDFSpoutGrid("Paper"),1);
		
		builder.setBolt("bolt_builder1", new  BoltBuilderGrid("Paper", "kNN"),1).shuffleGrouping("spout_paper");
		builder.setBolt("bolt_prober1", new  BoltProberGrid("MV","WORK","ANY"),1).shuffleGrouping("spout_work").shuffleGrouping("bolt_builder1").shuffleGrouping("bolt_prober2");
		builder.setBolt("bolt_prober2", new  BoltProberGrid("MV","Diplome","ANY"),1).shuffleGrouping("spout_diplome").shuffleGrouping("bolt_builder1").shuffleGrouping("bolt_prober1");
		
		if (args != null && args.length > 0) {
			
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			
			//StormSubmitter.
		}
		else {
			//config.setNumWorkers(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("RDFContinuous", config, builder.createTopology());
			Thread.sleep(9000);
			
			
			cluster.shutdown();
		}
		
	}
	

}
