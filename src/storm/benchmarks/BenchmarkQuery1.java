package storm.benchmarks;

import java.io.BufferedReader;
import java.io.InputStream;
import java.util.Scanner;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;


import storm.bolt.BoltBuilderGrid;
import storm.bolt.BoltProberGrid;

import storm.spout.RDFSpoutGrid;

public class BenchmarkQuery1 {
	
	public static void main(String[] args) throws Exception{
		
		stormCall(args);
		
	}
	
	
	public static void stormCall(String[] args) throws Exception

	{
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		//-----------------------

		builder.setSpout("spout_takecourse", new BenchmarkRDFSpout("takeCourse"),1);
		
		builder.setSpout("spout_type", new BenchmarkRDFSpout("type"),1);
		
		builder.setBolt("bolt_builder1", new  BenchmarkBoltBuilder("type", "GraduateStudent"),1).shuffleGrouping("spout_type");
		//String[] builderpredicates= {"type"};
		
		String Value = "http://www.Department0.University0.edu/Course42";
		//String Value = "Course42";
		
		builder.setBolt("bolt_prober", new  BenchmarkBoltProber("IV","takeCourse",Value),1).shuffleGrouping("spout_takecourse").shuffleGrouping("bolt_builder1");
		
		//--------------------------------------
		
		
		if (args != null && args.length > 0) {
			
			config.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			
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
