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
import storm.bolt.BoltBuilderGridTimebase;
import storm.bolt.BoltProberGrid;
import storm.bolt.BoltProberGridTimebase;
import storm.config.TopologyConfiguration;
import storm.spout.RDFSpoutGrid;
import storm.spout.RDFSpoutGridTimebase;

public class BenchmarkQuery {

	public static void main(String[] args) throws Exception{

		String TopologyName = "RDFBenchmark";
		String QueryNumber = "1";
		int NumberofWorkers = 1;
		int SlidingWindowTime = 10;
		int SlidingWindowSize = 400;
		int NumberofGenerations = 4;
		boolean isLocal = true;

		if (args != null && args.length == 5) {

			TopologyName = args[0];
			QueryNumber = args[1];
			NumberofWorkers =  Integer.parseInt(args[2]);
			SlidingWindowTime = Integer.parseInt(args[3]);
			SlidingWindowSize = SlidingWindowTime*500;
			NumberofGenerations = Integer.parseInt(args[4]);
			isLocal = false;
		}

		TopologyConfiguration.NUMBER_OF_GENERATIONS = NumberofGenerations;
		TopologyConfiguration.SLIDING_WINDOW_SIZE = SlidingWindowSize;
		TopologyConfiguration.GENERATION_SIZE = SlidingWindowSize / NumberofGenerations;

		System.out.println("NumberofGenerations: "+ NumberofGenerations);
		System.out.println("SlidingWindowSize: "+ SlidingWindowSize);
		System.out.println("GENERATION_SIZE: "+ TopologyConfiguration.GENERATION_SIZE);

		int WindowTime = SlidingWindowTime/NumberofGenerations;
		stormCall(TopologyName, NumberofWorkers, QueryNumber, WindowTime, isLocal);


	}


	public static void stormCall(String TopologyName, int NumberofWorkers, String QueryNumber,int WindowTime, boolean isLocal) throws Exception

	{
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();

		int tickFrequencyInSeconds = WindowTime;
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);


		//Check the Query Number
		if(QueryNumber.equalsIgnoreCase("1")) {

			builder.setSpout("spout_takecourse", new BenchmarkRDFSpout("takesCourse"),1);
			builder.setSpout("spout_type", new BenchmarkRDFSpout("type"),1);

			builder.setBolt("bolt_builder1", new  BenchmarkBoltBuilder("type", "GraduateStudent"),3).shuffleGrouping("spout_type");

			String Value = "http://www.Department0.University0.edu/GraduateCourse0";
			builder.setBolt("bolt_prober", new  BenchmarkBoltProber("takesCourse",Value),3).shuffleGrouping("spout_takecourse").allGrouping("bolt_builder1");

			TopologyConfiguration.NUMBER_BF1 = 3; //set this value equal to number of builder bolts

		}
		else if(QueryNumber.equalsIgnoreCase("4")) {

			builder.setSpout("spout_emailAddress", new BenchmarkRDFSpout("emailAddress"),1);
			builder.setSpout("spout_name", new BenchmarkRDFSpout("name"),1);
			builder.setSpout("spout_type", new BenchmarkRDFSpout("type"),1);

			builder.setBolt("bolt_builder1", new  BenchmarkBoltBuilder("type", "Professor"),3).shuffleGrouping("spout_type");

			builder.setBolt("bolt_prober1", new  BenchmarkBoltBuilderProber("emailAddress","ANY"),3).shuffleGrouping("spout_emailAddress").allGrouping("bolt_builder1").allGrouping("bolt_prober2");
			builder.setBolt("bolt_prober2", new  BenchmarkBoltBuilderProber("name","ANY"),3).shuffleGrouping("spout_name").allGrouping("bolt_builder1").allGrouping("bolt_prober1");

			TopologyConfiguration.NUMBER_BF1 = 3; //set this value equal to number of builder bolts
			TopologyConfiguration.NUMBER_BF2 = 3;
		}
		else if(QueryNumber.equalsIgnoreCase("3")) {

			builder.setSpout("spout_publicationAuthor", new BenchmarkRDFSpout("publicationAuthor"),1);
			builder.setSpout("spout_type", new BenchmarkRDFSpout("type"),1);

			builder.setBolt("bolt_builder1", new  BenchmarkBoltBuilder("type", "Publication"),3).shuffleGrouping("spout_type");

			String Value = "http://www.Department0.University0.edu/AssistantProfessor0";
			builder.setBolt("bolt_prober", new  BenchmarkBoltProber("publicationAuthor",Value),3).shuffleGrouping("spout_publicationAuthor").allGrouping("bolt_builder1");	

			TopologyConfiguration.NUMBER_BF1 = 3; //set this value equal to number of builder bolts
		}
		else if(QueryNumber.equalsIgnoreCase("11")) {

			builder.setSpout("spout_subOrganizationOf", new BenchmarkRDFSpout("subOrganizationOf"),1);
			builder.setSpout("spout_type", new BenchmarkRDFSpout("type"),1);

			builder.setBolt("bolt_builder1", new  BenchmarkBoltBuilder("type", "ResearchGroup"),3).shuffleGrouping("spout_type");

			String Value = "http://www.University0.edu";
			builder.setBolt("bolt_prober", new  BenchmarkBoltProber("subOrganizationOf",Value),3).shuffleGrouping("spout_subOrganizationOf").allGrouping("bolt_builder1");		

			TopologyConfiguration.NUMBER_BF1 = 3; //set this value equal to number of builder bolts
		}


		if (!isLocal) {

			config.setNumWorkers(NumberofWorkers);
			StormSubmitter.submitTopology(TopologyName, config, builder.createTopology());

		}
		else {
			config.setDebug(true);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TopologyName, config, builder.createTopology());
			Thread.sleep(12000);
			cluster.shutdown();
		}



	}




}
