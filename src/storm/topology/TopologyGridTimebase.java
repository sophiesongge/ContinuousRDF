package storm.topology;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.bolt.BoltBuilderGridTimebase;
import storm.bolt.BoltProberGridTimebase;
import storm.config.TopologyConfiguration;
import storm.spout.RDFSpoutGridTimebase;

public class TopologyGridTimebase {
	
	
	
	public static void main(String[] args) throws Exception{
		
		
		String TopologyName = "RDFContinuous";
		String JoinType = "MV";
		int NumberofWorkers = 1;
		int SlidingWindowSize = 900;
		int NumberofGenerations = 3;
		boolean isLocal = true;
		
		if (args != null && args.length == 5) {
			
			TopologyName = args[0];
			JoinType = args[1];
			NumberofWorkers =  Integer.parseInt(args[2]);
			SlidingWindowSize = Integer.parseInt(args[3]);
			NumberofGenerations = Integer.parseInt(args[4]);
			isLocal = false;
		}
		
		TopologyConfiguration.NUMBER_OF_GENERATIONS = NumberofGenerations;
		TopologyConfiguration.SLIDING_WINDOW_SIZE = SlidingWindowSize;
		TopologyConfiguration.GENERATION_SIZE = SlidingWindowSize / NumberofGenerations;
		
		System.out.println("NumberofGenerations: "+ NumberofGenerations);
		System.out.println("SlidingWindowSize: "+ SlidingWindowSize);
		System.out.println("GENERATION_SIZE: "+ TopologyConfiguration.GENERATION_SIZE);
		
		stormCall(TopologyName, NumberofWorkers, JoinType, isLocal);

	}
	
	public static void stormCall(String TopologyName, int NumberofWorkers, String JoinType, boolean isLocal) throws Exception

	{
		Config config = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		
		//Check the join type
		if(JoinType.equalsIgnoreCase("MV")) {
			
			builder.setSpout("spout_work", new RDFSpoutGridTimebase("Work"),1);
			builder.setSpout("spout_diplome", new RDFSpoutGridTimebase("Diplome"),1);
			builder.setSpout("spout_paper", new RDFSpoutGridTimebase("Paper"),1);
			
			builder.setBolt("bolt_builder1", new  BoltBuilderGridTimebase("Paper", "Paper0"),2).shuffleGrouping("spout_paper");
			builder.setBolt("bolt_prober1", new  BoltProberGridTimebase("MV","WORK","ANY"),3).shuffleGrouping("spout_work").shuffleGrouping("bolt_builder1").shuffleGrouping("bolt_prober2");
			builder.setBolt("bolt_prober2", new  BoltProberGridTimebase("MV","Diplome","ANY"),3).shuffleGrouping("spout_diplome").allGrouping("bolt_builder1").allGrouping("bolt_prober1");
			
			TopologyConfiguration.NUMBER_BF1 = 2;
			TopologyConfiguration.NUMBER_BF2 = 3;
			
		}
		else if(JoinType.equalsIgnoreCase("2V")) {
			
			builder.setSpout("spout_work", new RDFSpoutGridTimebase("Work"),1);
			builder.setSpout("spout_diplome", new RDFSpoutGridTimebase("Diplome"),1);
			builder.setSpout("spout_paper", new RDFSpoutGridTimebase("Paper"),1);
			
			builder.setBolt("bolt_builder1", new  BoltBuilderGridTimebase("Paper", "Paper0"),2).shuffleGrouping("spout_paper");
			builder.setBolt("bolt_builder2", new  BoltBuilderGridTimebase("Work", "Place0"),2).shuffleGrouping("spout_work");
			builder.setBolt("bolt_prober", new  BoltProberGridTimebase("2V","Diplome","ANY"),3).shuffleGrouping("spout_diplome").allGrouping("bolt_builder1").allGrouping("bolt_builder2");
			
			TopologyConfiguration.NUMBER_BF1 = 2;
			TopologyConfiguration.NUMBER_BF2 = 2;
		}
		else {
			
			builder.setSpout("spout_work", new RDFSpoutGridTimebase("Work"),1);
			builder.setSpout("spout_diplome", new RDFSpoutGridTimebase("Diplome"),1);
			builder.setSpout("spout_paper", new RDFSpoutGridTimebase("Paper"),1);
			builder.setBolt("bolt_builder1", new  BoltBuilderGridTimebase("Paper", "Paper0"),2).shuffleGrouping("spout_paper");
			builder.setBolt("bolt_builder2", new  BoltBuilderGridTimebase("Work", "Place0"),2).shuffleGrouping("spout_work");
			builder.setBolt("bolt_prober", new  BoltProberGridTimebase("1V","Diplome","Diplome0"),3).shuffleGrouping("spout_diplome").allGrouping("bolt_builder1").allGrouping("bolt_builder2");
			
			TopologyConfiguration.NUMBER_BF1 = 2;
			TopologyConfiguration.NUMBER_BF2 = 2;
		}
		
		
		if (!isLocal) {
			
			config.setNumWorkers(NumberofWorkers);
			StormSubmitter.submitTopology(TopologyName, config, builder.createTopology());
			
		}
		else {
			config.setDebug(true);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(TopologyName, config, builder.createTopology());
			Thread.sleep(9000);
			cluster.shutdown();
		}
		
	}
	

}
