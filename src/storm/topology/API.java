package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.bolt.BoltBuilderWithThreeBF;
import storm.bolt.BoltProberWithThreeBF;
import storm.rdf.Query;
import storm.spout.RDFSpoutWithThreeBF;

public class API{
	public static Query query;
		
	public static BufferedReader reader;
	
	public static List<Tuple> multiVarJoin(Tuple q) throws InterruptedException{		
		return stringJoin(q.getFields().get(0), "ANY", "ANY");
	}
	
	public static List<Tuple> doubleVarJoin(Tuple q) throws InterruptedException{		
		return stringJoin(q.getFields().get(0), q.getFields().get(1), "ANY");		
	}
	
	public static List<Tuple> singleVarJoin(Tuple q) throws InterruptedException{
		return stringJoin(q.getFields().get(0), q.getFields().get(1), q.getFields().get(2));
	}
	
	public static List<Tuple> stringJoin(String var1,String var2,String var3) throws InterruptedException{		
		Query input = new Query(var1,var2,var3);
		try {
			return runQuery(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; 
	}
	
	private static List<Tuple> runQuery(Query q) throws Exception{
		
		String filePath="./data/rdfdata.txt";
		File file = new File(filePath);
		reader = null;
		try{
			reader = new BufferedReader(new FileReader(file));
			return stormCall(q);
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
		return null; 
	}	
	
	private static List<Tuple> stormCall(Query query) throws Exception
	{		
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * Spout to read data from file then it emits tuple as (Subject, Predicate, Object)
		 * "bolt_builder" will create Bloom Filters by fields grouping by "Predicate"
		 * "bolt_prober" will probe Bloom Filters
		*/
		
		BoltBuilderWithThreeBF boltBuilder = new BoltBuilderWithThreeBF();
		boltBuilder.setQuery(query);
		
		BoltProberWithThreeBF prober = new BoltProberWithThreeBF();
		
		builder.setSpout("spout_getdata", new RDFSpoutWithThreeBF(true),1);
		builder.setBolt("bolt_builder", boltBuilder,3).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		builder.setBolt("bolt_prober", prober,1).shuffleGrouping("bolt_builder");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("RDFContinuous", config, builder.createTopology());
		Thread.sleep(30000);
		
		//Sander: cluster shutdown throws IOException, but adding try/catch states that it is an Unreachable catch block for IOException.
		try{
			cluster.shutdown();	
			throw new IOException("test");//Used as debug, otherwise we got the error saying this block couldn't generate an IOException
		} catch(IOException e){
			System.out.println("IOException when shutting down the cluster, continued afterwards, error message: " + e.getMessage());
		}
		return prober.queryResult;
	}
	

}
