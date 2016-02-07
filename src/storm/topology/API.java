package storm.topology;

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

public class API {
	public static Query query;
	
	public List<Tuple> singleVarJoin(String var) throws InterruptedException{		
		return multiVarJoin(var, "ANY", "ANY");
	}
	
	public List<Tuple> doubleVarJoin(String var1,String var2) throws InterruptedException{		
		return multiVarJoin(var1, var2, "ANY");		
	}
	
	public List<Tuple> multiVarJoin(String var1, String var2, String var3) throws InterruptedException{
		String[] returnval = new String[]{};
		query = new Query(var1,var2,var3);		
		Config config = new Config();
		
		TopologyBuilder builder = new TopologyBuilder();
		
		BoltBuilderWithThreeBF boltBuilder = new BoltBuilderWithThreeBF();
		boltBuilder.setQuery(query);
		
		builder.setSpout("spout_getdata", new RDFSpoutWithThreeBF(),1);
		builder.setBolt("bolt_builder", boltBuilder,3).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		//builder.setBolt("bolt_builder", new BoltBuilderWithThreeBF(),3).customGrouping("spout_getdata",new PredicateGrouping());
		builder.setBolt("bolt_prober", new BoltProberWithThreeBF(),1).shuffleGrouping("bolt_builder");
		
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
		return null;
		//return boltProber.queryResult;
		//how to obtain the actual results? Getting boltProber.queryResult gives nullPointerException
	}
}
