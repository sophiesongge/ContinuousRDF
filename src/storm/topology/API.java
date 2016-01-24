package storm.topology;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.bolt.BoltBuilderWithThreeBF;
import storm.bolt.BoltProberWithThreeBF;
import storm.rdf.Query;
import storm.spout.RDFSpoutWithThreeBF;

public class API {
	
	public String[] singleVarJoin(String var){		
		return multiVarJoin(var, "ANY", "ANY");
	}
	
	public String[] doubleVarJoin(String var1,String var2){		
		return multiVarJoin(var1, var2, "ANY");		
	}
	
	public String[] multiVarJoin(String var1, String var2, String var3){
		String[] returnval = new String[]{};
		Query query = new Query(var1,var2,var3);		
		Config config = new Config();
		BoltBuilderWithThreeBF boltBuilder = new BoltBuilderWithThreeBF();
		
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout_getdata", new RDFSpoutWithThreeBF(),3);
		builder.setBolt("bolt_builder", boltBuilder,1).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		builder.setBolt("bolt_prober", new BoltProberWithThreeBF(),1).shuffleGrouping("bolt_builder");
		
		return boltBuilder.results.getResults();
	}
}
