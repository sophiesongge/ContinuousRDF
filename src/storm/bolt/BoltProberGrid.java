package storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.bloomfilter.BloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltProberGrid implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter<String> bf1;
	private BloomFilter<String> bf2;
	private List<String> problist;
	
	private List<String> queryResult;
	
	public BoltProberGrid(String jointype) {
		// TODO Auto-generated constructor stub
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.bf1 = new BloomFilter(0.01, 10);
		this.bf2 = new BloomFilter(0.01, 10);
		queryResult = new ArrayList<String>();

	}

	public void execute(Tuple tuple) {
		
		String id = tuple.getStringByField("id");
		if(id.equals("bf1")) 
			bf1 = (BloomFilter<String>)tuple.getValueByField("bf");
		else if(id.equals("bf2"))
			bf2 = (BloomFilter<String>)tuple.getValueByField("bf");
		else {
			
			String jointype="onevariable";
			if (jointype.equalsIgnoreCase("onevariable")) {
				oneVariableJoin(tuple);
			} else if (jointype.equalsIgnoreCase("twovariable")){
				twoVariableJoin(tuple);
			} else if (jointype.equalsIgnoreCase("multivariable")){
				multiVariableJoin(tuple);
			}
			else {
				System.out.println("Error, con't identify join type");
			}
		}
	
	}

	private void oneVariableJoin(Tuple tuple) {
		
		String Subject = tuple.getStringByField("Subject");
		String Predicate = tuple.getStringByField("Predicate");
		String Object = tuple.getStringByField("Object");
		
		boolean contains1 = bf1.contains(Subject);
		boolean contains2 = bf2.contains(Subject);
		if(contains1 && contains2){
			collector.emit(new Values(Subject));
			queryResult.add(Subject);
		}
	}
	
	private void twoVariableJoin(Tuple tuple) {
		//the same for all 3 for now, keep seperate functions in case we might have to change this.
		variableJoin(tuple);
	}
	
	private void multiVariableJoin(Tuple tuple) {
		//the same for all 3 for now, keep seperate functions in case we might have to change this.
		variableJoin(tuple);
	}
	
	private void variableJoin(Tuple tuple) {
		for(int i=0;i<problist.size();i++)
		{
			String probitem = problist.get(i);
			boolean contains1 = bf1.contains(probitem);
			boolean contains2 = bf2.contains(probitem);
			if(contains1 && contains2){
				collector.emit(new Values(probitem));
				queryResult.add(probitem);
			}
		}			
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		System.out.println("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
