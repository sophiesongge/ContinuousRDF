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
	private BloomFilter[] bf1;
	private BloomFilter[] bf2;
	private int bf1_index=0;
	private int bf2_index=0;
	
	private List<String> problist;
	
	private List<String> queryResult;
	
	public BoltProberGrid(String jointype) {
		// TODO Auto-generated constructor stub
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		
		this.bf1 = new BloomFilter[3];
		this.bf2 = new BloomFilter[3];
		for(int i=0;i<3;i++) {
			this.bf1[i]= new BloomFilter(0.01, 10);
			this.bf2[i]= new BloomFilter(0.01, 10);
		}
		
		
		
		queryResult = new ArrayList<String>();

	}

	public void execute(Tuple tuple) {
		
		String id = tuple.getStringByField("id");
		if(id.equals("bf1")) {
			bf1[bf1_index%3] = (BloomFilter<String>)tuple.getValueByField("bf");
			bf1_index++;
		}
		else if(id.equals("bf2")) {
			bf2[bf2_index%3] = (BloomFilter<String>)tuple.getValueByField("bf");
			bf2_index++;
		}
			
		else {
			//add to list, 20 max size, 
			String jointype="onevariable";
			if (jointype.equalsIgnoreCase("onevariable")) {
				oneVariableJoin(tuple);
			} else if (jointype.equalsIgnoreCase("twovariable")){
				//twoVariableJoin(tuple);
			} else if (jointype.equalsIgnoreCase("multivariable")){
				//multiVariableJoin(tuple);
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
		
		boolean contains1=false;
		boolean contains2 = false;
		
		for(int i=0;i<3 && !contains1;i++) {
			contains1 = bf1[i].contains(Subject);
		}
		for(int i=0;i<3 && !contains2;i++) {
			contains2 = bf2[i].contains(Subject);
		} 
		
		if(contains1 && contains2){
			collector.emit(new Values(Subject));
			queryResult.add(Subject);
		}
		
		//write in file and delete this
	}
	/*
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
	*/
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		System.out.println("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
