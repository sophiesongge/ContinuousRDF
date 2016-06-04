package storm.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.eclipse.jetty.util.log.Log;

import storm.bloomfilter.BloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.metric.api.CountMetric;

public class BoltProberGrid implements IRichBolt,Serializable  {
	private OutputCollector collector;
	private BloomFilter[] bf1;
	private BloomFilter[] bf2;
	private BloomFilter bf3;
	
	private int bf1_index=0;
	private int bf2_index=0;
	
	private List<Tuple> problist;
	
	private List<String> queryResult;
	
	String JoinType="1V";
	String Predicate="";
	String PredicateValue="";
	
	private int maxGenerationSize=30;
	private int currentGenerationSize=0;
	private int slidingWindowNumber=0;
	
	FileWriter filerwriter=null;
	CountMetric _countMetric;
	
	public BoltProberGrid(String jointype, String predicate, String value) {
		// TODO Auto-generated constructor stub
		JoinType=jointype;
		Predicate=predicate;
		PredicateValue=value;
		
		
		
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
		bf3 = new BloomFilter(0.01, 10);
		problist = new ArrayList<Tuple>();
		queryResult = new ArrayList<String>();
		/*
		try {
			filerwriter = new FileWriter("Results_"+Predicate);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error");
			e.printStackTrace();
		}
		*/
		_countMetric = new CountMetric();
		context.registerMetric("result_count", _countMetric, 5);
	}

	public void execute(Tuple tuple) {
		
		try {
			if(JoinType.equals("MV")) {
				MultiVariableJoin(tuple);
				
			}
			else {
				OneTwoVariableJoin(tuple);
			}
		} catch (IOException e) {
			
		}
			
	}

	private void OneTwoVariableJoin(Tuple tuple) throws IOException {
		
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
			if (JoinType.equalsIgnoreCase("1V")) {
				if(tuple.getValueByField("Object").equals(PredicateValue))
					problist.add(tuple);
			}
			else
				problist.add(tuple);
			
			currentGenerationSize++;
			if(currentGenerationSize==maxGenerationSize) {
				
				List<Tuple> tuplelist=problist;
				Join(tuplelist);
				problist.clear();
				currentGenerationSize=0;
			}
			
		}

	}
	
	private void MultiVariableJoin(Tuple tuple) throws IOException {
		
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
			
			problist.add(tuple);
			bf3.add(tuple.getValueByField("Subject"));
			
			currentGenerationSize++;
			if(currentGenerationSize==maxGenerationSize) {
				
				BloomFilter<String> bf3ToSend=new BloomFilter(bf3);
				collector.emit(new Values("bf2",bf3ToSend));
				bf3.clear();
				
				List<Tuple> tuplelist=problist;
				Join(tuplelist);
				problist.clear();
				currentGenerationSize=0;
			}
			
		}
		
	}
	
	private void Join(List<Tuple> tuplelist) throws IOException {
		/*
		filerwriter.write("\n\n------------------------------------------------------------------\n\n");
		filerwriter.write("Results for Sliding Window: "+slidingWindowNumber++);
		*/
		for (Tuple tuple : tuplelist) {
			
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
			//write resultant tuples/triples from current sliding window in file 
			
			
			if(contains1 && contains2){
				queryResult.add(Subject);
				/*
				try {
					filerwriter.write(Subject+","+Predicate+","+Object);
					filerwriter.write("\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
				_countMetric.incr();
			}
			
		}
		Log.info("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
		System.out.println("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
		queryResult.clear();
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","bf"));
	}

	public void cleanup() {
		//System.out.println("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
