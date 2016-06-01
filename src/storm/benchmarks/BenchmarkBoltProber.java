package storm.benchmarks;

import java.io.FileWriter;
import java.io.IOException;
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

public class BenchmarkBoltProber implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter[] bf1;
	
	private int bf1_index=0;
	

	private List<Tuple> problist;

	private List<String> queryResult;

	String JoinType="1V";
	String Predicate="";
	String PredicateValue="";

	private int maxGenerationSize=30;
	private int currentGenerationSize=0;
	private int slidingWindowNumber=0;


	String[] BuilderPredicates= {"type"};;
	public BenchmarkBoltProber(String jointype, String predicate, String value) {
		// TODO Auto-generated constructor stub
		JoinType=jointype;
		Predicate=predicate;
		PredicateValue=value;
		//BuilderPredicates = builderpredicates;
		
	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;

		this.bf1 = new BloomFilter[3];
		
		for(int i=0;i<3;i++) {
			this.bf1[i]= new BloomFilter(0.01, 10);
		
		}
		
		problist = new ArrayList<Tuple>();
		queryResult = new ArrayList<String>();

	}

	public void execute(Tuple tuple) {

		
		try {
			OneTwoVariableJoin(tuple);

		} catch (IOException e) {

		}

	}

	private void OneTwoVariableJoin(Tuple tuple) throws IOException {

		String id = tuple.getStringByField("id");
		if(id.equals("bf"+BuilderPredicates[0])) {
			bf1[bf1_index%3] = (BloomFilter<String>)tuple.getValueByField("bf");
			bf1_index++;
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

	private void Join(List<Tuple> tuplelist) throws IOException {

		for (Tuple tuple : tuplelist) {

			String Subject = tuple.getStringByField("Subject");
			String Predicate = tuple.getStringByField("Predicate");
			String Object = tuple.getStringByField("Object");

			boolean contains1=false;

			for(int i=0;i<3 && !contains1;i++) {
				contains1 = bf1[i].contains(Subject);
			}


			if(contains1){
				queryResult.add(Subject);

				/*
				try {
					filerwriter.write(Subject+","+Predicate+","+Object);
					filerwriter.write("\n");
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				*/
			}

			
		}


	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id","bf"));
	}

	public void cleanup() {
		System.out.println("Size is: "+queryResult.size()+" Query Result is: "+ queryResult);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
