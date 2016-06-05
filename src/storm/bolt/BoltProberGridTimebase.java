package storm.bolt;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.jena.base.Sys;
import org.apache.storm.shade.org.eclipse.jetty.util.log.Log;

import storm.bloomfilter.BloomFilter;
import storm.bolt.BoltBuilderTimeBase.RemindTask;
import storm.config.TopologyConfiguration;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.metric.api.CountMetric;

public class BoltProberGridTimebase implements IRichBolt  {
	private OutputCollector collector;
	private int id;

	private ArrayList<BloomFilter[]> bf1;
	private ArrayList<BloomFilter[]> bf2;
	private BloomFilter bf3;

	private ArrayList<String> bf1_ids;
	private ArrayList<String> bf2_ids;

	private int[] bf1_index;
	private int[] bf2_index;

	private ArrayList<Tuple> problist[];
	private int problist_index=0;

	private List<String> queryResult;

	String JoinType="1V";
	String Predicate="";
	String PredicateValue="";

	private int NUM_BF1 = TopologyConfiguration.NUMBER_BF1;
	private int NUM_BF2 = TopologyConfiguration.NUMBER_BF2;
	private int GenerationSize = TopologyConfiguration.GENERATION_SIZE;
	private int NumberOfGenerations = TopologyConfiguration.NUMBER_OF_GENERATIONS;

	public BoltProberGridTimebase(String jointype, String predicate, String value) {
		// TODO Auto-generated constructor stub
		JoinType=jointype;
		Predicate=predicate;
		PredicateValue=value;

	}

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

		this.collector = collector;
		this.id =  context.getThisTaskId();

		this.bf1 = new ArrayList<BloomFilter[]>();
		this.bf2 = new ArrayList<BloomFilter[]>();

		this.bf1_ids = new ArrayList<String>();
		this.bf2_ids = new ArrayList<String>();

		this.bf1_index = new int[NUM_BF1];
		
		if(JoinType.equalsIgnoreCase("MV")){
			this.bf2_index = new int[3];
		}
		else
			this.bf2_index = new int[2];
		

		bf3 = new BloomFilter(0.01, GenerationSize);
		
		problist = new ArrayList[NumberOfGenerations];
		for(int i=0;i<NumberOfGenerations;i++) {
			problist[i] = new ArrayList<Tuple>();
		}
		//problist = new ArrayList<ArrayList<Tuple>>();
		//problist = new ArrayList<Tuple>();
		queryResult = new ArrayList<String>();

	}

	public void execute(Tuple tuple) {

		String tripleID = tuple.getStringByField("id");
		if(tripleID.equals("process")) {
			ProcessGeneration();
		}
		else {
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


	}

	private void OneTwoVariableJoin(Tuple tuple) throws IOException {

		String id = tuple.getStringByField("id");

		if(id.contains("bf1")) {

			if(bf1_ids.size()==0) {
				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf1.add(bf);
				bf1_ids.add(id);
				bf1_index[0]=1;
			}

			else if(bf1_ids.contains(id)) {
				int bfnumber = bf1_ids.indexOf(id);
				bf1.get(bfnumber)[bf1_index[bfnumber]%NumberOfGenerations] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf1_index[bfnumber] = (bf1_index[bfnumber] +1)%NumberOfGenerations;

			}
			else if(! bf1_ids.contains(id)){
				bf1_ids.add(id);
				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf1.add(bf);
				bf1_index[0]=1;
			}

		}

		if(id.contains("bf2")) {
			if(bf2_ids.size()==0) {

				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf2.add(bf);
				bf2_ids.add(id);
				bf2_index[0]=1;
			}

			else if(bf2_ids.contains(id)) {
				int bfnumber = bf2_ids.indexOf(id);
				bf2.get(bfnumber)[bf2_index[bfnumber]%NumberOfGenerations] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf2_index[bfnumber] = (bf2_index[bfnumber] +1)%NumberOfGenerations;
			}
			else if(! bf2_ids.contains(id)){
				bf2_ids.add(id);
				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf2.add(bf);
				bf2_index[0]=1;
			}

		}

		else if (id.equals("triple")){ 
			if (JoinType.equalsIgnoreCase("1V")) {
				if(tuple.getValueByField("Object").equals(PredicateValue))
					problist[problist_index].add(tuple);
			}
			else
				problist[problist_index].add(tuple);

		}

	}

	private void MultiVariableJoin(Tuple tuple) throws IOException {

		String id = tuple.getStringByField("id");

		if(id.contains("bf1")) {

			if(bf1_ids.size()==0) {
				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf1.add(bf);
				bf1_ids.add(id);
				bf1_index[0]=1;
			}

			else if(bf1_ids.contains(id)) {
				int bfnumber = bf1_ids.indexOf(id);
				bf1.get(bfnumber)[bf1_index[bfnumber]%NumberOfGenerations] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf1_index[bfnumber] = (bf1_index[bfnumber]+1)%NumberOfGenerations;

			}
			else if(! bf1_ids.contains(id)){
				bf1_ids.add(id);
				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf1.add(bf);
				bf1_index[0]=1;
			}

		}

		if(id.contains("bf2")) {
			if(bf2_ids.size()==0) {

				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf2.add(bf);
				bf2_ids.add(id);
				bf2_index[0]=1;
			}

			else if(bf2_ids.contains(id)) {
				int bfnumber = bf2_ids.indexOf(id);
				bf2.get(bfnumber)[bf2_index[bfnumber]%NumberOfGenerations] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf2_index[bfnumber] = (bf2_index[bfnumber]+1)%NumberOfGenerations;
			}
			else if(! bf2_ids.contains(id)){
				bf2_ids.add(id);
				BloomFilter[] bf = new BloomFilter[NumberOfGenerations];
				for(int i=0;i<NumberOfGenerations;i++) {
					bf[i]= new BloomFilter(0.01, GenerationSize);
				}
				bf[0] = (BloomFilter<String>)tuple.getValueByField("bf");
				bf2.add(bf);
				bf2_index[0]=1;
			}

		}

		else if (id.equals("triple")){

			problist[problist_index].add(tuple);
			bf3.add(tuple.getValueByField("Subject"));

		}

	}

	void ProcessGeneration() {

		if(this.JoinType.equals("MV")) {
			BloomFilter<String> bf3ToSend=new BloomFilter(bf3);
			collector.emit(new Values("bf2"+this.id,bf3ToSend));
			bf3.clear();
		}

		for(int i=0;i<NumberOfGenerations;i++) {
			List<Tuple> tuplelist=problist[i];
			try {
				Join(tuplelist);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		problist_index = (problist_index+1) % NumberOfGenerations;
	}

	private void Join(List<Tuple> tuplelist) throws IOException {

		for (Tuple tuple : tuplelist) {

			String Subject = tuple.getStringByField("Subject");
			String Predicate = tuple.getStringByField("Predicate");
			String Object = tuple.getStringByField("Object");

			boolean contains1=false;
			boolean contains2 = false;

			for(int i=0;i<NUM_BF1 && i< bf1.size() && !contains1;i++) {
				for(int j=0;j<NumberOfGenerations && !contains1;j++) {
					contains1 =  bf1.get(i)[j].contains(Subject);
				}
			}
			for(int i=0;i<NUM_BF2 && i< bf2.size() && !contains2;i++) {
				for(int j=0;j<NumberOfGenerations && !contains2;j++) {
					contains2 =  bf2.get(i)[j].contains(Subject);
				}
			} 
			//write resultant tuples/triples from current sliding window in file 

			if(contains1 && contains2){
				queryResult.add(Subject);
				
				this.collector.ack(tuple);
			}

		}

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
