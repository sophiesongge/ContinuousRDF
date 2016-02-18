package storm.bolt;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import com.sun.javafx.tk.Toolkit;

import storm.bloomfilter.BloomFilter;
import storm.rdf.Query;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltBuilderTimeBase implements IRichBolt {
	
	private OutputCollector collector;
	private static BloomFilter<String> bf1;
	private static BloomFilter<String> bf2;
	private static List<String> problist;
	
	
	private int id;
	
	String[] predicates = new String[3];
	String[] objects = new String[3];
	String[] v = new String[3];//v1, v2 and v3
	String p1,p2,p3;
	
	private int maxGenerationSize=100;
	private static int currentGenerationSize=0;
	private int maxProbListSize=50;
	private int currentProbListSize=0;
	
	String joinType="";
    Timer timer;
	
	public void setQuery(Query q){
		//the query used for filtering the data
		
		this.v[0] = q.getV1();
		this.v[1] = q.getV2();
		this.v[2] = q.getV3();	
	}
	/**
	 * initialization
	 */
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {	
		//initialize the emitter
		this.collector = collector;
		//initialize an empty Bloom Filter with fp=0.001 and maximum_element=20 
		this.bf1 = new BloomFilter(0.01, 10);
		this.bf2 = new BloomFilter(0.01, 10);
		problist = new ArrayList<String>();

		this.id = context.getThisTaskId();
	}
	
	/**
	 * The main method of Bolt, it will be called when the bolt receives a new tuple
	 * It will add the subject to the triple received into the Bloom Filter 
	 */
	public void execute(Tuple input) {
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		String Object = input.getStringByField("Object");
		
		query(Subject, Predicate, Object);
		
		timer = new Timer();
        timer.schedule(new RemindTask(),
                       10000,        //initial delay
                       1*10000);  //subsequent rate
		
	}

	class RemindTask extends TimerTask {
        public void run() {
            if (currentGenerationSize > 0) {
                
            	BloomFilter<String> bf1ToSend=new BloomFilter(bf1);
    			BloomFilter<String> bf2ToSend=new BloomFilter(bf2);
    			List<String> problistToSend=new ArrayList<String>(problist);
    			
    			collector.emit(new Values(joinType,bf1ToSend,bf2ToSend,problistToSend));
    			
    			System.out.println("Join Type is : "+ joinType);
    			System.out.println("Timer is excuted: and generation size is:  " +currentGenerationSize);
    			bf1.clear();
    			bf2.clear();
    			problist.clear();
    			currentGenerationSize=0;
    			currentProbListSize=0;
    			
            } else {
                
            	System.out.println("Timer is exited");
                //System.exit(0);   //Stops the AWT thread 
                                  //(and everything else)
            }
        }
    }
	
	public void query(String Subject,String Predicate, String Object) {
		
		//("1-variable join, to find the authors for paper kNN who works in INRIA and who has a Ph.D diplome:");
		//find s where p1=v1,p2=v2,p3=v3;
		
		//("2-variable join, to find the authors for paper kNN who works in INRIA and their diplome:");
		//find s where p1=v1,p2=v2,p3=any;
		
		//("multi-variable join, to find the authors for paper kNN, and the place they work, and their diplome: ");
		//find s where p1=v1,p2=any,p3=any;
		
		//todo: let the objects be automaticly defined on the user input
		String paper="Paper",work="Work",diplome="Diplome";

		String objectPaper="kNN",objectWork="ANY",objectDiplome="ANY";//start with all values ANY
		
		//define possible objects to possible relations
		String[] paperObjects = {"kNN"};
		String[] workObjects = {"INRIA","ECP"};
		String[] diplomeObjects = {"Ph.D","Master"};
		
		//check for all v values to which relation they belong
		//for now it's case insensitive 
		Boolean found = false;
		for(int i = 0; i < v.length; i++){
			found = false;
			//check if it's a paper object
			for(int j = 0; j < paperObjects.length; j++){
				if(v[i].equalsIgnoreCase(paperObjects[j])){
					found = true;
					objectPaper = v[i];
				}
			}
			
			if(!found){//not a paper object
				//check if it's a work object
				for(int j = 0; j < workObjects.length; j++){
					if(v[i].equalsIgnoreCase(workObjects[j])){
						found = true;
						objectWork = v[i];
					}
				}				
			}
			
			if(!found){//not a paper object, nor a work object
				//check if it's a work object
				for(int j = 0; j < diplomeObjects.length; j++){
					if(v[i].equalsIgnoreCase(diplomeObjects[j])){
						found = true;
						objectDiplome = v[i];
					}
				}				
			}
			//note: if it doesn't fit with a value, the object just stays at any
		}

		//String objectPaper=v1,objectWork=v2,objectDiplome=v3;
			
		HashMap<String, String> hmap = new HashMap<String, String>();
		hmap.put("Paper", objectPaper);
		hmap.put("Work", objectWork);
		hmap.put("Diplome", objectDiplome);
		
		// Identify the join type and set values of predicate and objects accordingly
		int countAny=0;
		int index=0;
		Set set = hmap.entrySet();
		Iterator iterator = set.iterator();
		while(iterator.hasNext()) {
			Map.Entry mentry = (Map.Entry)iterator.next();
			if(mentry.getValue().toString().equalsIgnoreCase("ANY")) {
				predicates[2-countAny]=mentry.getKey().toString();
				objects[2-countAny]=mentry.getValue().toString();
				countAny++;
			}
			else {
				predicates[index]=mentry.getKey().toString();
				objects[index]=mentry.getValue().toString();
				index++;
			}
		}
		
		if(countAny==0) {
			joinType="onevariable";
			oneVariableJoin(Subject, Predicate, Object);
		}
		else if(countAny==1) {
			joinType="twovariable";
			twoVariableJoin(Subject, Predicate, Object);
		}
		else if(countAny==2) {
			joinType="multivariable";
			multiVariableJoin(Subject, Predicate, Object);
		}
		else
			System.out.println("Error, can't identify join type");
		
		
	}

	public void oneVariableJoin(String Subject,String Predicate, String Object) {
		
		if(Predicate.equals(predicates[0])){
			if(Object.equals(objects[0])){
				bf1.add(Subject);
			}
		}
		else if(Predicate.equals(predicates[1])){
			if(Object.equals(objects[1])){
				bf2.add(Subject);
			}
		}
		else if(Predicate.equals(predicates[2])){
			if(Object.equals(objects[2])){
				problist.add(Subject);
			}
		}
		currentGenerationSize++;
		if(currentGenerationSize==maxGenerationSize || currentProbListSize==maxProbListSize)
		{
			BloomFilter<String> bf1ToSend=new BloomFilter(bf1);
			BloomFilter<String> bf2ToSend=new BloomFilter(bf2);
			List<String> problistToSend=new ArrayList<String>(problist);
			
			System.out.println("Bloom Filter 1 contains Lea: "+bf1.contains("Lea"));
			System.out.println("Bloom FilterS1 contains Lea: "+bf1ToSend.contains("Lea"));
			System.out.println("Bloom Filter 2 contains Lea: "+bf2.contains("Lea"));
			System.out.println("Bloom FilterS2 contains Lea: "+bf2ToSend.contains("Lea"));
			
			System.out.println("Bloom Filter 1 : "+bf1.bitset);
			System.out.println("Bloom FilterS1 : "+bf1ToSend.bitset);
			System.out.println("Bloom Filter 2 : "+bf2.bitset);
			System.out.println("Bloom FilterS2 : "+bf2ToSend.bitset);
			
			
			collector.emit(new Values("onevariable",bf1ToSend,bf2ToSend,problistToSend));
			
			bf1.clear();
			bf2.clear();
			problist.clear();
			
			currentGenerationSize=0;
			currentProbListSize=0;
		}
			
	}
	
	public void twoVariableJoin(String Subject,String Predicate, String Object) {
		
		if(Predicate.equals(predicates[0])){
			if(Object.equals(objects[0])){
				bf1.add(Subject);
			}
		}
		else if(Predicate.equals(predicates[1])){
			if(Object.equals(objects[1])){
				bf2.add(Subject);
			}
		}
		else if(Predicate.equals(predicates[2])){
			//if(Object.equals(objects[2])){
				problist.add(Subject);
			//}
		}
		currentGenerationSize++;
		if(currentGenerationSize>=maxGenerationSize || currentProbListSize==maxProbListSize)
		{
			BloomFilter<String> bf1ToSend=new BloomFilter(bf1);
			BloomFilter<String> bf2ToSend=new BloomFilter(bf2);
			List<String> problistToSend=new ArrayList<String>(problist);
			
			collector.emit(new Values("twovariable",bf1ToSend,bf2ToSend,problistToSend));
			bf1.clear();
			bf2.clear();
			problist.clear();
			currentGenerationSize=0;
			currentProbListSize=0;
		}
	}
	
	public void multiVariableJoin(String Subject,String Predicate, String Object) {
		
		if(Predicate.equals(predicates[0])){
			if(Object.equals(objects[0])){
				bf1.add(Subject);
			}
		}
		else if(Predicate.equals(predicates[1])){
			//if(Object.equals(objects[1])){
				bf2.add(Subject);
		}
		
		else if(Predicate.equals(predicates[2])){
			//if(Object.equals(objects[2])){
				problist.add(Subject);
			//}
		}
		
		currentGenerationSize++;
		if(currentGenerationSize==maxGenerationSize || currentProbListSize==maxProbListSize)
		{
			BloomFilter<String> bf1ToSend=new BloomFilter(bf1);
			BloomFilter<String> bf2ToSend=new BloomFilter(bf2);
			List<String> problistToSend=new ArrayList<String>(problist);
			
			collector.emit(new Values("multivariable",bf1ToSend,bf2ToSend,problistToSend));
			bf1.clear();
			bf2.clear();
			problist.clear();
			currentGenerationSize=0;
			currentProbListSize=0;
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("JoinType","bf1","bf2","problist"));
	}

	public void cleanup() {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
