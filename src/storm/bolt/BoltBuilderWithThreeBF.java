package storm.bolt;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import storm.bloomfilter.BloomFilter;
import storm.topology.TopologyWithThreeBF;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltBuilderWithThreeBF implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter<String> bf1;
	private BloomFilter<String> bf2;
	private int id;
	
	
	String[] predicates = new String[3];
	String[] objects = new String[3];
	String p1,p2,p3,v1,v2,v3;

	
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
		this.id = context.getThisTaskId();
		this.v1 = TopologyWithThreeBF.query.getV1();
		this.v2 = TopologyWithThreeBF.query.getV2();
		this.v3 = TopologyWithThreeBF.query.getV3();
	}
	
	/**
	 * The main method of Bolt, it will be called when the bolt receives a new tuple
	 * It will add the subject to the triple received into the Bloom Filter 
	 */
	public void execute(Tuple input) {
		String Subject = input.getStringByField("Subject");
		String Predicate = input.getStringByField("Predicate");
		String Object = input.getStringByField("Object");
		
		/* call corresponding function if you run
		 * 
		 * if you run oneVariableJoin Query Result will be : [Sophie, Justine, Fabrice]
		 * if you run twoVariableJoin Query Result will be : [Sophie, Justine, Fabrice, Lea]
		 * if you run multiVariableJoin Query Result will be : [Sophie, Justine, Fabrice, Lea, Frederic]
		 * 
		 * */
		
		query(Subject, Predicate, Object);
		//oneVariableJoin(Subject, Predicate, Object);
		//twoVariableJoin(Subject, Predicate, Object);
		//multiVariableJoin(Subject, Predicate, Object);
	}
	public void query(String Subject,String Predicate, String Object) {
		
		//("1-variable join, to find the authors for paper kNN who works in INRIA and who has a Ph.D diplome:");
		//find s where p1=v1,p2=v2,p3=v3;
		
		//("2-variable join, to find the authors for paper kNN who works in INRIA and their diplome:");
		//find s where p1=v1,p2=v2,p3=any;
		
		//("multi-variable join, to find the authors for paper kNN, and the place they work, and their diplome: ");
		//find s where p1=v1,p2=any,p3=any;
		
		String paper="Paper",work="Work",diplome="Diploma";
		String objectPaper="kNN",objectWork="INRIA",objectDiplome="Ph.D";

			
		HashMap<String, String> hmap = new HashMap<String, String>();
		
		hmap.put("Work", objectWork);
		hmap.put("Diploma", objectDiplome);
		hmap.put("Paper", objectPaper);
		// Identify the join type and set values of predicate and objects accordingly
		int countAny=0;
		int index=0;
		Set set = hmap.entrySet();
		Iterator iterator = set.iterator();
		while(iterator.hasNext()) {
			Map.Entry mentry = (Map.Entry)iterator.next();
			if(mentry.getValue().toString().equals("ANY")) {
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
		countAny=0;
		if(countAny==0) {
			oneVariableJoin(Subject, Predicate, Object);
		}
		else if(countAny==1) {
			twoVariableJoin(Subject, Predicate, Object);
		}
		else if(countAny==2) {
			multiVariableJoin(Subject, Predicate, Object);
		}
		else
			System.out.println("Error, con't identify join type");
			

	}

	public void oneVariableJoin(String Subject,String Predicate, String Object) {
		
		if(Predicate.equals(predicates[0])){
			if(Object.equals(objects[0])){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
			}
		}
		else if(Predicate.equals(predicates[1])){
			if(Object.equals(objects[1])){
				collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			}
		}
		else if(Predicate.equals(predicates[2])){
			if(Object.equals(objects[2])){
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
			}
		}
			
	}
	public void twoVariableJoin(String Subject,String Predicate, String Object) {
		
		if(Predicate.equals(predicates[0])){
			if(Object.equals(objects[0])){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
			}
		}
		else if(Predicate.equals(predicates[1])){
			if(Object.equals(objects[1])){
				collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			}
		}
		else if(Predicate.equals(predicates[2])){
			//if(Object.equals(objects[2])){
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
			//}
		}
		
	}
	
	public void multiVariableJoin(String Subject,String Predicate, String Object) {
		
		if(Predicate.equals(predicates[0])){
			if(Object.equals(objects[0])){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
			}
		}
		else if(Predicate.equals(predicates[1])){
			//if(Object.equals(objects[1])){
				collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			//}
		}
		else if(Predicate.equals(predicates[2])){
			//if(Object.equals(objects[2])){
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
			//}
		}
		
	}
	
	/*
	//("1-variable join, to find the authors for paper kNN who works in INRIA and who has a Ph.D diplome:");
	public void oneVariableJoin(String Subject,String Predicate, String Object) {
		
		
		if(Predicate.equals("Diplome")){
<<<<<<< HEAD
			if(Object.equals("Ph.D")){
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
=======
			if(Object.equals(v3)){//for example: Ph.D
				collector.emit(new Values("ProberTaskID_"+id, Subject));
>>>>>>> 845b3e7828e6b001d9c8f0a800202d769461666c
			}
		}else if(Predicate.equals("Work")){
			if(Object.equals(v2)){//for exmaple: INRIA
				collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			}
			
		}else if(Predicate.equals("Paper")){
<<<<<<< HEAD
			if(Object.equals("kNN")){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
=======
			if(Object.equals(v1)){//for example: kNN
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
>>>>>>> 845b3e7828e6b001d9c8f0a800202d769461666c
			}
			
		}
		
	}
	
	//("2-variable join, to find the authors for paper kNN who works in INRIA and their diplome:");
	public void twoVariableJoin(String Subject,String Predicate, String Object) {
		//v3 = ANY
		if(Predicate.equals("Diplome")){
			//if(Object.equals("Ph.D")){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
			//}
			
		}else if(Predicate.equals("Work")){
			if(Object.equals(v2)){
				collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			}
			
		}else if(Predicate.equals("Paper")){
			if(Object.equals(v1)){
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
			}
		}
		
	}
	
	//("multi-variable join, to find the authors for paper kNN, and the place they work, and their diplome: ");
	public void multiVariableJoin(String Subject,String Predicate, String Object) {
		//v3 = v2 = ANY
		if(Predicate.equals("Diplome")){
			//if(Object.equals("Ph.D")){
				collector.emit(new Values("ProberTaskID_"+id, Subject));
			//}
			
		}else if(Predicate.equals("Work")){
			//if(Object.equals("INRIA")){
				collector.emit(new Values("BuilderTaskID_1_"+id, Subject));
			//}
			
		}else if(Predicate.equals("Paper")){
			if(Object.equals(v1)){
				collector.emit(new Values("BuilderTaskID_2_"+id, Subject));
			}
		}
	}
	*/
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ID","Content"));
	}

	public void cleanup() {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
