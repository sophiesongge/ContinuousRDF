package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;

import org.apache.jena.rdf.model.*;
import org.apache.jena.util.FileManager;

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
	
	public static void readFile(){
		/*
		 * Steps todo:
		 * 1: read file
		 * 2: convert the file into interpretable tuples and data files (source file: RDFTopology.java:26)
		 * 3: set up up the topology to read the interpretable data files
		 * 4: run the topology for this 
		 * 5: for each tuple: get the results and write them to a file
		 */
		System.out.println("readFile called");
		// create an empty model
		Model model = ModelFactory.createDefaultModel();
		 String inputFileName="./data/University0_0.daml";
		 // use the FileManager to find the input file
		 InputStream in = FileManager.get().open( inputFileName );
		if (in == null) {
		    throw new IllegalArgumentException("File: " + inputFileName + " not found");
		}
		// read the RDF/XML file
		model.read(in, null);
		// the writer and reader that will be used to pass on to the RDFspout
		String tuples = "";
		// list the statements in the Model
		StmtIterator iter = model.listStatements();
		// print out the predicate, subject and object of each statement
		while (iter.hasNext()) {
		    Statement stmt      = iter.nextStatement();  // get next statement
		    Resource  subject   = stmt.getSubject();     // get the subject
		    Property  predicate = stmt.getPredicate();   // get the predicate
		    RDFNode   object    = stmt.getObject();      // get the object
		    
		    tuples = tuples + subject.toString();
		    tuples = tuples + predicate.toString();
		    if (object instanceof Resource) {
		    	tuples = tuples + object.toString();
		    } else {
		        // object is a literal
		    	tuples = tuples + object.toString();
		    }
		    //add a newline to tuples after each iteration
		    tuples = tuples + "\r\n";
		}
		BufferedReader br = new BufferedReader(new StringReader(tuples));
		RDFTopology topology = new RDFTopology();
		topology.setReader(br);
		try {
			topology.stormCall();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*Query input = new Query("a","b","c");
		
		try {
			return runQuery(input);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null; 
		*/
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
	
	public static void main(String[] args){
		//execute on the topology, not the API.
		//tell apout to read from this API
		
		//API api = new API();
		//api.readFile();
	}
	

}
