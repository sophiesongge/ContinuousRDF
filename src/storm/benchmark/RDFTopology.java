package storm.benchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class RDFTopology{
	
	/**
	 * Feedback Sophie: don't take subqueries, makes results uncomparable with others, use Queries 1, 3, 4, 5, 10, 11
	 * For query 4: add more bolts
	 * Start with the simple ones (4th later)
	 * 
	 * Extra package for benchmark tests, for testing, images of the queries are at http://swat.cse.lehigh.edu/projects/lubm/lubm.jpg
	 * The original text queries can be found at http://swat.cse.lehigh.edu/projects/lubm/queries-sparql.txt
	 * 
	 */
	
public static BufferedReader reader;
	
	public static void main(String[] args) throws Exception{
		System.out.println("Benchmark test");
		String filePath="./data/rdfdata.txt";
		//String filePath="./data/generated_data/University0_0.daml";
		File file = new File(filePath);
		reader = null;
		reader = null;
		try{
			reader = new BufferedReader(new FileReader(file));
			stormCall();
				
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
	}
	
	/**
	 * The function that calls all the other functions and keeps the overview
	 * @throws InterruptedException if the thread.sleep(10000) gets interrupted
	 */
	public static void stormCall() throws InterruptedException{

		System.out.println("Stormcall");
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * Spout to read data from file then it emits tuple as (Subject, Predicate, Object)
		 * Bolts to create bloom filters using fieldsGrouping on Predicate. 
		 * For now we are creating 3 bloomfilters for each predicate.
		*/
		builder.setSpout("spout_getdata", new RDFSpout(),1);
		//write own bolt, insert here, 3 is the parallelism factor of the bolts
		//now creates a bloomfilter for every triple
		//3 bolts: one for work/paper/diplome
		//for benchmark: need 2 bolts, one for prob and one for built
		//The one for prob needs to receive triples from the spout and also bloomfilter from the other bolt (to do this change bolt
		builder.setBolt("bolt_bloomfilter", new BoltCreatBF(),3).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		//BoltCreateTest() handles new data after the Bloomfilters have been created
		builder.setBolt("bolt_test", new BoltTest(),1).shuffleGrouping("spout_getdata");
		//use API of Jena to treat data
		//fieldgrouping: if we have P1, P2 and P3, then P1 always goes to bolt 1, P2 always goes to bolt 2 and P3 always goes to bolt 3, alternativly: shufflegroup to shuffle.
		
		/*
		 * This bolt is optional, but I have shifted code of this file to spout
		 * now spout is also doing formatting of data.
		 * builder.setBolt("bolt_formatter", new BoltsFormatter(),2).shuffleGrouping("spout_getdata");
		 */

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("RDFStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		//Sander: cluster shutdown throws IOException, but adding try/catch states that it is an Unreachable catch block for IOException.
		try{
			cluster.shutdown();	
			throw new IOException("test");//Used as debug, otherwise we got the error saying this block couldn't generate an IOException
		} catch(IOException e){
			System.out.println("IOException when shutting down the cluster, continued afterwards, error message: " + e.getMessage());
		}
	
		
		/* Result like this
		Bloom Filter with Predicate = Work has values = 11
		Bloom Filter with Predicate = Paper has values = 5
		Bloom Filter with Predicate = Diplome has values = 13
		*/
	}
	

}
