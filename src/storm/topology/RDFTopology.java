package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.bolt.BoltCreatBF;
import storm.spout.RDFSpout;



public class RDFTopology{
	
public static BufferedReader reader;
	
	public static void main(String[] args) throws Exception{
		
		
		String filePath="./data/rdfdata.txt";
		File file = new File(filePath);
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
	
	public static void stormCall() throws Exception
	{
		Config config = new Config();
		config.setDebug(true);
		
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * Spout to read data from file then it emits tuple as (Subject, Predicate, Object)
		 * Bolts to create bloom filters using fieldsGrouping on Predicate. 
		 * For now we are creating 3 bloomfilters for each predicate.
		*/
		builder.setSpout("spout_getdata", new RDFSpout(),1);
		builder.setBolt("bolt_bloomfilter", new BoltCreatBF(),3).fieldsGrouping("spout_getdata", new Fields("Predicate"));
		
		/*
		 * This bolt is optional, but I have shifted code of this file to spout
		 * now spout is also doing formatting of data.
		 * builder.setBolt("bolt_formatter", new BoltsFormatter(),2).shuffleGrouping("spout_getdata");
		 */

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("RDFStorm", config, builder.createTopology());
		Thread.sleep(10000);
		
		//Sander: cluster shutdown throws IOException, but adding try/catch states that it is an Unreachable catch block for IOException.
		//try{
			cluster.shutdown();	
			//java.io.IOException: Unable to delete file: C:\Users\Sander\AppData\Local\Temp\fb8a88e9-6550-4bbc-b270-dd73892c7a14\version-2\log.1
			//java.io.IOException: Unable to delete file: C:\Users\Sander\AppData\Local\Temp\659c8fe1-de36-4ec3-bf6f-927c664a1fc6\version-2\log.1
		/*} catch(IOException e){
			System.out.println("IOException when shutting down the cluster, continued afterwards, error message: " + e.getMessage());
		}*/

		
		/* Result like this
		Bloom Filter with Predicate = Work has values = 11
		Bloom Filter with Predicate = Paper has values = 5
		Bloom Filter with Predicate = Diplome has values = 13
		*/
	}
	

}
