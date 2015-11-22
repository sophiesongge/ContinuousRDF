package manager;
import java.util.Random;

import storm.bloomfilter.*;
import storm.bolt.*;
import storm.rdf.*;
import storm.spout.*;
import storm.topology.*;

public class Manager {
	
	String query = "";
	RDFTriple latest = new RDFTriple(" "," "," ");
	RDFSpout = new RDFSpout();
	
	public static void main(String[] args) {
		Manager manager = new Manager();
		manager.start("TODO");
		
		
	}
	
	void start(String input){
		//do stuff with the query
		query = input;
		try {
			stream();
		} catch (InterruptedException e) {
			System.out.println("interruptedException");
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	void stream() throws InterruptedException{
		int wait = 0;
		Random rand = new Random();
		while(true){
			wait = rand.nextInt(10000);//wait somewhere in between 0 and 10 seconds
			Thread.sleep(wait);//emulate randomness when new events occur
			
			//generate RDF triples etc
			//how to use RDFSpout class? Has no return values?
			//store new RDF triple in latest variable
		}
	}	
	
	void read(){
		//read triple latest
		//select what needs to be stored using the query string
		//store needed info, delete the rest
	}

}
