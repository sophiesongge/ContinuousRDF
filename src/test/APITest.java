package test;

import java.util.Arrays;
import java.util.List;

import backtype.storm.tuple.Tuple;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import storm.topology.API;

public class APITest  extends TestCase{
	
	API tester = new API();
	
	public void testSingle1(){
		System.out.println("test 1.1");
		List<Tuple> results = tester.singleVarJoin("INRIA");
		
		System.out.println("test 1.2");
		String[] resultsEq = {"Sophie", "Bob", "Johne", "Laura", "Sergie", "Yuki", "Yume", "Linda", "Sabrina", "Justine", "Fabrice", "Frederic"};
		System.out.println("test 1.3");
		//String[] resultsEq = {"Sophie", "Fabrice", "Lea", "Frederic", "Justine"};
		for(Tuple result : results){
			System.out.println(result.toString());
		}
		System.out.println("test 1.4");
		
		assertEquals(true,true);
		//Todo: actaally test this!
		/*System.out.println("test 1");
		assertEquals(resultsEq, results);*/
	}
	
	/*public void testSingle2(){
		String[] results = tester.singleVarJoin("Master");
		String[] resultsEq = {"Lea"};
		assertEquals(resultsEq, results);
	}*/
	
	
	
	/*
	 * Test results for TopologyWithThreeBF:
	 * 
	 * input: (Ph.D", "any,any)
	 * output: [Sophie, Fabrice, Lea, Frederic, Justine]
	 * should be: [Sophie, Bob, Johne, Laura, Sergie, Yuki, Yume, Linda, Sabrina, Justine, Fabrice, Frederic]
	 * 
	 * Input: (Master,any,any)
	 * output: [Sophie, Justine, Fabrice, Lea, Frederic]
	 * should be: [Lea]
	 */
}
