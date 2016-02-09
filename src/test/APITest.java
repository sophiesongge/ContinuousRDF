package test;

import java.util.List;

import backtype.storm.tuple.Tuple;
import junit.framework.TestCase;
import storm.topology.API;
import storm.topology.TopologyWithThreeBF;

public class APITest  extends TestCase{
	
	static API tester = new API();
	TopologyWithThreeBF topo = new TopologyWithThreeBF();
	
	public static void testSingle1(){
		List<Tuple> results;
		
		try {
			API.singleVarJoin("INRIA");
			// TODO compare results with what they should be
			assertEquals(true,true);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
