/**
 * @author Sander Breukink
 */
package storm.rdf;

import java.util.ArrayList;

public class Results{
	private ArrayList<String> results = new ArrayList<String>();

	public void add(String s) {
		results.add(s);
	}

	//returns the results as an array (for faster processing)
	public String[] getResults(){
		String [] returnValue = (String[]) results.toArray();
		return returnValue;
	}
	
}