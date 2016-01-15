/**
 * @author Sander Breukink
 */
package storm.rdf;

public class Query{
	private static String p1;
	private static String p2;
	private static String p3;
	
	public Query(String input1, String input2, String input3){
		this.setP1(input1);
		this.setP2(input2);
		this.setP3(input3);
	}

	public String getP1() {
		return p1;
	}

	public void setP1(String input1) {
		p1 = input1;
	}

	public String getP2() {
		return p2;
	}

	public void setP2(String input2) {
		p2 = input2;
	}

	public String getP3() {
		return p3;
	}

	public void setP3(String input3) {
		p3 = input3;
	}

	
	public String toString(){
		return "<"+p1+", "+p2+", "+p3+">";
	}
	
	
}