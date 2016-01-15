/**
 * @author Sander Breukink
 */
package storm.rdf;

public class Query{
	private static String v1;
	private static String v2;
	private static String v3;
	
	public Query(String input1, String input2, String input3){
		this.setV1(input1);
		this.setV2(input2);
		this.setP3(input3);
	}

	public String getV1() {
		return v1;
	}

	public void setV1(String input1) {
		v1 = input1;
	}

	public String getV2() {
		return v2;
	}

	public void setV2(String input2) {
		v2 = input2;
	}

	public String getV3() {
		return v3;
	}

	public void setP3(String input3) {
		v3 = input3;
	}

	
	public String toString(){
		return "<"+v1+", "+v2+", "+v3+">";
	}
	
	
}