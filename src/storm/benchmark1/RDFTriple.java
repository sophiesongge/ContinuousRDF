/**
 * @author gsong
 */
package storm.benchmark1;

public class RDFTriple {
	private String Subject;
	private String Predicate;
	private String Object;

	public RDFTriple(String subject, String predicate, String object) {
		this.setSubject(subject);
		this.setPredicate(predicate);
		this.setObject(object);
	}

	public String getSubject() {
		return Subject;
	}

	public void setSubject(String subject) {
		Subject = subject;
	}

	public String getPredicate() {
		return Predicate;
	}

	public void setPredicate(String predicate) {
		Predicate = predicate;
	}

	public String getObject() {
		return Object;
	}

	public void setObject(String object) {
		Object = object;
	}

	public String toString() {
		return "<" + Subject + ", " + Predicate + ", " + Object + ">";
	}

}