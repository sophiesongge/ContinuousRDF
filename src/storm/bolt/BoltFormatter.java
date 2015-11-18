package storm.bolt;

import java.util.HashMap;
import java.util.Map;

import storm.rdf.RDFTriple;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltFormatter implements IRichBolt {
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public void execute(Tuple input) {
		
		String rawTuple = input.getString(0);
		String parts[] = rawTuple.split(" +");
		String Subject = parts[0];				
		String Predicate = parts[1];
		String Object = parts[2];
		
		RDFTriple rdf = new RDFTriple(Subject, Predicate, Object);
		rdf.setSubject(Subject);
		rdf.setPredicate(Predicate);
		rdf.setObject(Object);
		
		collector.emit(new Values(Subject, Predicate, Object));
		collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Subject","Predicate","Object"));
	}

	public void cleanup() {
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
