package storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.bloomfilter.BloomFilter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltProber implements IRichBolt {
	private OutputCollector collector;
	private BloomFilter<String> bf1;
	private BloomFilter<String> bf2;
	private List<String> problist;

	private List<String> queryResult;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		this.bf1 = new BloomFilter(0.01, 10);
		this.bf2 = new BloomFilter(0.01, 10);
		queryResult = new ArrayList<String>();

	}

	public void execute(Tuple tuple) {

		String jointype = tuple.getStringByField("JoinType");
		bf1 = (BloomFilter<String>) tuple.getValueByField("bf1");
		bf2 = (BloomFilter<String>) tuple.getValueByField("bf2");

		problist = (List<String>) tuple.getValueByField("problist");
		System.out.println("Current Prob List is: " + problist.toString());

		if (jointype.equalsIgnoreCase("onevariable")) {
			oneVariableJoin(tuple);
		} else if (jointype.equalsIgnoreCase("twovariable")) {
			twoVariableJoin(tuple);
		} else if (jointype.equalsIgnoreCase("multivariable")) {
			multiVariableJoin(tuple);
		} else {
			System.out.println("Error, con't identify join type");
		}

	}

	public void oneVariableJoin(Tuple tuple) {

		for (int i = 0; i < problist.size(); i++) {
			String probitem = problist.get(i);
			System.out.println("Current Size of Bloome Filter 1 is: " + bf1.count());
			System.out.println("Current Size of Bloome Filter 2 is: " + bf2.count());
			boolean mycontains1 = bf1.contains("Sophie");
			System.out.println("Bloome Filter 1 contains Sophie: " + mycontains1);

			boolean contains1 = bf1.contains(probitem);
			boolean contains2 = bf2.contains(probitem);
			if (contains1 && contains2) {
				collector.emit(new Values(probitem));
				queryResult.add(probitem);
			}
		}
	}

	public void twoVariableJoin(Tuple tuple) {

		for (int i = 0; i < problist.size(); i++) {
			String probitem = problist.get(i);
			boolean contains1 = bf1.contains(probitem);
			boolean contains2 = bf2.contains(probitem);
			if (contains1 && contains2) {
				collector.emit(new Values(probitem));
				queryResult.add(probitem);
			}
		}
	}

	public void multiVariableJoin(Tuple tuple) {
		for (int i = 0; i < problist.size(); i++) {
			String probitem = problist.get(i);
			boolean contains1 = bf1.contains(probitem);
			boolean contains2 = bf2.contains(probitem);
			if (contains1 && contains2) {
				collector.emit(new Values(probitem));
				queryResult.add(probitem);
			}
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public void cleanup() {
		System.out.println("Size is: " + queryResult.size() + " Query Result is: " + queryResult);
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
