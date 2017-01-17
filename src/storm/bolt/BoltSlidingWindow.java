package storm.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.bloomfilter.BloomFilter;
import storm.bloomfilter.CountSlidingBF;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BoltSlidingWindow implements IRichBolt {

	private OutputCollector collector;

	public static List<String> queryResult;

	private CountSlidingBF csbf1;
	private CountSlidingBF csbf2;
	private CountSlidingBF csbf3;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;

		queryResult = new ArrayList<String>();

		csbf1 = new CountSlidingBF(10, 2, 0.01);
		csbf2 = new CountSlidingBF(10, 2, 0.01);
		csbf3 = new CountSlidingBF(10, 2, 0.01);
	}

	public void execute(Tuple tuple) {

		String jointype = tuple.getStringByField("JoinType");

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

		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if (id[0].equals("BuilderTaskID")) {
			if (id[1].equals("1")) {
				csbf1.add(tuple.getStringByField("Content"));

			} else {
				csbf2.add(tuple.getStringByField("Content"));
			}
		} else {
			boolean contains1 = csbf1.contains(tuple.getStringByField("Content"));
			boolean contains2 = csbf2.contains(tuple.getStringByField("Content"));
			if (contains1 && contains2) {
				collector.emit(new Values(tuple.getStringByField("Content")));
				queryResult.add(tuple.getStringByField("Content"));
			}
		}
	}

	public void twoVariableJoin(Tuple tuple) {

		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if (id[0].equals("BuilderTaskID")) {
			if (id[1].equals("1")) {
				csbf1.add(tuple.getStringByField("Content"));
			} else {
				csbf2.add(tuple.getStringByField("Content"));
			}
		} else {
			boolean contains1 = csbf1.contains(tuple.getStringByField("Content"));
			boolean contains2 = csbf2.contains(tuple.getStringByField("Content"));
			if (contains1 && contains2) {
				collector.emit(new Values(tuple.getStringByField("Content")));
				queryResult.add(tuple.getStringByField("Content"));
			}
		}
	}

	public void multiVariableJoin(Tuple tuple) {
		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if (id[0].equals("BuilderTaskID")) {
			if (id[1].equals("1")) {
				boolean contains2 = csbf2.contains(tuple.getStringByField("Content"));
				boolean contains3 = csbf3.contains(tuple.getStringByField("Content"));
				if (contains2 && contains3) {
					collector.emit(new Values(tuple.getStringByField("Content")));
					queryResult.add(tuple.getStringByField("Content"));
				} else {
					csbf1.add(tuple.getStringByField("Content"));
				}

			} else {
				boolean contains1 = csbf1.contains(tuple.getStringByField("Content"));
				boolean contains3 = csbf3.contains(tuple.getStringByField("Content"));
				if (contains1 && contains3) {
					collector.emit(new Values(tuple.getStringByField("Content")));
					queryResult.add(tuple.getStringByField("Content"));
				} else {
					csbf2.add(tuple.getStringByField("Content"));
				}
			}
		} else {

			boolean contains1 = csbf1.contains(tuple.getStringByField("Content"));
			boolean contains2 = csbf2.contains(tuple.getStringByField("Content"));
			if (contains1 && contains2) {
				collector.emit(new Values(tuple.getStringByField("Content")));
				queryResult.add(tuple.getStringByField("Content"));
			} else {
				csbf3.add(tuple.getStringByField("Content"));
			}

		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void cleanup() {
		System.out.println("Size is: " + queryResult.size() + " Sliding Window Query Result is: " + queryResult);
	}
}
