package storm.bolt;

import java.util.ArrayList;
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
	private BloomFilter bfp1;
	private BloomFilter bfp2;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.bfp1 = new BloomFilter();
		this.bfp2 = new BloomFilter();

	}

	public void execute(Tuple tuple) {
		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		ArrayList idList = new ArrayList();
		if(id[0].equals("BuilderTaskID")){
			if(!idList.contains(id[1])){
				idList.add(id[1]);
			}
			if(idList.get(0).equals(id[1])){
				bfp1.equals(tuple.getValueByField("Content"));
			}else{
				bfp2.equals(tuple.getValueByField("Content"));
			}
			collector.emit(null);
		}else{
			String str = tuple.getStringByField("Content");
			boolean contains1 = bfp1.contains(str);
			boolean contains2 = bfp2.contains(str);
			if(contains1 && contains2){
				collector.emit(new Values(tuple.getStringByField("Content")));
			}else{
				collector.emit(null);
			}
		}
		
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public void cleanup() {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
