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
	private BloomFilter<String> bfp1;
	private BloomFilter<String> bfp2;
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		this.bfp1 = new BloomFilter(0.01, 10);
		this.bfp2 = new BloomFilter(0.01, 10);

	}

	public void execute(Tuple tuple) {
		String input = tuple.getStringByField("ID");
		String[] id = input.split("_");
		if(id[0].equals("BuilderTaskID")){
			if(id[1].equals("1")){
				bfp1 = (BloomFilter<String>) tuple.getValueByField("Content");
				//bfp1.equals(tuple.getValueByField("Content"));
				//collector.emit(new Values(tuple.getStringByField("Content")));
				//bfp1.add(tuple.getStringByField("Content"));
				/*for(int i=0; i<bfp1.size(); i++){
					System.out.print(bfp1.getBit(i));
				}*/
			}else{
				bfp2 = (BloomFilter<String>) tuple.getValueByField("Content");
				//bfp2.equals(tuple.getValueByField("Content"));
				//collector.emit(new Values(tuple.getStringByField("Content")));
				//bfp2.add(tuple.getStringByField("Content"));
			}
		}else{
			boolean contains1 = bfp1.contains(tuple.getStringByField("Content"));
			boolean contains2 = bfp2.contains(tuple.getStringByField("Content"));
			if(contains1 && contains2){
				collector.emit(new Values(tuple.getStringByField("Content")));
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
