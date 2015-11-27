/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.spout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.rdf.RDFTriple;
import storm.topology.TestTopology;

public class TestSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	BufferedReader _reader; 
	
	/*
	 * @param stormConf: the configuration in the topology
	 * @param context: the context in the topology
	 * @param collector: emit the tuples from spout to bolt
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		//to initialize the collector
		this._collector = collector;
		this._rand = new Random();
		//to read the input file
		this._reader = TestTopology.reader;
	}
	
	/*
	 * The main method for spout
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		Utils.sleep(100);
		generateTuple();
	}

	public void generateTuple(){
		try{
			String tempsString = null;
			while((tempsString = _reader.readLine())!=null){
				String parts[] = tempsString.split(" +");
				String Subject = parts[0];				
				String Predicate = parts[1];
				String Object = parts[2];
				
				RDFTriple rdf = new RDFTriple(Subject, Predicate, Object);
				rdf.setSubject(Subject);
				rdf.setPredicate(Predicate);
				rdf.setObject(Object);
				
				//emit every line, Values is an instance of ArrayList
				_collector.emit(new Values(Subject, Predicate, Object));
				
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			System.out.println("Job is finished of this spout");
			Utils.sleep(10000);
		}
	}
	
	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields("RDFtuple"));
		declarer.declare(new Fields("Subject","Predicate","Object"));
		//declarer.declare(new Fields("tuple"));
	}

}