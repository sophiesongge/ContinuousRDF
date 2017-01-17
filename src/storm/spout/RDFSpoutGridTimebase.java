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
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.config.TopologyConfiguration;
import storm.rdf.RDFTriple;
import storm.topology.TopologyCountBase;
import storm.topology.TopologyGridTimebase;

public class RDFSpoutGridTimebase extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;
	Random _rand;
	String Predicate = null;

	int GenerationSize = TopologyConfiguration.GENERATION_SIZE;
	int currentGenerationSize = 0;

	int NumberOfGenerations = TopologyConfiguration.NUMBER_OF_GENERATIONS;
	int currentGenerationNumber = 0;

	boolean alreadyGenerate = false;

	public RDFSpoutGridTimebase(String p) {
		// TODO Auto-generated constructor stub
		Predicate = p;

	}

	/*
	 * @param stormConf: the configuration in the topology
	 * 
	 * @param context: the context in the topology
	 * 
	 * @param collector: emit the tuples from spout to bolt
	 * 
	 * @see backtype.storm.spout.ISpout#open(java.util.Map,
	 * backtype.storm.task.TopologyContext,
	 * backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// to initialize the collector
		this._collector = collector;
		this._rand = new Random();
	}

	/*
	 * The main method for spout
	 * 
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		Utils.sleep(1);
		{
			String Subject = null;
			String Object = null;

			int s = _rand.nextInt(100);
			int w = _rand.nextInt(10);
			int d = _rand.nextInt(30);
			int p = _rand.nextInt(20);

			Subject = "Name" + s;

			if (Predicate.equals("Work")) {
				Object = "Place" + w;
				// Object=(o==0)?"INRIA":"ECP";
				/*
				 * if(GenerationSize-currentGenerationSize<10) { Subject =
				 * "RName"+(GenerationSize-currentGenerationSize); Object =
				 * "INRIA" ; }
				 */

			} else if (Predicate.equals("Diplome")) {
				Object = "Diplome" + d;
				// Object=(o==0)?"Ph.D":"Master";
				/*
				 * if(GenerationSize-currentGenerationSize<10) { Subject =
				 * "RName"+(GenerationSize-currentGenerationSize); Object =
				 * "PhD"; }
				 */
			} else if (Predicate.equals("Paper")) {
				Object = "Paper" + p;
				// Object=(o==0)?"kNN":"hadoop";
				/*
				 * if(GenerationSize-currentGenerationSize<10) { Subject =
				 * "RName"+(GenerationSize-currentGenerationSize); Object =
				 * "kNN" ; }
				 */
			}

			String msgID = String.valueOf(System.currentTimeMillis());
			if (!alreadyGenerate && currentGenerationNumber == NumberOfGenerations - 1
					&& GenerationSize - currentGenerationSize < 21 && GenerationSize - currentGenerationSize > 10) {
				// if( (GenerationSize-currentGenerationSize) < 21 &&
				// (GenerationSize-currentGenerationSize) > 10) {

				Subject = "RName" + (GenerationSize - currentGenerationSize);
				if (Predicate.equals("Work"))
					Object = "INRIA";
				else if (Predicate.equals("Diplome"))
					Object = "PhD";
				else if (Predicate.equals("Paper"))
					Object = "kNN";

				_collector.emit(new Values(Subject, Predicate, Object, "triple", msgID), msgID);

			} else if (currentGenerationSize == GenerationSize) {
				currentGenerationSize = 0;
				currentGenerationNumber++;

				if (Predicate.equals("Work")) {
					_collector.emit(new Values(Subject, Predicate, Object, "process", msgID), msgID);
					Utils.sleep(1);
				} else if (Predicate.equals("Diplome")) {
					Utils.sleep(1);
					_collector.emit(new Values(Subject, Predicate, Object, "process", msgID), msgID);
				} else if (Predicate.equals("Paper")) {
					_collector.emit(new Values(Subject, Predicate, Object, "process", msgID), msgID);
					Utils.sleep(1);
				}

				// _collector.emit(new
				// Values(Subject,Predicate,Object,"process",msgID),msgID);

				if (currentGenerationNumber >= NumberOfGenerations) {
					currentGenerationNumber = 0;
					// alreadyGenerate=true;
					// Utils.sleep(1000);
				}
			} else {
				_collector.emit(new Values(Subject, Predicate, Object, "triple", msgID), msgID);
			}

			currentGenerationSize++;
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("Subject", "Predicate", "Object", "id", "timestamp"));
	}

}