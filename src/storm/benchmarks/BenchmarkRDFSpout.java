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
package storm.benchmarks;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.Map;
import java.util.Random;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.util.FileManager;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.rdf.RDFTriple;

public class BenchmarkRDFSpout extends BaseRichSpout implements Serializable {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;

	InputStream in;
	public static Model model;
	public static StmtIterator iter;
	private File sourceFile = null;
	private FileReader fr;
    private BufferedReader br;

	String gPredicate;

	public BenchmarkRDFSpout(String predicate) {
		// TODO Auto-generated constructor stub
		gPredicate = predicate;
				
		//open the file and start the model
		Model model = ModelFactory.createDefaultModel();
		String inputFileName="./data/University_combined.daml";
		
		// use the FileManager to find the input file
		InputStream in = FileManager.get().open( inputFileName );
		 
		if (in == null) {
		    throw new IllegalArgumentException("File: " + inputFileName + " not found");
		}
		
		// read the RDF/XML file
		model.read(in, null);

		// list the statements in the Model
		StmtIterator iter = model.listStatements();
	}

	/*
	 * @param stormConf: the configuration in the topology
	 * @param context: the context in the topology
	 * @param collector: emit the tuples from spout to bolt
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this._collector = collector;

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
		if(iter.hasNext()){
		    Statement stmt      = iter.nextStatement();  // get next statement
		    Resource  Subject   = stmt.getSubject();     // get the subject
		    Property  Predicate = stmt.getPredicate();   // get the predicate
		    RDFNode   Object    = stmt.getObject();      // get the object
		    			
			_collector.emit(new Values(Subject,Predicate,Object,"triple"));
		}else{
			throw new NullPointerException("No more elements");
		}
		// Here add your code to generate tuples in random order
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Subject","Predicate","Object","id"));

	}

}