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
package storm.benchmark2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
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
import storm.config.TopologyConfiguration;
import storm.rdf.RDFTriple;

public class BenchmarkRDFSpout2 extends BaseRichSpout implements Serializable {

	private static final long serialVersionUID = 1L;
	SpoutOutputCollector _collector;

	InputStream in;
	public Model model;
	public static List<Statement> iter_type;
	public static List<Statement> iter_takescourse;
	public static List<Statement> iter_publicationAuthor;
	public static List<Statement> iter_subOrganizationOf;
	
	public static List<Statement> iter_worksFor;
	public static List<Statement> iter_emailAddress;
	public static List<Statement> iter_name;


	public static int index_type=0;
	public static int index_takecourse=0;
	public static int index_publicationAuthor=0;
	public static int index_subOrganizationOf=0;

	public static int index_worksFor=0;
	public static int index_emailAddress=0;
	public static int index_name=0;
	
	String gPredicate;

	int GenerationSize = 20;
	int currentGenerationSize=0;

	public BenchmarkRDFSpout2(String predicate) {
		// TODO Auto-generated constructor stub
		gPredicate = predicate;

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

		//open the file and start the model
		Model model = ModelFactory.createDefaultModel();
		//String inputFileName="./data/University_combined.daml";
		String inputFileName="/Users/uybhatti/DataScience/project/ContinuousRDF/University0_0.daml";
		//String inputFileName="University0_0.daml";
		// use the FileManager to find the input file
		InputStream in = FileManager.get().open( inputFileName );

		if (in == null) {
			throw new IllegalArgumentException("File: " + inputFileName + " not found");
		}

		// read the RDF/XML file
		model.read(in, null);

		// list the statements in the Model
		//iter = model.listStatements();
		iter_type = model.listStatements().toList();
		iter_takescourse = model.listStatements().toList();
		iter_publicationAuthor = model.listStatements().toList();
		iter_subOrganizationOf = model.listStatements().toList();

		iter_worksFor = model.listStatements().toList();
		iter_emailAddress = model.listStatements().toList();
		iter_name = model.listStatements().toList();

	}

	/*
	 * The main method for spout
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	public void nextTuple() {
		Utils.sleep(5);
		generateTuple();

	}

	public void generateTuple(){
		if(gPredicate.equals("type")) {

			if(index_type < iter_type.size()){
				Statement stmt      = iter_type.get(index_type);  // get next statement
				Resource  Subject   = stmt.getSubject();     // get the subject
				Property  Predicate = stmt.getPredicate();   // get the predicate
				RDFNode   Object    = stmt.getObject();      // get the object

				String msgID = String.valueOf(System.currentTimeMillis());
				if(Predicate.toString().contains(gPredicate)) {
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"triple",msgID),msgID);

				}
				currentGenerationSize++;
				if(currentGenerationSize==GenerationSize) {
					currentGenerationSize=0;
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"process",msgID),msgID);
				}
				index_type++;

			}else{
				index_type=0;
				//throw new NullPointerException("No more elements");
			}

		}
		else if(gPredicate.equals("takesCourse")) {

			if(index_takecourse<iter_takescourse.size()) {
				Statement stmt      = iter_takescourse.get(index_takecourse);  // get next statement
				Resource  Subject   = stmt.getSubject();     // get the subject
				Property  Predicate = stmt.getPredicate();   // get the predicate
				RDFNode   Object    = stmt.getObject();      // get the object

				String msgID = String.valueOf(System.currentTimeMillis());
				if(Predicate.toString().contains(gPredicate)) {

					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"triple",msgID),msgID);

				}
				currentGenerationSize++;
				if(currentGenerationSize==GenerationSize) {
					currentGenerationSize=0;
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"process",msgID),msgID);
				}
				index_takecourse++;

			}else{
				index_takecourse=0;
				//throw new NullPointerException("No more elements");
			}

		} 


		else if(gPredicate.equals("publicationAuthor")) {

			if(index_publicationAuthor<iter_publicationAuthor.size()){
				Statement stmt      = iter_publicationAuthor.get(index_publicationAuthor);  // get next statement
				Resource  Subject   = stmt.getSubject();     // get the subject
				Property  Predicate = stmt.getPredicate();   // get the predicate
				RDFNode   Object    = stmt.getObject();      // get the object

				if(Predicate.toString().contains(gPredicate)) {

					String msgID = String.valueOf(System.currentTimeMillis());
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"triple",msgID),msgID);
					currentGenerationSize++;
					if(currentGenerationSize==GenerationSize) {
						currentGenerationSize=0;
						_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"process",msgID),msgID);
					}
				}

				index_publicationAuthor++;

			}else{
				index_publicationAuthor=0;
				//throw new NullPointerException("No more elements");
			}

		}

		else if(gPredicate.equals("subOrganization")) {

			if(index_subOrganizationOf<iter_subOrganizationOf.size()){
				Statement stmt      = iter_subOrganizationOf.get(index_subOrganizationOf);  // get next statement
				Resource  Subject   = stmt.getSubject();     // get the subject
				Property  Predicate = stmt.getPredicate();   // get the predicate
				RDFNode   Object    = stmt.getObject();      // get the object

				if(Predicate.toString().contains(gPredicate)) {

					String msgID = String.valueOf(System.currentTimeMillis());
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"triple",msgID),msgID);
					currentGenerationSize++;
					if(currentGenerationSize==GenerationSize) {
						currentGenerationSize=0;
						_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"process",msgID),msgID);
					}
				}
				index_subOrganizationOf++;

			}else{
				index_subOrganizationOf=0;
				//throw new NullPointerException("No more elements");
			}
		}
		
		else if(gPredicate.equals("emailAddress")) {

			if(index_emailAddress<iter_emailAddress.size()){
				Statement stmt      = iter_emailAddress.get(index_emailAddress);  // get next statement
				Resource  Subject   = stmt.getSubject();     // get the subject
				Property  Predicate = stmt.getPredicate();   // get the predicate
				RDFNode   Object    = stmt.getObject();      // get the object

				if(Predicate.toString().contains(gPredicate)) {

					String msgID = String.valueOf(System.currentTimeMillis());
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"triple",msgID),msgID);
					currentGenerationSize++;
					if(currentGenerationSize==GenerationSize) {
						currentGenerationSize=0;
						_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"process",msgID),msgID);
					}
				}
				index_emailAddress++;

			}else{
				index_emailAddress=0;
				//throw new NullPointerException("No more elements");
			}
		}
		
		else if(gPredicate.equals("name")) {

			if(index_name<iter_name.size()){
				Statement stmt      = iter_name.get(index_name);  // get next statement
				Resource  Subject   = stmt.getSubject();     // get the subject
				Property  Predicate = stmt.getPredicate();   // get the predicate
				RDFNode   Object    = stmt.getObject();      // get the object

				if(Predicate.toString().contains(gPredicate)) {

					String msgID = String.valueOf(System.currentTimeMillis());
					_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"triple",msgID),msgID);
					currentGenerationSize++;
					if(currentGenerationSize==GenerationSize) {
						currentGenerationSize=0;
						_collector.emit(new Values(Subject.toString(),Predicate.toString(),Object.toString(),"process",msgID),msgID);
					}
				}
				index_name++;

			}else{
				index_name=0;
				//throw new NullPointerException("No more elements");
			}
		}
		
		
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Subject","Predicate","Object","id","timestamp"));

	}

}