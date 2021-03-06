package storm.topology;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Scanner;

//import org.apache.jena.base.Sys;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.bolt.BoltBuilder;
import storm.bolt.BoltBuilderCountBase;
import storm.bolt.BoltProber;
import storm.bolt.BoltProberCountBase;
import storm.grouping.PredicateGrouping;
import storm.rdf.Query;
import storm.spout.RDFSpoutCountBase;
import storm.spout.TestSpout;

public class TopologyCountBase {

	public static BufferedReader reader;

	public static Query query;

	private static Scanner user_input;

	public static void main(String[] args) throws Exception {

		String filePath = "./data/rdfdata.txt";
		System.out.println("Please give the dat file path as argument");
		if (args == null || args.length <= 0) {

			System.out.println("Please give the dat file path as argument");
			return;

			// StormSubmitter.
		}

		InputStream is = ClassLoader.getSystemResourceAsStream("rdfdata.txt");

		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

		File file = new File(filePath);
		reader = null;
		try {
			// reader = new BufferedReader(new FileReader(file));
			reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
			stormCall(args);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		} catch (IOException e) {
			// do nothing
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
					// Do nothing
				}
			}
		}
	}

	public static void stormCall(String[] args) throws Exception

	{

		user_input = new Scanner(System.in);
		System.out.println("hello--First value? (put in ANY for any possible value)");
		String v1 = user_input.next();
		System.out.println("Second value? (put in ANY for any possible value)");
		String v2 = user_input.next();
		System.out.println("Third value? (put in ANY for any possible value)");
		String v3 = user_input.next();
		query = new Query(v1, v2, v3);

		Config config = new Config();
		config.setDebug(true);
		// Logger LOG = LoggerFactory.getLogger(RDFSpoutCountBase.class);

		TopologyBuilder builder = new TopologyBuilder();

		/*
		 * Spout to read data from file then it emits tuple as (Subject,
		 * Predicate, Object) "bolt_builder" will create Bloom Filters by fields
		 * grouping by "Predicate" "bolt_prober" will probe Bloom Filters
		 */
		BoltBuilderCountBase boltBuilder = new BoltBuilderCountBase();
		boltBuilder.setQuery(query);

		builder.setSpout("spout_getdata", new RDFSpoutCountBase(), 1);
		builder.setBolt("bolt_builder", boltBuilder, 3).customGrouping("spout_getdata", new PredicateGrouping());
		builder.setBolt("bolt_prober", new BoltProberCountBase(), 1).shuffleGrouping("bolt_builder");

		// -----------------------
		builder.setSpout("spout_1", new RDFSpoutCountBase(), 1);
		builder.setSpout("spout_2", new RDFSpoutCountBase(), 1);
		builder.setSpout("spout_3", new RDFSpoutCountBase(), 1);

		builder.setBolt("bolt_builder1", boltBuilder, 1).shuffleGrouping("spout_1");
		builder.setBolt("bolt_builder2", boltBuilder, 1).shuffleGrouping("spout_2");
		builder.setBolt("bolt_prober", boltBuilder, 1).shuffleGrouping("spout_3").shuffleGrouping("bolt_builder1")
				.shuffleGrouping("bolt_builder2");

		// -----------------------

		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], config, builder.createTopology());

			// StormSubmitter.
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("RDFContinuous", config, builder.createTopology());
			Thread.sleep(30000);

			cluster.shutdown();
		}

	}

}
