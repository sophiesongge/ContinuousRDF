package edu.lehigh.swat.bench.uba;

import java.io.File;
import java.util.Arrays;

public class Randomizer {

	public void randomize(){
		String folder = "E:\\Dropbox\\Sander\\College\\RDFProject\\git\\ContinuousRDF\\data";
		System.out.println("Files to randomize are in folder " + folder);
		File directory = new File(folder);
		String[] directoryContents = directory.list();
		for(String file : directoryContents){
			if(file.substring(file.length() - 5).equals(".daml")){
				//only randomize the .daml files
				System.out.println(file);				
			}
		}
		//listfiles();
	}
	
	public static void main(String[] args) {
		Randomizer randomizer = new Randomizer();
		randomizer.randomize();
	}

}
