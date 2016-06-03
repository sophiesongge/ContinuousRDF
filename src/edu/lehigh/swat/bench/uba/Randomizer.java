package edu.lehigh.swat.bench.uba;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

public class Randomizer {

	public void randomize(){
		String folder = "E:\\Dropbox\\Sander\\College\\RDFProject\\git\\ContinuousRDF\\data";
		System.out.println("Files to randomize are in folder " + folder);
		File directory = new File(folder);
		String[] directoryContents = directory.list();
		for(String file : directoryContents){//for each file
			if(file.substring(file.length() - 5).equals(".daml")){
				//only randomize the .daml files
				//file path: folder + "\\" + file
				try {
					ArrayList<String> fileElements = new ArrayList<String>();
					String curString = "";
					String curLine = null;
					
		            File curFile = new File(folder + "\\" + file);
					FileReader fr = new FileReader(curFile);
		            BufferedReader br = new BufferedReader(fr);
		            
		            //create an arraylist: one element for each RDF triple entry
					while ((curLine = br.readLine()) != null) {
						if(curLine.length() == 0){//empty line
							fileElements.add(curString);
							curString = "";
						}
					    curString = curString + curLine + System.getProperty("line.separator");
					}
					//add the last part
					fileElements.add(curString);
					
		            fr.close();
		            br.close();
		            System.out.println("File: " + file + ", elements: " + fileElements.size());
		            
		            //seperate first and last element, they should stay in the same place as they contain headers and footers
		            int lastIndex = fileElements.size() - 1;
		            String firstElem = fileElements.get(0);
		            String lastElem = fileElements.get(lastIndex);
		            fileElements.remove(0);
		            fileElements.remove(lastIndex - 1);
		            
		            //now randomize the elements
		            Collections.shuffle(fileElements, new Random(System.nanoTime()));
		            
		            //add first and last element again
		            fileElements.add(0, firstElem);
		            fileElements.add(lastElem);
		            
		            //now clear the file
		            PrintWriter writer = new PrintWriter(curFile);
		            writer.print("");
		            
		            //now add all the strings from the fileElements arrayList
		            for(String element : fileElements){
		            	writer.print(element);
		            }
		            writer.close();
		            
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args) {
		Randomizer randomizer = new Randomizer();
		randomizer.randomize();
	}

}
