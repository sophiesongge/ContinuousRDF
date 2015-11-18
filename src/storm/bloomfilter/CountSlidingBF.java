/**
 * Count based Sliding Window Bloom Filter
 * In this Bloom Filter we want to treat the latest n elements, and we update the Bloom Filter with the information of generation
 * @author gsong
 */
package storm.bloomfilter;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import storm.bloomfilter.BloomFilter;

public class CountSlidingBF<E>{
	
	static final Charset charset = Charset.forName("UTF-8");
	
	public int generationSize; //the maximum number of elements a generation will contain (The number of elements need to be update each time)
	public int numberOfGeneration; //the number of generation in a Sliding Window
	public int slidingWindowSize; //the maximum number of elements a sliding window will contain
	public int elementNumber; //the number of elements we have already treated in a Sliding Window
	public int k; //the number of hash functions
	public BloomFilter BloomFilter; //the Bloom Filter used in this Sliding Window
	public Map<Integer, Integer> Generation; //the Map to store the information of generation, with key:hash value (bitset index); value:generation ---- BloomFilter + Generation = Sliding BF
	
	/**
	 * A constructor
	 * @param slidingWindowSize
	 * @param numberOfGeneration
	 * @param c
	 * @param k
	 */
	public CountSlidingBF(int slidingWindowSize, int numberOfGeneration, double c, int k){
		this.generationSize = (int) Math.ceil(slidingWindowSize/numberOfGeneration);
		this.slidingWindowSize = slidingWindowSize;
		this.numberOfGeneration = numberOfGeneration;
		this.k = k;
		this.BloomFilter = new BloomFilter(c, slidingWindowSize, k);
		this.Generation = new HashMap();
	}
	
	/**
	 * A constructor
	 * @param slidingWindowSize
	 * @param numberOfGeneration
	 * @param bitSetSize
	 */
	public CountSlidingBF(int slidingWindowSize, int numberOfGeneration, int bitSetSize){
		this.slidingWindowSize = slidingWindowSize;
		this.numberOfGeneration = numberOfGeneration;
		this.generationSize = (int) Math.ceil(slidingWindowSize/numberOfGeneration);
		this.BloomFilter = new BloomFilter(bitSetSize, slidingWindowSize);
		this.Generation = new HashMap();
	}
	
	/**
	 * A constructor
	 * @param slidingWindowSize
	 * @param numberOfGeneration
	 * @param falsePositive
	 */
	public CountSlidingBF(int slidingWindowSize, int numberOfGeneration, double falsePositive){
		this.slidingWindowSize = slidingWindowSize;
		this.numberOfGeneration = numberOfGeneration;
		this.generationSize = (int) Math.ceil(slidingWindowSize/numberOfGeneration);
		this.BloomFilter = new BloomFilter(falsePositive, slidingWindowSize);
		this.Generation = new HashMap();
	}
	
	/**
	 * A method to add bytes in CSBF
	 * @param bytes
	 */
	public void add(byte[] bytes){
		int generationNumber = (int) Math.floor(elementNumber / generationSize) % numberOfGeneration;
		if(elementNumber > slidingWindowSize){
			if(elementNumber % generationSize == 0){
				ArrayList<Integer> index = new ArrayList<Integer>();
				for(Integer key : Generation.keySet()){
					if(Generation.get(key)==generationNumber){
						index.add(key);
					}
				}
				for(int i=0; i<index.size(); i++){
					BloomFilter.setBit(index.get(i), false);
				}
			}
		}
		int[] hashes = this.BloomFilter.createHashes(bytes, this.BloomFilter.getK());
		//System.out.println(generationNumber);
		for(int hash : hashes){
			this.Generation.put(Math.abs(hash % this.BloomFilter.bitSetSize), generationNumber);
		}
		
		BloomFilter.add(bytes);
		elementNumber ++;
		
	}
	
	/**
	 * A method to add strings in CSBF
	 * @param str
	 */
	public void add(String str){
		add(str.getBytes(charset));
	}
	
/*	public void add(String str){
		int[] hashes = this.BloomFilter.createHashes(str.getBytes(charset), this.BloomFilter.getK());
		int generationNumber = (int)Math.floor(elementNumber / generationSize);
		for(int hash : hashes){
			this.Generation.put(Math.abs(hash % this.BloomFilter.bitSetSize), generationNumber);
		}
		ArrayList<Integer> index = new ArrayList<Integer>();
		for(Integer key : Generation.keySet()){
			if(Generation.get(key)==0){
				index.add(key);
			}
		}
		BloomFilter.add(str);
		elementNumber ++;
		if(elementNumber > slidingWindowSize){
			for(int i=0; i<index.size(); i++){
				BloomFilter.setBit(index.get(i), false);
			}
			elementNumber = 1;
		}
	}*/
	
	/**
	 * A method to add elements in CSBF
	 * @param element
	 */
	public void add(E element) {
	       add(element.toString().getBytes(charset));
	    }
	
	/**
	 * A method to test whether a bytes[] is contained in the CSBF
	 * @param bytes
	 * @return
	 */
	 public boolean contains(byte[] bytes) {
	        return this.BloomFilter.contains(bytes);
	    }
	 
	 /**
	  * A method to test whether an object is contained in the CSBF
	  * @param element
	  * @return
	  */
	 public boolean contains(E element) {
	        return this.BloomFilter.contains(element);
	    }
	 
	 /**
	  * A method to test whether a string is contained in the CSBF
	  * @param str
	  * @return
	  */
	 public boolean contains(String str){
		 return this.BloomFilter.contains(str.getBytes(charset));
	 }
	
}