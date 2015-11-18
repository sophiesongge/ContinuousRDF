/**
 * @author Sophie
 */
package storm.bloomfilter;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class BenchmarkCSBF{
	static int elementNo = 36; // The number of elements to evaluate
	static Charset charset = Charset.forName("UTF-8");

	
	public static void printStatus(long start, long end){
		double diff = (end-start)/1000.0;
		System.out.println("The processing time is: "+diff+"s, "+"The speed is: "+(elementNo/diff)+" elements/second");
	}
	
	public static void main(String[] args){
		final Random r = new Random();
		
		//Generate elements
		List<String> elementList = new ArrayList<String>(elementNo);
		for(int i=0; i<elementNo; i++){
			byte[] b = new byte[200];
			r.nextBytes(b);
			elementList.add(new String(b));
		}
		
		//A new Count Based Sliding Window Bloom Filter with false positive 0.001, sliding window size 12, number of generation 3
		CountSlidingBF csbf = new CountSlidingBF(12, 3, 0.001);
		BloomFilter bf = new BloomFilter(0.001, 12);
		
		for(int i=0; i<24; i++){
			csbf.add(elementList.get(i));
		}

		
		for(int i=0; i<csbf.BloomFilter.size(); i++){
			System.out.print(csbf.BloomFilter.getBit(i)+" ");
		}
		System.out.print("\n");
		
		
		for(int i=0; i<24; i++){
			bf.add(elementList.get(i));
		}
		
		for(int i=0; i<bf.size(); i++){
			System.out.print(bf.getBit(i)+" ");
		}
		System.out.println("\n");
		
		System.out.println("When we have 24 elements, and the sliding window size is 12");
		System.out.println(csbf.contains(elementList.get(5))+": csbf doesn't contain this element, because it is in the first genenration");
		System.out.println(bf.contains(elementList.get(3))+": bf contains this element");
		
		System.out.println(csbf.contains(elementList.get(20))+": csbf contains this element, because it is in the second generation");
		System.out.println(bf.contains(elementList.get(20))+": bf contains this element too");
	}
}