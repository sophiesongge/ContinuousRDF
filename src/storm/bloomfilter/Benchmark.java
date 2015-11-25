/**
 * A Benchmark to evaluate the performance of BloomFilter
 * @author gsong
 */
package storm.bloomfilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Benchmark {

	private static int elementNo = 80000; // The number of elements to evaluate

	private BloomFilter<String> bf;

	public void printStatus(long start, long end) {
		double diff = (end - start) / 1000.0;
		System.out.println(
				"The processing time is: " + diff + "s, " + "The speed is: " + (elementNo / diff) + " elements/second");
	}

	private void testAdd(List<String> elementList) {
		System.out.print("Evaluation for method add() in BloomFilter: ");
		long start_add = System.currentTimeMillis();
		for (int i = 0; i < elementList.size(); i++) {
			bf.add(elementList.get(i));
		}
		long end_add = System.currentTimeMillis();
		printStatus(start_add, end_add);
	}

	private void testGet(List<String> elementList) {
		long start_contains = System.currentTimeMillis();
		for (int i = 0; i < elementList.size(); i++) {
			bf.contains(elementList.get(i));
		}
		long end_contains = System.currentTimeMillis();
		printStatus(start_contains, end_contains);
	}

	private void testFalsePositiveRate(List<String> addedElements, List<String> newElements) {
		long trueCount = 0;
		for (int i = 0; i < newElements.size(); i++) {

			if (bf.contains(newElements.get(i))) {
				//check it is really a false positive, not efficient at all
				if (!addedElements.contains(newElements.get(i))) {
					trueCount++;
				}
			}

		}
		System.out.println("False Positive rate " + (1.0 * trueCount / newElements.size()));
	}

	private void testSpeed(List<String> elementList, List<String> newElement) {
		this.testAdd(elementList);
		// Test for the method contains()
		System.out.print("Evaluation for method contains() with existing elements: ");
		this.testGet(elementList);

		System.out.print("Evaluation for method contains() with non-existing elements: ");
		this.testGet(newElement);

		System.out.print("Evaluation of false positive rate  ");
		this.testFalsePositiveRate(elementList, newElement);
		// long start_ncontains = System.currentTimeMillis();
		// for (int i = 0; i < elementNo; i++) {
		// bf.contains(newElement.get(i));
		// }
		// long end_ncontains = System.currentTimeMillis();
		// printStatus(start_ncontains, end_ncontains);
	}

	public static void main(String[] args) {
		final Random r = new Random();

		// Generate elements
		List<String> elementList = new ArrayList<String>(elementNo);
		for (int i = 0; i < elementNo; i++) {
			byte[] b = new byte[200];
			r.nextBytes(b);
			elementList.add(new String(b));
		}
	

		List<String> newElement = new ArrayList<String>(elementNo);
		for (int i = 0; i < elementNo; i++) {
			byte[] b = new byte[200];
			r.nextBytes(b);
			newElement.add(new String(b));
		}
		
		Benchmark bench = new Benchmark();

		// A new Bloom Filter with false positive 0.001, and maximum number of
		// elements elementNo
		bench.bf = new BloomFilter<String>(0.001, elementNo);
		System.out.println("Testing " + elementNo + " elements.");
		System.out.println("With k=" + bench.bf.getK() + " hash functions.");

		bench.testSpeed(elementList, newElement);

		/*
		 * //Test for the method containsAll() System.out.println(
		 * "Evaluation for method containsAll() with existing elements: "); long
		 * start_containsAll = System.currentTimeMillis(); for(int i=0;
		 * i<elementNo; i++){ bf.contains(elementList.get(i)); } long
		 * end_containsAll = System.currentTimeMillis();
		 * printStatus(start_containsAll, end_containsAll);
		 * 
		 * System.out.println(
		 * "Evaluation for method containsAll() with non-existing elements: ");
		 * long start_ncontainsAll = System.currentTimeMillis(); for(int i=0;
		 * i<elementNo; i++){ bf.contains(newElement.get(i)); } long
		 * end_ncontainsAll = System.currentTimeMillis();
		 * printStatus(start_ncontainsAll, end_ncontainsAll);
		 */

	}

}