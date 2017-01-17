/**
 * @author gsong
 */
package storm.benchmark1;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Collection;

public class BloomFilter<E> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public BitSet bitset;
	public int bitSetSize;
	public double bitsPerElement;
	public int maxElementNumber; // the maximum number of elements can be
									// inserted in the Bloom Filter
	public int elementNumber; // the number of elements actually added into the
								// Bloom Filter
	public int k; // the number of hash functions

	static final Charset charset = Charset.forName("UTF-8"); // to store the
																// hash values
																// as string

	static final String hashName = "MD5"; // The Message Digest Algorithm who
											// can produce a 128-bit (16-byte)
											// hash value. The hash functions
											// can be changed to other hash
											// algorithms (like SHA-1) if needed

	static final MessageDigest digestFunction;
	static { // The digest method is reused between instances
		MessageDigest tmp;
		try {
			tmp = java.security.MessageDigest.getInstance(hashName);
		} catch (NoSuchAlgorithmException e) {
			tmp = null;
		}
		digestFunction = tmp;
	}

	/**
	 * A constructor to construct an empty Bloom Filter, with the length c*m
	 * 
	 * @param c:
	 *            the number of bits used by each element
	 * @param m:
	 *            the maximum number of elements that this Bloom Filter can
	 *            contain
	 * @param k:
	 *            the number of hash functions
	 * 
	 */
	public BloomFilter(double c, int m, int k) {
		this.maxElementNumber = m;
		this.bitsPerElement = c;
		this.k = k;
		this.bitSetSize = (int) Math.ceil(m * c);
		elementNumber = 0;
		this.bitset = new BitSet(bitSetSize);
	}

	/**
	 * A constructor to construct an empty Bloom Filter, with k estimated from
	 * bitSetSize and maxElementNumber
	 * 
	 * @param bitSetSize:
	 *            the total bits used in the Bloom Filter
	 * @param maxElementNumber:
	 *            the maximum number of elements could be inserted in the Bloom
	 *            Filter
	 * 
	 */
	public BloomFilter(int bitSetSize, int maxElementNumber) {
		this(bitSetSize / (double) maxElementNumber, maxElementNumber,
				(int) Math.round((bitSetSize / (double) maxElementNumber) * Math.log(2.0)));

	}

	/**
	 * A constructor to construct an empty Bloom Filter, with: a given false
	 * positive probability, the number of bits used by each elements, and the
	 * number of hash functions is estimated to match the false positive
	 * probability
	 * 
	 * @param falsePositive:
	 *            the false positive probability
	 * @param maxElementNumber:
	 *            the maximum number of elements could be inserted in the Bloom
	 *            Filter
	 * 
	 */
	public BloomFilter(double falsePositive, int maxElementNumber) {
		this(Math.ceil(-(Math.log(falsePositive) / Math.log(2))) / Math.log(2), // c
																				// =
																				// k
																				// /
																				// ln(2)
				maxElementNumber, (int) Math.ceil(-(Math.log(falsePositive) / Math.log(2)))); // k
																								// =
																								// ceil(-log_2(p))
	}

	/**
	 * A constructor to construct a Bloom Filter based on an existing Bloom
	 * Filter
	 * 
	 * @param bitSetSize:
	 *            the total bits used in the Bloom Filter
	 * @param maxElementNumber:
	 *            the maximum number of elements could be inserted in the Bloom
	 *            Filter
	 * @param elementNumber:
	 *            the number of elements actually added into the Bloom Filter
	 * @param filteredData:
	 *            a BitSet representing an existing Bloom filter.
	 * 
	 */
	public BloomFilter(int bitSetSize, int maxElementNumber, int elementNumber, BitSet filteredData) {
		this(bitSetSize, maxElementNumber);
		this.bitset = filteredData;
		this.elementNumber = elementNumber;
	}

	public BloomFilter() {
		// TODO Auto-generated constructor stub
	}

	/**
	 *
	 * A method to generate digests for a bytes array, and split the result into
	 * 4-byte ints, then store them in an Array. The digest function is called
	 * until the required number of ints are produced
	 *
	 * @param data:
	 *            input data.
	 * @param numHash:
	 *            the number of hash values (ints) need to produce.
	 * 
	 * @return an array of int[]
	 * 
	 */
	public static int[] createHashes(byte[] data, int numHash) {
		int[] result = new int[numHash];

		int k = 0;
		byte counter = 0;
		while (k < numHash) {
			byte[] digest;
			synchronized (digestFunction) {
				digestFunction.update(counter);
				counter++;
				digest = digestFunction.digest(data);
			}

			for (int i = 0; i < digest.length / 4 && k < numHash; i++) {
				int h = 0;
				for (int j = (i * 4); j < (i * 4) + 4; j++) {
					h <<= 8;
					h |= ((int) digest[j]) & 0xFF;
				}
				result[k] = h;
				k++;
			}
		}
		return result;
	}

	/**
	 * A method to generate a digest for a bytes array.
	 *
	 * @param data:
	 *            input data.
	 * 
	 * @return digest as long.
	 */
	public static int createHash(byte[] data) {
		return createHashes(data, 1)[0];
	}

	/**
	 * A method to generate a digest for a String
	 * 
	 * @param str:
	 *            the input String
	 * @param charset:
	 *            the encoding of the input String
	 * 
	 * @return digest: as long
	 * 
	 */
	public static int createHash(String str, Charset charset) {
		return createHash(str.getBytes(charset));
	}

	/**
	 * A method to generate a digest for a String
	 *
	 * @param str:
	 *            the input data with UTF-8 encoding
	 * @return digest as long
	 */
	public static int createHash(String str) {
		return createHash(str, charset);
	}

	/**
	 * A method to compare the contents of two instances to see if they are
	 * equal.
	 *
	 * @param obj:
	 *            the object to compare to.
	 * 
	 * @return True if the contents of the objects are equal.
	 */
	public boolean isEquals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final BloomFilter<E> other = (BloomFilter<E>) obj;
		if (this.maxElementNumber != other.maxElementNumber) {
			return false;
		}
		if (this.k != other.k) {
			return false;
		}
		if (this.bitSetSize != other.bitSetSize) {
			return false;
		}
		if (this.bitset != other.bitset && (this.bitset == null || !this.bitset.equals(other.bitset))) {
			return false;
		}
		return true;
	}

	/**
	 * A method to calculate a hash code for this class.
	 * 
	 * @return hash code representing the contents of an instance of this class.
	 */
	public int hashCode() {
		int hash = 7;
		hash = 61 * hash + (this.bitset != null ? this.bitset.hashCode() : 0);
		hash = 61 * hash + this.maxElementNumber;
		hash = 61 * hash + this.bitSetSize;
		hash = 61 * hash + this.k;
		return hash;
	}

	/**
	 * A method to calculate the false positive probability given the number of
	 * inserted elements
	 *
	 * @param numberOfElements
	 *            number of inserted elements.
	 * 
	 * @return probability of a false positive.
	 * 
	 */
	public double getFalsePositiveProbability(double numberOfElements) {
		// (1 - e^(-k * n / m)) ^ k
		return Math.pow((1 - Math.exp(-k * (double) numberOfElements / (double) bitSetSize)), k);

	}

	/**
	 * A method to get the current false positive. The probability is calculated
	 * from the size of the Bloom filter(bitSetSize) and the current number of
	 * elements(elementNumber)
	 *
	 * @return probability of false positives.
	 */
	public double getFalsePositiveProbability() {
		return getFalsePositiveProbability(elementNumber);
	}

	/**
	 * A method to calculate the expected false positive. The probability is
	 * calculated from the maximum number of elements inserted in the Bloom
	 * Filter(maxElementNumber) and the size of the Bloom filter (bitSetSize).
	 *
	 * The value returned by this method is the maximum rate of false positive,
	 * assuming the number of inserted elements equals to the number of maximum
	 * inserted elements.
	 *
	 *
	 * @return maximum false positive.
	 */
	public double maxFalsePositiveProbability() {
		return getFalsePositiveProbability(maxElementNumber);
	}

	/**
	 * A method to initial the Bloom Filter with every bits equals to 0
	 */
	public void clear() {
		bitset.clear();
		elementNumber = 0;
	}

	/**
	 * A method to add an array of bytes to the Bloom Filter.
	 *
	 * @param bytes:
	 *            a bytes array to add to the Bloom Filter.
	 */
	public void add(byte[] bytes) {
		int[] hashes = createHashes(bytes, k);
		for (int hash : hashes)
			bitset.set(Math.abs(hash % bitSetSize), true);
		elementNumber++;
	}

	/**
	 * A method to add an object to the Bloom filter. The output from the
	 * object's toString() method is used as input to the hash functions.
	 *
	 * @param element:
	 *            is an element to register in the Bloom filter.
	 */
	public void add(E element) {
		add(element.toString().getBytes(charset));
	}

	/**
	 * A method to add all elements from a Collection to the Bloom filter
	 * 
	 * @param c:
	 *            Collection of elements.
	 */
	public void addAll(Collection<? extends E> c) {
		for (E element : c)
			add(element);
	}

	/**
	 * A method to see whether a bytes array could have been inserted into the
	 * Bloom Filter Returns true if yes Use getFalsePositiveProbability() to
	 * calculate the false positive probability.
	 *
	 * @param bytes:
	 *            a bytes array to check
	 * 
	 * @return true if the array could have been inserted into the Bloom filter.
	 */
	public boolean contains(byte[] bytes) {
		int[] hashes = createHashes(bytes, k);
		for (int hash : hashes) {
			if (!bitset.get(Math.abs(hash % bitSetSize))) {
				return false;
			}
		}
		return true;
	}

	/**
	 * A method to see whether an element could have been inserted into the
	 * Bloom Filter Returns true if yes. Use getFalsePositiveProbability() to
	 * calculate the false positive probability
	 *
	 * @param element:
	 *            element to check.
	 * 
	 * @return true if the element could have been inserted into the Bloom
	 *         filter.
	 */
	public boolean contains(E element) {
		return contains(element.toString().getBytes(charset));
	}

	/**
	 * A method to see whether all the elements of a Collection could have been
	 * inserted into the Bloom Filter Returns true if yes Use
	 * getFalsePositiveProbability() to calculate the false positive probability
	 * 
	 * @param c:
	 *            elements to check
	 * 
	 * @return true if all the elements in c could have been inserted into the
	 *         Bloom filter
	 */
	public boolean containsAll(Collection<? extends E> c) {
		for (E element : c)
			if (!contains(element))
				return false;
		return true;
	}

	/*
	 * public boolean contains(String str){ return
	 * contains(str.getBytes(charset)); }
	 */

	/**
	 * A method to return the value chosen for K
	 * 
	 * K is the optimal number of hash functions based on the size of the Bloom
	 * filter and the expected number of inserted elements.
	 *
	 * @return optimal k.
	 */
	public int getK() {
		return k;
	}

	/**
	 * A method to read a single bit from the Bloom filter.
	 * 
	 * @param bit:
	 *            the bit to read
	 * 
	 * @return true if the bit is set, false if it is not
	 */
	public boolean getBit(int bit) {
		return bitset.get(bit);
	}

	/**
	 * A method to set a single bit in the Bloom filter.
	 * 
	 * @param bit:
	 *            the bit to set.
	 * @param value:
	 *            true, if the bit is set; false, if the bit is cleared.
	 */
	public void setBit(int bit, boolean value) {
		bitset.set(bit, value);
	}

	/**
	 * A method to return the bit set used to store the Bloom filter.
	 * 
	 * @return the bit set representing the Bloom filter.
	 */
	public BitSet getBitSet() {
		return bitset;
	}

	/**
	 * A method to return the number of bits in the Bloom filter
	 *
	 * @return the size of the bitset used by the Bloom filter.
	 */
	public int size() {
		return this.bitSetSize;
	}

	/**
	 * A method to return the number of elements inserted to the Bloom filter
	 *
	 * @return number of elements added to the Bloom filter.
	 */
	public int count() {
		return this.elementNumber;
	}

	/**
	 * A method to returns the maximum number of elements to be inserted into
	 * the filter. This value is the same value as the one passed to the
	 * constructor.
	 *
	 * @return expected number of elements.
	 */
	public int getMaxNumberOfElements() {
		return maxElementNumber;
	}

	/**
	 * A method to get expected number of bits per element when the Bloom filter
	 * is full. This value is set by the constructor when the Bloom filter is
	 * created. See also getBitsPerElement().
	 *
	 * @return expected number of bits per element.
	 */
	public double getExpectedBitsPerElement() {
		return this.bitsPerElement;
	}

	/**
	 * A method to get actual number of bits per element based on the number of
	 * elements that have currently been inserted and the length of the Bloom
	 * filter. See also getExpectedBitsPerElement().
	 *
	 * @return number of bits per element.
	 */
	public double getBitsPerElement() {
		return this.bitSetSize / (double) elementNumber;
	}

}