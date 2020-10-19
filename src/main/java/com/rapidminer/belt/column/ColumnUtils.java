/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see
 * https://www.gnu.org/licenses/.
 */

package com.rapidminer.belt.column;

import java.util.Arrays;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.function.LongFunction;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.SortingLong;


/**
 * This class is used for some internal computations that are mostly used by columns or for column creation.
 *
 * @author Kevin Majchrzak
 */
public final class ColumnUtils {

	/**
	 * DoubleSparseColumn falls back to a dense column if its density is above this value.
	 */
	public static final double MAX_DENSITY_DOUBLE_SPARSE_COLUMN = 0.5d;

	static final int MOST_FREQUENT_VALUE_INDEX = 0;
	static final int MOST_FREQUENT_COUNT_INDEX = 1;

	/**
	 * Disallow instances of this class.
	 */
	private ColumnUtils() {
		throw new AssertionError("No com.rapidminer.belt.column.ColumnUtils instances for you!");
	}

	/**
	 * This method takes data and samples from it (uniformly distributed) to calculate an estimate of the original
	 * data's sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data
	 * as its default value. Otherwise is returns {@link Optional#empty()}. This methods running time is {@code
	 * O(sampleSize * log(sampleSize))} and its memory overhead is {@code O(sampleSize)}.
	 *
	 * @param sampleSize
	 * 		Number of samples to use.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @param data
	 * 		The original full data that this method will sample from.
	 * @param random
	 * 		An instance of {@link SplittableRandom} that will be used to sample the data. The random generator's seed can
	 * 		be set manually to achieve deterministic results with this method.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Double> estimateDefaultValue(int sampleSize, double minSparsity, double[] data, SplittableRandom random) {
		double[] samples = new double[sampleSize];
		for (int i = 0; i < samples.length; i++) {
			samples[i] = data[random.nextInt(data.length)];
		}
		return estimateDefaultValue(samples, minSparsity);
	}

	/**
	 * This method takes a uniformly distributed sample of some data and calculates an estimate of the original data's
	 * sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data as its
	 * default value. Otherwise is returns {@link Optional#empty()}. Please note that the given sample's order will be
	 * modified while calculating the estimate. This methods running time is {@code O(sampleSize * log(sampleSize))}.
	 *
	 * @param sample
	 * 		Uniformly distributes sample of the original data.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Double> estimateDefaultValue(double[] sample, double minSparsity) {
		double[] result = getMostFrequentValue(sample, false);
		double sparsity = result[MOST_FREQUENT_COUNT_INDEX] / sample.length;
		if (sparsity >= minSparsity) {
			return Optional.of(result[MOST_FREQUENT_VALUE_INDEX]);
		}
		return Optional.empty();
	}

	/**
	 * This method takes data and samples from it (uniformly distributed) to calculate an estimate of the original
	 * data's sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data
	 * as its default value. Otherwise it returns {@link Optional#empty()}. This methods running time is {@code
	 * O(sampleSize * log(sampleSize))} and its memory overhead is {@code O(sampleSize)}.
	 *
	 * @param sampleSize
	 * 		Number of samples to use.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @param data
	 * 		The original full data that this method will sample from.
	 * @param random
	 * 		An instance of {@link SplittableRandom} that will be used to sample the data. The random generator's seed can
	 * 		be set manually to achieve deterministic results with this method.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Long> estimateDefaultValue(int sampleSize, double minSparsity, long[] data, SplittableRandom random) {
		long[] samples = new long[sampleSize];
		for (int i = 0; i < samples.length; i++) {
			samples[i] = data[random.nextInt(data.length)];
		}
		return estimateDefaultValue(samples, minSparsity);
	}

	/**
	 * This method takes a uniformly distributed sample of some data and calculates an estimate of the original data's
	 * sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data as its
	 * default value. Otherwise is returns {@link Optional#empty()}. Please note that the given sample's order will be
	 * modified while calculating the estimate. This methods running time is {@code O(sampleSize * log(sampleSize))}.
	 *
	 * @param sample
	 * 		Uniformly distributes sample of the original data.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Long> estimateDefaultValue(long[] sample, double minSparsity) {
		long[] result = getMostFrequentValue(sample, false);
		double sparsity = (double) result[MOST_FREQUENT_COUNT_INDEX] / sample.length;
		if (sparsity >= minSparsity) {
			return Optional.of(result[MOST_FREQUENT_VALUE_INDEX]);
		}
		return Optional.empty();
	}

	/**
	 * This method takes data and samples from it (uniformly distributed) to calculate an estimate of the original
	 * data's sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data
	 * as its default value. Otherwise it returns {@link Optional#empty()}. This methods running time is {@code
	 * O(sampleSize * log(sampleSize))} and its memory overhead is {@code O(sampleSize)}.
	 *
	 * @param sampleSize
	 * 		Number of samples to use.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @param data
	 * 		The original full data that this method will sample from.
	 * @param random
	 * 		An instance of {@link SplittableRandom} that will be used to sample the data. The random generator's seed can
	 * 		be set manually to achieve deterministic results with this method.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Integer> estimateDefaultValue(int sampleSize, double minSparsity, int[] data, SplittableRandom random) {
		int[] samples = new int[sampleSize];
		for (int i = 0; i < samples.length; i++) {
			samples[i] = data[random.nextInt(data.length)];
		}
		return estimateDefaultValue(samples, minSparsity);
	}

	/**
	 * This method takes a uniformly distributed sample of some data and calculates an estimate of the original data's
	 * sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data as its
	 * default value. Otherwise is returns {@link Optional#empty()}. Please note that the given sample's order will be
	 * modified while calculating the estimate. This methods running time is {@code O(sampleSize * log(sampleSize))}.
	 *
	 * @param sample
	 * 		Uniformly distributes sample of the original data.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Integer> estimateDefaultValue(int[] sample, double minSparsity) {
		int[] result = getMostFrequentValue(sample, false);
		double sparsity = (double) result[MOST_FREQUENT_COUNT_INDEX] / sample.length;
		if (sparsity >= minSparsity) {
			return Optional.of(result[MOST_FREQUENT_VALUE_INDEX]);
		}
		return Optional.empty();
	}

	/**
	 * This method takes data and samples from it (uniformly distributed) to calculate an estimate of the original
	 * data's sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data
	 * as its default value. Otherwise it returns {@link Optional#empty()}. This methods running time is {@code
	 * O(sampleSize * log(sampleSize))} and its memory overhead is {@code O(sampleSize)}.
	 *
	 * @param sampleSize
	 * 		Number of samples to use.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @param data
	 * 		The original full data that this method will sample from.
	 * @param random
	 * 		An instance of {@link SplittableRandom} that will be used to sample the data. The random generator's seed can
	 * 		be set manually to achieve deterministic results with this method.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Short> estimateDefaultValue(int sampleSize, double minSparsity, short[] data, SplittableRandom random) {
		short[] samples = new short[sampleSize];
		for (int i = 0; i < samples.length; i++) {
			samples[i] = data[random.nextInt(data.length)];
		}
		int[] result = getMostFrequentValue(samples, false);
		double sparsity = ((double) result[MOST_FREQUENT_COUNT_INDEX]) / sampleSize;
		if (sparsity >= minSparsity) {
			return Optional.of((short) result[MOST_FREQUENT_VALUE_INDEX]);
		}
		return Optional.empty();
	}

	/**
	 * This method takes data and samples from it (uniformly distributed) to calculate an estimate of the original
	 * data's sparsity. If the estimate is {@code >= minSparsity}, it returns the most common value in the sampled data
	 * as its default value. Otherwise it returns {@link Optional#empty()}. This methods running time is {@code
	 * O(sampleSize * log(sampleSize))} and its memory overhead is {@code O(sampleSize)}.
	 *
	 * @param sampleSize
	 * 		Number of samples to use.
	 * @param minSparsity
	 * 		Data with sparsity greater or equal to this value is regarded as sparse and a default value is returned. Other
	 * 		data is regarded as dense and nothing is returned.
	 * @param data
	 * 		The original full data that this method will sample from.
	 * @param random
	 * 		An instance of {@link SplittableRandom} that will be used to sample the data. The random generator's seed can
	 * 		be set manually to achieve deterministic results with this method.
	 * @return The estimated default value, if the sampled data is sparse or {@link Optional#empty()}, otherwise.
	 */
	public static Optional<Byte> estimateDefaultValue(int sampleSize, double minSparsity, byte[] data, SplittableRandom random) {
		byte[] samples = new byte[sampleSize];
		for (int i = 0; i < samples.length; i++) {
			samples[i] = data[random.nextInt(data.length)];
		}
		int[] result = getMostFrequentValue(samples, false);
		double sparsity = ((double) result[MOST_FREQUENT_COUNT_INDEX]) / sampleSize;
		if (sparsity >= minSparsity) {
			return Optional.of((byte) result[MOST_FREQUENT_VALUE_INDEX]);
		}
		return Optional.empty();
	}

	/**
	 * Returns a two dimensional array. The first array entry contains the most frequent value in the given array (or
	 * one of the most common values if there are multiple). The second entry contains the actual frequency of the most
	 * frequent value.
	 * <p>
	 * This method copies the given data, sorts the copy and finds the most frequent value by linear traversal.
	 * Therefore its running time is {@code O(n*log(n))} and it has a memory overhead for copying the original array.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will not be modified.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static double[] getMostFrequentValue(double[] data) {
		return getMostFrequentValue(data, true);
	}

	/**
	 * Returns a two dimensional array. The first array entry contains the most frequent value in the given array (or
	 * one of the most common values if there are multiple). The second entry contains the actual frequency of the most
	 * frequent value.
	 * <p>
	 * This method copies the given data, sorts the copy and finds the most frequent value by linear traversal.
	 * Therefore its running time is {@code O(n*log(n))} and it has a memory overhead for copying the original array.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will not be modified.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static long[] getMostFrequentValue(long[] data) {
		return getMostFrequentValue(data, true);
	}

	/**
	 * Returns a two dimensional array. The first array entry contains the most frequent value in the given array (or
	 * one of the most common values if there are multiple). The second entry contains the actual frequency of the most
	 * frequent value.
	 * <p>
	 * This method copies the given data, sorts the copy and finds the most frequent value by linear traversal.
	 * Therefore its running time is {@code O(n*log(n))} and it has a memory overhead for copying the original array.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will not be modified.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static int[] getMostFrequentValue(short[] data) {
		return getMostFrequentValue(data, true);
	}

	/**
	 * Returns a two dimensional array. The first array entry contains the most frequent value in the given array (or
	 * one of the most common values if there are multiple). The second entry contains the actual frequency of the most
	 * frequent value.
	 * <p>
	 * This method copies the given data, sorts the copy and finds the most frequent value by linear traversal.
	 * Therefore its running time is {@code O(n*log(n))} and it has a memory overhead for copying the original array.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will not be modified.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static int[] getMostFrequentValue(byte[] data) {
		return getMostFrequentValue(data, true);
	}

	/**
	 * Returns a two dimensional array. The first array entry contains the most frequent value in the given array (or
	 * one of the most common values if there are multiple). The second entry contains the actual frequency of the most
	 * frequent value.
	 * <p>
	 * This method copies the given data, sorts the copy and finds the most frequent value by linear traversal.
	 * Therefore its running time is {@code O(n*log(n))} and it has a memory overhead for copying the original array.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will not be modified.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static int[] getMostFrequentValue(int[] data) {
		return getMostFrequentValue(data, true);
	}

	/**
	 * Similar to {@link #getMostFrequentValue(double[])} but with the option, not to copy the data. Please note that if
	 * the data is not copied, the given data array will be modified.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will be modified if and only if the
	 * 		second parameter {@code copyData} is set to {@code false}.
	 * @param copyData
	 * 		This method copies the given data array if and only if this value is set to {@code true}.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static double[] getMostFrequentValue(double[] data, boolean copyData) {
		double[] array = copyData ? Arrays.copyOf(data, data.length) : data;
		Arrays.sort(array);

		// find the max frequency using linear traversal
		int mostFrequentCount = 1;
		double mostFrequent = array[0];
		int currentCount = 1;

		for (int i = 1; i < array.length; i++) {
			if (array[i] == array[i - 1] || (Double.isNaN(array[i]) && Double.isNaN(array[i - 1]))) {
				currentCount++;
			} else {
				if (currentCount > mostFrequentCount) {
					mostFrequentCount = currentCount;
					mostFrequent = array[i - 1];
				}
				currentCount = 1;
			}
		}

		// If last element is most frequent
		if (currentCount > mostFrequentCount) {
			mostFrequentCount = currentCount;
			mostFrequent = array[array.length - 1];
		}
		double[] result = new double[2];
		result[MOST_FREQUENT_VALUE_INDEX] = mostFrequent;
		result[MOST_FREQUENT_COUNT_INDEX] = mostFrequentCount;
		return result;
	}

	/**
	 * Similar to {@link #getMostFrequentValue(long[])} but with the option, not to copy the data. Please note that if
	 * the data is not copied, the given data array will be modified.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will be modified if and only if the
	 * 		second parameter {@code copyData} is set to {@code false}.
	 * @param copyData
	 * 		This method copies the given data array if and only if this value is set to {@code true}.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static long[] getMostFrequentValue(long[] data, boolean copyData) {
		long[] array = copyData ? Arrays.copyOf(data, data.length) : data;
		Arrays.sort(array);

		// find the max frequency using linear traversal
		int mostFrequentCount = 1;
		long mostFrequent = array[0];
		int currentCount = 1;

		for (int i = 1; i < array.length; i++) {
			if (array[i] == array[i - 1]) {
				currentCount++;
			} else {
				if (currentCount > mostFrequentCount) {
					mostFrequentCount = currentCount;
					mostFrequent = array[i - 1];
				}
				currentCount = 1;
			}
		}

		// If last element is most frequent
		if (currentCount > mostFrequentCount) {
			mostFrequentCount = currentCount;
			mostFrequent = array[array.length - 1];
		}
		long[] result = new long[2];
		result[MOST_FREQUENT_VALUE_INDEX] = mostFrequent;
		result[MOST_FREQUENT_COUNT_INDEX] = mostFrequentCount;
		return result;
	}

	/**
	 * Similar to {@link #getMostFrequentValue(int[])} but with the option, not to copy the data. Please note that if
	 * the data is not copied, the given data array will be modified.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will be modified if and only if the
	 * 		second parameter {@code copyData} is set to {@code true}.
	 * @param copyData
	 * 		This method copies the given data array if and only if this value is set to {@code true}.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static int[] getMostFrequentValue(int[] data, boolean copyData) {
		int[] array = copyData ? Arrays.copyOf(data, data.length) : data;
		Arrays.sort(array);

		// find the max frequency using linear traversal
		int mostFrequentCount = 1;
		int mostFrequent = array[0];
		int currentCount = 1;

		for (int i = 1; i < array.length; i++) {
			if (array[i] == array[i - 1]) {
				currentCount++;
			} else {
				if (currentCount > mostFrequentCount) {
					mostFrequentCount = currentCount;
					mostFrequent = array[i - 1];
				}
				currentCount = 1;
			}
		}

		// If last element is most frequent
		if (currentCount > mostFrequentCount) {
			mostFrequentCount = currentCount;
			mostFrequent = array[array.length - 1];
		}
		int[] result = new int[2];
		result[MOST_FREQUENT_VALUE_INDEX] = mostFrequent;
		result[MOST_FREQUENT_COUNT_INDEX] = mostFrequentCount;
		return result;
	}

	/**
	 * Similar to {@link #getMostFrequentValue(short[])} but with the option, not to copy the data. Please note that if
	 * the data is not copied, the given data array will be modified.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will be modified if and only if the
	 * 		second parameter {@code copyData} is set to {@code true}.
	 * @param copyData
	 * 		This method copies the given data array if and only if this value is set to {@code true}.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static int[] getMostFrequentValue(short[] data, boolean copyData) {
		short[] array = copyData ? Arrays.copyOf(data, data.length) : data;
		Arrays.sort(array);

		// find the max frequency using linear traversal
		int mostFrequentCount = 1;
		short mostFrequent = array[0];
		int currentCount = 1;

		for (int i = 1; i < array.length; i++) {
			if (array[i] == array[i - 1]) {
				currentCount++;
			} else {
				if (currentCount > mostFrequentCount) {
					mostFrequentCount = currentCount;
					mostFrequent = array[i - 1];
				}
				currentCount = 1;
			}
		}

		// If last element is most frequent
		if (currentCount > mostFrequentCount) {
			mostFrequentCount = currentCount;
			mostFrequent = array[array.length - 1];
		}
		int[] result = new int[2];
		result[MOST_FREQUENT_VALUE_INDEX] = mostFrequent;
		result[MOST_FREQUENT_COUNT_INDEX] = mostFrequentCount;
		return result;
	}

	/**
	 * Similar to {@link #getMostFrequentValue(byte[])} but with the option, not to copy the data. Please note that if
	 * the data is not copied, the given data array will be modified.
	 *
	 * @param data
	 * 		This method returns the most frequent value in this array. The given array will be modified if and only if the
	 * 		second parameter {@code copyData} is set to {@code true}.
	 * @param copyData
	 * 		This method copies the given data array if and only if this value is set to {@code true}.
	 * @return A two dimensional array. The first array entry contains the most frequent value and the second one its
	 * actual frequency.
	 * @throws IndexOutOfBoundsException
	 * 		If the length of the given array is zero.
	 * @throws NullPointerException
	 * 		If the given array is {@code null}.
	 */
	static int[] getMostFrequentValue(byte[] data, boolean copyData) {
		byte[] array = copyData ? Arrays.copyOf(data, data.length) : data;
		Arrays.sort(array);

		// find the max frequency using linear traversal
		int mostFrequentCount = 1;
		byte mostFrequent = array[0];
		int currentCount = 1;

		for (int i = 1; i < array.length; i++) {
			if (array[i] == array[i - 1]) {
				currentCount++;
			} else {
				if (currentCount > mostFrequentCount) {
					mostFrequentCount = currentCount;
					mostFrequent = array[i - 1];
				}
				currentCount = 1;
			}
		}

		// If last element is most frequent
		if (currentCount > mostFrequentCount) {
			mostFrequentCount = currentCount;
			mostFrequent = array[array.length - 1];
		}
		int[] result = new int[2];
		result[MOST_FREQUENT_VALUE_INDEX] = mostFrequent;
		result[MOST_FREQUENT_COUNT_INDEX] = mostFrequentCount;
		return result;
	}

	/**
	 * Fills the given array with the longs encoded by the given sparse data representation. Used e.g. by {@link
	 * DateTimeLowPrecisionSparseColumn#fillSecondsIntoArray(long[], int)} and {@link
	 * TimeSparseColumn#fillNanosIntoArray(long[], int)}.
	 *
	 * @param array
	 * 		the array to be filled
	 * @param arrayStartIndex
	 * 		the index at which to start filling the array. Must be non-negative.
	 * @param rowIndex
	 * 		the row index from where to start reading the sparse long column implementation.
	 * @param defaultValue
	 * 		the long data's default value
	 * @param nonDefaultIndices
	 * 		indices of non default values
	 * @param nonDefaultValues
	 * 		the non default values corresponding to the non-default indices
	 * @param size
	 * 		the logical data size
	 */
	static void fillSparseLongsIntoLongArray(long[] array, int arrayStartIndex, int rowIndex, long defaultValue,
											 int[] nonDefaultIndices, long[] nonDefaultValues, int size) {
		// fill all free slots in the array with default values
		for (int i = arrayStartIndex, len = array.length; i < len; i++) {
			array[i] = defaultValue;
		}

		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(array.length - arrayStartIndex + rowIndex, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition + arrayStartIndex - rowIndex] = nonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * Fills the given array with the bytes encoded by the given sparse data representation. Used e.g. by {@link
	 * CategoricalSparseColumn#getByteData()}.
	 *
	 * @param array
	 * 		the array to be filled
	 * @param arrayStartIndex
	 * 		the index at which to start filling the array
	 * @param defaultValue
	 * 		the byte data's default value
	 * @param nonDefaultIndices
	 * 		indices of non default values
	 * @param nonDefaultValues
	 * 		the non default values corresponding to the non-default indices
	 * @param size
	 * 		the logical data size
	 */
	static void fillSparseBytesIntoByteArray(byte[] array, int arrayStartIndex, byte defaultValue,
											 int[] nonDefaultIndices, byte[] nonDefaultValues, int size) {
		// fill all free slots in the array with default values
		for (int i = arrayStartIndex, len = array.length; i < len; i++) {
			array[i] = defaultValue;
		}

		int maxRowIndex = Math.min(array.length - arrayStartIndex, size);

		if (nonDefaultIndices.length > 0) {
			int nonDefaultIndex = 0;
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition + arrayStartIndex] = nonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * Fills the given array with the shorts encoded by the given sparse data representation. Used e.g. by {@link
	 * CategoricalSparseColumn#getShortData()}.
	 *
	 * @param array
	 * 		the array to be filled
	 * @param arrayStartIndex
	 * 		the index at which to start filling the array
	 * @param defaultValue
	 * 		the short data's default value
	 * @param nonDefaultIndices
	 * 		indices of non default values
	 * @param nonDefaultValues
	 * 		the non default values corresponding to the non-default indices
	 * @param size
	 * 		the logical data size
	 */
	static void fillSparseShortsIntoShortArray(short[] array, int arrayStartIndex, short defaultValue,
											   int[] nonDefaultIndices, short[] nonDefaultValues, int size) {
		// fill all free slots in the array with default values
		for (int i = arrayStartIndex, len = array.length; i < len; i++) {
			array[i] = defaultValue;
		}

		int maxRowIndex = Math.min(array.length - arrayStartIndex, size);

		if (nonDefaultIndices.length > 0) {
			int nonDefaultIndex = 0;
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition + arrayStartIndex] = nonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * Fills the given array with the ints encoded by the given sparse data representation. Used e.g. by {@link
	 * CategoricalSparseColumn#getIntData()}.
	 *
	 * @param array
	 * 		the array to be filled
	 * @param arrayStartIndex
	 * 		the index at which to start filling the array
	 * @param defaultValue
	 * 		the int data's default value
	 * @param nonDefaultIndices
	 * 		indices of non default values
	 * @param nonDefaultValues
	 * 		the non default values corresponding to the non-default indices
	 * @param size
	 * 		the logical data size
	 */
	static void fillSparseIntsIntoIntArray(int[] array, int arrayStartIndex, int defaultValue,
										   int[] nonDefaultIndices, int[] nonDefaultValues, int size) {
		// fill all free slots in the array with default values
		for (int i = arrayStartIndex, len = array.length; i < len; i++) {
			array[i] = defaultValue;
		}

		int maxRowIndex = Math.min(array.length - arrayStartIndex, size);

		if (nonDefaultIndices.length > 0) {
			int nonDefaultIndex = 0;
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition + arrayStartIndex] = nonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * Takes a sparse long column representation (default value, non-default indices, non-default values) and an object
	 * array and a longToObjectFunction from long to object. Maps the columns long values to object values and fills
	 * them into the given object array.
	 *
	 * @param array
	 * 		The object array to be filled.
	 * @param rowIndex
	 * 		The row index from where to start reading the sparse long column implementation.
	 * @param arrayOffset
	 * 		The offset at which to start filling the given object array.
	 * @param arrayStepSize
	 * 		The step size to apply when filling the object array.
	 * @param defaultValue
	 * 		The sparse long column's default value (mapped to its object representation).
	 * @param nonDefaultIndices
	 * 		The sparse long column's non-default indices.
	 * @param nonDefaultValues
	 * 		The sparse long column's non-default values.
	 * @param longToObjectFunction
	 * 		The mapping that will be used to map from long values to object values.
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the given {@code rowIndex} ist smaller zero.
	 * @throws IllegalArgumentException
	 * 		if the given {@code arrayStepSize} is smaller one.
	 */
	static void fillSparseLongsIntoObjectArray(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize,
											   Object defaultValue, int[] nonDefaultIndices,
											   long[] nonDefaultValues, int size, LongFunction<Object> longToObjectFunction) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValue;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						longToObjectFunction.apply(nonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * Takes a sparse long column representation (default value, non-default indices, non-default values) and an object
	 * array and a mapping from long to object. Maps the columns long values to object values and fills them into the
	 * given object array.
	 *
	 * @param array
	 * 		The object array to be filled.
	 * @param rowIndex
	 * 		The row index from where to start reading the sparse long column implementation.
	 * @param defaultValue
	 * 		The sparse long column's default value (mapped to its object representation).
	 * @param nonDefaultIndices
	 * 		The sparse long column's non-default indices.
	 * @param nonDefaultValues
	 * 		The sparse long column's non-default values.
	 * @param longToObjectFunction
	 * 		The mapping that will be used to map from long values to object values.
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the given {@code rowIndex} ist smaller zero.
	 */
	static void fillSparseLongsIntoObjectArray(Object[] array, int rowIndex, Object defaultValue, int[] nonDefaultIndices,
											   long[] nonDefaultValues, int size, LongFunction<Object> longToObjectFunction) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValue);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);

		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = longToObjectFunction.apply(nonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * Maps a sparse column based on its intern sparse long representation. See {@link Column#map(int[], boolean)} for a
	 * more precise explanation. The method is used for example in {@link DateTimeHighPrecisionSparseColumn#map(int[],
	 * boolean)}, {@link DateTimeLowPrecisionSparseColumn#map(int[], boolean)} and {@link TimeSparseColumn#map(int[],
	 * boolean)}. It returns a {@link MapSparseLongColumnResult} which contains the resulting non-default indices /
	 * values iff the resulting data is also sparse (its density is below the given {@code maxDensity} threshold). Iff
	 * the resulting data is not sparse anymore the method {@link MapSparseLongColumnResult#isSparse()} of the result
	 * will return false and the resulting non-default indices / values will be undefined.
	 *
	 * @param mapping
	 * 		The mapping to apply.
	 * @param defaultValue
	 * 		The sparse columns default value.
	 * @param missingValue
	 * 		The value that represents the missing value.
	 * @param nonDefaultIndices
	 * 		The non-default indices.
	 * @param nonDefaultValues
	 * 		The non-default values.
	 * @param size
	 * 		The size of the column to map.
	 * @param maxDensity
	 * 		Usually this method will map the sparse column to a new sparse column. If the resulting columns density would
	 * 		be above this value, a dense column will be returned.
	 * @return a {@link MapSparseLongColumnResult} (containing the resulting non-default indices / values iff the
	 * resulting data is sparse).
	 */
	static MapSparseLongColumnResult mapSparseLongColumn(int[] mapping, long defaultValue, long missingValue,
														 int[] nonDefaultIndices, long[] nonDefaultValues,
														 int size, double maxDensity) {
		SparseBitmap bitMap = new SparseBitmap(defaultValue == missingValue, nonDefaultIndices, size);

		int numberOfNonDefaults = bitMap.countNonDefaultIndices(mapping);
		if (mapping.length * maxDensity < numberOfNonDefaults) {
			// column is not sparse enough anymore
			return new MapSparseLongColumnResult(false, null, null);
		}

		int[] newNonDefaultIndices = new int[numberOfNonDefaults];
		long[] newNonDefaultValues = new long[numberOfNonDefaults];
		int nonDefaultIndex = 0;
		for (int i = 0; i < mapping.length; i++) {
			int bitMapIndex = bitMap.get(mapping[i]);
			if (bitMapIndex != SparseBitmap.DEFAULT_INDEX) {
				newNonDefaultIndices[nonDefaultIndex] = i;
				if (bitMapIndex == SparseBitmap.OUT_OF_BOUNDS_INDEX) {
					newNonDefaultValues[nonDefaultIndex++] = missingValue;
				} else {
					newNonDefaultValues[nonDefaultIndex++] = nonDefaultValues[bitMapIndex];
				}
			}
		}

		return new MapSparseLongColumnResult(true, newNonDefaultIndices, newNonDefaultValues);
	}

	/**
	 * Wrapper class for the results of {@link ColumnUtils#mapSparseLongColumn(int[], long, long, int[], long[], int,
	 * double)}. See the method descriptions for details on the results.
	 */
	static final class MapSparseLongColumnResult {
		private final boolean isSparse;
		private final int[] newNonDefaultIndices;
		private final long[] newNonDefaultValues;

		private MapSparseLongColumnResult(boolean isSparse, int[] newNonDefaultIndices, long[] newNonDefaultValues) {
			this.isSparse = isSparse;
			this.newNonDefaultIndices = newNonDefaultIndices;
			this.newNonDefaultValues = newNonDefaultValues;
		}

		/**
		 * Return {@code true} iff the mapped data is still sparse. The resulting non-default indices / values are
		 * undefined for non-sparse results.
		 *
		 * @return {@code true} iff the mapped data is still sparse.
		 */
		boolean isSparse() {
			return isSparse;
		}

		/**
		 * The new non-default indices resulting from applying the given mapping. The result is undefined iff {@link
		 * #isSparse()} return false.
		 *
		 * @return integer array with non-default indices.
		 */
		int[] newNonDefaultIndices() {
			return newNonDefaultIndices;
		}

		/**
		 * The new non-default values resulting from applying the given mapping. The result is undefined iff {@link
		 * #isSparse()} return false.
		 *
		 * @return long array with non-default indices.
		 */
		long[] newNonDefaultValues() {
			return newNonDefaultValues;
		}
	}

	/**
	 * The method will return the smallest {@code i} with {@code values[i] >= index}. Returns an {@code i >=
	 * values.length} if no such {@code i} exists.
	 *
	 * @param values
	 * 		Some integer values.
	 * @param index
	 * 		Some integer index.
	 * @return Will return the smallest {@code i} with {@code values[i] >= index} or an {@code i >= values.length} if no
	 * such {@code i} exists.
	 */
	static int findNextIndex(int[] values, int index) {
		int nextIndex = index == 0 ? 0 : Arrays.binarySearch(values, index);
		if (nextIndex < 0) {
			// see documentation of binary search
			nextIndex = -nextIndex - 1;
		}
		return nextIndex;
	}

	/**
	 * Sorts a sparse column based on its intern sparse long representation. See {@link Column#sort(Order)} for a more
	 * precise explanation. The method is used for example in {@link DateTimeHighPrecisionSparseColumn#sort(Order)},
	 * {@link DateTimeLowPrecisionSparseColumn#sort(Order)} and {@link TimeSparseColumn#sort(Order)}.
	 */
	static int[] sortSparseLongs(Order order, long defaultValue, int[] nonDefaultIndices, long[] nonDefaultValues, int size) {
		// 1) calc sorted mapping for the non-default values and 1x the default value
		// as the default value does not have an index yet, we need to add an artificial index for it
		int newIndexForDefault = nonDefaultIndices.length;
		long[] toSort = Arrays.copyOf(nonDefaultValues, newIndexForDefault + 1);
		toSort[newIndexForDefault] = defaultValue;
		int[] sortedMapping = SortingLong.sort(toSort, order);
		// find where default value index is in sorted result
		int defaultPosition = 0;
		for (int i = 0; i < sortedMapping.length; i++) {
			if (sortedMapping[i] == newIndexForDefault) {
				defaultPosition = i;
				break;
			}
		}
		// 2) map the non-default indices using the sorted mapping
		// (This will lead to a 0 at the defaultPosition because newIndexForDefault is out of range for the
		// nonDefaultIndices array. But that is no problem as we are not using this value.)
		int[] sortedNonDefaultIndices = Mapping.apply(nonDefaultIndices, sortedMapping);

		// 3) add the mapped non-default indices with values < the default value to the result
		int[] sortResult = new int[size];
		System.arraycopy(sortedNonDefaultIndices, 0, sortResult, 0, defaultPosition);

		// 4) add the default indices after that
		int index = defaultPosition;
		int last = -1;
		for (int i : nonDefaultIndices) {
			for (int j = last + 1; j < i; j++) {
				sortResult[index++] = j;
			}
			last = i;
		}
		// add the default indices after last non-default until size
		for (int j = last + 1; j < size; j++) {
			sortResult[index++] = j;
		}

		// 5) add the rest of the non-default indices after that
		if (defaultPosition < sortedNonDefaultIndices.length - 1) {
			System.arraycopy(sortedNonDefaultIndices, defaultPosition + 1, sortResult, index,
					sortResult.length - index);
		}

		return sortResult;
	}
}
