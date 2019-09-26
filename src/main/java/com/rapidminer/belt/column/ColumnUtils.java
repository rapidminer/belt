/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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


/**
 * This class is used for some internal computations that occur during column creation.
 *
 * @author Kevin Majchrzak
 */
final class ColumnUtils {

	static final int MOST_FREQUENT_VALUE_INDEX = 0;
	static final int MOST_FREQUENT_COUNT_INDEX = 1;

	private ColumnUtils() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
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
	static Optional<Double> estimateDefaultValue(int sampleSize, double minSparsity, double[] data, SplittableRandom random) {
		double[] samples = new double[sampleSize];
		Arrays.setAll(samples, i -> data[random.nextInt(data.length)]);
		double[] result = getMostFrequentValue(samples, false);
		double sparsity = result[MOST_FREQUENT_COUNT_INDEX] / sampleSize;
		if (sparsity >= minSparsity) {
			return Optional.of(result[MOST_FREQUENT_VALUE_INDEX]);
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
	 * Similar to {@link #getMostFrequentValue(double[])} but with the option, not to copy the data. Please note that if
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

}
