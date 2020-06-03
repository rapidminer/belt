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

package com.rapidminer.belt.util;

import static java.lang.Math.random;
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class SortingTests {

	private static final double EPSILON = 1e-10;

	public static class InsertionSort {

		@Test
		public void testAscendingOfSortedInput() {
			double[] data = new double[]{0.713, 0.901, 0.961, 1.140, 2.070, 3.036, 3.385, 3.467};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			Sorting.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInput() {
			double[] data = new double[]{3.351, 2.532, 2.475, 2.350, 1.729, 1.268, 0.283, 0.161};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			Sorting.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfRandomInput() {
			double[] data = new double[308];
			Arrays.setAll(data, i -> random());

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			Sorting.ascendingInsertionSort(data, indices, 0, data.length);
			double[] customSorting = new double[data.length];
			Arrays.setAll(customSorting, i -> data[indices[i]]);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testAscendingStability() {
			double[] data = new double[]{-0.001, 0.157, 0.423, 0.423, 0.350, -0.089, 0.157, 0.375};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{5, 0, 1, 6, 4, 7, 2, 3};
			Sorting.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSubArray() {
			double[] data = new double[]{0.470, 0.418, 0.444, -0.500, -0.351, 0.363, 0.346, -0.222};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 3, 4, 5, 2, 6, 7};
			Sorting.ascendingInsertionSort(data, indices, 2, data.length - 2);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingWithShuffledIndices() {
			double[] data = new double[]{0.470, 0.444, 0.444, -0.500, -0.351, 0.363, 0.346, -0.222};
			int[] indices = new int[]{2, 6, 7, 1, 0, 4, 3, 5};
			int[] expected = new int[]{3, 4, 7, 6, 5, 2, 1, 0};
			Sorting.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInput() {
			double[] data = new double[]{3.467, 3.385, 3.036, 2.070, 1.140, 0.961, 0.901, 0.713};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			Sorting.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInput() {
			double[] data = new double[]{0.161, 0.283, 1.268, 1.729, 2.350, 2.475, 2.532, 3.351};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			Sorting.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfRandomInput() {
			double[] data = new double[308];
			Arrays.setAll(data, i -> random());

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);
			double[] reversedJdkSorting = new double[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSorting[data.length - i - 1] = jdkSorting[i];
			}

			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			Sorting.descendingInsertionSort(data, indices, 0, data.length);
			double[] customSorting = new double[data.length];
			Arrays.setAll(customSorting, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testDescendingStability() {
			double[] data = new double[]{-0.001, 0.157, 0.423, 0.423, 0.350, -0.089, 0.157, 0.375};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{2, 3, 7, 4, 1, 6, 0, 5};
			Sorting.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSubArray() {
			double[] data = new double[]{0.470, 0.418, 0.444, -0.500, -0.351, 0.363, 0.346, -0.222};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 5, 4, 3, 6, 7};
			Sorting.descendingInsertionSort(data, indices, 2, data.length - 2);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingWithShuffledIndices() {
			double[] data = new double[]{0.470, 0.444, 0.444, -0.500, -0.351, 0.363, 0.346, -0.222};
			int[] indices = new int[]{2, 6, 7, 1, 0, 4, 3, 5};
			int[] expected = new int[]{0, 2, 1, 5, 6, 7, 4, 3};
			Sorting.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

	}

	public static class BinaryInsertionSort {

		@Test
		public void testAscending() {
			double data[] = new double[]{-0d, 0d, -0d, 0d, -0d, -0d, 0d, 0d};
			int[] indices = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			int[] expected = new int[]{0, 2, 4, 5, 1, 3, 6, 7};
			Sorting.ascendingBinaryInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingWithShuffledIndices() {
			double data[] = new double[]{-0d, 0d, -0d, 0d, -0d, -0d, 0d, 0d};
			int[] indices = new int[]{3, 6, 5, 4, 7, 2, 0, 1};
			int[] expected = new int[]{5, 4, 2, 0, 3, 6, 7, 1};
			Sorting.ascendingBinaryInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescending() {
			double data[] = new double[]{-0d, 0d, -0d, 0d, -0d, -0d, 0d, 0d};
			int[] indices = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			int[] expected = new int[]{1, 3, 6, 7, 0, 2, 4, 5};
			Sorting.descendingBinaryInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingWithShuffledIndices() {
			double data[] = new double[]{-0d, 0d, -0d, 0d, -0d, -0d, 0d, 0d};
			int[] indices = new int[]{3, 6, 5, 4, 7, 2, 0, 1};
			int[] expected = new int[]{3, 6, 7, 1, 5, 4, 2, 0};
			Sorting.descendingBinaryInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

	}

	public static class Merge {

		@Test
		public void testAscendingOfSortedInputWithEvenSplit() {
			double[] data = new double[]{0, 1, 2, 3, 4, 5, 6, 7};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			Sorting.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSortedInputWithOddSplit() {
			double[] data = new double[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			Sorting.ascendingMerge(data, indices, new int[data.length], 0, 3, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfInterleavedInputWithEvenSplit() {
			double[] data = new double[]{0, 2, 4, 6, 1, 3, 5, 7};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 4, 1, 5, 2, 6, 3, 7};
			Sorting.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfInterleavedInputWithOddSplit() {
			double[] data = new double[]{0, 2, 4, 6, 8, 1, 3, 5, 7};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 5, 1, 6, 2, 7, 3, 8, 4};
			Sorting.ascendingMerge(data, indices, new int[data.length], 0, 5, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInputWithEvenSplit() {
			double[] data = new double[]{0, 1, 2, 3, -4, -3, -2, -1};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 0, 1, 2, 3};
			Sorting.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInputWithOddSplit() {
			double[] data = new double[]{0, 1, 2, 3, -5, -4, -3, -2, -1};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 8, 0, 1, 2, 3};
			Sorting.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSubArray() {
			double[] data = new double[]{0, 0, 0, 0, 1, 2, 5, 6, 2, 4, 5, 9, 0, 0, 0, 0};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 8, 9, 6, 10, 7, 11, 12, 13, 14, 15};
			Sorting.ascendingMerge(data, indices, new int[data.length], 4, 8, 12);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInputWithEvenSplit() {
			double[] data = new double[]{7, 6, 5, 4, 3, 2, 1, 0};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			Sorting.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInputWithOddSplit() {
			double[] data = new double[]{8, 7, 6, 5, 4, 3, 2, 1, 0};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			Sorting.descendingMerge(data, indices, new int[data.length], 0, 3, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfInterleavedInputWithEvenSplit() {
			double[] data = new double[]{7, 5, 3, 1, 6, 4, 2, 0};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 4, 1, 5, 2, 6, 3, 7};
			Sorting.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfInterleavedInputWithOddSplit() {
			double[] data = new double[]{8, 6, 4, 2, 0, 7, 5, 3, 1};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 5, 1, 6, 2, 7, 3, 8, 4};
			Sorting.descendingMerge(data, indices, new int[data.length], 0, 5, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInputWithEvenSplit() {
			double[] data = new double[]{3, 2, 1, 0, 7, 6, 5, 4};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 0, 1, 2, 3};
			Sorting.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInputWithOddSplit() {
			double[] data = new double[]{3, 2, 1, 0, 8, 7, 6, 5, 4};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 8, 0, 1, 2, 3};
			Sorting.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSubArray() {
			double[] data = new double[]{0, 0, 0, 0, 6, 5, 2, 1, 9, 5, 4, 2, 0, 0, 0, 0};
			int indices[] = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 8, 4, 5, 9, 10, 6, 11, 7, 12, 13, 14, 15};
			Sorting.descendingMerge(data, indices, new int[data.length], 4, 8, 12);
			assertArrayEquals(expected, indices);
		}

	}

	public static class Sort {

		@Test
		public void testAscSortWithoutNans() {
			double[] data = new double[]{0.371, 0.124, 0.593, 0.523, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{7, 1, 5, 6, 0, 3, 2, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithLeadingNaNs() {
			double[] data = new double[]{Double.NaN, Double.NaN, Double.NaN, 0.523, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{7, 5, 6, 3, 4, 0, 1, 2};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithTrailingNaNs() {
			double[] data = new double[]{0.371, 0.124, 0.593, 0.523, 0.991, 0.135, Double.NaN, Double.NaN};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{1, 5, 0, 3, 2, 4, 6, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithInterleavedNaNs() {
			double[] data = new double[]{0.371, Double.NaN, 0.593, Double.NaN, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{7, 5, 6, 0, 2, 4, 1, 3};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithAllNaNs() {
			double[] data = new double[123];
			Arrays.fill(data, Double.NaN);
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithLeadingZeros() {
			double[] data = new double[]{0.0, 0.0, -0.0, 0.523, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{2, 0, 1, 7, 5, 6, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithInterleavedZeros() {
			double[] data = new double[]{-0.398, 0.0, -0.0, 0.523, 0.991, -0.135, 0.182, 0.108};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{0, 5, 2, 1, 7, 6, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithTrailingZeros() {
			double[] data = new double[]{-0.398, 0.0, -0.0, -0.523, -0.991, -0.135, -0.182, -0.108};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{4, 3, 0, 6, 5, 7, 2, 1};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithAllZeros() {
			double[] data = new double[]{-0d, -0d, 0d, 0d, -0d, 0d, 0d, -0d};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{0, 1, 4, 7, 2, 3, 5, 6};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithSpecialValues() {
			double[] data = new double[]{Double.NaN, -0.886, 0d, Double.POSITIVE_INFINITY, -0d, 0.238,
					Double.NaN, Double.NEGATIVE_INFINITY};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{7, 1, 4, 2, 5, 3, 0, 6};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithZerosFollowedByNaN() {
			double[] data = new double[]{0d, Double.NaN, -0.886, -0d, 0d, Double.NaN, Double.NEGATIVE_INFINITY};
			int[] indices = Sorting.ascendingSort(data);
			int[] expected = new int[]{6, 2, 3, 0, 4, 1, 5};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfSortedInput() {
			double[] data = new double[595];
			Arrays.setAll(data, i -> i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			int indices[] = Sorting.ascendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfReverseInput() {
			double[] data = new double[397];
			Arrays.setAll(data, i -> -i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> data.length - i - 1);
			int[] indices = Sorting.ascendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfRandomInput() {
			double[] data = new double[3923];
			Arrays.setAll(data, i -> {
				double x = random();
				if (x < 0.10) {
					return Double.NaN;
				} else if (x < 0.125) {
					return -0d;
				} else if (x < 0.15) {
					return 0d;
				} else if (x < 0.175) {
					return Double.NEGATIVE_INFINITY;
				} else if (x < 0.2) {
					return Double.POSITIVE_INFINITY;
				} else {
					return 2 * random() - 1;
				}
			});

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			int[] indices = Sorting.ascendingSort(data);
			double[] customSorting = new double[data.length];
			Arrays.setAll(customSorting, i -> data[indices[i]]);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testDescSortWithoutNans() {
			double[] data = new double[]{0.371, 0.124, 0.593, 0.523, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{4, 2, 3, 0, 6, 5, 1, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithLeadingNaNs() {
			double[] data = new double[]{Double.NaN, Double.NaN, Double.NaN, 0.523, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{0, 1, 2, 4, 3, 6, 5, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithTrailingNaNs() {
			double[] data = new double[]{0.371, 0.124, 0.593, 0.523, 0.991, 0.135, Double.NaN, Double.NaN};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{6, 7, 4, 2, 3, 0, 5, 1};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithInterleavedNaNs() {
			double[] data = new double[]{0.371, Double.NaN, 0.593, Double.NaN, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{1, 3, 4, 2, 0, 6, 5, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithAllNaNs() {
			double[] data = new double[266];
			Arrays.fill(data, Double.NaN);
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithLeadingZeros() {
			double[] data = new double[]{-0.398, -0.0, 0.0, -0.523, -0.991, -0.135, -0.182, -0.108};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{2, 1, 7, 5, 6, 0, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithInterleavedZeros() {
			double[] data = new double[]{-0.398, -0.0, 0.0, 0.523, 0.991, -0.135, 0.182, 0.108};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{4, 3, 6, 7, 2, 1, 5, 0};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithTrailingZeros() {
			double[] data = new double[]{0.0, -0.0, 0.0, 0.523, 0.991, 0.135, 0.182, 0.108};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{4, 3, 6, 5, 7, 0, 2, 1};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithAllZeros() {
			double[] data = new double[]{-0d, -0d, 0d, 0d, -0d, 0d, 0d, -0d};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{2, 3, 5, 6, 0, 1, 4, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithSpecialValues() {
			double[] data = new double[]{Double.NaN, -0.886, 0d, Double.POSITIVE_INFINITY, -0d, 0.238,
					Double.NaN, Double.NEGATIVE_INFINITY};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{0, 6, 3, 5, 2, 4, 1, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithNaNsFollowedZeros() {
			double[] data = new double[]{0d, Double.NaN, -0.886, -0d, 0d, Double.NaN, Double.NEGATIVE_INFINITY};
			int[] indices = Sorting.descendingSort(data);
			int[] expected = new int[]{1, 5, 0, 4, 3, 2, 6};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfSortedInput() {
			double[] data = new double[395];
			Arrays.setAll(data, i -> -i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			int indices[] = Sorting.descendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfReverseInput() {
			double[] data = new double[624];
			Arrays.setAll(data, i -> i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> data.length - i - 1);
			int[] indices = Sorting.descendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfRandomInput() {
			double[] data = new double[2435];
			Arrays.setAll(data, i -> {
				double x = random();
				if (x < 0.10) {
					return Double.NaN;
				} else if (x < 0.125) {
					return -0d;
				} else if (x < 0.15) {
					return 0d;
				} else if (x < 0.175) {
					return Double.NEGATIVE_INFINITY;
				} else if (x < 0.2) {
					return Double.POSITIVE_INFINITY;
				} else {
					return 2 * random() - 1;
				}
			});

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);
			double[] reversedJdkSorting = new double[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSorting[data.length - i - 1] = jdkSorting[i];
			}

			int[] indices = Sorting.descendingSort(data);
			double[] customSorting = new double[data.length];
			Arrays.setAll(customSorting, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSorting, customSorting, EPSILON);
		}

	}


	public static class SortingApi {

		@Test(expected = NullPointerException.class)
		public void testNullSrcArray() {
			Sorting.sort(null, Order.ASCENDING);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSortingOrder() {
			Sorting.sort(new double[0], null);
		}

		@Test
		public void testSortingOfEmptyArrays() {
			int[] ascending = Sorting.sort(new double[0], Order.ASCENDING);
			int[] descending = Sorting.sort(new double[0], Order.DESCENDING);
			int[] expected = new int[0];
			assertArrayEquals(expected, ascending);
			assertArrayEquals(expected, descending);
		}

		@Test
		public void testAscendingSort() {
			double data[] = new double[7283];
			Arrays.setAll(data, i -> Math.random());

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			int[] indices = Sorting.sort(data, Order.ASCENDING);
			double[] customSorting = new double[data.length];
			Arrays.setAll(customSorting, i -> data[indices[i]]);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testDescendingSort() {
			double data[] = new double[7283];
			Arrays.setAll(data, i -> Math.random());

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);
			double[] reversedJdkSorting = new double[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSorting[data.length - i - 1] = jdkSorting[i];
			}

			int[] indices = Sorting.sort(data, Order.DESCENDING);
			double[] customSorting = new double[data.length];
			Arrays.setAll(customSorting, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSorting, customSorting, EPSILON);
		}

	}

}
