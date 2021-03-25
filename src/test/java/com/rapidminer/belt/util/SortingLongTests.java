/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.SplittableRandom;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * Tests the {@link SortingLong} class.
 *
 * @author Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class SortingLongTests {

	public static class InsertionSort {

		@Test
		public void testAscendingOfSortedInput() {
			long[] data = new long[]{713, 901, 961, 1140, 2070, 3036, 3385, 3467};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingLong.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInput() {
			long[] data = new long[]{3351, 2532, 2475, 2350, 1729, 1268, 283, 161};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			SortingLong.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfRandomInput() {
			long[] data = new long[38];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextLong());

			long[] jdkSortingLong = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingLong);

			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			SortingLong.ascendingInsertionSort(data, indices, 0, data.length);
			long[] customSortingLong = new long[data.length];
			Arrays.setAll(customSortingLong, i -> data[indices[i]]);

			assertArrayEquals(jdkSortingLong, customSortingLong);
		}

		@Test
		public void testAscendingStability() {
			long[] data = new long[]{-1, 157, 423, 423, 350, -89, 157, 375};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{5, 0, 1, 6, 4, 7, 2, 3};
			SortingLong.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSubArray() {
			long[] data = new long[]{470, 418, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 3, 4, 5, 2, 6, 7};
			SortingLong.ascendingInsertionSort(data, indices, 2, data.length - 2);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingWithShuffledIndices() {
			long[] data = new long[]{470, 444, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[]{2, 6, 7, 1, 0, 4, 3, 5};
			int[] expected = new int[]{3, 4, 7, 6, 5, 2, 1, 0};
			SortingLong.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInput() {
			long[] data = new long[]{3467, 3385, 3360, 2700, 1140, 961, 910, 713};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingLong.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInput() {
			long[] data = new long[]{161, 283, 1268, 1729, 2350, 2475, 2532, 3351};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			SortingLong.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfRandomInput() {
			long[] data = new long[38];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextLong());

			long[] jdkSortingLong = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingLong);
			long[] reversedJdkSortingLong = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSortingLong[data.length - i - 1] = jdkSortingLong[i];
			}

			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			SortingLong.descendingInsertionSort(data, indices, 0, data.length);
			long[] customSortingLong = new long[data.length];
			Arrays.setAll(customSortingLong, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSortingLong, customSortingLong);
		}

		@Test
		public void testDescendingStability() {
			long[] data = new long[]{-1, 157, 423, 423, 350, -89, 157, 375};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{2, 3, 7, 4, 1, 6, 0, 5};
			SortingLong.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSubArray() {
			long[] data = new long[]{470, 418, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 5, 4, 3, 6, 7};
			SortingLong.descendingInsertionSort(data, indices, 2, data.length - 2);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingWithShuffledIndices() {
			long[] data = new long[]{470, 444, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[]{2, 6, 7, 1, 0, 4, 3, 5};
			int[] expected = new int[]{0, 2, 1, 5, 6, 7, 4, 3};
			SortingLong.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

	}

	public static class Merge {

		@Test
		public void testAscendingOfSortedInputWithEvenSplit() {
			long[] data = new long[]{0, 1, 2, 3, 4, 5, 6, 7};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSortedInputWithOddSplit() {
			long[] data = new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 0, 3, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfInterleavedInputWithEvenSplit() {
			long[] data = new long[]{0, 2, 4, 6, 1, 3, 5, 7};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 4, 1, 5, 2, 6, 3, 7};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfInterleavedInputWithOddSplit() {
			long[] data = new long[]{0, 2, 4, 6, 8, 1, 3, 5, 7};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 5, 1, 6, 2, 7, 3, 8, 4};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 0, 5, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInputWithEvenSplit() {
			long[] data = new long[]{0, 1, 2, 3, -4, -3, -2, -1};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 0, 1, 2, 3};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInputWithOddSplit() {
			long[] data = new long[]{0, 1, 2, 3, -5, -4, -3, -2, -1};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 8, 0, 1, 2, 3};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSubArray() {
			long[] data = new long[]{0, 0, 0, 0, 1, 2, 5, 6, 2, 4, 5, 9, 0, 0, 0, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 8, 9, 6, 10, 7, 11, 12, 13, 14, 15};
			SortingLong.ascendingMerge(data, indices, new int[data.length], 4, 8, 12);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInputWithEvenSplit() {
			long[] data = new long[]{7, 6, 5, 4, 3, 2, 1, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingLong.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInputWithOddSplit() {
			long[] data = new long[]{8, 7, 6, 5, 4, 3, 2, 1, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			SortingLong.descendingMerge(data, indices, new int[data.length], 0, 3, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfInterleavedInputWithEvenSplit() {
			long[] data = new long[]{7, 5, 3, 1, 6, 4, 2, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 4, 1, 5, 2, 6, 3, 7};
			SortingLong.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfInterleavedInputWithOddSplit() {
			long[] data = new long[]{8, 6, 4, 2, 0, 7, 5, 3, 1};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 5, 1, 6, 2, 7, 3, 8, 4};
			SortingLong.descendingMerge(data, indices, new int[data.length], 0, 5, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInputWithEvenSplit() {
			long[] data = new long[]{3, 2, 1, 0, 7, 6, 5, 4};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 0, 1, 2, 3};
			SortingLong.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInputWithOddSplit() {
			long[] data = new long[]{3, 2, 1, 0, 8, 7, 6, 5, 4};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 8, 0, 1, 2, 3};
			SortingLong.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSubArray() {
			long[] data = new long[]{0, 0, 0, 0, 6, 5, 2, 1, 9, 5, 4, 2, 0, 0, 0, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 8, 4, 5, 9, 10, 6, 11, 7, 12, 13, 14, 15};
			SortingLong.descendingMerge(data, indices, new int[data.length], 4, 8, 12);
			assertArrayEquals(expected, indices);
		}

	}

	public static class Sort {

		@Test
		public void testAscSort() {
			long[] data = new long[]{371, 124, 593, 523, 991, 135, 182, 18};
			int[] indices = SortingLong.ascendingSort(data);
			int[] expected = new int[]{7, 1, 5, 6, 0, 3, 2, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithLeadingZeros() {
			long[] data = new long[]{0, 0, -0, 523, 991, 135, 182, 18};
			int[] indices = SortingLong.ascendingSort(data);
			int[] expected = new int[]{0, 1, 2, 7, 5, 6, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithInterleavedZeros() {
			long[] data = new long[]{-398, 0, -0, 523, 991, -135, 182, 18};
			int[] indices = SortingLong.ascendingSort(data);
			int[] expected = new int[]{0, 5, 1, 2, 7, 6, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithTrailingZeros() {
			long[] data = new long[]{-398, 0, -0, -523, -991, -135, -182, -18};
			int[] indices = SortingLong.ascendingSort(data);
			int[] expected = new int[]{4, 3, 0, 6, 5, 7, 1, 2};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfSortedInput() {
			long[] data = new long[595];
			Arrays.setAll(data, i -> i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			int[] indices = SortingLong.ascendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfReverseInput() {
			long[] data = new long[397];
			Arrays.setAll(data, i -> -i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> data.length - i - 1);
			int[] indices = SortingLong.ascendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfRandomInput() {
			long[] data = new long[3923];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextLong());

			long[] jdkSortingLong = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingLong);

			int[] indices = SortingLong.ascendingSort(data);
			long[] customSortingLong = new long[data.length];
			Arrays.setAll(customSortingLong, i -> data[indices[i]]);

			assertArrayEquals(jdkSortingLong, customSortingLong);
		}

		@Test
		public void testDescSort() {
			long[] data = new long[]{371, 124, 593, 523, 991, 135, 182, 18};
			int[] indices = SortingLong.descendingSort(data);
			int[] expected = new int[]{4, 2, 3, 0, 6, 5, 1, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithLeadingZeros() {
			long[] data = new long[]{-398, -0, 0, -523, -991, -135, -182, -18};
			int[] indices = SortingLong.descendingSort(data);
			int[] expected = new int[]{1, 2, 7, 5, 6, 0, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithInterleavedZeros() {
			long[] data = new long[]{-398, -0, 0, 523, 991, -135, 182, 18};
			int[] indices = SortingLong.descendingSort(data);
			int[] expected = new int[]{4, 3, 6, 7, 1, 2, 5, 0};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithTrailingZeros() {
			long[] data = new long[]{0, -0, 0, 523, 991, 135, 182, 18};
			int[] indices = SortingLong.descendingSort(data);
			int[] expected = new int[]{4, 3, 6, 5, 7, 0, 1, 2};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfSortedInput() {
			long[] data = new long[395];
			Arrays.setAll(data, i -> -i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			int[] indices = SortingLong.descendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfReverseInput() {
			long[] data = new long[624];
			Arrays.setAll(data, i -> i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> data.length - i - 1);
			int[] indices = SortingLong.descendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfRandomInput() {
			long[] data = new long[3923];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextLong());

			long[] jdkSortingLong = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingLong);
			long[] reversedJdkSortingLong = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSortingLong[data.length - i - 1] = jdkSortingLong[i];
			}

			int[] indices = SortingLong.descendingSort(data);
			long[] customSortingLong = new long[data.length];
			Arrays.setAll(customSortingLong, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSortingLong, customSortingLong);
		}

	}


	public static class SortingLongApi {

		@Test(expected = NullPointerException.class)
		public void testNullSrcArray() {
			SortingLong.sort(null, Order.ASCENDING);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSortingLongOrder() {
			SortingLong.sort(new long[0], null);
		}

		@Test
		public void testSortingLongOfEmptyArrays() {
			int[] ascending = SortingLong.sort(new long[0], Order.ASCENDING);
			int[] descending = SortingLong.sort(new long[0], Order.DESCENDING);
			int[] expected = new int[0];
			assertArrayEquals(expected, ascending);
			assertArrayEquals(expected, descending);
		}

		@Test
		public void testAscendingSort() {
			long[] data = new long[7283];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextLong());

			long[] jdkSortingLong = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingLong);

			int[] indices = SortingLong.sort(data, Order.ASCENDING);
			long[] customSortingLong = new long[data.length];
			Arrays.setAll(customSortingLong, i -> data[indices[i]]);

			assertArrayEquals(jdkSortingLong, customSortingLong);
		}

		@Test
		public void testDescendingSort() {
			long[] data = new long[7283];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextLong());

			long[] jdkSortingLong = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingLong);
			long[] reversedJdkSortingLong = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSortingLong[data.length - i - 1] = jdkSortingLong[i];
			}

			int[] indices = SortingLong.sort(data, Order.DESCENDING);
			long[] customSortingLong = new long[data.length];
			Arrays.setAll(customSortingLong, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSortingLong, customSortingLong);
		}

	}

}
