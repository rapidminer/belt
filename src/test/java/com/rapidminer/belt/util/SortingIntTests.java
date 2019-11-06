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

package com.rapidminer.belt.util;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.SplittableRandom;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * Tests the {@link SortingInt} class.
 *
 * @author Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class SortingIntTests {

	public static class InsertionSort {

		@Test
		public void testAscendingOfSortedInput() {
			int[] data = new int[]{713, 901, 961, 1140, 2070, 3036, 3385, 3467};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingInt.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInput() {
			int[] data = new int[]{3351, 2532, 2475, 2350, 1729, 1268, 283, 161};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			SortingInt.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfRandomInput() {
			int[] data = new int[38];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextInt());

			int[] jdkSortingInt = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingInt);

			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			SortingInt.ascendingInsertionSort(data, indices, 0, data.length);
			int[] customSortingInt = new int[data.length];
			Arrays.setAll(customSortingInt, i -> data[indices[i]]);

			assertArrayEquals(jdkSortingInt, customSortingInt);
		}

		@Test
		public void testAscendingStability() {
			int[] data = new int[]{-1, 157, 423, 423, 350, -89, 157, 375};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{5, 0, 1, 6, 4, 7, 2, 3};
			SortingInt.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSubArray() {
			int[] data = new int[]{470, 418, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 3, 4, 5, 2, 6, 7};
			SortingInt.ascendingInsertionSort(data, indices, 2, data.length - 2);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingWithShuffledIndices() {
			int[] data = new int[]{470, 444, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[]{2, 6, 7, 1, 0, 4, 3, 5};
			int[] expected = new int[]{3, 4, 7, 6, 5, 2, 1, 0};
			SortingInt.ascendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInput() {
			int[] data = new int[]{3467, 3385, 3360, 2700, 1140, 961, 910, 713};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingInt.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInput() {
			int[] data = new int[]{161, 283, 1268, 1729, 2350, 2475, 2532, 3351};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			SortingInt.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfRandomInput() {
			int[] data = new int[38];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextInt());

			int[] jdkSortingInt = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingInt);
			int[] reversedJdkSortingInt = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSortingInt[data.length - i - 1] = jdkSortingInt[i];
			}

			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			SortingInt.descendingInsertionSort(data, indices, 0, data.length);
			int[] customSortingInt = new int[data.length];
			Arrays.setAll(customSortingInt, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSortingInt, customSortingInt);
		}

		@Test
		public void testDescendingStability() {
			int[] data = new int[]{-1, 157, 423, 423, 350, -89, 157, 375};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{2, 3, 7, 4, 1, 6, 0, 5};
			SortingInt.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSubArray() {
			int[] data = new int[]{470, 418, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 5, 4, 3, 6, 7};
			SortingInt.descendingInsertionSort(data, indices, 2, data.length - 2);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingWithShuffledIndices() {
			int[] data = new int[]{470, 444, 444, -500, -351, 363, 346, -222};
			int[] indices = new int[]{2, 6, 7, 1, 0, 4, 3, 5};
			int[] expected = new int[]{0, 2, 1, 5, 6, 7, 4, 3};
			SortingInt.descendingInsertionSort(data, indices, 0, data.length);
			assertArrayEquals(expected, indices);
		}

	}

	public static class Merge {

		@Test
		public void testAscendingOfSortedInputWithEvenSplit() {
			int[] data = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSortedInputWithOddSplit() {
			int[] data = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 0, 3, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfInterleavedInputWithEvenSplit() {
			int[] data = new int[]{0, 2, 4, 6, 1, 3, 5, 7};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 4, 1, 5, 2, 6, 3, 7};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfInterleavedInputWithOddSplit() {
			int[] data = new int[]{0, 2, 4, 6, 8, 1, 3, 5, 7};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 5, 1, 6, 2, 7, 3, 8, 4};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 0, 5, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInputWithEvenSplit() {
			int[] data = new int[]{0, 1, 2, 3, -4, -3, -2, -1};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 0, 1, 2, 3};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfReversedInputWithOddSplit() {
			int[] data = new int[]{0, 1, 2, 3, -5, -4, -3, -2, -1};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 8, 0, 1, 2, 3};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscendingOfSubArray() {
			int[] data = new int[]{0, 0, 0, 0, 1, 2, 5, 6, 2, 4, 5, 9, 0, 0, 0, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 8, 9, 6, 10, 7, 11, 12, 13, 14, 15};
			SortingInt.ascendingMerge(data, indices, new int[data.length], 4, 8, 12);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInputWithEvenSplit() {
			int[] data = new int[]{7, 6, 5, 4, 3, 2, 1, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7};
			SortingInt.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSortedInputWithOddSplit() {
			int[] data = new int[]{8, 7, 6, 5, 4, 3, 2, 1, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8};
			SortingInt.descendingMerge(data, indices, new int[data.length], 0, 3, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfInterleavedInputWithEvenSplit() {
			int[] data = new int[]{7, 5, 3, 1, 6, 4, 2, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 4, 1, 5, 2, 6, 3, 7};
			SortingInt.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfInterleavedInputWithOddSplit() {
			int[] data = new int[]{8, 6, 4, 2, 0, 7, 5, 3, 1};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 5, 1, 6, 2, 7, 3, 8, 4};
			SortingInt.descendingMerge(data, indices, new int[data.length], 0, 5, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInputWithEvenSplit() {
			int[] data = new int[]{3, 2, 1, 0, 7, 6, 5, 4};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 0, 1, 2, 3};
			SortingInt.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfReversedInputWithOddSplit() {
			int[] data = new int[]{3, 2, 1, 0, 8, 7, 6, 5, 4};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{4, 5, 6, 7, 8, 0, 1, 2, 3};
			SortingInt.descendingMerge(data, indices, new int[data.length], 0, 4, data.length);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescendingOfSubArray() {
			int[] data = new int[]{0, 0, 0, 0, 6, 5, 2, 1, 9, 5, 4, 2, 0, 0, 0, 0};
			int[] indices = new int[data.length];
			Arrays.setAll(indices, i -> i);
			int[] expected = new int[]{0, 1, 2, 3, 8, 4, 5, 9, 10, 6, 11, 7, 12, 13, 14, 15};
			SortingInt.descendingMerge(data, indices, new int[data.length], 4, 8, 12);
			assertArrayEquals(expected, indices);
		}

	}

	public static class Sort {

		@Test
		public void testAscSort() {
			int[] data = new int[]{371, 124, 593, 523, 991, 135, 182, 18};
			int[] indices = SortingInt.ascendingSort(data);
			int[] expected = new int[]{7, 1, 5, 6, 0, 3, 2, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithLeadingZeros() {
			int[] data = new int[]{0, 0, -0, 523, 991, 135, 182, 18};
			int[] indices = SortingInt.ascendingSort(data);
			int[] expected = new int[]{0, 1, 2, 7, 5, 6, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithInterleavedZeros() {
			int[] data = new int[]{-398, 0, -0, 523, 991, -135, 182, 18};
			int[] indices = SortingInt.ascendingSort(data);
			int[] expected = new int[]{0, 5, 1, 2, 7, 6, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortWithTrailingZeros() {
			int[] data = new int[]{-398, 0, -0, -523, -991, -135, -182, -18};
			int[] indices = SortingInt.ascendingSort(data);
			int[] expected = new int[]{4, 3, 0, 6, 5, 7, 1, 2};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfSortedInput() {
			int[] data = new int[595];
			Arrays.setAll(data, i -> i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			int[] indices = SortingInt.ascendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfReverseInput() {
			int[] data = new int[397];
			Arrays.setAll(data, i -> -i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> data.length - i - 1);
			int[] indices = SortingInt.ascendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testAscSortOfRandomInput() {
			int[] data = new int[3923];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextInt());

			int[] jdkSortingInt = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingInt);

			int[] indices = SortingInt.ascendingSort(data);
			int[] customSortingInt = new int[data.length];
			Arrays.setAll(customSortingInt, i -> data[indices[i]]);

			assertArrayEquals(jdkSortingInt, customSortingInt);
		}

		@Test
		public void testDescSort() {
			int[] data = new int[]{371, 124, 593, 523, 991, 135, 182, 18};
			int[] indices = SortingInt.descendingSort(data);
			int[] expected = new int[]{4, 2, 3, 0, 6, 5, 1, 7};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithLeadingZeros() {
			int[] data = new int[]{-398, -0, 0, -523, -991, -135, -182, -18};
			int[] indices = SortingInt.descendingSort(data);
			int[] expected = new int[]{1, 2, 7, 5, 6, 0, 3, 4};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithInterleavedZeros() {
			int[] data = new int[]{-398, -0, 0, 523, 991, -135, 182, 18};
			int[] indices = SortingInt.descendingSort(data);
			int[] expected = new int[]{4, 3, 6, 7, 1, 2, 5, 0};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortWithTrailingZeros() {
			int[] data = new int[]{0, -0, 0, 523, 991, 135, 182, 18};
			int[] indices = SortingInt.descendingSort(data);
			int[] expected = new int[]{4, 3, 6, 5, 7, 0, 1, 2};
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfSortedInput() {
			int[] data = new int[395];
			Arrays.setAll(data, i -> -i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> i);
			int[] indices = SortingInt.descendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfReverseInput() {
			int[] data = new int[624];
			Arrays.setAll(data, i -> i);
			int[] expected = new int[data.length];
			Arrays.setAll(expected, i -> data.length - i - 1);
			int[] indices = SortingInt.descendingSort(data);
			assertArrayEquals(expected, indices);
		}

		@Test
		public void testDescSortOfRandomInput() {
			int[] data = new int[3923];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextInt());

			int[] jdkSortingInt = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingInt);
			int[] reversedJdkSortingInt = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSortingInt[data.length - i - 1] = jdkSortingInt[i];
			}

			int[] indices = SortingInt.descendingSort(data);
			int[] customSortingInt = new int[data.length];
			Arrays.setAll(customSortingInt, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSortingInt, customSortingInt);
		}

	}


	public static class SortingIntApi {

		@Test(expected = NullPointerException.class)
		public void testNullSrcArray() {
			SortingInt.sort(null, Order.ASCENDING);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSortingIntOrder() {
			SortingInt.sort(new int[0], null);
		}

		@Test
		public void testSortingIntOfEmptyArrays() {
			int[] ascending = SortingInt.sort(new int[0], Order.ASCENDING);
			int[] descending = SortingInt.sort(new int[0], Order.DESCENDING);
			int[] expected = new int[0];
			assertArrayEquals(expected, ascending);
			assertArrayEquals(expected, descending);
		}

		@Test
		public void testAscendingSort() {
			int[] data = new int[7283];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextInt());

			int[] jdkSortingInt = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingInt);

			int[] indices = SortingInt.sort(data, Order.ASCENDING);
			int[] customSortingInt = new int[data.length];
			Arrays.setAll(customSortingInt, i -> data[indices[i]]);

			assertArrayEquals(jdkSortingInt, customSortingInt);
		}

		@Test
		public void testDescendingSort() {
			int[] data = new int[7283];
			SplittableRandom random = new SplittableRandom();
			Arrays.setAll(data, i -> random.nextInt());

			int[] jdkSortingInt = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSortingInt);
			int[] reversedJdkSortingInt = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				reversedJdkSortingInt[data.length - i - 1] = jdkSortingInt[i];
			}

			int[] indices = SortingInt.sort(data, Order.DESCENDING);
			int[] customSortingInt = new int[data.length];
			Arrays.setAll(customSortingInt, i -> data[indices[i]]);

			assertArrayEquals(reversedJdkSortingInt, customSortingInt);
		}

	}

}
