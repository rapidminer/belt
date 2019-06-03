/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;


/**
 * Utility methods for sorting.
 *
 * @author Michael Knopf
 */
public final class Sorting {

	private static final int MIN_DIVIDE = 16;

	// Suppress default constructor for noninstantiability
	private Sorting() {
		throw new AssertionError();
	}

	/**
	 * Sorts the indices {@code [0, 1, ..., length-1]} with respect to the given comparator and order.
	 *
	 * @param length
	 * 		the length of the index array
	 * @param comparator
	 * 		the comparator to use
	 * @param order
	 * 		the sorting order
	 * @return the sorted indices
	 */
	public static int[] sort(int length, Comparator<Integer> comparator, Order order) {
		Objects.requireNonNull(comparator, "Comparator must not be null");
		Objects.requireNonNull(order, "Sorting order must not be null");
		Integer[] indices = new Integer[length];
		for (int i = 0; i < indices.length; i++) {
			indices[i] = i;
		}
		switch (order) {
			case ASCENDING:
				return sort(indices, comparator);
			case DESCENDING:
				return sort(indices, comparator.reversed());
			default:
				throw new UnsupportedOperationException("Unsupported sorting order");
		}
	}

	/**
	 * Sorts the given indices array with respect to the comparator and converts the result to a primitive array.
	 *
	 * @param indices
	 * 		the indices to sort
	 * @param comparator
	 * 		the comparator to use
	 * @return the sorted indices as primitives
	 */
	private static int[] sort(Integer[] indices, Comparator<Integer> comparator) {
		Arrays.sort(indices, comparator);
		int[] intIndices = new int[indices.length];
		for (int i = 0; i < indices.length; i++) {
			intIndices[i] = indices[i];
		}
		return intIndices;
	}

	/**
	 * Indirect stable sort of double values using the given order. This method does not modify the given source array.
	 * Instead, it computes and returns an index mapping that if applied to the input results in a sorted sequence.
	 * Following {@link Arrays#sort(double[])}, this method uses the total order imposed by the method
	 * {@link Double#compareTo}.
	 *
	 * @param src
	 *            the source array (remains unchanged))
	 * @param order
	 *            the sorting order
	 * @return the index mapping resulting in a sorted sequence
	 * @throws NullPointerException
	 *             if the source array or the order is {@code null}
	 */
	public static int[] sort(double[] src, Order order) {
		Objects.requireNonNull(src, "Source array must not be null");
		Objects.requireNonNull(order, "Sorting order must not be null");
		switch (order) {
			case ASCENDING:
				return ascendingSort(src);
			case DESCENDING:
				return descendingSort(src);
			default:
				throw new UnsupportedOperationException("Unsupported sorting order");
		}
	}

	/**
	 * Indirect stable ascending sort of double values. This method does not modify the given source array. Instead, it
	 * computes and returns an index mapping that if applied to the input results in a sorted sequence. Following
	 * {@link Arrays#sort(double[])}, this method uses the total order imposed by the method {@link Double#compareTo}.
	 *
	 * <p>
	 * Implementation note: the algorithm consist of three phases. First, {@link Double#NaN} values are moved to the end
	 * of the index array. Second, all remaining items are sorted using a merge-insertion sort hybrid. Finally, zero
	 * items ({@code -0d} and {@code 0d}) are sorted w.r.t. their sign bit using a modified insertion sort.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @return the index mapping resulting in a sorted sequence
	 */
	static int[] ascendingSort(double[] src) {
		int firstNaN = 0;
		int[] indices = new int[src.length];

		// fill index array with non-NaN items (in order)
		for (int i = 0; i < indices.length; i++) {
			if (!Double.isNaN(src[i])) {
				indices[firstNaN++] = i;
			}
		}

		// fill end of the index array with NaN items (in order)
		if (firstNaN < indices.length) {
			int j = src.length - 1;
			for (int i = src.length - 1; i >= 0 && j >= firstNaN; i--) {
				if (Double.isNaN(src[i])) {
					indices[j--] = i;
				}
			}
		}


		// sort non-NaN values
		int[] buffer = new int[firstNaN];
		ascendingMergeSort(src, indices, buffer, 0, firstNaN);

		// find interval containing all zero items (if any)
		int leftBound = 0;
		int rightBound = firstNaN;

		int pivot = rightBound;
		while (leftBound < pivot) {
			int middle = (leftBound + pivot) / 2;
			double value = src[indices[middle]];
			if (value < 0d) {
				leftBound = middle + 1;
			} else {
				pivot = middle;
			}
		}

		pivot = leftBound;
		while (pivot < rightBound - 1) {
			int middle = (pivot + rightBound) / 2;
			double value = src[indices[middle]];
			if (value > 0d) {
				rightBound = middle;
			} else {
				pivot = middle;
			}
		}

		// sort zero items w.r.t. to their sign bit
		if (leftBound < rightBound - 1) {
			ascendingBinaryInsertionSort(src, indices, leftBound, rightBound);
		}

		return indices;
	}

	/**
	 * Indirect stable descending sort of double values. This method does not modify the given source array. Instead, it
	 * computes and returns an index mapping that if applied to the input results in a sorted sequence. Following
	 * {@link Arrays#sort(double[])}, this method uses the inverse of the total order imposed by the method
	 * {@link Double#compareTo}.
	 *
	 * <p>
	 * Implementation note: the algorithm consist of three phases. First, non-{@link Double#NaN} values are moved to the
	 * back of the index array. Second, all remaining items are sorted using a merge-insertion sort hybrid. Finally,
	 * zero items ({@code -0d} and {@code 0d}) are sorted w.r.t. their sign bit using a modified insertion sort.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @return the index mapping resulting in a sorted sequence
	 */
	static int[] descendingSort(double[] src) {
		int firstNumber;
		int[] indices = new int[src.length];
		firstNumber = src.length;

		// fill back of index array with non-Nan items (keeping order)
		for (int i = indices.length - 1; i >= 0; i--) {
			if (!Double.isNaN(src[i])) {
				indices[--firstNumber] = i;
			}
		}

		// fill start of the index array with NaN items (in order)
		if (firstNumber > 0) {
			int j = 0;
			for (int i = 0; i < indices.length && j < firstNumber; i++) {
				if (Double.isNaN(src[i])) {
					indices[j++] = i;
				}
			}
		}

		// sort non-NaN values
		int[] buffer = new int[indices.length];
		descendingMergeSort(src, indices, buffer, firstNumber, indices.length);

		// find interval containing all zero items (if any)
		int leftBound = firstNumber;
		int rightBound = indices.length;

		int pivot = rightBound;
		while (leftBound < pivot) {
			int middle = (leftBound + pivot) / 2;
			double value = src[indices[middle]];
			if (value > 0d) {
				leftBound = middle + 1;
			} else {
				pivot = middle;
			}
		}

		pivot = leftBound;
		while (pivot < rightBound - 1) {
			int middle = (pivot + rightBound) / 2;
			double value = src[indices[middle]];
			if (value < 0d) {
				rightBound = middle;
			} else {
				pivot = middle;
			}
		}

		// sort zero items w.r.t. to their sign bit
		if (leftBound < rightBound - 1) {
			descendingBinaryInsertionSort(src, indices, leftBound, rightBound);
		}

		return indices;
	}

	/**
	 * Indirect ascending merge sort. This method does not modify the given source array. Instead, it changes the
	 * entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping and that the subset of values in the source
	 * array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param buffer
	 * 		buffer for temporary results (will be modified in the range {@code [start, end)}
	 * @param start
	 * 		the start of the interval to sort (inclusive)
	 * @param end
	 * 		the end of the interval to sort (exclusive)
	 */
	static void ascendingMergeSort(double[] src, int[] indices, int[] buffer, int start, int end) {
		if (start + MIN_DIVIDE < end) {
			int middle = start + Integer.highestOneBit((end - start) / 2);
			ascendingMergeSort(src, indices, buffer, start, middle);
			ascendingMergeSort(src, indices, buffer, middle, end);
			ascendingMerge(src, indices, buffer, start, middle, end);
		} else {
			ascendingInsertionSort(src, indices, start, end);
		}
	}

	/**
	 * Indirect descending merge sort. This method does not modify the given source array. Instead, it changes the
	 * entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping and that the subset of values in the source
	 * array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param buffer
	 * 		buffer for temporary results (will be modified in the range {@code [start, end)}
	 * @param start
	 * 		the start of the interval to sort (inclusive)
	 * @param end
	 * 		the end of the interval to sort (exclusive)
	 */
	static void descendingMergeSort(double[] src, int[] indices, int[] buffer, int start, int end) {
		if (start + MIN_DIVIDE < end) {
			int middle = start + Integer.highestOneBit((end - start) / 2);
			descendingMergeSort(src, indices, buffer, start, middle);
			descendingMergeSort(src, indices, buffer, middle, end);
			descendingMerge(src, indices, buffer, start, middle, end);
		} else {
			descendingInsertionSort(src, indices, start, end);
		}
	}

	/**
	 * Indirect ascending merge implementation. This method does not modify the given source array. Instead, it changes
	 * the entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping, that the intervals {@code [start, middle)}
	 * and {@code [middle, end)} are sorted indirectly in ascending order, and that the interval {@code [start, end)} of
	 * values in the source array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param buffer
	 * 		buffer for temporary results (will be modified in the range {@code [start, end)}
	 * @param start
	 * 		the start of the left sub-interval to sort (inclusive)
	 * @param middle
	 * 		the start of the right sub-interval to sort (inclusive)
	 * @param end
	 * 		the end of the right sub-interval to sort (exclusive)
	 */
	static void ascendingMerge(double[] src, int[] indices, int[] buffer, int start, int middle, int end) {
		System.arraycopy(indices, start, buffer, start, end - start);
		int leftPosition = start;
		int rightPosition = middle;
		int srcPosition = start;
		while (leftPosition < middle && rightPosition < end) {
			if (src[buffer[leftPosition]] <= src[buffer[rightPosition]]) {
				indices[srcPosition++] = buffer[leftPosition++];
			} else {
				indices[srcPosition++] = buffer[rightPosition++];
			}
		}
		if (leftPosition < middle) {
			System.arraycopy(buffer, leftPosition, indices, srcPosition, end - srcPosition);
		} else if (rightPosition < end) {
			System.arraycopy(buffer, rightPosition, indices, srcPosition, end - srcPosition);
		}
	}

	/**
	 * Indirect descending merge implementation. This method does not modify the given source array. Instead, it changes
	 * the entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping, that the intervals {@code [start, middle)}
	 * and {@code [middle, end)} are sorted indirectly in descending order, and that the interval {@code [start, end)}
	 * of values in the source array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param buffer
	 * 		buffer for temporary results (will be modified in the range {@code [start, end)}
	 * @param start
	 * 		the start of the left sub-interval to sort (inclusive)
	 * @param middle
	 * 		the start of the right sub-interval to sort (inclusive)
	 * @param end
	 * 		the end of the right sub-interval to sort (exclusive)
	 */
	static void descendingMerge(double[] src, int[] indices, int[] buffer, int start, int middle, int end) {
		System.arraycopy(indices, start, buffer, start, end - start);
		int leftPosition = start;
		int rightPosition = middle;
		int srcPosition = start;
		while (leftPosition < middle && rightPosition < end) {
			if (src[buffer[leftPosition]] >= src[buffer[rightPosition]]) {
				indices[srcPosition++] = buffer[leftPosition++];
			} else {
				indices[srcPosition++] = buffer[rightPosition++];
			}
		}
		if (leftPosition < middle) {
			System.arraycopy(buffer, leftPosition, indices, srcPosition, end - srcPosition);
		} else if (rightPosition < end) {
			System.arraycopy(buffer, rightPosition, indices, srcPosition, end - srcPosition);
		}
	}

	/**
	 * Indirect ascending insertion sort. This method does not modify the given source array. Instead, it changes the
	 * entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping and that the subset of values in the source
	 * array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param start
	 * 		the start of the interval to sort (inclusive)
	 * @param end
	 * 		the end of the interval to sort (exclusive)
	 */
	static void ascendingInsertionSort(double[] src, int[] indices, int start, int end) {
		for (int i = start + 1; i < end; i++) {
			int ix = indices[i];
			double x = src[ix];
			int j = i - 1;
			while (j >= start && src[indices[j]] > x) {
				indices[j + 1] = indices[j];
				j--;
			}
			indices[j + 1] = ix;
		}
	}

	/**
	 * Indirect descending insertion sort. This method does not modify the given source array. Instead, it changes the
	 * entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping and that the subset of values in the source
	 * array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param start
	 * 		the start of the interval to sort (inclusive)
	 * @param end
	 * 		the end of the interval to sort (exclusive)
	 */
	static void descendingInsertionSort(double[] src, int[] indices, int start, int end) {
		for (int i = start + 1; i < end; i++) {
			int ix = indices[i];
			double x = src[ix];
			int j = i - 1;
			while (j >= start && src[indices[j]] < x) {
				indices[j + 1] = indices[j];
				j--;
			}
			indices[j + 1] = ix;
		}
	}

	/**
	 * Indirect ascending insertion sort that only considers the sign bit of the given values. In other words, it
	 * partitions the input into negative and positive values. In particular, it differentiates between the values
	 * {@code -0d} and {@code 0d}. This method does not modify the given source array. Instead, it changes the entries
	 * in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping and that the subset of values in the source
	 * array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param start
	 * 		the start of the interval to sort (inclusive)
	 * @param end
	 * 		the end of the interval to sort (exclusive)
	 */
	static void ascendingBinaryInsertionSort(double[] src, int[] indices, int start, int end) {
		for (int i = start + 1; i < end; i++) {
			int ix = indices[i];
			if (Double.doubleToRawLongBits(src[ix]) < 0) {
				int j = i - 1;
				while (j >= start && Double.doubleToRawLongBits(src[indices[j]]) >= 0) {
					indices[j + 1] = indices[j];
					j--;
				}
				indices[j + 1] = ix;
			}
		}
	}

	/**
	 * Indirect descending insertion sort that only considers the sign bit of the given values. In other words, it
	 * partitions the input into negative and positive values. In particular, it differentiates between the values
	 * {@code -0d} and {@code 0d}. This method does not modify the given source array. Instead, it changes the entries
	 * in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping and that the subset of values in the source
	 * array does not contain NaNs.
	 *
	 * @param src
	 * 		the source array (remains unchanged))
	 * @param indices
	 * 		the index mapping (will be reordered)
	 * @param start
	 * 		the start of the interval to sort (inclusive)
	 * @param end
	 * 		the end of the interval to sort (exclusive)
	 */
	static void descendingBinaryInsertionSort(double[] src, int[] indices, int start, int end) {
		for (int i = start + 1; i < end; i++) {
			int ix = indices[i];
			if (Double.doubleToRawLongBits(src[ix]) >= 0) {
				int j = i - 1;
				while (j >= start && Double.doubleToRawLongBits(src[indices[j]]) < 0) {
					indices[j + 1] = indices[j];
					j--;
				}
				indices[j + 1] = ix;
			}
		}
	}

}
