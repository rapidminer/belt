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

import java.util.Arrays;
import java.util.Objects;


/**
 * Similar to {@link SortingLong} but for integer values and with additional methods for sorting partially.
 *
 * @author Kevin Majchrzak
 * @see SortingLong
 * @see Sorting
 */
public final class SortingInt {

	private static final int MIN_DIVIDE = 16;

	// Suppress default constructor for noninstantiability
	private SortingInt() {
		throw new AssertionError();
	}

	/**
	 * Indirect stable sort of int values using the given order. This method does not modify the given source array.
	 * Instead, it computes and returns an index mapping that if applied to the input results in a sorted sequence.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @param order
	 * 		the sorting order
	 * @return the index mapping resulting in a sorted sequence
	 * @throws NullPointerException
	 * 		if the source array or the order is {@code null}
	 */
	public static int[] sort(int[] src, Order order) {
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
	 * Similar to {@link #sort(int[], Order)} but sorts only the specified range of the given indices array. It sorts
	 * the indices array (will be modified) according to the int values stored in src (will not be modified).
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @param indices
	 * 		the indices array (the range between {@code start} and {@code end} will be sorted)
	 * @param start
	 * 		The subarray of indices for the range {@code [start, end]} will be sorted.
	 * @param end
	 * 		The subarray of indices for the range {@code [start, end]} will be sorted.
	 * @param buffer
	 * 		Optionally a buffer of the same length as the src array can be provided. Caution: If no buffer is provided (if
	 * 		buffer is {@code null}) a new buffer of the same length as the src array will be created. Throws an
	 * 		IllegalArgumentException for buffers of wrong length.
	 * @param order
	 * 		The order that the partition of the array will be sorted by.
	 */
	public static void sortPartially(int[] src, int[] indices, int start, int end, int[] buffer, Order order) {
		Objects.requireNonNull(src, "Source array must not be null");
		Objects.requireNonNull(order, "Sorting order must not be null");
		Objects.requireNonNull(indices, "Indices must not be null");
		if (buffer != null && buffer.length != src.length) {
			throw new IllegalArgumentException("Buffer size musst equal src array size");
		}
		switch (order) {
			case ASCENDING:
				partialAscendingSort(src, indices, start, end, buffer != null ? buffer : new int[src.length]);
				break;
			case DESCENDING:
				partialDescendingSort(src, indices, start, end, buffer != null ? buffer : new int[src.length]);
				break;
			default:
				throw new UnsupportedOperationException("Unsupported sorting order");
		}
	}

	/**
	 * Indirect stable ascending sort of int values. This method does not modify the given source array. Instead, it
	 * computes and returns an index mapping that if applied to the input results in a sorted sequence.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @return the index mapping resulting in a sorted sequence
	 */
	static int[] ascendingSort(int[] src) {
		int[] indices = new int[src.length];
		for(int i = 0; i < indices.length; i++){
			indices[i] = i;
		}
		int[] buffer = new int[src.length];
		ascendingMergeSort(src, indices, buffer, 0, src.length);
		return indices;
	}

	/**
	 * Indirect stable descending sort of int values. This method does not modify the given source array. Instead, it
	 * computes and returns an index mapping that if applied to the input results in a sorted sequence.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @return the index mapping resulting in a sorted sequence
	 */
	static int[] descendingSort(int[] src) {
		int[] indices = new int[src.length];
		for(int i = 0; i < indices.length; i++){
			indices[i] = i;
		}
		int[] buffer = new int[src.length];
		descendingMergeSort(src, indices, buffer, 0, src.length);
		return indices;
	}

	/**
	 * Indirect stable ascending sort of int values. This method does not modify the given source array. Instead, it
	 * computes and returns an index mapping that if applied to the input results in a sorted sequence.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @param indices
	 * 		The indices array will be reordered in the range {@code [start, end]}.
	 * @param start
	 * 		The indices array will be reordered in the range {@code [start, end]}.
	 * @param end
	 * 		The indices array will be reordered in the range {@code [start, end]}.
	 * @param buffer
	 * 		buffer for temporary results (will be modified in the range {@code [start, end)}
	 */
	private static void partialAscendingSort(int[] src, int[] indices, int start, int end, int[] buffer) {
		ascendingMergeSort(src, indices, buffer, start, end);
	}

	/**
	 * Indirect stable descending sort of int values. This method does not modify the given source array. Instead, it
	 * computes and returns an index mapping that if applied to the input results in a sorted sequence.
	 *
	 * @param src
	 * 		the source array (remains unchanged)
	 * @param indices
	 * 		The indices array will be reordered in the range {@code [start, end]}.
	 * @param start
	 * 		The indices array will be reordered in the range {@code [start, end]}.
	 * @param end
	 * 		The indices array will be reordered in the range {@code [start, end]}.
	 * @param buffer
	 * 		for temporary results (will be modified in the range {@code [start, end)}
	 */
	private static void partialDescendingSort(int[] src, int[] indices, int start, int end, int[] buffer) {
		descendingMergeSort(src, indices, buffer, start, end);
	}

	/**
	 * Indirect ascending merge sort. This method does not modify the given source array. Instead, it changes the
	 * entries in the given index mapping.
	 *
	 * <p>The method assumes that the index array contains a valid mapping.
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
	private static void ascendingMergeSort(int[] src, int[] indices, int[] buffer, int start, int end) {
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
	 * <p>The method assumes that the index array contains a valid mapping.
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
	private static void descendingMergeSort(int[] src, int[] indices, int[] buffer, int start, int end) {
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
	 * <p>The method assumes that the index array contains a valid mapping and that the intervals {@code [start,
	 * middle)} and {@code [middle, end)} are sorted indirectly in ascending order.
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
	static void ascendingMerge(int[] src, int[] indices, int[] buffer, int start, int middle, int end) {
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
	 * <p>The method assumes that the index array contains a valid mapping and that the intervals {@code [start,
	 * middle)} and {@code [middle, end)} are sorted indirectly in descending order.
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
	static void descendingMerge(int[] src, int[] indices, int[] buffer, int start, int middle, int end) {
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
	 * <p>The method assumes that the index array contains a valid mapping.
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
	static void ascendingInsertionSort(int[] src, int[] indices, int start, int end) {
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
	 * <p>The method assumes that the index array contains a valid mapping.
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
	static void descendingInsertionSort(int[] src, int[] indices, int start, int end) {
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

}
