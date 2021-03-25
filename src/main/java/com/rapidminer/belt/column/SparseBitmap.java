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

package com.rapidminer.belt.column;

/**
 * Immutable bitmap used by sparse column implementations like {@link DoubleSparseColumn} or {@link
 * TimeSparseColumn} to store non-default indices. Logically this data structure represents a set of non-default
 * indices in the range of {@code [0,size]}. All indices in this range that are not explicitly specified as non-default
 * are regarded as default.
 * <p>
 * The data structure uses {@code 1.5} bits of memory per entry and its {@link #get(int)} method takes {@code O(1)}
 * time.
 *
 * @author Kevin Majchrzak
 * @see DoubleSparseColumn
 * @see TimeSparseColumn
 */
class SparseBitmap {

	/**
	 * The code relies on the fact that this is 64 because we are using 64 bit longs to represent the buckets. Do not
	 * change it.
	 */
	private static final int BUCKET_SIZE = 64;

	/**
	 * This is used to avoid division for performance. It can be applied to an index to get the corresponding bucket:
	 * {@code bucket = index / BUCKET_SIZE = index >> INDEX_TO_BUCKET_MASK}
	 */
	private static final int INDEX_TO_BUCKET_MASK = 6;

	/**
	 * This index represents the default value.
	 */
	static final int DEFAULT_INDEX = -1;

	/**
	 * This index represents indices that are out of bounds.
	 */
	static final int OUT_OF_BOUNDS_INDEX = -2;

	/**
	 * 64 bit per bucket and 1 bit per bucket entry. If the bit is 0 the entry is a default index. Otherwise it is one
	 * of the non-default indices.
	 */
	private final long[] bitMap;

	/**
	 * The buckets are represented via a one-dimensional array to avoid the memory overhead for a two-dimensional array.
	 * This array contains the buckets' start indices.
	 */
	private final int[] bucketOffsets;

	/**
	 * The logical size of the bitmap.
	 */
	private final int size;

	/**
	 * True iff the default value is the missing value (e.g. {@link Double#NaN} for double values or {@link
	 * TimeColumn#MISSING_VALUE} for time values).
	 */
	private final boolean defaultIsMissing;

	/**
	 * Takes the the size of the original data and the non-default indices. It then constructs a new immutable sparse
	 * bitmap.
	 *
	 * @param defaultIsMissing
	 * 		Should be set to true iff the default value is the missing value (e.g. {@link Double#NaN} for double values or
	 *        {@link TimeColumn#MISSING_VALUE} for time values).
	 * @param nonDefaultIndices
	 * 		Indices of all values that do not equal the default value.
	 * @param size
	 * 		The number of elements in the original data.
	 */
	SparseBitmap(boolean defaultIsMissing, int[] nonDefaultIndices, int size) {
		this.size = size;
		this.defaultIsMissing = defaultIsMissing;
		bitMap = new long[size / BUCKET_SIZE + 1];
		initializeBitMap(nonDefaultIndices);
		bucketOffsets = new int[bitMap.length];
		initializeBucketOffsets(nonDefaultIndices);
	}

	/**
	 * Maps the given index to its position in the non-default indices array iff the index is a non-default index and
	 * inside of the arrays bounds. Iff the index is a default index this method returns {@link #DEFAULT_INDEX} (see
	 * {@link #isDefaultIndex(int)}). Otherwise the index is not a default index and out of bounds and this method
	 * returns {@link #OUT_OF_BOUNDS_INDEX}.
	 *
	 * @return The index' position in the non-default indices array if it is one of the non-default indices. A negative
	 * number otherwise (see explanation above).
	 */
	int get(int index) {
		if (index < 0 || index >= size) {
			if (defaultIsMissing) {
				return DEFAULT_INDEX;
			}
			return OUT_OF_BOUNDS_INDEX;
		}
		int bucket = index >> INDEX_TO_BUCKET_MASK;
		if ((bitMap[bucket] & (1L << index)) != 0) {
			int intraBucketIndex = toIntraBucketIndex(index, bucket);
			return bucketOffsets[bucket] + intraBucketIndex; // non-default value
		} else {
			return DEFAULT_INDEX;
		}
	}

	/**
	 * Counts the number of non-default indices in the given array. Indices that are outside of the arrays bounds are
	 * also counted as non-defaults iff the default value is not the missing value (e.g. {@link Double#NaN} for double
	 * values or {@link TimeColumn#MISSING_VALUE} for time values).
	 *
	 * @param indices
	 * 		Array with indices.
	 * @return The Number of non-default indices in the specified array.
	 */
	int countNonDefaultIndices(int[] indices) {
		int count = 0;
		for (int i : indices) {
			if (i < 0 || i >= size) {
				if (!defaultIsMissing) {
					count++;
				}
			} else {
				int bucket = i >> INDEX_TO_BUCKET_MASK;
				if ((bitMap[bucket] & (1L << i)) != 0) {
					count++;
				}
			}
		}
		return count;
	}

	/**
	 * The logical size of the bitmap.
	 */
	int size() {
		return size;
	}

	/**
	 * Returns true iff the given index is a default index. Indices out of the bounds are also regarded as default if
	 * and only if the default value is the missing value (e.g. {@link Double#NaN} for double values or {@link
	 * TimeColumn#MISSING_VALUE} for time values).
	 */
	boolean isDefaultIndex(int index) {
		if (index < 0 || index >= size) {
			return defaultIsMissing;
		} else {
			int bucket = index >> INDEX_TO_BUCKET_MASK;
			return (bitMap[bucket] & (1L << index)) == 0;
		}
	}

	/**
	 * Sets the non-default indices to 1 in the intern bitmap.
	 */
	private void initializeBitMap(int[] nonDefaultIndices) {
		for (int index : nonDefaultIndices) {
			int bucket = index >> INDEX_TO_BUCKET_MASK;
			bitMap[bucket] |= 1L << index;
		}
	}

	/**
	 * Sets the bucket offsets in the {@link #bucketOffsets} array.
	 */
	private void initializeBucketOffsets(int[] nonDefaultIndices) {
		int bucketOffset = 0;
		int nonDefaultIndex = 0;
		for (int bucket = 0, endOfBucket = BUCKET_SIZE; bucket < bucketOffsets.length; bucket++, endOfBucket += BUCKET_SIZE) {
			bucketOffsets[bucket] = bucketOffset;
			while (nonDefaultIndex < nonDefaultIndices.length && nonDefaultIndices[nonDefaultIndex] < endOfBucket) {
				bucketOffset++;
				nonDefaultIndex++;
			}
		}
	}

	/**
	 * Takes the logical index of a value and its bucket and returns the index that this value has in the intern bucket
	 * representation. Throws an {@link IndexOutOfBoundsException} if the specified bucket is out of bounds.
	 */
	private int toIntraBucketIndex(int index, int bucket) {
		// counts the number of 1 bits in the bucket that occur right from the specified index
		long rightBitsMask = (1L << index) - 1;
		return Long.bitCount(bitMap[bucket] & rightBitsMask);
	}

}
