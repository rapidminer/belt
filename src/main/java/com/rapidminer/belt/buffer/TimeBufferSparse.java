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

package com.rapidminer.belt.buffer;

import java.time.LocalTime;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.util.ArrayBuilderConfiguration;
import com.rapidminer.belt.util.IntegerArrayBuilder;
import com.rapidminer.belt.util.LongArrayBuilder;


/**
 * Sparse time buffer implementation with fixed length, storing nanoseconds of the day as long values. The buffer is
 * write only. The {@link #setNext(long)} method can be used to set the next free position in the buffer to the given
 * value. The {@link #setNext(int, long)} method can be used to set a specific position to the given value. In this case
 * all values at smaller indices (that have not already been set) will be set to the buffer's default value. This is
 * more efficient than calling {@link #setNext(long)} for every default value. Please note that values, once they are
 * set, cannot be modified.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 * <p>
 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient for
 * data with a sparsity of {@code >= 50%}. The sparse buffer comes with a constant memory overhead so that it is
 * recommended not to use it for very small data ({@code <1024} data points).
 *
 * @author Kevin Majchrzak
 * @see TimeBuffer
 * @see Buffers
 */
public final class TimeBufferSparse {

	/**
	 * The maximum size a chunk can have relative to the overall buffer size. This means that a chunk will never be
	 * larger than {@code MAX_RELATIVE_CHUNK_SIZE * size + 1}.
	 */
	private static final double MAX_RELATIVE_CHUNK_SIZE = 0.01;

	/**
	 * The buffer's logical size.
	 */
	private final int size;

	/**
	 * The buffer's (usually most common) default value.
	 */
	private final long defaultValue;

	/**
	 * True if the buffer cannot be modified anymore.
	 */
	private boolean frozen = false;

	/**
	 * pool of chunks used for memory efficient representation of the sparse buffer
	 */
	private IntegerArrayBuilder nonDefaultIndices;

	/**
	 * pool of chunks used for memory efficient representation of the sparse buffer
	 */
	private LongArrayBuilder nonDefaultValues;

	/**
	 * The next free index in the buffer.
	 */
	private int nextLogicalIndex;

	/**
	 * The column that has been created by the buffer or {@code null} if the buffer has not been frozen yet.
	 */
	private Column column;

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type id {@link TypeId#TIME}.
	 *
	 * @param defaultValue
	 * 		the data's most common default value.
	 * @param length
	 * 		the length of the buffer
	 */
	TimeBufferSparse(LocalTime defaultValue, int length) {
		this(defaultValue == null ? TimeColumn.MISSING_VALUE : defaultValue.toNanoOfDay(), length);
	}

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type id {@link TypeId#TIME}.
	 *
	 * @param defaultValue
	 * 		the data's most common default value.
	 * @param length
	 * 		the length of the buffer
	 */
	TimeBufferSparse(long defaultValue, int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Sparse time buffer cannot have negative length: " + length);
		}
		checkValueRange(defaultValue);
		this.size = length;
		this.defaultValue = defaultValue;
		nextLogicalIndex = 0;
		ArrayBuilderConfiguration config = new ArrayBuilderConfiguration(null, null,
				(int) (MAX_RELATIVE_CHUNK_SIZE * size + 1));
		nonDefaultIndices = new IntegerArrayBuilder(config);
		nonDefaultValues = new LongArrayBuilder(config);
	}

	/**
	 * Sets the value at the next index to the given value.
	 *
	 * @param time
	 * 		The local time to set.
	 * @throws IndexOutOfBoundsException
	 * 		If attempting to add more than {@link #size()} values.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 */
	public void setNext(LocalTime time) {
		setNext(nextLogicalIndex, time);
	}

	/**
	 * Sets the value at the next index to the given value.
	 *
	 * @param nanosOfDay
	 * 		the nanoseconds of the day from 00:00 to 23:59:59.999999999.
	 * @throws IndexOutOfBoundsException
	 * 		If attempting to add more than {@link #size()} values.
	 * @throws IllegalArgumentException
	 * 		If nanosOfDay is smaller than {@link TimeBuffer#MIN_NANO} or bigger than {@link TimeBuffer#MAX_NANO}.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 */
	public void setNext(long nanosOfDay) {
		setNext(nextLogicalIndex, nanosOfDay);
	}

	/**
	 * Sets the value at the given index to the given value. All indices {<=} the given index that have not been set so
	 * far will be set to the default value. Please note that you cannot modify an index smaller than or equal to the
	 * given index after calling this method. Trying to access an index {@code <=} the largest index that has already
	 * been set will lead to an {@link IllegalArgumentException}.
	 *
	 * @param index
	 * 		an index inside of the buffers bounds (0 inclusive to {@link TimeBufferSparse#size()} exclusive) at which to
	 * 		set the value.
	 * @param nanosOfDay
	 * 		the nanoseconds of the day from 00:00 to 23:59:59.999999999.
	 * @throws IllegalArgumentException
	 * 		If nanosOfDay is smaller than {@link TimeBuffer#MIN_NANO} or bigger than {@link TimeBuffer#MAX_NANO}.
	 * 		<p> Or if index is {@code <=} the largest index that has already been set.
	 * @throws IndexOutOfBoundsException
	 * 		if the specified index is outside of the buffer's bounds.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 */
	public synchronized void setNext(int index, long nanosOfDay) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		checkIndexRange(index);
		checkValueRange(nanosOfDay);
		if (nanosOfDay != defaultValue) {
			nonDefaultIndices.setNext(index);
			nonDefaultValues.setNext(nanosOfDay);
		}
		nextLogicalIndex = index + 1;
	}

	/**
	 * Sets the value at the given index to the given {@link LocalTime}. All indices {<=} the given index that have not
	 * been set so far will be set to the default value. Please note that you cannot modify an index smaller than or
	 * equal to the given index after calling this method. Trying to access an index {@code <=} the largest index that
	 * has already been set will lead to an {@link IllegalArgumentException}.
	 *
	 * @param index
	 * 		the index at which to set the value
	 * @param time
	 * 		the instance of local time that will be added to the buffer
	 * @throws IndexOutOfBoundsException
	 * 		if the specified index is outside of the buffer's bounds.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 * @throws IllegalArgumentException
	 * 		if index is {@code <=} the largest index that has already been set.
	 */
	public void setNext(int index, LocalTime time) {
		long value = time == null ? TimeColumn.MISSING_VALUE : time.toNanoOfDay();
		setNext(index, value);
	}

	/**
	 * Returns the buffer's overall size (not the number of elements currently inside the buffer).
	 *
	 * @return the buffers size.
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns a column constructed from this buffer. The buffer will be frozen and becomes read-only. In contrast to
	 * constructing a new buffer from a column this does not copy the data.
	 *
	 * @return a column with the data from this buffer
	 */
	public synchronized Column toColumn() {
		if (!frozen) {
			frozen = true;
			generateColumnAndFreeChunks();
		}
		return column;
	}

	@Override
	public String toString() {
		return "Sparse time buffer of length " + size + " with default value " + defaultValue;
	}

	/**
	 * Returns the number of non-default Values (for testing).
	 */
	int getNumberOfNonDefaults(){
		return nonDefaultIndices.size();
	}

	/**
	 * Checks if the given value is inside of the the valid range for nanoseconds of the day and throws an {@link
	 * IllegalArgumentException} of this is not the case.
	 */
	private void checkValueRange(long value) {
		if (value != TimeColumn.MISSING_VALUE && (value < TimeBuffer.MIN_NANO || value > TimeBuffer.MAX_NANO)) {
			throw new IllegalArgumentException("Nanoseconds must be in the range " + TimeBuffer.MIN_NANO + " to " +
					TimeBuffer.MAX_NANO);
		}
	}

	/**
	 * Throws an exception if the specified index is out of the buffer's bounds.
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the index is out of bounds.
	 */
	private void checkIndexRange(int index) {
		if (index < 0 || index >= size) {
			throw new ArrayIndexOutOfBoundsException(index);
		}
		if (index < nextLogicalIndex) {
			throw new IllegalArgumentException("Cannot modify index " + index
					+ ". An equal or larger index has already been set.");
		}
	}

	/**
	 * Generates the final column resulting from the buffer after it has been frozen. Furthermore, the method frees the
	 * memory used by the buffer's chunks, as the chunks are not needed anymore.
	 */
	private void generateColumnAndFreeChunks() {
		int[] finalNonDefaultIndices = nonDefaultIndices.getData();
		// free memory for index chunks
		nonDefaultIndices = null;
		long[] finalNonDefaultValues = nonDefaultValues.getData();
		// free memory for value chunks
		nonDefaultValues = null;
		column = ColumnAccessor.get().newSparseTimeColumn(defaultValue, finalNonDefaultIndices,
				finalNonDefaultValues, size);
	}

}
