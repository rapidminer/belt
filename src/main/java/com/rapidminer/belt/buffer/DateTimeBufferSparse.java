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

package com.rapidminer.belt.buffer;

import java.time.Instant;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.util.ArrayBuilderConfiguration;
import com.rapidminer.belt.util.IntegerArrayBuilder;
import com.rapidminer.belt.util.LongArrayBuilder;


/**
 * Abstract super class of sparse date time buffers. The buffer is write only. The {@link #setNext(long)} method can be
 * used to set the next free index in the buffer to the given value. The {@link #setNext(int, long)} method can be used
 * to set a specific position to the given value. In this case all values at smaller indices (that have not already been
 * set) will be set to the buffer's default value. This is more efficient than calling {@link #setNext(long)} for every
 * default value. Please note that values, once they are set, cannot be modified.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 * <p>
 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient for
 * data with a sparsity of {@code >= 50%}. The sparse buffer comes with a constant memory overhead so that it is
 * recommended not to use it for very small data ({@code <1024} data points).
 *
 * @author Kevin Majchrzak
 * @see DateTimeBuffer
 * @see NanoDateTimeBufferSparse
 * @see SecondDateTimeBufferSparse
 * @see Buffers
 */
public abstract class DateTimeBufferSparse {

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
	 * {@code true} if the buffer cannot be modified anymore.
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
	 * The high precision nano second data. This data is stored densely.
	 */
	private int[] nanos;

	/**
	 * The column that has been created by the buffer or {@code null} if the buffer is not frozen yet.
	 */
	private Column column;

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#DATE_TIME}. The method does not take default nanoseconds as only seconds are stored sparsely and
	 * nanos will default to {@code 0}.
	 *
	 * @param defaultSeconds
	 * 		the data's most common default seconds value.
	 * @param length
	 * 		the length of the buffer
	 * @param highPrecision
	 * 		if true, the resulting buffer will be of nanosecond precision. Otherwise it will ignore nanoseconds.
	 * @throws IllegalArgumentException
	 * 		if length is negative or defaultSeconds is out of bounds.
	 */
	protected DateTimeBufferSparse(long defaultSeconds, int length, boolean highPrecision) {
		if (length < 0) {
			throw new IllegalArgumentException("Sparse date time buffer cannot have negative length: " + length);
		}
		checkValueRange(defaultSeconds, 0);
		this.size = length;
		nanos = highPrecision ? new int[size] : null;
		this.defaultValue = defaultSeconds;
		nextLogicalIndex = 0;
		ArrayBuilderConfiguration config = new ArrayBuilderConfiguration(null, null,
				(int) (MAX_RELATIVE_CHUNK_SIZE * size + 1));
		nonDefaultIndices = new IntegerArrayBuilder(config);
		nonDefaultValues = new LongArrayBuilder(config);
	}

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#DATE_TIME}. The default value's nanoseconds are ignored as only seconds are stored sparsely and
	 * nanos will default to {@code 0}.
	 *
	 * @param defaultSeconds
	 * 		the data's most common default seconds value represented via an {@link Instant}.
	 * @param length
	 * 		the length of the buffer
	 * @param highPrecision
	 * 		if true, the resulting buffer will be of nanosecond precision.
	 * @throws IllegalArgumentException
	 * 		if length is negative or defaultSeconds is out of bounds.
	 */
	protected DateTimeBufferSparse(Instant defaultSeconds, int length, boolean highPrecision) {
		this(defaultSeconds == null ? DateTimeColumn.MISSING_VALUE : defaultSeconds.getEpochSecond(), length, highPrecision);
	}

	/**
	 * Sets the time value at the next index to the given value. If the buffer is of low precision (see {@link
	 * #DateTimeBufferSparse(long, int, boolean)}), nanoseconds will be ignored.
	 *
	 * @param time
	 * 		the time value to set.
	 * @throws IndexOutOfBoundsException
	 * 		If attempting to add more than {@link #size()} values.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 */
	public void setNext(Instant time) {
		setNext(nextLogicalIndex, time);
	}

	/**
	 * Sets the values at the next position to the given values. If the buffer is of low precision (see {@link
	 * #DateTimeBufferSparse(long, int, boolean)}), nanoseconds will be ignored.
	 *
	 * @param epochSeconds
	 * 		the seconds of epoch in the range {@link DateTimeBuffer#MIN_SECOND} to {@link DateTimeBuffer#MAX_SECOND}.
	 * @param nanoSeconds
	 * 		the nanoseconds of day in the range {@link NanosecondDateTimeBuffer#MIN_NANO} to {@link
	 *        NanosecondDateTimeBuffer#MAX_NANO}. If the buffer is of low precision (see {@link #DateTimeBufferSparse(long,
	 *        int, boolean)}), nanoseconds will be ignored.
	 * @throws IllegalArgumentException
	 * 		if epochSeconds or nanoSeconds are out of range
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 * @throws IndexOutOfBoundsException
	 * 		If attempting to add more than {@link #size()} values.
	 */
	public void setNext(long epochSeconds, int nanoSeconds) {
		setNext(nextLogicalIndex, epochSeconds, nanoSeconds);
	}

	/**
	 * Sets the value at the next index to the given value.
	 *
	 * @param epochSeconds
	 * 		the seconds of epoch in the range {@link DateTimeBuffer#MIN_SECOND} to {@link DateTimeBuffer#MAX_SECOND}.
	 * @throws IllegalArgumentException
	 * 		if epochSeconds is smaller than {@link DateTimeBuffer#MIN_SECOND} or bigger than {@link
	 *        DateTimeBuffer#MAX_SECOND}
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 * @throws IndexOutOfBoundsException
	 * 		If attempting to add more than {@link #size()} values.
	 */
	public void setNext(long epochSeconds) {
		setNext(epochSeconds, 0);
	}

	/**
	 * Sets the value at the given index to the given value. All indices {@code <=} the given index that have not been set so
	 * far will be set to the default seconds and {@code 0} nanoseconds. Please note that you cannot modify an index
	 * smaller than or equal to the given index after calling this method. Trying to access an index {@code <=} the
	 * largest index that has already been set will lead to an {@link IllegalArgumentException}.
	 * <p> If the buffer is of low precision (see {@link #DateTimeBufferSparse(long, int, boolean)}), nanoseconds will
	 * be ignored.
	 *
	 * @param index
	 * 		an index inside of the buffers bounds (0 inclusive to {@link DateTimeBufferSparse#size()} exclusive) at which
	 * 		to set the value.
	 * @param time
	 * 		the time value to set
	 * @throws IllegalArgumentException
	 * 		If index is {@code <=} the largest index that has already been set.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 * @throws IndexOutOfBoundsException
	 * 		if the specified index is outside of the buffer's bounds.
	 */
	public void setNext(int index, Instant time) {
		long seconds = time == null ? DateTimeColumn.MISSING_VALUE : time.getEpochSecond();
		int nanoSeconds = time == null ? 0 : time.getNano();
		setNext(index, seconds, nanoSeconds);
	}

	/**
	 * Sets the value at the given index to the given value. All indices {@code <=} the given index that have not been set so
	 * far will be set to the default seconds and {@code 0} nanoseconds. Please note that you cannot modify an index
	 * smaller than or equal to the given index after calling this method. Trying to access an index {@code <=} the
	 * largest index that has already been set will lead to an {@link IllegalArgumentException}.
	 * <p> If the buffer is of low precision (see {@link #DateTimeBufferSparse(long, int, boolean)}), nanoseconds will
	 * be ignored.
	 *
	 * @param index
	 * 		an index inside of the buffers bounds (0 inclusive to {@link DateTimeBufferSparse#size()} exclusive) at which
	 * 		to set the value.
	 * @param epochSeconds
	 * 		the seconds of epoch in the range {@link DateTimeBuffer#MIN_SECOND} to {@link DateTimeBuffer#MAX_SECOND}.
	 * @param nanoSeconds
	 * 		the nanoseconds of day in the range {@link NanosecondDateTimeBuffer#MIN_NANO} to {@link
	 *        NanosecondDateTimeBuffer#MAX_NANO}. If the buffer is of low precision (see {@link #DateTimeBufferSparse(long,
	 *        int, boolean)}), nanoseconds will be ignored.
	 * @throws IllegalArgumentException
	 * 		if epochSeconds or nanosSeconds are out of bounds
	 * 		<p> or if index is {@code <=} the largest index that has already been set.
	 * @throws IllegalStateException
	 * 		if the buffer is frozen (see {@link #toColumn()}).
	 * @throws IndexOutOfBoundsException
	 * 		if the specified index is outside of the buffer's bounds.
	 */
	public synchronized void setNext(int index, long epochSeconds, int nanoSeconds) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		checkIndexRange(index);
		checkValueRange(epochSeconds, nanoSeconds);
		if (nanos != null) {
			nanos[index] = nanoSeconds;
		}
		if (epochSeconds != defaultValue) {
			nonDefaultIndices.setNext(index);
			nonDefaultValues.setNext(epochSeconds);
		}
		nextLogicalIndex = index + 1;
	}

	/**
	 * Sets the value at the given index to the given value. All indices {@code <=} the given index that have not been set so
	 * far will be set to the default value. Please note that you cannot modify an index smaller than or equal to the
	 * given index after calling this method. Trying to access an index {@code <=} the largest index that has already
	 * been set will lead to an {@link IllegalArgumentException}.
	 *
	 * @param index
	 * 		an index inside of the buffers bounds (0 inclusive to {@link DateTimeBufferSparse#size()} exclusive) at which
	 * 		to set the value.
	 * @param epochSeconds
	 * 		the seconds of epoch in the range {@link DateTimeBuffer#MIN_SECOND} to {@link DateTimeBuffer#MAX_SECOND}.
	 * @throws IllegalArgumentException
	 * 		if epochSeconds is out of bounds or if index is {@code <=} the largest index that has already been set.
	 */
	public void setNext(int index, long epochSeconds) {
		setNext(index, epochSeconds, 0);
	}

	public int size() {
		return size;
	}

	@Override
	public String toString() {
		if (nanos == null) {
			return "Sparse low precision date time buffer of length " + size + " with default value " + defaultValue;
		} else {
			return "Sparse high precision date time buffer of length " + size + " with default value " + defaultValue;
		}
	}

	/**
	 * Returns the number of non-default Values (for testing).
	 */
	int getNumberOfNonDefaults() {
		return nonDefaultValues.size();
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
	 * Checks if the given values are in bounds and throws an exception if this is not the case.
	 */
	private void checkValueRange(long epochSeconds, int nanoseconds) {
		if (epochSeconds != DateTimeColumn.MISSING_VALUE) {
			if (epochSeconds < DateTimeBuffer.MIN_SECOND || epochSeconds > DateTimeBuffer.MAX_SECOND) {
				throw new IllegalArgumentException("Seconds must be in the range " + DateTimeBuffer.MIN_SECOND + " to " +
						DateTimeBuffer.MAX_SECOND);
			}
			if (nanoseconds < NanosecondDateTimeBuffer.MIN_NANO || nanoseconds > NanosecondDateTimeBuffer.MAX_NANO) {
				throw new IllegalArgumentException("Nanoseconds must be in the range 0 to 999,999,999.");
			}
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
		column = ColumnAccessor.get().newSparseDateTimeColumn(defaultValue, finalNonDefaultIndices,
				finalNonDefaultValues, nanos, size);
	}

	public synchronized Column toColumn() {
		if (!frozen) {
			frozen = true;
			generateColumnAndFreeChunks();
		}
		return column;
	}

}
