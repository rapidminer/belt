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

package com.rapidminer.belt.buffer;

import java.time.LocalTime;
import java.util.Arrays;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.TimeColumn;


/**
 * A buffer for time instances with nanosecond precision. Wraps a primitive long array.
 *
 * @author Gisa Meier
 * @see Buffers
 */
public final class TimeBuffer {

	static final long MIN_NANO = LocalTime.MIN.toNanoOfDay();
	static final long MAX_NANO = LocalTime.MAX.toNanoOfDay();

	private final long[] nanoData;
	private boolean frozen = false;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#TIME}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param initialize
	 * 		if {@code true} the buffer is initially filled with missing values
	 */
	TimeBuffer(int length, boolean initialize) {
		nanoData = new long[length];
		if (initialize) {
			//ensure that values that are not set are missing instead of 00:00
			Arrays.fill(nanoData, TimeColumn.MISSING_VALUE);
		}
	}

	/**
	 * Creates a buffer by copying the data from the given time column.
	 *
	 * @param column
	 * 		a column of type id {@link TypeId#TIME}
	 */
	TimeBuffer(Column column) {
			nanoData = new long[column.size()];
		if (column instanceof TimeColumn) {
			TimeColumn timeColumn = (TimeColumn) column;
			timeColumn.fillNanosIntoArray(nanoData, 0);
		} else {
			throw new AssertionError();
		}
	}

	/**
	 * Retrieves the value at the given index.
	 *
	 * <p>Set operations are not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index to look up
	 * @return the value at the index
	 */
	public LocalTime get(int index) {
		long s = nanoData[index];
		if (s == TimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return LocalTime.ofNanoOfDay(s);
		}
	}


	/**
	 * Sets the data at the given index to the given value.
	 *
	 * To set missing values please use {@code set(index, null)}.
	 *
	 * <p>This method is not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param nanoOfDay
	 * 		the nanoseconds of the day from from 00:00 to 23:59:59.999999999
	 * @throws IllegalArgumentException
	 * 		if epochSeconds is smaller than {@link #MIN_NANO} or bigger than {@link #MAX_NANO}
	 */
	public void set(int index, long nanoOfDay) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (nanoOfDay < MIN_NANO || nanoOfDay > MAX_NANO) {
			throw new IllegalArgumentException("Nanoseconds must be in the range " + MIN_NANO + " to " + MAX_NANO);
		}
		nanoData[index] = nanoOfDay;
	}

	/**
	 * Sets the data at the given index to the given time.
	 *
	 * <p>This method is not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param time
	 * 		the time it should be set to, can be {@code null}
	 */
	public void set(int index, LocalTime time) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (time == null) {
			nanoData[index] = TimeColumn.MISSING_VALUE;
		} else {
			nanoData[index] = time.toNanoOfDay();
		}
	}

	/**
	 * @return the size of the buffer
	 */
	public int size() {
		return nanoData.length;
	}

	/**
	 * Creates a new time column from the buffer.
	 *
	 * @return a new {@link TimeColumn}
	 */
	public TimeColumn toColumn() {
		frozen = true;
		return ColumnAccessor.get().newTimeColumn(nanoData);
	}

	@Override
	public String toString() {
		return BufferPrinter.print(this);
	}
}
