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
import java.util.Arrays;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.DateTimeColumn;


/**
 * A buffer for date time instances without sub-second precision. Wraps a primitive array for seconds.
 *
 * @author Gisa Meier
 */
final class SecondDateTimeBuffer extends DateTimeBuffer {

	private final long[] secondsData;
	private boolean frozen = false;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#DATE_TIME} of high
	 * precision (i.e. with nano seconds).
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param initialize
	 * 		if {@code true} the buffer is initially filled with missing values
	 */
	SecondDateTimeBuffer(int length, boolean initialize) {
		secondsData = new long[length];
		if(initialize) {
			//ensure that values that are not set are missing instead of 1.1.1970
			Arrays.fill(secondsData, DateTimeColumn.MISSING_VALUE);
		}
	}

	/**
	 * Creates a buffer by copying the data from the given date-time column.
	 *
	 * @param column
	 * 		a column of type id {@link TypeId#DATE_TIME}
	 */
	SecondDateTimeBuffer(Column column) {
		secondsData = new long[column.size()];
		if (column instanceof DateTimeColumn) {
			DateTimeColumn dateColumn = (DateTimeColumn) column;
			dateColumn.fillSecondsIntoArray(secondsData, 0);
		} else {
			throw new AssertionError();
		}
	}

	@Override
	public Instant get(int index) {
		long s = secondsData[index];
		if (s == DateTimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s);
		}
	}

	@Override
	public void set(int index, long epochSeconds) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (epochSeconds < MIN_SECOND || epochSeconds > MAX_SECOND) {
			throw new IllegalArgumentException("Seconds must be in the range " + MIN_SECOND + " to " + MAX_SECOND);
		}
		secondsData[index] = epochSeconds;
	}

	/**
	 * Sets the data at the given index to the given instant, ignoring everything below second precision. For storing
	 * with nano-second precision please use a {@link NanosecondDateTimeBuffer} instead.
	 *
	 * <p>This method is not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param instant
	 * 		the instant it should be set to, can be {@code null}
	 */
	@Override
	public void set(int index, Instant instant) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (instant == null) {
			secondsData[index] = DateTimeColumn.MISSING_VALUE;
		} else {
			secondsData[index] = instant.getEpochSecond();
		}
	}

	@Override
	public void set(int index, long epochSeconds, int nanoseconds) {
		set(index, epochSeconds);
	}

	@Override
	public int size() {
		return secondsData.length;
	}

	@Override
	public DateTimeColumn toColumn() {
		frozen = true;
		return ColumnAccessor.get().newDateTimeColumn(secondsData, null);
	}

}
