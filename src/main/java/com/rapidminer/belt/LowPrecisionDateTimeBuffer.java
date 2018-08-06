/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2018 RapidMiner GmbH
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

package com.rapidminer.belt;

import java.time.Instant;
import java.util.Arrays;

import com.rapidminer.belt.Column.TypeId;
import com.rapidminer.belt.util.Mapping;


/**
 * A buffer for date time instances without sub-second precision. Wraps a primitive array for seconds.
 *
 * @author Gisa Meier
 */
public final class LowPrecisionDateTimeBuffer extends AbstractDateTimeBuffer {

	/**
	 * Error message when trying to change frozen buffer
	 */
	private static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";

	private final long[] secondsData;
	private boolean frozen = false;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#DATE_TIME} of high
	 * precision (i.e. with nano seconds).
	 *
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public LowPrecisionDateTimeBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		secondsData = new long[length];
		//ensure that values that are not set are missing instead of 1.1.1970
		Arrays.fill(secondsData, DateTimeColumn.MISSING_VALUE);
	}

	/**
	 * Creates a buffer by copying the data from the given date-time column.
	 *
	 * @param column
	 * 		a column of type id {@link TypeId#DATE_TIME}
	 * @throws IllegalArgumentException
	 * 		if the column is not of type id {@link TypeId#DATE_TIME}
	 */
	public LowPrecisionDateTimeBuffer(Column column) {
		if (column.type().id() != TypeId.DATE_TIME) {
			throw new IllegalArgumentException("Column must be of type id date-time");
		}
		if (column instanceof DateTimeColumn) {
			DateTimeColumn dateColumn = (DateTimeColumn) column;
			secondsData = Arrays.copyOf(dateColumn.getSeconds(), column.size());
		} else if (column instanceof MappedDateTimeColumn) {
			MappedDateTimeColumn dateColumn = (MappedDateTimeColumn) column;
			int[] mapping = dateColumn.getMapping();
			secondsData = Mapping.apply(dateColumn.getSeconds(), mapping, DateTimeColumn.MISSING_VALUE);
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
	 * @param epochSeconds
	 * 		the number of seconds from 1970-01-01T00:00:00Z, can be negative but must be in range {@link
	 * 		Instant#MIN_SECOND} to {@link Instant#MAX_SECOND}
	 * @throws IllegalArgumentException
	 * 		if epochSeconds is smaller than {@link Instant#MIN_SECOND} or bigger than {@link Instant#MAX_SECOND}
	 */
	public void set(int index, long epochSeconds) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		if (epochSeconds < MIN_SECOND || epochSeconds > MAX_SECOND) {
			throw new IllegalArgumentException("Seconds must be in the range " + MIN_SECOND + " to " + MAX_SECOND);
		}
		secondsData[index] = epochSeconds;
	}

	/**
	 * Sets the data at the given index to the given instant, ignoring everything below second precision. For storing
	 * with nano-second precision please use a {@link HighPrecisionDateTimeBuffer} instead.
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
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		if (instant == null) {
			secondsData[index] = DateTimeColumn.MISSING_VALUE;
		} else {
			secondsData[index] = instant.getEpochSecond();
		}
	}

	@Override
	public int size() {
		return secondsData.length;
	}

	@Override
	public DateTimeColumn toColumn() {
		frozen = true;
		return new DateTimeColumn(secondsData);
	}

}
