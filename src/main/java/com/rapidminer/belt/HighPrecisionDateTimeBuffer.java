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
 * A buffer for date-time instances with the same range and precision as Java's {@link Instant}. Wraps two primitive
 * arrays for seconds and nanoseconds respectively.
 *
 * @author Gisa Meier
 */
public final class HighPrecisionDateTimeBuffer extends AbstractDateTimeBuffer{

	/**
	 * Error message when trying to change frozen buffer
	 */
	private static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";

	private static final int MIN_NANO = Instant.MIN.getNano();
	private static final int MAX_NANO = Instant.MAX.getNano();

	private final long[] secondsData;
	private final int[] nanosData;
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
	public HighPrecisionDateTimeBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		secondsData = new long[length];
		//ensure that values that are not set are missing instead of 1.1.1970
		Arrays.fill(secondsData, DateTimeColumn.MISSING_VALUE);
		nanosData = new int[length];
	}

	/**
	 * Creates a buffer by copying the data from the given date-time column.
	 *
	 * @param column
	 * 		a column of type id {@link Column.TypeId#DATE_TIME}
	 * @throws IllegalArgumentException
	 * 		if the column is not of type id {@link Column.TypeId#DATE_TIME}
	 */
	public HighPrecisionDateTimeBuffer(Column column) {
		if (column.type().id() != TypeId.DATE_TIME) {
			throw new IllegalArgumentException("Column must be of type id date-time");
		}
		if (column instanceof DateTimeColumn) {
			DateTimeColumn dateColumn = (DateTimeColumn) column;
			secondsData = Arrays.copyOf(dateColumn.getSeconds(), column.size());
			int[] oldNanos = dateColumn.getNanos();
			nanosData = oldNanos == null ? new int[column.size()] : Arrays.copyOf(oldNanos, oldNanos.length);
		} else if (column instanceof MappedDateTimeColumn) {
			MappedDateTimeColumn dateColumn = (MappedDateTimeColumn) column;
			int[] mapping = dateColumn.getMapping();
			secondsData = Mapping.apply(dateColumn.getSeconds(), mapping, DateTimeColumn.MISSING_VALUE);
			int[] oldNanos = dateColumn.getNanos();
			nanosData = oldNanos == null ? new int[column.size()] : Mapping.apply(oldNanos, mapping);
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
			return Instant.ofEpochSecond(s, nanosData[index]);
		}
	}


	/**
	 * Sets the data at the given index to a {@link Instant} value by providing the epoch seconds and nanoseconds
	 * separately.
	 *
	 * <p>This method is not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param epochSeconds
	 * 		the number of seconds from 1970-01-01T00:00:00Z, can be negative but must be in the range {@link
	 * 		Instant#MIN_SECOND} to {@link Instant#MAX_SECOND}
	 * @param nanoseconds
	 * 		the number of nanoseconds, must be in the range 0 to 999,999,999.
	 * @throws IllegalArgumentException
	 * 		if epochSeconds is smaller than {@link Instant#MIN_SECOND} and {@link Instant#MAX_SECOND} or nanoseconds is
	 * 		negative or bigger than 999,999,999
	 */
	public void set(int index, long epochSeconds, int nanoseconds) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		if (epochSeconds < MIN_SECOND || epochSeconds > MAX_SECOND) {
			throw new IllegalArgumentException("Seconds must be in the range " + MIN_SECOND + " to " + MAX_SECOND);
		}
		if (nanoseconds < MIN_NANO || nanoseconds > MAX_NANO) {
			throw new IllegalArgumentException("Nanoseconds must be in the range 0 to 999,999,999.");
		}
		secondsData[index] = epochSeconds;
		nanosData[index] = nanoseconds;
	}
	
	@Override
	public void set(int index, Instant instant) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		if (instant == null) {
			secondsData[index] = DateTimeColumn.MISSING_VALUE;
		} else {
			secondsData[index] = instant.getEpochSecond();
			nanosData[index] = instant.getNano();
		}
	}

	@Override
	public int size() {
		return secondsData.length;
	}

	@Override
	public DateTimeColumn toColumn() {
		frozen = true;
		return new DateTimeColumn(secondsData, nanosData);
	}

}
