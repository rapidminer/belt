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
public final class NanosecondDateTimeBuffer extends DateTimeBuffer {

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
	 * @param initialize
	 * 		if {@code true} the buffer is initially filled with missing values
	 */
	NanosecondDateTimeBuffer(int length, boolean initialize) {
		secondsData = new long[length];
		if(initialize) {
			//ensure that values that are not set are missing instead of 1.1.1970
			Arrays.fill(secondsData, DateTimeColumn.MISSING_VALUE);
		}
		nanosData = new int[length];
	}

	/**
	 * Creates a buffer by copying the data from the given date-time column.
	 *
	 * @param column
	 * 		a column of type id {@link Column.TypeId#DATE_TIME}
	 */
	NanosecondDateTimeBuffer(Column column) {
		if (column instanceof DateTimeColumn) {
			DateTimeColumn dateColumn = (DateTimeColumn) column;
			secondsData = Arrays.copyOf(dateColumn.getSeconds(), column.size());
			nanosData = Arrays.copyOf(dateColumn.getNanos(), column.size());
		} else if (column instanceof MappedDateTimeColumn) {
			MappedDateTimeColumn dateColumn = (MappedDateTimeColumn) column;
			int[] mapping = dateColumn.getMapping();
			secondsData = Mapping.apply(dateColumn.getSeconds(), mapping, DateTimeColumn.MISSING_VALUE);
			nanosData = Mapping.apply(dateColumn.getNanos(), mapping);
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

	@Override
	public void set(int index, long epochSeconds, int nanoseconds) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
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
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (instant == null) {
			secondsData[index] = DateTimeColumn.MISSING_VALUE;
		} else {
			secondsData[index] = instant.getEpochSecond();
			nanosData[index] = instant.getNano();
		}
	}

	@Override
	public void set(int index, long epochSeconds) {
		set(index, epochSeconds, 0);
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
