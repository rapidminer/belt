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

import java.time.LocalTime;
import java.util.Arrays;

import com.rapidminer.belt.Column.TypeId;
import com.rapidminer.belt.util.Mapping;


/**
 * A buffer for time instances without nanosecond precision. Wraps a primitive double array.
 *
 * @author Gisa Meier
 */
public final class TimeColumnBuffer {

	/**
	 * Error message when trying to change frozen buffer
	 */
	private static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";

	private static final long MIN_NANO = LocalTime.MIN.toNanoOfDay();
	private static final long MAX_NANO = LocalTime.MAX.toNanoOfDay();

	private final long[] nanoData;
	private boolean frozen = false;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#TIME}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public TimeColumnBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		nanoData = new long[length];
		//ensure that values that are not set are missing instead of 00:00
		Arrays.fill(nanoData, TimeColumn.MISSING_VALUE);
	}

	/**
	 * Creates a buffer by copying the data from the given time column.
	 *
	 * @param column
	 * 		a column of type id {@link TypeId#TIME}
	 * @throws IllegalArgumentException
	 * 		if the column is not of type id {@link TypeId#TIME}
	 */
	public TimeColumnBuffer(Column column) {
		if (column.type().id() != TypeId.TIME) {
			throw new IllegalArgumentException("Column must be of type id time");
		}
		if (column instanceof TimeColumn) {
			TimeColumn timeColumn = (TimeColumn) column;
			nanoData = Arrays.copyOf(timeColumn.getNanos(), column.size());
		} else if (column instanceof MappedTimeColumn) {
			MappedTimeColumn dateColumn = (MappedTimeColumn) column;
			int[] mapping = dateColumn.getMapping();
			nanoData = Mapping.apply(dateColumn.getNanos(), mapping, TimeColumn.MISSING_VALUE);
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
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
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
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
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
		return new TimeColumn(nanoData);
	}

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}
}
