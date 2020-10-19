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


package com.rapidminer.belt.column.io;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.time.LocalTime;
import java.util.Arrays;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.TimeColumn;


/**
 * Builder for time columns out of {@link ByteBuffer}s.
 *
 * @author Gisa Meier
 */
public final class TimeColumnBuilder {

	private static final int SHIFT_FOR_8_BYTE_NUMBER = 3;

	private static final long MIN_NANO = LocalTime.MIN.toNanoOfDay();
	private static final long MAX_NANO = LocalTime.MAX.toNanoOfDay();

	private final long[] data;

	private int position;

	/**
	 * Creates a column builder for a {@link Column.TypeId#TIME} column of the given length where the data can be put
	 * via {@link ByteBuffer}s containing long values representing nanoseconds of the day.
	 *
	 * @param size
	 * 		the length of the column to construct
	 * @throws IllegalArgumentException
	 * 		if the size is negative
	 */
	public TimeColumnBuilder(int size) {
		if (size < 0) {
			throw new IllegalArgumentException("Number of elements must be positive");
		}
		this.data = new long[size];
		this.position = 0;
	}

	/**
	 * Returns the current position until which the second data has been written.
	 *
	 * @return the row index that is written next
	 */
	public int position(){
		return position;
	}

	/**
	 * Puts the long values in the buffer as nanoseconds of the day data into the column starting at the current {@link
	 * #position()}.
	 *
	 * @param buffer
	 * 		a buffer containing long values representing nanoseconds of the day or {@link Long#MAX_VALUE} for missing
	 * 		value
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if a value in the buffer is neither between 0 and 86399999999999 (inclusive) nor represents the missing value
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 */
	public TimeColumnBuilder put(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException("Buffer must not be null");
		}
		LongBuffer wrapper = buffer.asLongBuffer();
		int length = Math.min(wrapper.remaining(), data.length - position);
		wrapper.get(data, position, length);
		checkArray(data, position, position + length);

		buffer.position(wrapper.position() << SHIFT_FOR_8_BYTE_NUMBER);
		position += length;
		return this;
	}

	/**
	 * Creates a column as defined by the builder. If the current {@link #position()} is smaller than the originally
	 * defined column size, the remaining values will be missing values.
	 *
	 * @return a new column
	 */
	public Column toColumn() {
		if (position < data.length) {
			Arrays.fill(data, position, data.length, TimeColumn.MISSING_VALUE);
			position = data.length;
		}
		return ColumnAccessor.get().newTimeColumn(data);
	}

	/**
	 * Checks if the values in the data array between from and to are in the allowed range.
	 */
	private void checkArray(long[] data, int from, int to) {
		for (int i = from; i < to; i++) {
			long nanoOfDay = data[i];
			if (TimeColumn.MISSING_VALUE != nanoOfDay && (nanoOfDay < MIN_NANO || nanoOfDay > MAX_NANO)) {
				throw new IllegalArgumentException("Nanoseconds " + nanoOfDay + " not in the range " + MIN_NANO +
						" to " + MAX_NANO);
			}
		}
	}

}
