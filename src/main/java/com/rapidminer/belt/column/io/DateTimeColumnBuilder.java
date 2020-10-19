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
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.time.Instant;
import java.util.Arrays;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.DateTimeColumn;


/**
 * Builder for date-time columns out of {@link ByteBuffer}s.
 *
 * @author Gisa Meier
 */
public final class DateTimeColumnBuilder {

	private static final String MSG_NULL_BUFFER = "Buffer must not be null";
	private static final int SHIFT_FOR_8_BYTE_NUMBER = 3;
	private static final int SHIFT_FOR_4_BYTE_NUMBER = 2;

	private static final long MIN_SECOND = Instant.MIN.getEpochSecond();
	private static final long MAX_SECOND = Instant.MAX.getEpochSecond();
	private static final int MIN_NANO = Instant.MIN.getNano();
	private static final int MAX_NANO = Instant.MAX.getNano();


	private final long[] secondData;
	private int[] nanoData;

	private int position;
	private int nanoPosition;

	/**
	 * Creates a column builder for a {@link Column.TypeId#DATE_TIME} column of the given length. The second data
	 * can be put via {@link ByteBuffer}s containing long values representing seconds since epoch. Optionally, the
	 * nanosecond data can be put via {@link ByteBuffer}s containing int values representing the subsecond part.
	 *
	 * @param size
	 * 		the length of the column to construct
	 * @throws IllegalArgumentException
	 * 		if the size is negative
	 */
	public DateTimeColumnBuilder(int size) {
		if (size < 0) {
			throw new IllegalArgumentException("Number of elements must be positive");
		}
		this.secondData = new long[size];
		this.position = 0;
	}

	/**
	 * Returns the current position until which the second data has been written.
	 *
	 * @return the row index that is written next
	 */
	public int position() {
		return position;
	}

	/**
	 * Puts the long values in the buffer as seconds data into the column starting at the current {@link #position()}.
	 *
	 * @param buffer
	 * 		a buffer containing long values representing seconds since epoch or {@link Long#MAX_VALUE} for missing
	 * 		value
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if a value in the buffer is neither in the (inclusive) range -31557014167219200 to 31556889864403199 nor
	 * 		represents the missing value
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 */
	public DateTimeColumnBuilder putSeconds(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException(MSG_NULL_BUFFER);
		}
		LongBuffer wrapper = buffer.asLongBuffer();
		int length = Math.min(wrapper.remaining(), secondData.length - position);
		wrapper.get(secondData, position, length);
		checkSecondArray(secondData, position, position + length);

		buffer.position(wrapper.position() << SHIFT_FOR_8_BYTE_NUMBER);
		position += length;
		return this;
	}

	/**
	 * Returns the current position until which the nanosecond data has been written.
	 *
	 * @return the row index that is written next
	 */
	public int nanoPosition() {
		return nanoPosition;
	}

	/**
	 * Puts the int values in the buffer as optional subsecond data into the column starting at the current
	 * {@link #position()}.
	 *
	 * @param buffer
	 * 		a buffer containing int values representing nanoseconds
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if a value in the buffer is not in the nanosecond range 0 to 999,999,999 (inclusive)
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 */
	public DateTimeColumnBuilder putNanos(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException(MSG_NULL_BUFFER);
		}
		if (nanoData == null) {
			nanoData = new int[secondData.length];
		}
		IntBuffer wrapper = buffer.asIntBuffer();
		int length = Math.min(wrapper.remaining(), nanoData.length - nanoPosition);
		wrapper.get(nanoData, nanoPosition, length);
		checkNanoArray(nanoData, nanoPosition, nanoPosition + length);

		buffer.position(wrapper.position() << SHIFT_FOR_4_BYTE_NUMBER);
		nanoPosition += length;
		return this;
	}

	/**
	 * Creates a column as defined by the builder. If the current {@link #position()} is smaller than the originally
	 * defined column size, the remaining values will be missing values.
	 *
	 * @return a new column
	 */
	public Column toColumn() {
		if (position < secondData.length) {
			Arrays.fill(secondData, position, secondData.length, DateTimeColumn.MISSING_VALUE);
			position = secondData.length;
		}
		return ColumnAccessor.get().newDateTimeColumn(secondData, nanoData);
	}

	/**
	 * Checks if the values in the data array between from and to are in the allowed range.
	 */
	private void checkSecondArray(long[] data, int from, int to) {
		for (int i = from; i < to; i++) {
			long seconds = data[i];
			if (DateTimeColumn.MISSING_VALUE != seconds && (seconds < MIN_SECOND || seconds > MAX_SECOND)) {
				throw new IllegalArgumentException("Seconds " + seconds + " not in the range " + MIN_SECOND + " to" +
						" " + MAX_SECOND);
			}
		}
	}

	/**
	 * Checks if the values in the data array between from and to are in the allowed range for nanoseconds.
	 */
	private void checkNanoArray(int[] data, int from, int to) {
		for (int i = from; i < to; i++) {
			int nanoseconds = data[i];
			if (nanoseconds < MIN_NANO || nanoseconds > MAX_NANO) {
				throw new IllegalArgumentException("Nanosecond " + nanoseconds + " not in the range 0 to 999,999," +
						"999.");
			}

		}
	}

}
