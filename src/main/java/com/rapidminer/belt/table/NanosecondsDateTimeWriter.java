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

package com.rapidminer.belt.table;

import java.time.Instant;
import java.util.Arrays;

import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.DateTimeColumn;


/**
 * A buffer for row-wise writing of date-time instances with the same range and precision as Java's {@link Instant}.
 * Wraps two primitive arrays for seconds and nanoseconds respectively.
 *
 * @author Gisa Meier
 */
final class NanosecondsDateTimeWriter implements ComplexWriter {

	private long[] secondsData;
	private int[] nanosData;
	private boolean frozen = false;
	private int size;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#DATE_TIME} of high
	 * precision (i.e. with nano seconds).
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	NanosecondsDateTimeWriter(int length) {
		secondsData = new long[length];
		nanosData = new int[length];
		size = length;
	}


	NanosecondsDateTimeWriter() {
		secondsData = new long[0];
		nanosData = new int[0];
		size = 0;
	}

	/**
	 * Sets the data at the given index to the given instant.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param instant
	 * 		the time it should be set to, can be {@code null}
	 */
	private void set(int index, Instant instant) {
		if (instant == null) {
			secondsData[index] = DateTimeColumn.MISSING_VALUE;
		} else {
			secondsData[index] = instant.getEpochSecond();
			nanosData[index] = instant.getNano();
		}
	}

	/**
	 * Ensures that the buffer has the capacity for the given length.
	 */
	private void resize(int length) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		size = length;
		if (length <= secondsData.length) {
			return;
		}
		int oldLength = secondsData.length;
		int newLength = Math.max(Math.max(NumericRowWriter.MIN_NON_EMPTY_SIZE, length), oldLength + (oldLength >>> 1));
		secondsData = Arrays.copyOf(secondsData, newLength);
		nanosData = Arrays.copyOf(nanosData, newLength);
	}

	@Override
	public void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height) {
		resize(height);
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, height);
		int copyIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (copyIndex < max) {
			// the cast is safe because we check the data that is put into the buffer
			@SuppressWarnings("unchecked")
			Instant instant = (Instant) buffer[bufferIndex];
			set(copyIndex, instant);
			bufferIndex += bufferStepSize;
			copyIndex++;
		}
	}

	/**
	 * Freezes this column buffer and copies the data to the final size.
	 */
	private void freeze() {
		frozen = true;
		if (secondsData.length > size) {
			secondsData = Arrays.copyOf(secondsData, size);
			nanosData = Arrays.copyOf(nanosData, size);
		}
	}

	/**
	 * Return the writer's second data. Used e.g. for checking the data's sparsity.
	 */
	long[] getSeconds() {
		return secondsData;
	}

	/**
	 * Return the writer's nanosecond data. Used e.g. for checking the data's sparsity.
	 */
	int[] getNanos() {
		return nanosData;
	}

	@Override
	public DateTimeColumn toColumn() {
		freeze();
		return ColumnAccessor.get().newDateTimeColumn(secondsData, nanosData);
	}

}
