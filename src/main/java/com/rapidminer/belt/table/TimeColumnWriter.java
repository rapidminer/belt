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

package com.rapidminer.belt.table;

import java.time.LocalTime;
import java.util.Arrays;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.TimeColumn;


/**
 * A buffer for row-wise writing of time instances with nanosecond precision. Wraps a primitive long array.
 *
 * @author Gisa Meier
 */
final class TimeColumnWriter implements ComplexWriter {

	private long[] nanoData;
	private boolean frozen = false;
	private int size;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#TIME}.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	TimeColumnWriter(int length) {
		nanoData = new long[length];
		size = length;
	}

	/**
	 * Creates a buffer of starting length zero to create a {@link Column} of type id {@link TypeId#TIME}.
	 */
	TimeColumnWriter() {
		nanoData = new long[0];
		size = 0;
	}


	/**
	 * Sets the data at the given index to the given time.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param time
	 * 		the time it should be set to, can be {@code null}
	 */
	private void set(int index, LocalTime time) {
		if (time == null) {
			nanoData[index] = TimeColumn.MISSING_VALUE;
		} else {
			nanoData[index] = time.toNanoOfDay();
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
		if (length <= nanoData.length) {
			return;
		}
		int oldLength = nanoData.length;
		int newLength = Math.max(Math.max(NumericRowWriter.MIN_NON_EMPTY_SIZE, length), oldLength + (oldLength >>> 1));
		nanoData = Arrays.copyOf(nanoData, newLength);
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
			LocalTime instant = (LocalTime) buffer[bufferIndex];
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
		if (nanoData.length > size) {
			nanoData = Arrays.copyOf(nanoData, size);
		}
	}

	/**
	 * Return the writers data. Used e.g. for checking the data's sparsity.
	 */
	long[] getData() {
		return nanoData;
	}

	@Override
	public TimeColumn toColumn() {
		freeze();
		return ColumnAccessor.get().newTimeColumn(nanoData);
	}

}
