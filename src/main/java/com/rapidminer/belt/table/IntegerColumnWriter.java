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

package com.rapidminer.belt.table;

import java.util.Arrays;

import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column;


/**
 * A {@link NumericColumnWriter} that can change its size and contains double values without fractional digits.
 *
 * @author Gisa Meier
 */
final class IntegerColumnWriter implements NumericColumnWriter {


	private static final double[] PLACEHOLDER_BUFFER = new double[0];

	private double[] data;
	private boolean frozen = false;
	private int size;

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	IntegerColumnWriter(int length) {
		data = new double[length];
		size = length;
	}

	/**
	 * Creates a buffer with starting size zero.
	 */
	IntegerColumnWriter() {
		data = PLACEHOLDER_BUFFER;
		size = 0;
	}

	/**
	 * Rounds values that are not NaN or infinite.
	 */
	private double round(double value) {
		double integerValue = value;
		if (Double.isFinite(integerValue)) {
			//round values that are not NaN, +- infinity
			integerValue = Math.round(integerValue);
		}
		return integerValue;
	}

	/**
	 * Ensures that the buffer has the capacity for the given length.
	 */
	private void resize(int length) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		size = length;
		if (length <= data.length) {
			return;
		}
		int oldLength = data.length;
		int newLength = Math.max(Math.max(NumericRowWriter.MIN_NON_EMPTY_SIZE, length), oldLength + (oldLength >>> 1));
		data = Arrays.copyOf(data, newLength);
	}


	@Override
	public void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height) {
		resize(height);
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, height);
		int copyIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (copyIndex < max) {
			data[copyIndex] = round(buffer[bufferIndex]);
			bufferIndex += bufferStepSize;
			copyIndex++;
		}
	}

	@Override
	public Column toColumn() {
		freeze();
		return ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER, data);
	}

	/**
	 * Return the writers data. Used e.g. for checking the data's sparsity.
	 */
	double[] getData() {
		return data;
	}

	/**
	 * Freezes this column buffer and copies the data to the final size.
	 */
	private void freeze() {
		frozen = true;
		if (data.length > size) {
			data = Arrays.copyOf(data, size);
		}
	}


}
