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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Writer for a {@link CategoricalColumn} with category index format {@link Format#SIGNED_INT32} that can
 * hold {@link Integer#MAX_VALUE} many different categories. The category indices are stored as {@code int}.
 *
 * @author Gisa Meier
 */
class Int32CategoricalWriter<T> implements ComplexWriter {

	private static final int[] PLACEHOLDER_BUFFER = new int[0];
	private boolean frozen = false;
	private final Map<T, Integer> indexLookup = new HashMap<>();
	private final List<T> valueLookup = new ArrayList<>();

	private final ColumnType<T> columnType;

	private int size;
	private int[] data;

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	Int32CategoricalWriter(ColumnType<T> columnType, int length) {
		this.data = new int[length];
		this.valueLookup.add(null); //position 0 stands for missing value, i.e. null
		this.columnType = columnType;
	}

	Int32CategoricalWriter(ColumnType<T> columnType) {
		this.data = PLACEHOLDER_BUFFER;
		this.valueLookup.add(null); //position 0 stands for missing value, i.e. null
		this.size = 0;
		this.columnType = columnType;
	}

	private void set(int index, T value) {
		if (value == null) {
			//set NaN
			data[index] = 0;
		} else {
			Integer mappingIndex = indexLookup.get(value);
			if (mappingIndex != null) {
				data[index] = mappingIndex;
			} else {
				int newMappingIndex = valueLookup.size();
				valueLookup.add(value);
				indexLookup.put(value, newMappingIndex);
				data[index] = newMappingIndex;
			}
		}
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


	@Override
	public CategoricalColumn<T> toColumn() {
		freeze();
		return new SimpleCategoricalColumn<>(columnType, data, valueLookup);
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
	public void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height) {
		resize(height);
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, height);
		int copyIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (copyIndex < max) {
			// the cast is safe because we check the data that is put into the buffer
			@SuppressWarnings("unchecked")
			T value = (T) buffer[bufferIndex];
			set(copyIndex, value);
			bufferIndex += bufferStepSize;
			copyIndex++;
		}
	}
}