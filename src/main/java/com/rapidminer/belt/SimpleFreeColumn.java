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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Simple implementation of a {@link FreeColumn}.
 *
 * @author Gisa Meier
 */
class SimpleFreeColumn<R> extends FreeColumn<R> {

	private final Object[] data;

	protected SimpleFreeColumn(ColumnType<R> type, Object[] data) {
		super(type, data.length);
		this.data = data;
	}


	@Override
	void fill(Object[] buffer, int rowIndex) {
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(buffer.length, data.length - start);
		System.arraycopy(data, start, buffer, 0, length);
	}

	@Override
	void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, data.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = data[rowIndex];
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		if (preferView || mapping.length > data.length * MappedDoubleArrayColumn.MAPPING_THRESHOLD) {
			return new MappedFreeColumn<>(type(), data, mapping);
		} else {
			return new SimpleFreeColumn<>(type(), Mapping.apply(data, mapping));
		}
	}

	@Override
	long writeToChannel(FileChannel channel, long startPosition) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Object[] getData() {
		return data;
	}

	@Override
	protected int[] sort(Order order) {
		Comparator<R> comparator = type().comparator();
		if (comparator == null) {
			throw new UnsupportedOperationException();
		}
		Comparator<R> comparatorWithNull = Comparator.nullsLast(comparator);
		return Sorting.sort(data.length,
				(a, b) -> {
					// the cast is safe because the data is of this type
					@SuppressWarnings("unchecked")
					R valueA = (R) data[a];
					@SuppressWarnings("unchecked")
					R valueB = (R) data[b];
					return comparatorWithNull.compare(valueA, valueB);
				}, order);
	}
}