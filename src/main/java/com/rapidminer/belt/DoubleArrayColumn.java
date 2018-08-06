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
import java.util.Objects;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;



/**
 * Naive {@link Column} implementation backed by a single double array.
 *
 * @author Michael Knopf
 */
class DoubleArrayColumn extends NumericColumn {

	/** Mappings of a relative size smaller than this threshold are implemented via a deep copy. */
	private static final double MAPPING_THRESHOLD = 0.1;

	private final ColumnType<Void> columntype;
	private final double[] data;

	/**
	 * Creates a new column with data stored in a double array.
	 *
	 * @param src
	 * 		the data for the column
	 * @throws NullPointerException
	 * 		if the source array is {@code null}
	 */
	DoubleArrayColumn(double[] src) {
		this(TypeId.REAL, src);
	}

	/**
	 * Creates a new column with data stored in a double array and the given type id.
	 *
	 * @param src
	 * 		the data for the column
	 * @param type
	 * 		the column type id
	 * @throws NullPointerException
	 * 		if the source array or the id is {@code null}
	 */
	DoubleArrayColumn(TypeId type, double[] src) {
		super(Objects.requireNonNull(src, "Source array must not be null").length);
		this.columntype = type == TypeId.INTEGER ? ColumnTypes.INTEGER : ColumnTypes.REAL;
		data = src;
	}

	@Override
	void fill(double[] buffer, int rowIndex) {
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(buffer.length, data.length - start);
		System.arraycopy(data, start, buffer, 0, length);
	}

	@Override
	void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
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
	public ColumnType<Void> type() {
		return columntype;
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		if (preferView || mapping.length > data.length * MAPPING_THRESHOLD) {
			return new MappedDoubleArrayColumn(columntype.id(), data, mapping);
		} else {
			return new DoubleArrayColumn(columntype.id(), Mapping.apply(data, mapping));
		}
	}

	@Override
	int[] sort(Order order) {
		return Sorting.sort(data, order);
	}

	@Override
	long writeToChannel(FileChannel channel, long startPosition) throws IOException {
		return TableFiler.writeDoubleArray(startPosition,channel,data,Integer.MAX_VALUE);
	}

}
