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

package com.rapidminer.belt.column;

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

	private static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
	static final DoubleArrayColumn EMPTY_INT_COLUMN = new DoubleArrayColumn(TypeId.INTEGER,
			EMPTY_DOUBLE_ARRAY);
	static final DoubleArrayColumn EMPTY_REAL_COLUMN = new DoubleArrayColumn(EMPTY_DOUBLE_ARRAY);

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
	public void fill(double[] array, int rowIndex) {
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(array.length, data.length - start);
		System.arraycopy(data, start, array, 0, length);
	}

	@Override
	public void fill(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		} else if (arrayStepSize == 1) {
			if (startIndex < data.length && arrayOffset < array.length) {
				System.arraycopy(data, startIndex, array, arrayOffset,
						Math.min(data.length - startIndex, array.length - arrayOffset));
			}
		} else {
			int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, data.length);
			int rowIndex = startIndex;
			int arrayIndex = arrayOffset;
			while (rowIndex < max) {
				array[arrayIndex] = data[rowIndex];
				arrayIndex += arrayStepSize;
				rowIndex++;
			}
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
	public int[] sort(Order order) {
		return Sorting.sort(data, order);
	}

}
