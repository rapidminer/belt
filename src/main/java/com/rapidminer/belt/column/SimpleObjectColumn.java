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

package com.rapidminer.belt.column;

import java.util.Comparator;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Simple implementation of an {@link ObjectColumn}.
 *
 * @author Gisa Meier
 */
class SimpleObjectColumn<R> extends ObjectColumn<R> {

	private final Object[] data;

	protected SimpleObjectColumn(ColumnType<R> type, Object[] data) {
		super(type, data.length);
		this.data = data;
	}


	@Override
	public void fill(Object[] array, int rowIndex) {
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(array.length, data.length - start);
		System.arraycopy(data, start, array, 0, length);
	}

	@Override
	public void fill(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		if (arrayStepSize == 1) {
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
	Column map(int[] mapping, boolean preferView) {
		if (preferView || mapping.length > data.length * MappedDoubleArrayColumn.MAPPING_THRESHOLD) {
			return new MappedObjectColumn<>(type(), data, mapping);
		} else {
			return new SimpleObjectColumn<>(type(), Mapping.apply(data, mapping));
		}
	}

	@Override
	public int[] sort(Order order) {
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