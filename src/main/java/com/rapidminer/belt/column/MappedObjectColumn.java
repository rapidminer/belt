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
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Mapped column with object data that is not categorical. Data can be accessed via an {@link ObjectReader}.
 *
 * @author Gisa Meier
 */
class MappedObjectColumn<R> extends ObjectColumn<R> implements CacheMappedColumn {

	private final Object[] data;
	private final int[] mapping;

	MappedObjectColumn(ColumnType<R> type, Object[] data, int[] mapping) {
		super(type, mapping.length);
		this.data = Objects.requireNonNull(data, "Data must not be null");
		this.mapping = mapping;
	}

	@Override
	public void fill(Object[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int position = mapping[j];
			if (position < 0 || position >= data.length) {
				array[i] = null;
			} else {
				array[i] = data[position];
			}
		}
	}

	@Override
	public void fill(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, mapping.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int position = mapping[rowIndex];
			if (position < 0 || position >= data.length) {
				array[arrayIndex] = null;
			} else {
				array[arrayIndex] = data[position];
			}
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		int[] mergedMapping = Mapping.merge(mapping, this.mapping);
		return mapMerged(mergedMapping, preferView);
	}

	@Override
	public Column map(int[] mapping, boolean preferView, Map<int[], int[]> cache) {
		int[] merged = cache.computeIfAbsent(mapping, k -> Mapping.merge(mapping, this.mapping));
		return mapMerged(merged, preferView);
	}

	private Column mapMerged(int[] mergedMapping, boolean preferView) {
		if (preferView || mergedMapping.length > data.length * MappedDoubleArrayColumn.MAPPING_THRESHOLD) {
			return new MappedObjectColumn<>(type(), data, mergedMapping);
		} else {
			return new SimpleObjectColumn<>(type(), Mapping.apply(data, mergedMapping));
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
					int indexA = mapping[a];
					// the cast is safe because the data is of this type
					@SuppressWarnings("unchecked")
					R valueA = indexA < 0 || indexA > data.length ? null : (R) data[indexA];
					int indexB = mapping[b];
					@SuppressWarnings("unchecked")
					R valueB = indexB < 0 || indexB > data.length ? null : (R) data[indexB];
					return comparatorWithNull.compare(valueA, valueB);
				}, order);
	}

}
