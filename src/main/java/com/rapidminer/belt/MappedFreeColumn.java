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

import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Column with data associated to integer categories. Data can be accessed via a {@link CategoricalColumnReader} or a
 * {@link ColumnReader} together with access to the mapping by {@link #getDictionary(Class)}.
 *
 * @author Gisa Meier
 */
class MappedFreeColumn<R> extends FreeColumn<R> implements CacheMappedColumn {

	private final Object[] data;
	private final int[] mapping;

	MappedFreeColumn(ColumnType<R> type, Object[] data, int[] mapping) {
		super(type, mapping.length);
		this.data = Objects.requireNonNull(data, "Data must not be null");
		this.mapping = mapping;
	}

	@Override
	void fill(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int position = mapping[j];
			if (position < 0 || position >= data.length) {
				buffer[i] = null;
			} else {
				buffer[i] = data[position];
			}
		}
	}

	@Override
	void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int position = mapping[rowIndex];
			if (position < 0 || position >= data.length) {
				buffer[bufferIndex] = null;
			} else {
				buffer[bufferIndex] = data[position];
			}
			bufferIndex += bufferStepSize;
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
		int[] merged = cache.computeIfAbsent(getRowMapping(), k -> Mapping.merge(mapping, this.mapping));
		return mapMerged(merged, preferView);
	}

	private Column mapMerged(int[] mergedMapping, boolean preferView) {
		if (preferView || mergedMapping.length > data.length * MappedDoubleArrayColumn.MAPPING_THRESHOLD) {
			return new MappedFreeColumn<>(type(), data, mergedMapping);
		} else {
			return new SimpleFreeColumn<>(type(), Mapping.apply(data, mergedMapping));
		}
	}

	@Override
	long writeToChannel(FileChannel channel, long startPosition) {
		throw new UnsupportedOperationException();
	}

	/**
	 * The Row mapping.
	 *
	 * @return the mapping
	 */
	protected int[] getRowMapping() {
		return mapping;
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
