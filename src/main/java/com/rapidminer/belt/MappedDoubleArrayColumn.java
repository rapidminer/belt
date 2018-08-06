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
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * A {@link Column} implementation backed by a double array with remapped indices. The remapping is evaluated lazily.
 *
 * @author Gisa Schaefer
 */
class MappedDoubleArrayColumn extends NumericColumn implements CacheMappedColumn {

	/** Mappings of a relative size smaller than this threshold are implemented via a deep copy. */
	static final double MAPPING_THRESHOLD = 0.1;

	private final ColumnType<Void> columnType;
	private final double[] data;
	private final int[] mapping;

	/**
	 * Creates a new column where the data from src is reordered by the given mapping.
	 *
	 * @param src
	 *            the data source
	 * @param mapping
	 *            the new order
	 * @param type the column type id
	 * @throws NullPointerException
	 *             if source array or mapping is {@code null}
	 */
	MappedDoubleArrayColumn(TypeId type, double[] src, int[] mapping) {
		super(Objects.requireNonNull(mapping, "Mapping must not be null").length);
		this.columnType = type == TypeId.INTEGER ? ColumnTypes.INTEGER : ColumnTypes.REAL;
		this.data = Objects.requireNonNull(src, "Source array must not be null");
		this.mapping = mapping;
	}

	@Override
	void fill(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int position = mapping[j];
			if (position < 0 || position >= data.length) {
				buffer[i] = Double.NaN;
			} else {
				buffer[i] = data[position];
			}
		}
	}

	@Override
	void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int position = mapping[rowIndex];
			if (position < 0 || position >= data.length) {
				buffer[bufferIndex] = Double.NaN;
			} else {
				buffer[bufferIndex] = data[position];
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	public ColumnType<Void> type() {
		return columnType;
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		return mapMerged(Mapping.merge(mapping, this.mapping), preferView);
	}

	@Override
	public Column map(int[] mapping, boolean preferView, Map<int[], int[]> cache) {
		int[] merged = cache.computeIfAbsent(this.mapping, k -> Mapping.merge(mapping, this.mapping));
		return mapMerged(merged, preferView);
	}

	@Override
	int[] sort(Order order) {
		return Sorting.sort(Mapping.apply(data, mapping), order);
	}

	@Override
	long writeToChannel(FileChannel channel, long startPosition) throws IOException {
		return TableFiler.writeMappedDoubleArray(startPosition, channel, data, mapping,Integer.MAX_VALUE);
	}

	private Column mapMerged(int[] mergedMapping, boolean preferView) {
		if (preferView || mergedMapping.length > data.length * MAPPING_THRESHOLD) {
			return new MappedDoubleArrayColumn(columnType.id(), data, mergedMapping);
		} else {
			return new DoubleArrayColumn(columnType.id(), Mapping.apply(data, mergedMapping));
		}
	}

}
