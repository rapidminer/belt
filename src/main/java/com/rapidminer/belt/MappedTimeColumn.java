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
import java.time.LocalTime;
import java.util.Comparator;
import java.util.Map;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Mapped time column with the same range and precision as Java's {@link LocalTime}. Wraps a primitive array for
 * nanoseconds.
 *
 * @author Gisa Meier
 */
class MappedTimeColumn extends Column implements CacheMappedColumn {

	/**
	 * Mappings of a relative size smaller than this threshold are implemented via a deep copy.
	 */
	private static final double MAPPING_THRESHOLD = 0.1;

	private final long[] nanos;
	private final int[] mapping;

	/**
	 * Creates a new mapped time column interpreting the given {@code long} values as nanoseconds of the day (see
	 * {@link LocalTime#toNanoOfDay}).
	 *
	 * @param nanos
	 * 		the nanoseconds of the day
	 * @param mapping
	 * 		the index mapping
	 */
	MappedTimeColumn(long[] nanos, int[] mapping) {
		super(TimeColumn.CAPABILITIES, mapping.length);
		this.nanos = nanos;
		this.mapping = mapping;
	}


	@Override
	void fill(Object[] buffer, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + buffer.length, size());
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			buffer[i++] = lookupObject(offset);
		}
	}

	@Override
	void fill(double[] buffer, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + buffer.length, size());
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			buffer[i++] = lookupDouble(offset);
		}
	}

	@Override
	void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookupObject(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookupDouble(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
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

	private Column mapMerged(int[] mergedMapping, boolean preferView) {
		if (preferView || mergedMapping.length > size() * MAPPING_THRESHOLD) {
			return new MappedTimeColumn(nanos, mergedMapping);
		} else {
			return new TimeColumn(Mapping.apply(nanos, mergedMapping, TimeColumn.MISSING_VALUE));
		}
	}

	@Override
	int[] sort(Order order) {
		return Sorting.sort(size(), Comparator.comparingLong(this::lookupLong), order);
	}

	@Override
	public ColumnType<LocalTime> type() {
		return ColumnTypes.TIME;
	}

	private LocalTime lookupObject(int i) {
		int mapped = mapping[i];
		if (mapped < 0 || mapped > nanos.length) {
			return null;
		} else {
			long s = nanos[mapped];
			if (s == TimeColumn.MISSING_VALUE) {
				return null;
			} else {
				return LocalTime.ofNanoOfDay(s);
			}
		}
	}

	private double lookupDouble(int i) {
		int mapped = mapping[i];
		if (mapped < 0 || mapped > nanos.length) {
			return Double.NaN;
		} else {
			long s = nanos[mapped];
			if (s == TimeColumn.MISSING_VALUE) {
				return Double.NaN;
			} else {
				return s;
			}
		}
	}

	private long lookupLong(int i) {
		int mapped = mapping[i];
		if (mapped < 0 || mapped > nanos.length) {
			return TimeColumn.MISSING_VALUE;
		} else {
			return nanos[mapped];
		}
	}


	@Override
	long writeToChannel(FileChannel channel, long startPosition) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}


	/**
	 * @return the nanoseconds data array
	 */
	long[] getNanos() {
		return nanos;
	}


	/**
	 * @return the mapping
	 */
	int[] getMapping() {
		return mapping;
	}

}
