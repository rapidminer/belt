/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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

import java.time.LocalTime;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Mapped implementation of a {@link TimeColumn}.
 *
 * @author Gisa Meier
 */
class MappedTimeColumn extends TimeColumn implements CacheMappedColumn {

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
		super(mapping.length);
		this.nanos = nanos;
		this.mapping = mapping;
	}


	@Override
	public void fill(Object[] array, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + array.length, size());
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			array[i++] = lookupObject(offset);
		}
	}

	@Override
	public void fill(double[] array, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + array.length, size());
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			array[i++] = lookupDouble(offset);
		}
	}

	@Override
	public void fill(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = lookupObject(rowIndex);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	public void fill(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = lookupDouble(rowIndex);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	public void fillNanosIntoArray(long[] array, int arrayStartIndex) {
		fillNanos(array, arrayStartIndex, 0);
	}

	@Override
	void fill(long[] array, int rowIndex) {
		fillNanos(array, 0, rowIndex);
	}

	/**
	 * Fills the nanosecond data into the array.
	 *
	 * @param array
	 * 		the array to fill into
	 * @param arrayStartIndex
	 * 		the index in the array to start filling
	 * @param rowIndex
	 * 		the row index to start from
	 */
	private void fillNanos(long[] array, int arrayStartIndex, int rowIndex) {
		int end = Math.min(mapping.length - rowIndex, array.length - arrayStartIndex);
		for (int i = 0; i < end; i++) {
			array[arrayStartIndex + i] = lookupLong(i + rowIndex);
		}
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		return mapMerged(Mapping.merge(mapping, this.mapping), preferView);
	}

	@Override
	public Column map(int[] mapping, boolean preferView, ConcurrentHashMap<int[], CompletableFuture<int[]>> cache) {
		int[] merged = waitForOrCompute(this.mapping, mapping, cache);
		return mapMerged(merged, preferView);
	}

	private Column mapMerged(int[] mergedMapping, boolean preferView) {
		if (preferView || mergedMapping.length > size() * MAPPING_THRESHOLD) {
			return new MappedTimeColumn(nanos, mergedMapping);
		} else {
			return new SimpleTimeColumn(Mapping.apply(nanos, mergedMapping, TimeColumn.MISSING_VALUE));
		}
	}

	@Override
	public int[] sort(Order order) {
		return Sorting.sort(size(), Comparator.comparingLong(this::lookupLong), order);
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

}
