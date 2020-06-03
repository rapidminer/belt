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

import java.time.Instant;
import java.util.Comparator;
import java.util.Map;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;

/**
 * Mapped implementation of a {@link DateTimeColumn}.
 *
 * @author Michael Knopf
 */
class MappedDateTimeColumn extends DateTimeColumn implements CacheMappedColumn {

	private final long[] seconds;
	private final int[] nanos;
	private final int[] mapping;
	private final boolean highPrecision;

	/**
	 * Creates a new mapped date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}).
	 *
	 * <p>If sub-second precision is required, use {@link #MappedDateTimeColumn(long[], int[], int[])} instead.
	 *
	 * @param seconds
	 * 		the epoch seconds
	 * @param mapping
	 * 		the index mapping
	 */
	MappedDateTimeColumn(long[] seconds, int[] mapping) {
		super(mapping.length);
		this.seconds = seconds;
		this.nanos = null;
		this.mapping = mapping;
		this.highPrecision = false;
	}

	/**
	 * Creates a mapped new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}) and the given {@code int} values as nanoseconds (see {@link Instant#getNano()}.
	 *
	 * <p>If second precision is sufficient, use {@link #MappedDateTimeColumn(long[], int[])} instead.
	 *
	 * @param seconds
	 * 		the epoch seconds
	 * @param nanos
	 * 		the nanoseconds
	 * @param mapping
	 * 		the index mapping
	 */
	MappedDateTimeColumn(long[] seconds, int[] nanos, int[] mapping) {
		super(mapping.length);
		this.seconds = seconds;
		this.nanos = nanos;
		this.mapping = mapping;
		this.highPrecision = true;
	}

	@Override
	public void fill(Object[] array, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + array.length, size());
		if (highPrecision) {
			fillHighPrecision(array, start, end);
		} else {
			fillLowPrecision(array, start, end);
		}
	}

	private void fillLowPrecision(Object[] array, int start, int end) {
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			int index = mapping[offset];
			if (index < 0 || index >= seconds.length) {
				array[i++] = null;
			} else {
				long s = seconds[index];
				if (s == DateTimeColumn.MISSING_VALUE) {
					array[i++] = null;
				} else {
					array[i++] = Instant.ofEpochSecond(s);
				}
			}
		}
	}

	private void fillHighPrecision(Object[] array, int start, int end) {
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			int index = mapping[offset];
			if (index < 0 || index >= seconds.length) {
				array[i++] = null;
			} else {
				long s = seconds[index];
				if (s == DateTimeColumn.MISSING_VALUE) {
					array[i++] = null;
				} else {
					array[i++] = Instant.ofEpochSecond(s, nanos[index]);
				}
			}
		}
	}

	@Override
	public void fill(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		if (highPrecision) {
			fillHighPrecision(array, startIndex, max, arrayOffset, arrayStepSize);
		} else {
			fillLowPrecision(array, startIndex, max, arrayOffset, arrayStepSize);
		}
	}

	private void fillLowPrecision(Object[] array, int startIndex, int maxIndex, int arrayOffset, int arrayStepSize) {
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < maxIndex) {
			array[arrayIndex] = lookupLowPrecision(rowIndex);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	private void fillHighPrecision(Object[] array, int startIndex, int maxIndex, int arrayOffset,
								   int arrayStepSize) {
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < maxIndex) {
			array[arrayIndex] = lookupHighPrecision(rowIndex);
			arrayIndex += arrayStepSize;
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
			return highPrecision
					? new MappedDateTimeColumn(seconds, nanos, mergedMapping)
					: new MappedDateTimeColumn(seconds, mergedMapping);
		} else {
			return highPrecision
					? new SimpleDateTimeColumn(Mapping.apply(seconds, mergedMapping, DateTimeColumn.MISSING_VALUE),
					Mapping.apply(nanos, mergedMapping))
					: new SimpleDateTimeColumn(Mapping.apply(seconds, mergedMapping, DateTimeColumn.MISSING_VALUE));
		}
	}

	@Override
	public int[] sort(Order order) {
		Comparator<Instant> comparator = ColumnType.DATETIME.comparator();
		Comparator<Instant> comparatorWithNull = Comparator.nullsLast(comparator);
		if (highPrecision) {
			return Sorting.sort(size(), (a, b) -> comparatorWithNull.compare(lookupHighPrecision(a),
					lookupHighPrecision(b)), order);
		} else {
			return Sorting.sort(size(), (a, b) -> comparatorWithNull.compare(lookupLowPrecision(a),
					lookupLowPrecision(b)), order);
		}
	}

	private Instant lookupLowPrecision(int i) {
		int mapped = mapping[i];
		if (mapped < 0 || mapped > seconds.length) {
			return null;
		} else {
			long s = seconds[mapped];
			if (s == DateTimeColumn.MISSING_VALUE) {
				return null;
			} else {
				return Instant.ofEpochSecond(s);
			}
		}
	}

	private Instant lookupHighPrecision(int i) {
		int mapped = mapping[i];
		if (mapped < 0 || mapped > seconds.length) {
			return null;
		} else {
			long s = seconds[mapped];
			if (s == DateTimeColumn.MISSING_VALUE) {
				return null;
			} else {
				return Instant.ofEpochSecond(s, nanos[mapped]);
			}
		}
	}


	@Override
	public boolean hasSubSecondPrecision() {
		return highPrecision;
	}

	@Override
	public void fillSecondsIntoArray(long[] array, int arrayStartIndex) {
		int end = Math.min(mapping.length, array.length - arrayStartIndex);
		for (int i = 0; i < end; i++) {
			int index = mapping[i];
			if (index < 0 || index >= seconds.length) {
				array[arrayStartIndex + i] = DateTimeColumn.MISSING_VALUE;
			} else {
				array[arrayStartIndex + i] = seconds[index];
			}
		}
	}

	@Override
	public void fillNanosIntoArray(int[] array, int arrayStartIndex) {
		if (nanos == null) {
			return;
		}
		int end = Math.min(mapping.length, array.length - arrayStartIndex);
		for (int i = 0; i < end; i++) {
			int index = mapping[i];
			if (index < 0 || index >= nanos.length) {
				array[arrayStartIndex + i] = 0;
			} else {
				array[arrayStartIndex + i] = nanos[index];
			}
		}
	}

}
