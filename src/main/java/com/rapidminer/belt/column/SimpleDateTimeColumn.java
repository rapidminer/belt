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

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.SortingInt;
import com.rapidminer.belt.util.SortingLong;


/**
 * Simple implementation of a {@link DateTimeColumn}.
 *
 * @author Michael Knopf
 */
class SimpleDateTimeColumn extends DateTimeColumn {

	static final SimpleDateTimeColumn EMPTY_DATE_TIME_COLUMN = new SimpleDateTimeColumn(new long[0]);

	private final long[] seconds;
	private final int[] nanos;
	private final boolean highPrecision;

	/**
	 * Creates a new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}).
	 *
	 * <p>If sub-second precision is required, use {@link #SimpleDateTimeColumn(long[], int[])} instead.
	 *
	 * @param seconds
	 * 		the epoch seconds
	 */
	SimpleDateTimeColumn(long[] seconds) {
		super(seconds.length);
		this.seconds = seconds;
		this.nanos = null;
		this.highPrecision = false;
	}

	/**
	 * Creates a new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}) and the given {@code int} values as nanoseconds (see {@link Instant#getNano()}.
	 *
	 * <p>If second precision is sufficient, use {@code null} for the nanoseconds instead.
	 *
	 * @param seconds
	 * 		the epoch seconds
	 * @param nanos
	 * 		the nanoseconds
	 */
	SimpleDateTimeColumn(long[] seconds, int[] nanos) {
		super(seconds.length);
		this.seconds = seconds;
		this.nanos = nanos;
		this.highPrecision = nanos != null;
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
			array[i++] = lookupLowPrecision(offset);
		}
	}

	private void fillHighPrecision(Object[] array, int start, int end) {
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			array[i++] = lookupHighPrecision(offset);
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
		if (preferView || mapping.length > size() * MAPPING_THRESHOLD) {
			return highPrecision
					? new MappedDateTimeColumn(seconds, nanos, mapping)
					: new MappedDateTimeColumn(seconds, mapping);

		} else {
			return highPrecision
					? new SimpleDateTimeColumn(Mapping.apply(seconds, mapping, MISSING_VALUE), Mapping.apply(nanos, mapping))
					: new SimpleDateTimeColumn(Mapping.apply(seconds, mapping, MISSING_VALUE));
		}
	}

	@Override
	public int[] sort(Order order) {
		if (highPrecision) {
			return sortHighPrecision(order);
		} else {
			return SortingLong.sort(seconds, order);
		}
	}

	@Override
	public boolean hasSubSecondPrecision() {
		return highPrecision;
	}


	@Override
	public void fillSecondsIntoArray(long[] array, int arrayStartIndex) {
		int length = Math.min(seconds.length, array.length - arrayStartIndex);
		System.arraycopy(seconds, 0, array, arrayStartIndex, length);
	}

	@Override
	public void fillNanosIntoArray(int[] array, int arrayStartIndex) {
		if (nanos == null) {
			return;
		}
		int length = Math.min(nanos.length, array.length - arrayStartIndex);
		System.arraycopy(nanos, 0, array, arrayStartIndex, length);
	}

	private Instant lookupLowPrecision(int i) {
		long s = seconds[i];
		if (s == SimpleDateTimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s);
		}
	}

	private Instant lookupHighPrecision(int i) {
		long s = seconds[i];
		if (s == SimpleDateTimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s, nanos[i]);
		}
	}

	/**
	 * Sorts first by seconds (long values). For equal long values we then call {@link SortingInt#sortPartially(int[],
	 * int[], int, int, int[], Order)} to sort these subgroups.
	 */
	private int[] sortHighPrecision(Order order) {
		// sort by seconds of epoch first
		int[] sorting = SortingLong.sort(seconds, order);
		// for equal seconds of epoch we want to sort by nanos
		int marker = 0;
		int[] buffer = null;
		for (int position = 1; position < sorting.length; position++) {
			if (seconds[sorting[position - 1]] != seconds[sorting[position]]) {
				if (position - marker > 1) {
					// The last two or more elements had the same value
					if (buffer == null) {
						// initialize buffer used for sorting
						buffer = new int[sorting.length];
					}
					SortingInt.sortPartially(nanos, sorting, marker, position, buffer, order);
				}
				marker = position;
			}
		}
		// Check whether last value was part of uniform interval
		if (sorting.length - 1 - marker > 1) {
			if (buffer == null) {
				// initialize buffer used for sorting
				buffer = new int[sorting.length];
			}
			SortingInt.sortPartially(nanos, sorting, marker, sorting.length, buffer, order);
		}
		return sorting;
	}

}
