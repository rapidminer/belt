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
import java.time.Instant;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Date-time column with the same range and precision as Java's {@link Instant}. Wraps two primitive arrays for seconds
 * and nanoseconds respectively.
 *
 * @author Michael Knopf
 */
class DateTimeColumn extends Column {

	/** Mappings of a relative size smaller than this threshold are implemented via a deep copy. */
	private static final double MAPPING_THRESHOLD = 0.1;

	static final Set<Capability> CAPABILITIES = EnumSet.of(Capability.OBJECT_READABLE, Capability.SORTABLE);

	/**
	 * Java's {@link Instant} implementation does not utilize {@link Long#MIN_VALUE}. Thus, we can use it to encode
	 * missing entries.
	 */
	static final long MISSING_VALUE = Long.MIN_VALUE;

	private final long[] seconds;
	private final int[] nanos;
	private final boolean highPrecision;

	/**
	 * Creates a new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}).
	 *
	 * <p>If sub-second precision is required, use {@link #DateTimeColumn(long[], int[])} instead.
	 *
	 * @param seconds
	 * 		the epoch seconds
	 */
	DateTimeColumn(long[] seconds) {
		super(CAPABILITIES, seconds.length);
		this.seconds = seconds;
		this.nanos = null;
		this.highPrecision = false;
	}

	/**
	 * Creates a new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}) and the given {@code int} values as nanoseconds (see {@link Instant#getNano()}.
	 *
	 * <p>If second precision is sufficient, use {@link #DateTimeColumn(long[])} instead.
	 *
	 * @param seconds
	 * 		the epoch seconds
	 * @param nanos
	 * 		the nanoseconds
	 */
	DateTimeColumn(long[] seconds, int[] nanos) {
		super(CAPABILITIES, seconds.length);
		this.seconds = seconds;
		this.nanos = nanos;
		this.highPrecision = true;
	}

	@Override
	void fill(Object[] buffer, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + buffer.length, size());
		if (highPrecision) {
			fillHighPrecision(buffer, start, end);
		} else {
			fillLowPrecision(buffer, start, end);
		}
	}

	private void fillLowPrecision(Object[] buffer, int start, int end) {
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			buffer[i++] = lookupLowPrecision(offset);
		}
	}

	private void fillHighPrecision(Object[] buffer, int start, int end) {
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			buffer[i++] = lookupHighPrecision(offset);
		}
	}

	@Override
	void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		if (highPrecision) {
			fillHighPrecision(buffer, startIndex, max, bufferOffset, bufferStepSize);
		} else {
			fillLowPrecision(buffer, startIndex, max, bufferOffset, bufferStepSize);
		}
	}

	private void fillLowPrecision(Object[] buffer, int startIndex, int maxIndex, int bufferOffset, int bufferStepSize) {
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < maxIndex) {
			buffer[bufferIndex] = lookupLowPrecision(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	private void fillHighPrecision(Object[] buffer, int startIndex, int maxIndex, int bufferOffset,
								   int bufferStepSize) {
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < maxIndex) {
			buffer[bufferIndex] = lookupHighPrecision(rowIndex);
			bufferIndex += bufferStepSize;
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
					? new DateTimeColumn(Mapping.apply(seconds, mapping, MISSING_VALUE), Mapping.apply(nanos, mapping))
					: new DateTimeColumn(Mapping.apply(seconds, mapping, MISSING_VALUE));
		}
	}

	@Override
	int[] sort(Order order) {
		Comparator<Instant> comparator = ColumnTypes.DATETIME.comparator();
		Comparator<Instant> comparatorWithNull = Comparator.nullsLast(comparator);
		if (highPrecision) {
			return Sorting.sort(size(), (a, b) -> comparatorWithNull.compare(lookupHighPrecision(a),
					lookupHighPrecision(b)), order);
		} else {
			return Sorting.sort(size(), (a, b) -> comparatorWithNull.compare(lookupLowPrecision(a),
					lookupLowPrecision(b)), order);
		}
	}

	@Override
	public ColumnType<Instant> type() {
		return ColumnTypes.DATETIME;
	}

	private Instant lookupLowPrecision(int i) {
		long s = seconds[i];
		if (s == DateTimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s);
		}
	}

	private Instant lookupHighPrecision(int i) {
		long s = seconds[i];
		if (s == DateTimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s, nanos[i]);
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
	 * @return the seconds data array
	 */
	long[] getSeconds() {
		return seconds;
	}

	/**
	 * @return the nanosecond data array, can be {@code null}
	 */
	int[] getNanos() {
		return nanos;
	}

}
