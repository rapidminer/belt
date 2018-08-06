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
import java.util.Map;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;

/**
 * Mapped date-time column with the same range and precision as Java's {@link Instant}. Wraps two primitive arrays for
 * seconds and nanoseconds respectively.
 *
 * @author Michael Knopf
 */
class MappedDateTimeColumn extends Column implements CacheMappedColumn {

	/** Mappings of a relative size smaller than this threshold are implemented via a deep copy. */
	private static final double MAPPING_THRESHOLD = 0.1;

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
		super(DateTimeColumn.CAPABILITIES, mapping.length);
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
		super(DateTimeColumn.CAPABILITIES, mapping.length);
		this.seconds = seconds;
		this.nanos = nanos;
		this.mapping = mapping;
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
			int index = mapping[offset];
			if (index < 0 || index >= seconds.length) {
				buffer[i++] = null;
			} else {
				long s = seconds[index];
				if (s == DateTimeColumn.MISSING_VALUE) {
					buffer[i++] = null;
				} else {
					buffer[i++] = Instant.ofEpochSecond(s);
				}
			}
		}
	}

	private void fillHighPrecision(Object[] buffer, int start, int end) {
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			int index = mapping[offset];
			if (index < 0 || index >= seconds.length) {
				buffer[i++] = null;
			} else {
				long s = seconds[index];
				if (s == DateTimeColumn.MISSING_VALUE) {
					buffer[i++] = null;
				} else {
					buffer[i++] = Instant.ofEpochSecond(s, nanos[index]);
				}
			}
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
					? new DateTimeColumn(Mapping.apply(seconds, mergedMapping, DateTimeColumn.MISSING_VALUE),
					Mapping.apply(nanos, mergedMapping))
					: new DateTimeColumn(Mapping.apply(seconds, mergedMapping, DateTimeColumn.MISSING_VALUE));
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

	/**
	 * @return the mapping
	 */
	int[] getMapping() {
		return mapping;
	}

}
