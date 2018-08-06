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
import java.util.EnumSet;
import java.util.Set;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Time column with the same range and precision as Java's {@link LocalTime}. Wraps a primitive array for nanoseconds.
 *
 * @author Gisa Meier
 */
class TimeColumn extends Column {

	/**
	 * Mappings of a relative size smaller than this threshold are implemented via a deep copy.
	 */
	private static final double MAPPING_THRESHOLD = 0.1;

	static final Set<Capability> CAPABILITIES =
			EnumSet.of(Capability.OBJECT_READABLE, Capability.NUMERIC_READABLE, Capability.SORTABLE);

	/**
	 * Java's {@link LocalTime#toNanoOfDay()} implementation does not reach {@link Long#MAX_VALUE}. Use max instead of
	 * min to have the missings at the end when sorting long values.
	 */
	static final long MISSING_VALUE = Long.MAX_VALUE;

	private final long[] nanoOfDay;

	/**
	 * Creates a new time column interpreting the given {@code long} values as nanoseconds of the day (see {@link
	 * LocalTime#toNanoOfDay()}).
	 *
	 * @param nanoOfDay
	 * 		the nanoseconds of the day
	 */
	TimeColumn(long[] nanoOfDay) {
		super(CAPABILITIES, nanoOfDay.length);
		this.nanoOfDay = nanoOfDay;
	}

	@Override
	void fill(Object[] buffer, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int end = Math.min(start + buffer.length, size());
		int i = 0;
		for (int offset = start; offset < end; offset++) {
			buffer[i++] = lookupLocalTime(offset);
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
			buffer[bufferIndex] = lookupLocalTime(rowIndex);
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
		if (preferView || mapping.length > size() * MAPPING_THRESHOLD) {
			return new MappedTimeColumn(nanoOfDay, mapping);
		} else {
			return new TimeColumn(Mapping.apply(nanoOfDay, mapping, MISSING_VALUE));
		}
	}

	@Override
	int[] sort(Order order) {
		return Sorting.sort(size(), Comparator.comparingLong(a -> nanoOfDay[a]), order);
	}

	@Override
	public ColumnType<LocalTime> type() {
		return ColumnTypes.TIME;
	}

	private LocalTime lookupLocalTime(int i) {
		long s = nanoOfDay[i];
		if (s == TimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return LocalTime.ofNanoOfDay(s);
		}
	}

	private double lookupDouble(int i) {
		long s = nanoOfDay[i];
		if (s == TimeColumn.MISSING_VALUE) {
			return Double.NaN;
		} else {
			return s;
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
	 * @return the nanoseconds of day data array
	 */
	long[] getNanos() {
		return nanoOfDay;
	}

}
