/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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
import java.util.Arrays;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;


/**
 * Sparse implementation of a {@link TimeColumn}. It uses a more memory efficient representation of the data than the
 * default dense {@link SimpleTimeColumn} while still being comparably fast or faster on sparse data.
 *
 * @author Kevin Majchrzak
 */
class TimeSparseColumn extends TimeColumn {

	/**
	 * TimeSparseColumn falls back to a dense column if its density is above this value.
	 */
	private static final double MAX_DENSITY = 0.5d;

	/**
	 * The indices that correspond to values that do not equal the default (usually most common) value of the column.
	 * These indices need to be sorted in ascending order.
	 */
	private final int[] nonDefaultIndices;

	/**
	 * The values of the column that do not equal the default value. They need to be specified in the same order as
	 * their corresponding indices in the non-default indices array so that column[nonDefaultIndices[i]] =
	 * nonDefaultValues[i] for all i in [0,nonDefaultIndices.length-1].
	 */
	private final long[] nonDefaultValues;

	/**
	 * The default (most common) nanos of day value.
	 */
	private final long defaultValue;

	/**
	 * The result of {@link #nanosOfDayToDouble(long)} applied to {@link #defaultValue}.
	 */
	private final double defaultValueAsDouble;


	/**
	 * The result of {@link #nanosOfDayToLocalTime(long)} applied to {@link #defaultValue}.
	 */
	private final LocalTime defaultValueAsLocalTime;

	/**
	 * The number of data rows stored by this column (logical column size).
	 */
	private final int size;

	/**
	 * Creates a new time column interpreting the given {@code long} values as nanoseconds of day (see {@link
	 * LocalTime#ofNanoOfDay(long)} ).
	 *
	 * @param nanoOfDay
	 * 		the nanoseconds of the day
	 */
	TimeSparseColumn(long defaultValue, long[] nanoOfDay) {
		super(nanoOfDay.length);
		this.size = nanoOfDay.length;
		this.defaultValue = defaultValue;
		defaultValueAsDouble = nanosOfDayToDouble(defaultValue);
		this.defaultValueAsLocalTime = nanosOfDayToLocalTime(defaultValue);
		int numberOfNonDefaults = 0;
		for (long l : nanoOfDay) {
			if (l != defaultValue) {
				numberOfNonDefaults++;
			}
		}
		nonDefaultIndices = new int[numberOfNonDefaults];
		nonDefaultValues = new long[numberOfNonDefaults];
		int index = 0;
		for (int i = 0; i < nanoOfDay.length; i++) {
			if (nanoOfDay[i] != defaultValue) {
				nonDefaultIndices[index] = i;
				nonDefaultValues[index++] = nanoOfDay[i];
			}
		}
	}

	/**
	 * Creates a new sparse time column from the given sparse data (similar to {@link
	 * DoubleSparseColumn#DoubleSparseColumn(TypeId, double, int[], double[], int)}).
	 */
	TimeSparseColumn(long defaultValue, int[] nonDefaultIndices, long[] nonDefaultValues, int size) {
		super(size);
		this.size = size;
		this.defaultValue = defaultValue;
		defaultValueAsDouble = nanosOfDayToDouble(defaultValue);
		this.defaultValueAsLocalTime = nanosOfDayToLocalTime(defaultValue);
		this.nonDefaultIndices = nonDefaultIndices;
		this.nonDefaultValues = nonDefaultValues;
	}

	@Override
	public void fill(Object[] array, int rowIndex) {
		ColumnUtils.fillSparseLongsIntoObjectArray(array, rowIndex, defaultValueAsLocalTime, nonDefaultIndices,
				nonDefaultValues, size, this::nanosOfDayToLocalTime);
	}

	@Override
	public void fill(double[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsDouble);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = nanosOfDayToDouble(nonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	public void fill(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		ColumnUtils.fillSparseLongsIntoObjectArray(array, rowIndex, arrayOffset, arrayStepSize, defaultValueAsLocalTime,
				nonDefaultIndices, nonDefaultValues, size, this::nanosOfDayToLocalTime);
	}

	@Override
	public void fill(double[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsDouble;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						nanosOfDayToDouble(nonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	public void fillNanosIntoArray(long[] array, int arrayStartIndex) {
		ColumnUtils.fillSparseLongsIntoLongArray(array, arrayStartIndex, defaultValue, nonDefaultIndices,
				nonDefaultValues, size);
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		if (mapping.length == 0) {
			return stripData();
		}
		ColumnUtils.MapSparseLongColumnResult result = ColumnUtils.mapSparseLongColumn(mapping, defaultValue,
				MISSING_VALUE, nonDefaultIndices, nonDefaultValues, size, MAX_DENSITY);
		if (result.isSparse()) {
			return new TimeSparseColumn(defaultValue, result.newNonDefaultIndices(),
					result.newNonDefaultValues(), mapping.length);
		} else {
			return makeDenseColumn(mapping);
		}
	}

	@Override
	public int[] sort(Order order) {
		return ColumnUtils.sortSparseLongs(order, defaultValue, nonDefaultIndices, nonDefaultValues, size);
	}

	/**
	 * Returns the columns default value (usually but not necessarily the most common value).
	 *
	 * @return the columns default value.
	 */
	long getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Converts the given nanos to their corresponding time values by using {@link LocalTime#ofNanoOfDay(long)}. {@link
	 * TimeColumn#MISSING_VALUE} is converted to {@code null}.
	 */
	private LocalTime nanosOfDayToLocalTime(long nanos) {
		if (nanos == TimeColumn.MISSING_VALUE) {
			return null;
		} else {
			return LocalTime.ofNanoOfDay(nanos);
		}
	}

	/**
	 * Converts the given nanos to their corresponding double value. {@link TimeColumn#MISSING_VALUE} is converted to
	 * {@link Double#NaN}.
	 */
	private double nanosOfDayToDouble(long nanos) {
		if (nanos == TimeColumn.MISSING_VALUE) {
			return Double.NaN;
		} else {
			return nanos;
		}
	}

	/**
	 * Creates a {@link SimpleTimeColumn} by applying the given mapping to this column.
	 */
	private Column makeDenseColumn(int[] mapping) {
		long[] nanoOfDay = new long[size];
		fillNanosIntoArray(nanoOfDay, 0);
		return new SimpleTimeColumn(Mapping.apply(nanoOfDay, mapping, MISSING_VALUE));
	}

}
