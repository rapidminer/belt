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

import java.time.Instant;
import java.util.Arrays;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.SortingInt;


/**
 * High precision sparse {@link DateTimeColumn} implementation. It uses a more memory efficient representation of the
 * data than the default dense columns while still being comparably fast or faster on sparse data.
 *
 * @author Kevin Majchrzak
 * @see DateTimeColumn
 */
class DateTimeHighPrecisionSparseColumn extends DateTimeColumn {

	/**
	 * DateTimeHighPrecisionSparseColumn falls back to a dense column if its density is above this value.
	 */
	private static final double MAX_DENSITY = 0.375d;

	/**
	 * The indices that correspond to seconds of epoch values that do not equal the default (usually most common)
	 * seconds of epoch value of the column. These indices need to be sorted in ascending order.
	 */
	private final int[] nonDefaultIndices;

	/**
	 * The second values of the column that do not equal the default value. They need to be specified in the same order
	 * as their corresponding indices in the non-default indices array so that column[nonDefaultIndices[i]] =
	 * nonDefaultValues[i] for all i in [0,nonDefaultIndices.length-1].
	 */
	private final long[] nonDefaultValues;

	/**
	 * The default (most common) seconds of epoch value.
	 */
	private final long defaultValue;

	/**
	 * The number of data rows stored by this column (logical column size).
	 */
	private final int size;

	/**
	 * High precision nanosecond data.
	 */
	private final int[] nanos;

	/**
	 * Creates a new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}) and the given {@code int} values as nanoseconds (see {@link Instant#getNano()}.
	 *
	 * <p>If second precision is sufficient, use {@link DateTimeLowPrecisionSparseColumn} instead.
	 *
	 * @param seconds
	 * 		the seconds of epoch
	 * @param nanos
	 * 		the nanoseconds
	 */
	DateTimeHighPrecisionSparseColumn(long defaultValue, long[] seconds, int[] nanos) {
		super(seconds.length);
		this.size = seconds.length;
		this.defaultValue = defaultValue;
		this.nanos = nanos;
		int numberOfNonDefaults = 0;
		for (long l : seconds) {
			if (l != defaultValue) {
				numberOfNonDefaults++;
			}
		}
		nonDefaultIndices = new int[numberOfNonDefaults];
		nonDefaultValues = new long[numberOfNonDefaults];
		int index = 0;
		for (int i = 0; i < seconds.length; i++) {
			if (seconds[i] != defaultValue) {
				nonDefaultIndices[index] = i;
				nonDefaultValues[index++] = seconds[i];
			}
		}
	}

	/**
	 * Creates a new sparse date time column from the given sparse data (similar to {@link
	 * DoubleSparseColumn#DoubleSparseColumn(TypeId, double, int[], double[], int)}).
	 */
	DateTimeHighPrecisionSparseColumn(long defaultValue, int[] nonDefaultIndices, long[] nonDefaultValues, int size, int[] nanos) {
		super(size);
		this.size = size;
		this.defaultValue = defaultValue;
		this.nonDefaultIndices = nonDefaultIndices;
		this.nonDefaultValues = nonDefaultValues;
		this.nanos = nanos;
	}

	@Override
	public void fill(Object[] array, final int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			for (int currentRowIndex = rowIndex; currentRowIndex < maxRowIndex; currentRowIndex++) {
				if (currentRowIndex == nonDefaultPosition) {
					// add non-default value
					array[currentRowIndex - rowIndex] = secondsAndNanosToInstant(nonDefaultValues[nonDefaultIndex], nanos[currentRowIndex]);
					nonDefaultIndex++;
					if (nonDefaultIndex >= nonDefaultIndices.length) {
						// no more non-defaults. fill the rest with defaults
						currentRowIndex++;
						while (currentRowIndex < maxRowIndex) {
							// add default value
							array[currentRowIndex - rowIndex] = secondsAndNanosToInstant(defaultValue, nanos[currentRowIndex]);
							currentRowIndex++;
						}
						break;
					}
					nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
				} else {
					// add default value
					array[currentRowIndex - rowIndex] = secondsAndNanosToInstant(defaultValue, nanos[currentRowIndex]);
				}
			}
		} else {
			// only default values
			// fill all free slots in the array with default values
			int currentRowIndex = rowIndex;
			for (int i = 0, len = array.length; i < len && currentRowIndex < maxRowIndex; i++) {
				array[i] = secondsAndNanosToInstant(defaultValue, nanos[currentRowIndex++]);
			}
		}
	}

	@Override
	public void fill(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			for (int currentRowIndex = rowIndex; currentRowIndex < maxRowIndex; currentRowIndex++) {
				if (currentRowIndex == nonDefaultPosition) {
					// add non-default value
					array[(currentRowIndex - rowIndex) * arrayStepSize + arrayOffset] = secondsAndNanosToInstant(nonDefaultValues[nonDefaultIndex], nanos[currentRowIndex]);
					nonDefaultIndex++;
					if (nonDefaultIndex >= nonDefaultIndices.length) {
						// no more non-defaults. fill the rest with defaults
						currentRowIndex++;
						while (currentRowIndex < maxRowIndex) {
							// add default value
							array[(currentRowIndex - rowIndex) * arrayStepSize + arrayOffset] = secondsAndNanosToInstant(defaultValue, nanos[currentRowIndex]);
							currentRowIndex++;
						}
						break;
					}
					nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
				} else {
					// add default value
					array[(currentRowIndex - rowIndex) * arrayStepSize + arrayOffset] = secondsAndNanosToInstant(defaultValue, nanos[currentRowIndex]);
				}
			}
		} else {
			// only default values
			// fill all free slots in the array with default values
			int currentRowIndex = rowIndex;
			for (int i = arrayOffset, len = array.length; i < len && currentRowIndex < maxRowIndex; i += arrayStepSize) {
				array[i] = secondsAndNanosToInstant(defaultValue, nanos[currentRowIndex++]);
			}
		}
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		if (mapping.length == 0) {
			return stripData();
		}
		ColumnUtils.MapSparseLongColumnResult result = ColumnUtils.mapSparseLongColumn(mapping, defaultValue,
				MISSING_VALUE, nonDefaultIndices, nonDefaultValues, size, MAX_DENSITY);
		if (result.isSparse()) {
			return new DateTimeHighPrecisionSparseColumn(defaultValue, result.newNonDefaultIndices(),
					result.newNonDefaultValues(), mapping.length, Mapping.apply(nanos, mapping));
		} else {
			return makeDenseColumn(mapping);
		}
	}

	@Override
	public int[] sort(Order order) {
		if (nonDefaultValues.length == 0) {
			// only default values. therefore sort by nanos
			return SortingInt.sort(nanos, order);
		}
		// sort by seconds of epoch first
		int[] sorting = ColumnUtils.sortSparseLongs(order, defaultValue, nonDefaultIndices,
				nonDefaultValues, size);
		// for equal seconds of epoch we want to sort by nanos
		SparseBitmap bitMap = new SparseBitmap(defaultValue == MISSING_VALUE, nonDefaultIndices, size);
		int[] buffer = new int[sorting.length]; // buffer for SortingInt.sortPartially
		int marker = 0;
		int start = 1;
		long lastNewValue = nonDefaultValues[0];
		int numberOfDefaultValues = size - nonDefaultIndices.length;
		if (bitMap.get(sorting[0]) < 0) {
			// This handles the special case that the default value is first
			SortingInt.sortPartially(nanos, sorting, 0, numberOfDefaultValues, buffer, order);
			start = numberOfDefaultValues + 1;
			marker = numberOfDefaultValues;
		}
		for (int position = start; position < sorting.length; position++) {
			int currentNonDefaultIndex = bitMap.get(sorting[position]);
			if (currentNonDefaultIndex >= 0) {
				long currentValue = nonDefaultValues[currentNonDefaultIndex];
				if (lastNewValue != currentValue) {
					if (position - marker > 1) {
						// The last two or more elements had the same value
						SortingInt.sortPartially(nanos, sorting, marker, position, buffer, order);
					}
					marker = position;
					lastNewValue = currentValue;
				}
			} else {
				// This handles the special case that the current value is the default value
				if (position - marker > 1) {
					// The last two or more elements had the same value
					SortingInt.sortPartially(nanos, sorting, marker, position, buffer, order);
				}
				// sort the default values and update marker and position
				SortingInt.sortPartially(nanos, sorting, position, position + numberOfDefaultValues, buffer, order);
				position += numberOfDefaultValues;
				marker = position;
				if (position < sorting.length) {
					lastNewValue = nonDefaultValues[bitMap.get(sorting[position])];
				}
			}
		}
		// Check whether last value was part of uniform interval
		if (sorting.length - 1 - marker > 1) {
			SortingInt.sortPartially(nanos, sorting, marker, sorting.length, buffer, order);
		}
		return sorting;
	}

	/**
	 * Takes seconds of epoch and nanos and creates a new instance from it. {@link #MISSING_VALUE} seconds will be
	 * mapped to {@code null}.
	 *
	 * @param s
	 * 		seconds of epoch
	 * @param n
	 * 		nanos
	 * @return the corresponding instance or null for the missing value
	 */
	private Instant secondsAndNanosToInstant(long s, long n) {
		if (s == MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s, n);
		}
	}

	@Override
	public boolean hasSubSecondPrecision() {
		return true;
	}


	@Override
	public void fillSecondsIntoArray(long[] array, int arrayStartIndex) {
		ColumnUtils.fillSparseLongsIntoLongArray(array, arrayStartIndex, defaultValue, nonDefaultIndices,
				nonDefaultValues, size);
	}

	@Override
	public void fillNanosIntoArray(int[] array, int arrayStartIndex) {
		int length = Math.min(nanos.length, array.length - arrayStartIndex);
		System.arraycopy(nanos, 0, array, arrayStartIndex, length);
	}

	/**
	 * Returns the column's default value (usually but not necessarily the most common value).
	 *
	 * @return the column's default value.
	 */
	long getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Creates a {@link SimpleDateTimeColumn} by applying the given mapping to this column.
	 */
	private Column makeDenseColumn(int[] mapping) {
		long[] seconds = new long[size];
		fillSecondsIntoArray(seconds, 0);
		return new SimpleDateTimeColumn(Mapping.apply(seconds, mapping, MISSING_VALUE), Mapping.apply(nanos, mapping));
	}

}
