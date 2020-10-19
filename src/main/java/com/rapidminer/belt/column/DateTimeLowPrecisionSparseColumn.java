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


/**
 * Low precision sparse {@link DateTimeColumn} implementation. It uses a more memory efficient representation of the
 * data than the default dense columns while still being comparably fast or faster on sparse data.
 *
 * @author Kevin Majchrzak
 */
class DateTimeLowPrecisionSparseColumn extends DateTimeColumn {

	/**
	 * DateTimeLowPrecisionSparseColumn falls back to a dense column if its density is above this value.
	 */
	private static final double MAX_DENSITY = 0.375d;

	/**
	 * The indices that correspond to values that do not equal the default (usually most common) seconds of epoch value of the column.
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
	 * The default (most common) seconds of epoch value.
	 */
	private final long defaultValue;

	/**
	 * The result of {@link Instant#ofEpochSecond(long)} applied to {@link #defaultValue}.
	 */
	private final Instant defaultValueAsInstant;

	/**
	 * The number of data rows stored by this column (logical column size).
	 */
	private final int size;

	/**
	 * Creates a new date-time column interpreting the given {@code long} values as epoch seconds (see {@link
	 * Instant#getEpochSecond()}).
	 *
	 * <p>If sub-second precision is required, use {@link DateTimeHighPrecisionSparseColumn} instead.
	 *
	 * @param seconds
	 * 		the seconds of epoch
	 */
	DateTimeLowPrecisionSparseColumn(long defaultValue, long[] seconds) {
		super(seconds.length);
		this.size = seconds.length;
		this.defaultValue = defaultValue;
		this.defaultValueAsInstant = secondsOfEpochToInstant(defaultValue);
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
	DateTimeLowPrecisionSparseColumn(long defaultValue, int[] nonDefaultIndices, long[] nonDefaultValues, int size) {
		super(size);
		this.size = size;
		this.defaultValue = defaultValue;
		this.defaultValueAsInstant = secondsOfEpochToInstant(defaultValue);
		this.nonDefaultIndices = nonDefaultIndices;
		this.nonDefaultValues = nonDefaultValues;
	}

	@Override
	public void fill(Object[] array, int rowIndex) {
		ColumnUtils.fillSparseLongsIntoObjectArray(array, rowIndex, defaultValueAsInstant, nonDefaultIndices,
				nonDefaultValues, size, this::secondsOfEpochToInstant);
	}

	@Override
	public void fill(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		ColumnUtils.fillSparseLongsIntoObjectArray(array, rowIndex, arrayOffset, arrayStepSize, defaultValueAsInstant,
				nonDefaultIndices, nonDefaultValues, size, this::secondsOfEpochToInstant);
	}

	@Override
	void fillSeconds(long[] array, int rowIndex) {
		ColumnUtils.fillSparseLongsIntoLongArray(array, 0, rowIndex, defaultValue, nonDefaultIndices,
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
			return new DateTimeLowPrecisionSparseColumn(defaultValue, result.newNonDefaultIndices(),
					result.newNonDefaultValues(), mapping.length);
		} else {
			return makeDenseColumn(mapping);
		}
	}

	@Override
	public int[] sort(Order order) {
		return ColumnUtils.sortSparseLongs(order, defaultValue, nonDefaultIndices, nonDefaultValues, size);
	}

	private Instant secondsOfEpochToInstant(long s) {
		if (s == MISSING_VALUE) {
			return null;
		} else {
			return Instant.ofEpochSecond(s);
		}
	}

	@Override
	public boolean hasSubSecondPrecision() {
		return false;
	}


	@Override
	public void fillSecondsIntoArray(long[] array, int arrayStartIndex) {
		ColumnUtils.fillSparseLongsIntoLongArray(array, arrayStartIndex,0, defaultValue, nonDefaultIndices,
				nonDefaultValues, size);
	}

	@Override
	public void fillNanosIntoArray(int[] array, int arrayStartIndex) {
		// Nothing to do as we do not have nanos
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
	 * Creates a {@link SimpleDateTimeColumn} by applying the given mapping to this column.
	 */
	private Column makeDenseColumn(int[] mapping) {
		long[] seconds = new long[size];
		fillSecondsIntoArray(seconds, 0);
		return new SimpleDateTimeColumn(Mapping.apply(seconds, mapping, MISSING_VALUE));
	}

}
