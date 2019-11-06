/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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


/**
 * Date-time column with the same range and precision as Java's {@link Instant}. Wraps two primitive arrays for seconds
 * and nanoseconds respectively.
 *
 * @author Gisa Meier
 */
public abstract class DateTimeColumn extends Column {

	/** Mappings of a relative size smaller than this threshold are implemented via a deep copy. */
	protected static final double MAPPING_THRESHOLD = 0.1;

	/**
	 * Java's {@link Instant} implementation does not utilize {@link Long#MAX_VALUE} (see {@link Instant#MAX}). Thus, we
	 * can use it to encode missing entries.
	 */
	public static final long MISSING_VALUE = Long.MAX_VALUE;

	DateTimeColumn(int size) {
		super(size);
	}

	@Override
	public Column stripData() {
		return SimpleDateTimeColumn.EMPTY_DATE_TIME_COLUMN;
	}

	@Override
	public ColumnType<Instant> type() {
		return ColumnTypes.DATETIME;
	}

	@Override
	public String toString() {
		return ColumnPrinter.print(this);
	}

	/**
	 * @return whether the column has subsecond precision
	 */
	public abstract boolean hasSubSecondPrecision();

	/**
	 * Fills the raw second data into the given array.
	 *
	 * @param array
	 * 		the array to fill the data into
	 * @param arrayStartIndex
	 * 		the index in the array where to start
	 */
	public abstract void fillSecondsIntoArray(long[] array, int arrayStartIndex);

	/**
	 * Fills the raw nanosecond data into the given array using the mapping. Does nothing if the column does not
	 * support
	 * sub-second precision.
	 *
	 * @param array
	 * 		the array to fill the data into
	 * @param arrayStartIndex
	 * 		the index in the array where to start
	 */
	public abstract void fillNanosIntoArray(int[] array, int arrayStartIndex) ;


}
