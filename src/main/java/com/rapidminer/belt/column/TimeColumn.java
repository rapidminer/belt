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


/**
 * Time column with the same range and precision as Java's {@link LocalTime}. Wraps a primitive array for nanoseconds.
 *
 * @author Gisa Meier
 */
public abstract class TimeColumn extends Column {

	/**
	 * Java's {@link LocalTime#toNanoOfDay()} implementation does not reach {@link Long#MAX_VALUE}. Use max instead of
	 * min to have the missings at the end when sorting long values.
	 */
	public static final long MISSING_VALUE = Long.MAX_VALUE;

	/**
	 * Mappings of a relative size smaller than this threshold are implemented via a deep copy.
	 */
	protected static final double MAPPING_THRESHOLD = 0.1;


	TimeColumn(int size) {
		super(size);
	}

	/**
	 * Fills the nanoseconds data into the given array.
	 *
	 * @param array
	 * 		the array to fill the data into
	 * @param arrayStartIndex
	 * 		the index in the array where to start
	 */
	public abstract void fillNanosIntoArray(long[] array, int arrayStartIndex);

	/**
	 * Fills the nanoseconds data into the given array.
	 *
	 * @param array
	 * 		the array to fill the data into
	 * @param rowIndex
	 * 		the rowIndex to start from
	 */
	abstract void fill(long[] array, int rowIndex);

	@Override
	public Column stripData() {
		return SimpleTimeColumn.EMPTY_TIME_COLUMN;
	}

	@Override
	public ColumnType<LocalTime> type() {
		return ColumnType.TIME;
	}

	@Override
	public String toString() {
		return ColumnPrinter.print(this);
	}


}
