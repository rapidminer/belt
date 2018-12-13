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

/**
 * A row of a {@link Table} that supports reading different formats. In contrast to {@link NumericRow}, {@link CategoricalRow}
 * and {@link ObjectRow}, a {@link MixedRow} allows to read the entries as numbers, category indices or objects at the
 * same time.
 *
 * @author Gisa Meier
 * @see MixedRowReader
 */
public interface MixedRow {

	/**
	 * Returns the numeric value of the this row at the given index. Returns {@link Double#NaN} in case there is
	 * no numeric value at this index. This method is well-defined for indices zero (including) to
	 * {@link #width()} (excluding).
	 * <p>This method does not perform any range checks.
	 *
	 * @param index
	 * 		the index
	 * @return the value of the row at the given index if the value is {@link Column.Capability#NUMERIC_READABLE}
	 * and {@link Double#NaN} otherwise
	 */
	double getNumeric(int index);

	/**
	 * Returns the category index value of the this row at the given index if the entry is part of a
	 * {@link Column.Category#CATEGORICAL} column. Returns {@code 0} in case of a {@link Column.Category#OBJECT} column
	 * and {@code (int) getNumeric(index)} in case of a {@link Column.Category#NUMERIC} column.
	 * This method is well-defined for indices zero (including) to {@link #width()} (excluding).
	 * <p>This method does not perform any range checks.
	 *
	 * @param index
	 * 		the index
	 * @return the category value index of the row at the given index for {@link Column.Category#CATEGORICAL} entries
	 */
	int getIndex(int index);

	/**
	 * Returns the object value of the this row at the given index. Returns {@code null} in case there is
	 * no object value at this index.
	 * This method is well-defined for indices zero (including) to {@link #width()} (excluding).
	 * <p>This method does not perform any range checks.
	 *
	 * @param index
	 * 		the index
	 * @return the value of the row at the given index if the value is {@link Column.Capability#OBJECT_READABLE}
	 * and {@code null} otherwise
	 */
	Object getObject(int index);

	/**
	 * Returns the object value of the this row at the given index. Returns {@code null} in case there is
	 * no object value at this index.
	 * This method is well-defined for indices zero (including) to {@link #width()} (excluding).
	 * <p>This method does not perform any range checks.
	 *
	 * @param index
	 * 		the index
	 * @param type
	 * 		the expected return type
	 * @param <T>
	 * 		the expected return type
	 * @return the value of the row at the given index if the value is {@link Column.Capability#OBJECT_READABLE}
	 * and {@code null} otherwise
	 */
	<T> T getObject(int index, Class<T> type);

	/**
	 * @return the number of values in the row
	 */
	int width();

	/**
	 * Returns the position of the row in the table (0-based).
	 *
	 * @return the row position
	 */
	int position();
}

