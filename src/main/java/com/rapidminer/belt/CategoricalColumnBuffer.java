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

import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Temporary random access buffer that can be used to define a {@link CategoricalColumn}.
 *
 * @author Gisa Meier
 */
public interface CategoricalColumnBuffer<T> {

	/**
	 * @return the size of the buffer
	 */
	int size();

	/**
	 * Returns the number of different values that were fed into this buffer. {@code null} values are not counted.
	 *
	 * @return the number of different values
	 */
	int differentValues();


	/**
	 * @return the category index format of the buffer determining the maximal number of categories
	 */
	Format indexFormat();

	/**
	 * Returns a column of the given type using the buffer's data. The buffer becomes read-only. In contrast to
	 * constructing a new buffer from a column this method does not copy the data.
	 *
	 * @param type
	 * 		the column type
	 * @return the categorical column
	 */
	CategoricalColumn<T> toColumn(ColumnType<T> type);

}