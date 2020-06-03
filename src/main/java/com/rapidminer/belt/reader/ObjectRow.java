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

package com.rapidminer.belt.reader;

import com.rapidminer.belt.table.Table;


/**
 * A row of a {@link Table} that allows to read entries as objects.
 *
 * @param <T>
 * 		the type of all objects in this row
 * @author Gisa Meier
 * @see ObjectRowReader
 */
public interface ObjectRow<T> {

	/**
	 * Returns the value of the this row at the given index. This method is well-defined for indices zero (including) to
	 * {@link #width()} (excluding). <p>This method does not perform any range checks.
	 *
	 * @param index
	 * 		the index
	 * @return the value of the row at the given index
	 */
	T get(int index);

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

