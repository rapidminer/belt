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

import com.rapidminer.belt.Column.TypeId;

/**
 * Temporary random access buffer that can be used to define a {@link Column}.
 *
 * @author Gisa Schaefer
 * @see #toColumn()
 */
public abstract class ColumnBuffer {

	/**
	 * Retrieves the value at the given index.
	 *
	 * <p>Set operations are not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index to look up
	 * @return the value at the index
	 */
	public abstract double get(int index);

	/**
	 * Sets the data at the given index to the given value.
	 *
	 * <p>This method is not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param value
	 * 		the value to set
	 * @throws IllegalStateException
	 * 		if called after the buffer was used to create a {@link Column}
	 */
	public abstract void set(int index, double value);

	/**
	 * @return the size of the buffer
	 */
	public abstract int size();


	/**
	 * Returns the type id of the buffer (see {@link ColumnType#id()}).
	 *
	 * @return the type id of the buffer
	 */
	public abstract TypeId type();


	/**
	 * Returns a column constructed from this buffer. The buffer becomes read-only. In contrast to constructing a new
	 * buffer from a column this does not copy the data.
	 *
	 * @return a column with the data from this buffer
	 */
	public Column toColumn() {
		freeze();
		return new DoubleArrayColumn(type(), getData());
	}

	/**
	 * Freezes the current state of the buffer. It becomes read-only. Should be called when a buffer is used to create a
	 * {@link Column}.
	 */
	protected abstract void freeze();

	/**
	 * @return the underlying data array
	 */
	protected abstract double[] getData();
}
