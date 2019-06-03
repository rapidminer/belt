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

package com.rapidminer.belt.buffer;

import java.util.List;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Temporary random access buffer that can be used to define a {@link CategoricalColumn}.
 *
 * @author Gisa Meier
 * @see Buffers
 */
public abstract class CategoricalBuffer<T> {

	/**
	 * Retrieves the value at the given index. This method is thread-safe and can be used alongside invocations of
	 * {@link #set} and {@link #setSave}. However, this method might not return the latest value set on another thread.
	 *
	 * @param index
	 * 		the index to look up
	 * @return the value at the index
	 */
	public abstract T get(int index);

	/**
	 * Sets the data at the given index to the given value.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param value
	 * 		the value to set
	 * @throws IllegalStateException
	 * 		if called after the buffer was used to create a {@link Column}
	 */
	public abstract void set(int index, T value);


	/**
	 * Tries to set the data at the given index to the given value. Returns {@code true} if it worked and {@code false}
	 * if the buffer format cannot hold any more different values.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param value
	 * 		the value to set
	 * @return {@code false} if the buffer contained already to many different values to take this value, {@code true}
	 * otherwise
	 * @throws IllegalStateException
	 * 		if called after the buffer was used to create a {@link Column}
	 */
	public abstract boolean setSave(int index, T value);


	/**
	 * Returns the number of different values that were fed into this buffer. {@code null} values are not counted.
	 *
	 * @return the number of different values
	 */
	public abstract int differentValues();

	/**
	 * @return the category index format of the buffer determining the maximal number of categories
	 */
	public abstract Format indexFormat();

	/**
	 * @return the size of the buffer
	 */
	public abstract int size();

	/**
	 * @return the mapping
	 */
	abstract List<T> getMapping();

	/**
	 * Freezes the current state of the buffer. It becomes read-only. Should be called when a buffer is used to create a
	 * {@link Column}.
	 */
	abstract void freeze();


	@Override
	public String toString() {
		return BufferPrinter.print(this);
	}

	/**
	 * Returns a column of the given type using the buffer's data. The buffer becomes read-only. In contrast to
	 * constructing a new buffer from a column this method does not copy the data.
	 *
	 * @param type
	 * 		the column type
	 * @return the categorical column
	 */
	public abstract CategoricalColumn<T> toColumn(ColumnType<T> type);

	/**
	 * Creates a boolean column with the given positive value if the dictionary has at most two values. The positive
	 * value must be either one of the dictionary values, making the other value, if it exists, negative. Or in case
	 * of a dictionary with only one value the positive value can be {@code null} making the only value negative.
	 *
	 * @param type
	 * 		the column type of the column
	 * @param positiveValue
	 * 		the positive value or {@code null}
	 * @return a categorical column with a boolean {@link Dictionary}
	 * @throws IllegalArgumentException
	 * 		if the given positive value is not one of the values, if there are more than two values in the dictionary
	 * 		or if positive value is {@code null} in case there are two values in the dictionary
	 */
	public abstract CategoricalColumn<T> toBooleanColumn(ColumnType<T> type, T positiveValue);

}