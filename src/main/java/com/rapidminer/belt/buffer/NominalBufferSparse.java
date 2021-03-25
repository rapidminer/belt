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

package com.rapidminer.belt.buffer;

import java.util.List;
import java.util.Objects;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Temporary random access buffer that can be used to define a {@link CategoricalColumn}. The buffer is write only. The
 * {@link #setNext(String)} method can be used to set the next free index in the buffer to the given value. The {@link
 * #setNext(int, String)} method can be used to set a specific position to the given value. In this case all values at
 * smaller indices (that have not already been set) will be set to the buffer's default value. This is more efficient
 * than calling {@link #setNext(String)} for every default value. Please note that values, once they are set, cannot be
 * modified.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 * <p>
 * The buffer's efficiency will depend on the data's sparsity and number of different categories. The buffer will
 * usually be memory and time efficient for
 * <ul>
 * <li> {@code > 65535} categories (32 bit data) and a sparsity of {@code >= 50%}.
 * <li> {@code > 255} and {@code <= 65535} categories (16 bit data) and a sparsity of {@code >= 66%}.
 * <li> {@code <= 255} categories (8 bit data) and a sparsity of {@code >= 80%}.
 * </ul>
 * <p> The sparse buffer comes with a constant memory overhead so that it is
 * recommended not to use it for very small data ({@code <1024} data points).
 *
 * @author Kevin Majchrzak
 * @see Buffers
 */
public abstract class NominalBufferSparse {

	protected final ColumnType<String> type;

	protected NominalBufferSparse(ColumnType<String> type) {
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		this.type = type;
	}

	/**
	 * Sets the value at the next index to the given value.
	 *
	 * @param value
	 * 		the value to set.
	 * @throws IndexOutOfBoundsException
	 * 		If attempting to add more than {@link #size()} values.
	 * @throws IllegalStateException
	 * 		If the buffer is frozen (see {@link #toColumn()}).
	 * @throws IllegalArgumentException
	 * 		If attempting to add more than the maximum number of different categories determined by the buffer's {@link
	 *        #indexFormat()}.
	 */
	public abstract void setNext(String value);

	/**
	 * Sets the value at the given index to the given value. All indices {@code <=} the given index that have not been set so
	 * far will be set to the default value. Please note that you cannot modify an index smaller than or equal to the
	 * given index after calling this method. Trying to access an index {@code <=} the largest index that has already
	 * been set will lead to an {@link IllegalArgumentException}.
	 *
	 * @param index
	 * 		an index inside of the buffers bounds (0 inclusive to {@link #size()} exclusive) at which to set the value.
	 * @param value
	 * 		the value to set
	 * @throws IllegalArgumentException
	 * 		if index is {@code <=} the largest index that has already been set
	 * 		<p> or if attempting to add more than the maximum number of different categories determined by the buffer's
	 *        {@link #indexFormat()}.
	 * @throws IndexOutOfBoundsException
	 * 		if the specified index is outside of the buffer's bounds.
	 * @throws IllegalStateException
	 * 		if the buffer is read-only (see {@link #toColumn()}).
	 */
	public abstract void setNext(int index, String value);


	/**
	 * Tries to set the data at the given index to the given value. Returns {@code true} if it worked and {@code false}
	 * if the buffer format cannot hold any more different values. All indices {@code <=} the given index that have not been
	 * set so far will be set to the default value. Please note that you cannot modify an index smaller than or equal to
	 * the given index after calling this method. Trying to access an index {@code <=} the largest index that has
	 * already been set will lead to an {@link IllegalArgumentException}.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param value
	 * 		the value to set
	 * @return {@code false} if the buffer contained already to many different values to take this value, {@code true}
	 * otherwise
	 * @throws IllegalStateException
	 * 		if the buffer is read-only (see {@link #toColumn()}).
	 * @throws IllegalArgumentException
	 * 		if index is {@code <=} the largest index that has already been set
	 * 		<p> or if attempting to add more than the maximum number of different categories determined by the buffer's
	 *        {@link #indexFormat()}.
	 * @throws IndexOutOfBoundsException
	 * 		if the specified index is outside of the buffer's bounds.
	 */
	public abstract boolean setNextSave(int index, String value);


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
	 * Returns the buffer's overall size (not the number of elements currently inside the buffer).
	 *
	 * @return the buffers size.
	 */
	public abstract int size();

	/**
	 * @return the mapping
	 */
	abstract List<String> getMapping();

	/**
	 * Returns a column of the given type using the buffer's data. The buffer becomes read-only. In contrast to
	 * constructing a new buffer from a column this method does not copy the data.
	 *
	 * @return the categorical column
	 */
	public abstract CategoricalColumn toColumn();

	/**
	 * Creates a boolean column with the given positive value if the dictionary has at most two values. The positive
	 * value must be either one of the dictionary values, making the other value, if it exists, negative. Or in case of
	 * a dictionary with only one value the positive value can be {@code null} making the only value negative.
	 *
	 * @param positiveValue
	 * 		the positive value or {@code null}
	 * @return a categorical column with a boolean {@link Dictionary}
	 * @throws IllegalArgumentException
	 * 		if the given positive value is not one of the values, if there are more than two values in the dictionary or if
	 * 		positive value is {@code null} in case there are two values in the dictionary
	 */
	public abstract CategoricalColumn toBooleanColumn(String positiveValue);

	/**
	 * Return the number of non-default values for testing.
	 */
	abstract int getNumberOfNonDefaults();

}