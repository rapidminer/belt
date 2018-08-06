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

import java.util.Objects;
import com.rapidminer.belt.Column.Category;
import com.rapidminer.belt.Column.TypeId;


/**
 * Temporary random access buffer that can be used to define a {@link CategoricalColumn}. The current implementation has
 * a naive version of a mapping and does not compactify the mapping if values are not used anymore.
 */
public class FreeColumnBuffer<T> {

	/**
	 * Error message when trying to change frozen buffer
	 */
	private static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";

	private final Object[] data;
	private boolean frozen = false;


	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public FreeColumnBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		data = new Object[length];
	}


	/**
	 * Copies the data of the given column into a buffer. Throws an {@link UnsupportedOperationException} if the column
	 * category is not {@link Column.Capability#OBJECT_READABLE}.
	 *
	 * @param elementType
	 * 		the desired id of the buffer, must be a super id of the column id
	 * @param column
	 * 		the column to convert to a buffer
	 * @param elementType
	 * 		the desired type of the buffer, must be a super type of the column type
	 */
	public FreeColumnBuffer(Class<T> elementType, Column column) {
		if (!elementType.isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException("Element id is not super id of " + column.type().elementType());
		}
		data = new Object[column.size()];
		column.fill(data, 0);
	}


	/**
	 * Retrieves the value at the given index.
	 *
	 * @param index
	 * 		the index to look up
	 * @return the value at the index
	 */
	public T get(int index) {
		// the cast is safe because only safe data can come in
		@SuppressWarnings("unchecked")
		T result = (T) data[index];
		return result;
	}

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
	public void set(int index, T value) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		data[index] = value;
	}


	/**
	 * @return the size of the buffer
	 */
	public int size() {
		return data.length;
	}


	/**
	 * Freezes the current state of the buffer. It becomes read-only. Should be called when a buffer is used to create a
	 * {@link Column}.
	 */
	protected void freeze() {
		frozen = true;
	}


	/**
	 * @return the underlying data array
	 */
	protected Object[] getData() {
		return data;
	}

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}

	public FreeColumn<T> toColumn(ColumnType<T> type) {
		Objects.requireNonNull(type, "TypeId must not be null");
		freeze();
		if (type.id() != TypeId.CUSTOM || type.category() != Category.FREE) {
			throw new IllegalArgumentException("Column type must be custom and of category free");
		}
		return new SimpleFreeColumn<>(type, data);
	}
}