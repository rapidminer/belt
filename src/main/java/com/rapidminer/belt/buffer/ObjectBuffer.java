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

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;


/**
 * Temporary random access buffer that can be used to define a {@link Column} of category {@link Column.Category#OBJECT}.
 *
 * @author Gisa Meier
 * @see Buffers
 */
public final class ObjectBuffer<T> {

	private final Object[] data;
	private final ColumnType<T> type;
	private boolean frozen = false;


	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	ObjectBuffer(ColumnType<T> type, int length) {
		if (type.category() != Category.OBJECT || type.id() == TypeId.TIME || type.id() == TypeId.DATE_TIME) {
			throw new IllegalArgumentException("Column type must be non-date-time and of category object");
		}
		data = new Object[length];
		this.type = type;
	}

	/**
	 * Copies the data of the given column into a buffer. Throws an {@link UnsupportedOperationException} if the column
	 * category is not {@link Column.Capability#OBJECT_READABLE}.
	 *
	 * @param column
	 * 		the column to convert to a buffer
	 * @param type
	 * 		the desired type of the buffer, must be a super type of the column type
	 */
	ObjectBuffer(ColumnType<T> type, Column column) {
		if (type.category() != Category.OBJECT || type.id() == TypeId.TIME || type.id() == TypeId.DATE_TIME) {
			throw new IllegalArgumentException("Column type must be non-date-time and of category object");
		}
		if (!type.elementType().isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException("Element type is not super type of " + column.type().elementType());
		}
		this.type = type;
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
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
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
		return BufferPrinter.print(this);
	}

	public Column toColumn() {
		freeze();
		return ColumnAccessor.get().newObjectColumn(type, data);
	}
}