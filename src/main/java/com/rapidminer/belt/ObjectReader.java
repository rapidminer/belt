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
 * Buffered reader for {@link Column}s with capability {@link Column.Capability#OBJECT_READABLE}.
 *
 * @author Michael Knopf, Gisa Meier
 * @see Readers
 */
public final class ObjectReader<T> {

	/**
	 * Placeholder buffer used before initialization.
	 */
	private static final Object[] PLACEHOLDER_BUFFER = new Object[0];

	private final Column column;
	private final int columnLength;
	private int bufferSize;

	private Object[] buffer;
	private int bufferOffset;
	private int bufferRowIndex;

	/**
	 * same as buffer.length but stored in extra variable for performance reasons
	 */
	private int bufferLength = 0;

	/**
	 * Creates a new column reader.
	 *
	 * @param src
	 * 		the column to read, must be have the capability {@link Column.Capability#OBJECT_READABLE}
	 * @param type
	 * 		the desired return type, must be a super type of the element type of the column
	 * @param bufferSizeHint
	 * 		a suggested buffer size
	 * @param length
	 * 		the number of elements allowed to be read from the start of the column
	 * @throws NullPointerException
	 * 		if the source column is {@code null}
	 */
	ObjectReader(Column src, Class<T> type, int bufferSizeHint, int length) {
		// do not use Objects#requireNonNull(...) to prevent additional method call
		if (src == null) {
			throw new NullPointerException("Column must not be null");
		}
		if (!type.isAssignableFrom(src.type().elementType())) {
			throw new IllegalArgumentException("Element type is not super type of " + src.type().elementType());
		}
		column = src;
		this.columnLength = length;
		bufferSize = bufferSizeHint;
		if (length < bufferSizeHint) {
			bufferSize = length;
		}
		if (bufferSize < NumericReader.MIN_BUFFER_SIZE) {
			bufferSize = NumericReader.MIN_BUFFER_SIZE;
		}

		buffer = PLACEHOLDER_BUFFER;
		bufferRowIndex = 0;
		bufferOffset = 0;
	}

	/**
	 * @return the next column value
	 */
	public T read() {
		if (bufferRowIndex >= bufferLength) {
			if (buffer == PLACEHOLDER_BUFFER) {
				buffer = new Object[bufferSize];
				bufferLength = bufferSize;
			} else {
				bufferOffset += bufferSize;
			}
			column.fill(buffer, bufferOffset);
			bufferRowIndex = 0;
		}
		// the cast is safe because of the check in the constructor
		@SuppressWarnings("unchecked")
		T result = (T) buffer[bufferRowIndex++];
		return result;
	}

	/**
	 * @return the number of the remaining values
	 */
	public int remaining() {
		return columnLength - (bufferOffset + bufferRowIndex);
	}

	/**
	 * @return {@code true} iff further values can be read
	 */
	public boolean hasRemaining() {
		return bufferOffset + bufferRowIndex < columnLength;
	}

	/**
	 * Returns current position in the column (0-based). Returns {@link Readers#BEFORE_FIRST_ROW} if the reader is
	 * before the first position, i.e., {@link #read()} has not been called.
	 *
	 * @return the row position
	 */
	public int position() {
		return bufferRowIndex + bufferOffset - 1;
	}

	/**
	 * Sets the reader position to the given row but without loading the data. The next {@link #read()} returns the data
	 * at row {@code position+1}. Note that this can invalidate the buffer so that a new buffer is filled for the next
	 * read.
	 *
	 * @param position
	 * 		the position where to move
	 * @throws IndexOutOfBoundsException
	 * 		if the given position is smaller than -1
	 */
	public void setPosition(int position) {
		if (position < Readers.BEFORE_FIRST_ROW) {
			throw new IndexOutOfBoundsException("Position must not be smaller than -1");
		}
		if (position + 1 - bufferOffset < bufferLength && position - bufferOffset >= Readers.BEFORE_FIRST_ROW) {
			bufferRowIndex = position + 1 - bufferOffset;
		} else {
			bufferOffset = position + 1 - bufferLength;
			bufferRowIndex = bufferLength;
		}
	}

	@Override
	public String toString() {
		return "Object column reader (" + columnLength + ")" + "\n" + "Position: " + position();
	}

}