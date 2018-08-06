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
 * Buffered reader for {@link Column}s of with underlying int categories.
 *
 * @author Michael Knopf, Gisa Meier
 */
public final class CategoricalColumnReader {

	/**
	 * The category for missing value.
	 */
	public static final int MISSING_CATEGORY = 0;

	/**
	 * Placeholder buffer used before initialization.
	 */
	private static final int[] PLACEHOLDER_BUFFER = new int[0];

	private final Column column;
	private final int columnLength;
	private int bufferSize;

	private int[] buffer;
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
	 *            the column to read, must be of column category {@link Column.Category#CATEGORICAL}
	 * @throws NullPointerException
	 *             if the source column is {@code null}
	 */
	public CategoricalColumnReader(Column src) {
		this(src, ColumnReader.DEFAULT_BUFFER_SIZE, src.size());
	}

	/**
	 * Creates a new column reader.
	 *
	 * @param src
	 *            the column to read, must be of column category {@link Column.Category#CATEGORICAL}
	 * @param length
	 *            the number of elements allowed to be read from the start of the column
	 * @throws NullPointerException
	 *             if the source column is {@code null}
	 */
	public CategoricalColumnReader(Column src, int length) {
		this(src, ColumnReader.DEFAULT_BUFFER_SIZE, length);
	}

	/**
	 * Creates a new column reader.
	 *
	 * @param src
	 *            the column to read, must be of column category {@link Column.Category#CATEGORICAL}
	 * @param bufferSizeHint
	 *            a suggested buffer size
	 * @param length
	 *            the number of elements allowed to be read from the start of the column
	 * @throws NullPointerException
	 *             if the source column is {@code null}
	 */
	CategoricalColumnReader(Column src, int bufferSizeHint, int length) {
		// do not use Objects#requireNonNull(...) to prevent additional method call
		if (src == null) {
			throw new NullPointerException("Column must not be null");
		}
		column = src;
		this.columnLength = length;
		bufferSize = bufferSizeHint;
		if (length < bufferSizeHint) {
			bufferSize = length;
		}
		if (bufferSize < ColumnReader.MIN_BUFFER_SIZE) {
			bufferSize = ColumnReader.MIN_BUFFER_SIZE;
		}

		buffer = PLACEHOLDER_BUFFER;
		bufferRowIndex = 0;
		bufferOffset = 0;
	}

	/**
	 * @return the next column value
	 */
	public int read() {
		if (bufferRowIndex >= bufferLength) {
			if (buffer == PLACEHOLDER_BUFFER) {
				buffer = new int[bufferSize];
				bufferLength = bufferSize;
			} else {
				bufferOffset += bufferSize;
			}
			column.fill(buffer, bufferOffset);
			bufferRowIndex = 0;
		}
		return buffer[bufferRowIndex++];
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
	 * Returns current position in the column (0-based). Returns {@link Row#BEFORE_FIRST} if the reader
	 * is before the first position, i.e., {@link #read()} has not been called.
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
		if (position < Row.BEFORE_FIRST) {
			throw new IndexOutOfBoundsException("Position must not be smaller than -1");
		}
		if (position + 1 - bufferOffset < bufferLength && position - bufferOffset >= Row.BEFORE_FIRST) {
			bufferRowIndex = position + 1 - bufferOffset;
		} else {
			bufferOffset = position + 1 - bufferLength;
			bufferRowIndex = bufferLength;
		}
	}

	@Override
	public String toString() {
		return "Categorical column reader (" + columnLength + ")" + "\n" + "Position: " + position();
	}

}
