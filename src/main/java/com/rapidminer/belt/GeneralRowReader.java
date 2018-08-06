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

/**
 * Buffered row-oriented reader for multiple {@link Column}s of different capabilities.
 *
 * <p>In its initial state the reader does not point at a valid row. Thus, one must invoke {@link #move()} at least once
 * before reading values using {@link #getNumeric(int)}, {@link #getIndex(int)} or {@link #getObject(int)}. Example:
 *
 * <pre>{@code
 * GeneralRowReader reader = ...;
 * while(reader.hasRemaining()) {
 *     reader.move();
 *     for (int i = 0; i < reader.width(); i++) {
 *         ... = reader.getNumeric(i);
 *     }
 * }
 * }</pre>
 *
 * <p>Given the column-oriented design of Belt, {@link RowReader}s do not perform as well as column readers.
 * Thus, whenever possible the use of {@link ColumnReader}s, {@link CategoricalColumnReader}s and
 * {@link ObjectColumnReader}s is preferred. Furthermore, a {@link RowReader}, {@link CategoricalRowReader} or a
 * {@link ObjectRowReader} performs better than a {@link GeneralRowReader} and should be preferred instead whenever
 * possible.
 *
 * @author Michael Knopf, Gisa Meier
 */
public final class GeneralRowReader implements GeneralRow {

	/**
	 * The desired elements in a buffer so that the size is not more than 256 KB
	 */
	private static final int BUFFER_ELEMENTS = 256 * 1024 / 8 / 2;

	/**
	 * Maximal number of rows in a buffer
	 */
	private static final int MAX_BUFFER_ROWS = ColumnReader.SMALL_BUFFER_SIZE;

	/**
	 * Minimal number of rows in a buffer
	 */
	private static final int MIN_BUFFER_ROWS = ColumnReader.MIN_BUFFER_SIZE;

	/**
	 * Placeholder numeric buffer used before initialization.
	 */
	private static final double[] PLACEHOLDER_BUFFER = new double[0];

	private final Column[] columns;
	private final boolean[] numericFillable;
	private final boolean[] objectFillable;
	private final int tableHeight;
	private final int bufferWidth;
	private final int bufferHeight;

	private double[] numericBuffer;
	private Object[] objectBuffer;
	private int bufferOffset;
	private int bufferRowIndex;
	private int rowIndex;

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link GeneralRowReader}s do not perform as well as
	 * column readers. Thus, whenever possible the use of {@link ColumnReader}s, {@link CategoricalColumnReader}s and
	 * {@link ObjectColumnReader}s is preferred. Furthermore, a {@link RowReader}, {@link CategoricalRowReader} or a
	 * {@link ObjectRowReader} performs better than a {@link GeneralRowReader} and should be preferred whenever
	 * possible.
	 *
	 * @param table
	 * 		the table to read
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public GeneralRowReader(Table table) {
		this(Objects.requireNonNull(table, "Table must not be null").getColumns(), BUFFER_ELEMENTS);
	}


	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link GeneralRowReader}s do not perform as well as
	 * column readers. Thus, whenever possible the use of {@link ColumnReader}s, {@link CategoricalColumnReader}s and
	 * {@link ObjectColumnReader}s is preferred. Furthermore, a {@link RowReader}, {@link CategoricalRowReader} or a
	 * {@link ObjectRowReader} performs better than a {@link GeneralRowReader} and should be preferred whenever
	 * possible.
	 *
	 * @param first
	 * 		the first column to read
	 * @param second
	 * 		the second column to read
	 * @param columns
	 * 		further columns to read
	 * @throws NullPointerException
	 * 		if the first or the second column are {@code null}
	 */
	public GeneralRowReader(Column first, Column second, Column... columns) {
		this(getColumnArray(first, second, columns), BUFFER_ELEMENTS);
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link GeneralRowReader}s do not perform as well as
	 * column readers. Thus, whenever possible the use of {@link ColumnReader}s, {@link CategoricalColumnReader}s and
	 * {@link ObjectColumnReader}s is preferred. Furthermore, a {@link RowReader}, {@link CategoricalRowReader} or a
	 * {@link ObjectRowReader} performs better than a {@link GeneralRowReader} and should be preferred whenever
	 * possible.
	 *
	 * @param src
	 * 		the columns to read
	 * @throws NullPointerException
	 * 		if the source columns array is {@code null}
	 */
	public GeneralRowReader(Column[] src) {
		this(src, BUFFER_ELEMENTS);
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link GeneralRowReader}s do not perform as well as
	 * column readers. Thus, whenever possible the use of {@link ColumnReader}s, {@link CategoricalColumnReader}s and
	 * {@link ObjectColumnReader}s is preferred. Furthermore, a {@link RowReader}, {@link CategoricalRowReader} or a
	 * {@link ObjectRowReader} performs better than a {@link GeneralRowReader} and should be preferred whenever
	 * possible.
	 *
	 * @param src
	 * 		the columns to read
	 * @param bufferElementsHint
	 * 		the desired number of elements in the buffer, might be ignored if it results in a value outside the range
	 * 		[{@link #MIN_BUFFER_ROWS}, {@link #MAX_BUFFER_ROWS}]
	 * @throws NullPointerException
	 * 		if the source columns array is {@code null}
	 */
	GeneralRowReader(Column[] src, int bufferElementsHint) {
		columns = Objects.requireNonNull(src, "The source columns array must not be null");
		bufferWidth = columns.length;
		tableHeight = bufferWidth > 0 ? columns[0].size() : 0;
		if (bufferWidth > 0) {
			// at least min buffer rows, at most max buffer rows or table height, otherwise rows to fit buffer elements
			bufferHeight = Math.min(Math.max(MIN_BUFFER_ROWS, bufferElementsHint / bufferWidth),
					Math.min(tableHeight, MAX_BUFFER_ROWS));
		} else {
			bufferHeight = 0;
		}
		numericBuffer = PLACEHOLDER_BUFFER;
		numericFillable = new boolean[bufferWidth];
		objectFillable = new boolean[bufferWidth];
		storeCapabilities();
		rowIndex = Row.BEFORE_FIRST;
		bufferRowIndex = 0;
		bufferOffset = -bufferHeight;
	}

	private void storeCapabilities() {
		int i = 0;
		for (Column column : columns) {
			if (column.hasCapability(Column.Capability.NUMERIC_READABLE)) {
				numericFillable[i] = true;
			}
			if (column.hasCapability(Column.Capability.OBJECT_READABLE)) {
				objectFillable[i] = true;
			}
			i++;
		}
	}

	/**
	 * Moves the reader to the next row.
	 */
	public void move() {
		if (bufferRowIndex >= numericBuffer.length - bufferWidth) {
			if (numericBuffer == PLACEHOLDER_BUFFER) {
				numericBuffer = new double[bufferWidth * bufferHeight];
				objectBuffer = new Object[numericBuffer.length];
			}
			bufferOffset += bufferHeight;
			for (int i = 0; i < columns.length; i++) {
				refillBuffer(i);
			}
			bufferRowIndex = 0;
		} else {
			bufferRowIndex += bufferWidth;
		}
		rowIndex++;
	}

	/**
	 * Refills the buffers if the column with the given index is capable of doing so or with {@link Double#NaN} or
	 * {@code null} otherwise.
	 */
	private void refillBuffer(int columnIndex) {
		if (numericFillable[columnIndex]) {
			columns[columnIndex].fill(numericBuffer, bufferOffset, columnIndex, bufferWidth);
		} else {
			for (int i = columnIndex; i < numericBuffer.length; i += bufferWidth) {
				numericBuffer[i] = Double.NaN;
			}
		}
		if (objectFillable[columnIndex]) {
			columns[columnIndex].fill(objectBuffer, bufferOffset, columnIndex, bufferWidth);
		} else {
			for (int i = columnIndex; i < objectBuffer.length; i += bufferWidth) {
				objectBuffer[i] = null;
			}
		}
	}


	/**
	 * @return the number of remaining rows
	 */
	public int remaining() {
		return tableHeight - rowIndex - 1;
	}

	/**
	 * @return {@code true} iff further rows can be read
	 */
	public boolean hasRemaining() {
		return rowIndex < tableHeight - 1;
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>This method does never advance the current row. Before invoking this
	 * method, you will have to call {@link #move()} at least once.
	 * @see #move()
	 */
	@Override
	public double getNumeric(int index) {
		return numericBuffer[bufferRowIndex + index];
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>This method does never advance the current row. Before invoking this
	 * method, you will have to call {@link #move()} at least once.
	 * @see #move()
	 */
	@Override
	public int getIndex(int index) {
		return (int) numericBuffer[bufferRowIndex + index];
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>This method does never advance the current row. Before invoking this
	 * method, you will have to call {@link #move()} at least once.
	 * @see #move()
	 */
	@Override
	public Object getObject(int index) {
		return objectBuffer[bufferRowIndex + index];
	}

	/**
	 * @return the number of values per row
	 */
	@Override
	public int width() {
		return bufferWidth;
	}

	/**
	 * {@inheritDoc} Returns {@link Row#BEFORE_FIRST} if the reader is before the first position, i.e., {@link #move()}
	 * has not been called.
	 */
	@Override
	public int position() {
		return rowIndex;
	}

	/**
	 * Sets the reader position to the given row but without loading the data. The next {@link #move()} loads the data
	 * and goes to row {@code position+1}. Note that this can invalidate the buffer so that a new buffer is filled for
	 * the next move.
	 *
	 * @param position
	 * 		the position where to move
	 * @throws IndexOutOfBoundsException
	 * 		if the given position smaller than -1
	 */
	public void setPosition(int position) {
		if (position + 1 < 0) {
			throw new IndexOutOfBoundsException("Position must not be smaller than -1");
		}
		if (position + 1 >= bufferOffset && position + 1 < bufferOffset + bufferHeight) {
			bufferRowIndex = (position + 1 - bufferOffset) * bufferWidth - bufferWidth;
		} else {
			bufferOffset = position + 1 - bufferHeight;
			bufferRowIndex = numericBuffer.length;
		}
		rowIndex = position;
	}

	@Override
	public String toString() {
		return "General Row reader (" + tableHeight + "x" + bufferWidth + ")" + "\n" + "Row position: " + position();
	}

	/**
	 * Creates one column array from the given input.
	 */
	private static Column[] getColumnArray(Column first, Column second, Column[] columns) {
		Objects.requireNonNull(first, "First column must not be null");
		Objects.requireNonNull(second, "Second Column must not be null");
		if (columns == null || columns.length == 0) {
			return new Column[]{first, second};
		} else {
			Column[] allColumns = new Column[columns.length + 2];
			System.arraycopy(columns, 0, allColumns, 2, columns.length);
			allColumns[0] = first;
			allColumns[1] = second;
			return allColumns;
		}
	}
}
