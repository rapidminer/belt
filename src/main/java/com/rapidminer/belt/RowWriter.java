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

import java.util.List;

import com.rapidminer.belt.Column.TypeId;


/**
 * Writer to create a {@link Table} from row-wise data.
 *
 * @author Gisa Meier
 */
public final class RowWriter {

	/**
	 * Placeholder buffer used before initialization.
	 */
	private static final double[] PLACEHOLDER_BUFFER = new double[0];

	/**
	 * The height of the internal buffer.
	 */
	private static final int BUFFER_HEIGHT = ColumnReader.SMALL_BUFFER_SIZE;

	private final GrowingColumnBuffer[] columns;
	private final String[] columnLabels;
	private final int bufferWidth;

	private double[] buffer = PLACEHOLDER_BUFFER;
	private int bufferOffset = -BUFFER_HEIGHT;
	private int bufferRowIndex = 0;
	private int rowIndex = -1;

	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size 0.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 */
	RowWriter(List<String> columnLabels) {
		int numberOfColumns = columnLabels.size();
		columns = new GrowingRealBuffer[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = new GrowingRealBuffer();
		}
		bufferWidth = numberOfColumns;
	}

	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size 0.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param types
	 * 		the types of the columns to construct
	 */
	RowWriter(List<String> columnLabels, List<TypeId> types) {
		int numberOfColumns = columnLabels.size();
		columns = new GrowingColumnBuffer[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = getBufferForType(types.get(i));
		}
		bufferWidth = numberOfColumns;
	}

	/**
	 * Returns an integer or real growing column buffer without set length.
	 */
	private GrowingColumnBuffer getBufferForType(TypeId type) {
		if (type == TypeId.INTEGER) {
			return new GrowingIntegerBuffer();
		}
		return new GrowingRealBuffer();
	}

	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size expectedRows.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param expectedRows
	 * 		the expected number of rows
	 */
	RowWriter(List<String> columnLabels, int expectedRows) {
		int numberOfColumns = columnLabels.size();
		columns = new GrowingRealBuffer[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = new GrowingRealBuffer(expectedRows);
		}
		bufferWidth = numberOfColumns;
	}


	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size expectedRows.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param types
	 * 		the types of the columns to construct
	 * @param expectedRows
	 * 		the expected number of rows
	 */
	RowWriter(List<String> columnLabels, List<Column.TypeId> types, int expectedRows) {
		int numberOfColumns = columnLabels.size();
		columns = new GrowingColumnBuffer[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = getBufferForType(types.get(i), expectedRows);
		}
		bufferWidth = numberOfColumns;
	}

	/**
	 * Returns an integer or real growing column buffer with the given length.
	 */
	private GrowingColumnBuffer getBufferForType(TypeId type, int expectedRows) {
		if (type == TypeId.INTEGER) {
			return new GrowingIntegerBuffer(expectedRows);
		}
		return new GrowingRealBuffer(expectedRows);
	}

	/**
	 * Moves the reader to the next row.
	 */
	public void move() {
		if (bufferRowIndex >= buffer.length - bufferWidth) {
			if (buffer == PLACEHOLDER_BUFFER) {
				buffer = new double[bufferWidth * BUFFER_HEIGHT];
			} else {
				writeBuffer();
			}
			bufferOffset += BUFFER_HEIGHT;
			bufferRowIndex = 0;
		} else {
			bufferRowIndex += bufferWidth;
		}
		rowIndex++;
	}


	/**
	 * Sets the value at the given column index.  This method is well-defined for indices zero (including) to {@link
	 * #width()} (excluding).
	 *
	 * <p>This method does not perform any range checks. Nor does it ever advance the current row. Before invoking this
	 * method, you will have to call {@link #move()} at least once.
	 *
	 * @param index
	 * 		the column index
	 * @param value
	 * 		the value to set
	 */
	public void set(int index, double value) {
		buffer[bufferRowIndex + index] = value;
	}

	/**
	 * Creates a {@link Table} from the data inside the row writer. The row writer cannot be changed afterwards.
	 *
	 * @return a new table
	 * @throws IllegalArgumentException
	 * 		if the stored column labels are not valid to create a table
	 */
	public Table create() {
		writeBuffer();
		Column[] finalColumns = new Column[columns.length];
		for (int i = 0; i < columns.length; i++) {
			columns[i].freeze();
			finalColumns[i] = new DoubleArrayColumn(columns[i].type(), columns[i].getData());
		}
		return new Table(finalColumns, columnLabels);
	}

	/**
	 * Writes the writer buffer to the column buffers.
	 */
	private void writeBuffer() {
		for (int i = 0; i < columns.length; i++) {
			columns[i].fill(buffer, bufferOffset, i, bufferWidth, rowIndex + 1);
		}
	}

	/**
	 * @return the number of values per row
	 */
	public int width() {
		return columns.length;
	}


	@Override
	public String toString() {
		return "Row writer (" + (rowIndex + 1) + "x" + bufferWidth + ")";
	}
}
