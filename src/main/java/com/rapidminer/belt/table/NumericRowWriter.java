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

package com.rapidminer.belt.table;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.reader.NumericReader;


/**
 * Writer to create a {@link Table} from row-wise numeric data.
 *
 * @author Gisa Meier
 * @see Writers
 */
public final class NumericRowWriter {

	/**
	 * Minimal size for non-empty buffers.
	 */
	static final int MIN_NON_EMPTY_SIZE = 8;

	/**
	 * Placeholder buffer used before initialization.
	 */
	private static final double[] PLACEHOLDER_BUFFER = new double[0];

	/**
	 * The height of the internal buffer.
	 */
	private static final int BUFFER_HEIGHT = NumericReader.SMALL_BUFFER_SIZE;

	private final NumericColumnWriter[] columns;
	private final String[] columnLabels;
	private final int bufferWidth;
	private final boolean initialize;

	private double[] buffer = PLACEHOLDER_BUFFER;
	private int bufferOffset = -BUFFER_HEIGHT;
	private int bufferRowIndex = 0;
	private int rowIndex = -1;


	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size 0.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@link false} values
	 * 		for	indices that are not explicitly set are undetermined
	 */
	NumericRowWriter(List<String> columnLabels, boolean initialize) {
		int numberOfColumns = columnLabels.size();
		columns = new NumericColumnWriter[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = new RealColumnWriter();
		}
		bufferWidth = numberOfColumns;
		this.initialize = initialize;
	}

	/**
	 * Creates a row writer to create a numeric {@link Table} with the given column labels and starting row size 0.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param types
	 * 		the types of the columns to construct, either {@link ColumnTypes#INTEGER} or {@link ColumnTypes#REAL}
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@link false} values
	 * 		for	indices that are not explicitly set are undetermined
	 */
	NumericRowWriter(List<String> columnLabels, List<ColumnType<Void>> types, boolean initialize) {
		int numberOfColumns = columnLabels.size();
		columns = new NumericColumnWriter[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = getBufferForType(Objects.requireNonNull(types.get(i), "column type must not be null").id());
		}
		bufferWidth = numberOfColumns;
		this.initialize = initialize;
	}

	/**
	 * Creates a row writer to create a numeric {@link Table} with the given column labels and starting row size
	 * expectedRows.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param expectedRows
	 * 		the expected number of rows
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@link false} values
	 * 		for	indices that are not explicitly set are undetermined
	 */
	NumericRowWriter(List<String> columnLabels, int expectedRows, boolean initialize) {
		int numberOfColumns = columnLabels.size();
		columns = new NumericColumnWriter[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = new RealColumnWriter(expectedRows);
		}
		bufferWidth = numberOfColumns;
		this.initialize = initialize;
	}

	/**
	 * Creates a row writer to create a numeric {@link Table} with the given column labels and starting row size
	 * expectedRows.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param types
	 * 		the types of the columns to construct, either {@link ColumnTypes#INTEGER} or {@link ColumnTypes#REAL}
	 * @param expectedRows
	 * 		the expected number of rows
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@link false} values
	 * 		for	indices that are not explicitly set are undetermined
	 */
	NumericRowWriter(List<String> columnLabels, List<ColumnType<Void>> types, int expectedRows, boolean initialize) {
		int numberOfColumns = columnLabels.size();
		columns = new NumericColumnWriter[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			columns[i] = getBufferForType(Objects.requireNonNull(types.get(i), "column type must not be null").id(),
					expectedRows);
		}
		bufferWidth = numberOfColumns;
		this.initialize = initialize;
	}

	/**
	 * Returns an integer or real growing column buffer without set length.
	 */
	private NumericColumnWriter getBufferForType(TypeId type) {
		if (type == TypeId.INTEGER) {
			return new IntegerColumnWriter();
		}else if(type == TypeId.REAL) {
			return new RealColumnWriter();
		}
		throw new IllegalArgumentException("Type not supported for numeric row writer.");
	}


	/**
	 * Returns an integer or real growing column buffer with the given length.
	 */
	private NumericColumnWriter getBufferForType(TypeId type, int expectedRows) {
		if (type == TypeId.INTEGER) {
			return new IntegerColumnWriter(expectedRows);
		} else if (type == TypeId.REAL) {
			return new RealColumnWriter(expectedRows);
		}
		throw new IllegalArgumentException("Type not supported for numeric row writer.");
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
			if (initialize) {
				Arrays.fill(buffer, Double.NaN);
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
			finalColumns[i] = columns[i].toColumn();
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
