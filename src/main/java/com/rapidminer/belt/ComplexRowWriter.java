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
import java.util.Objects;


/**
 * Writer to create a {@link Table} from row-wise non-numeric data.
 *
 * @author Gisa Meier
 */
final class ComplexRowWriter {

	/**
	 * Placeholder buffer used before initialization.
	 */
	private static final Object[] PLACEHOLDER_BUFFER = new Object[0];

	/**
	 * The height of the internal buffer.
	 */
	private static final int BUFFER_HEIGHT = NumericReader.SMALL_BUFFER_SIZE;

	private final ComplexWriter[] columns;
	private final Class<?>[] classes;
	private final String[] columnLabels;
	private final int bufferWidth;


	private Object[] buffer = PLACEHOLDER_BUFFER;
	private int bufferOffset = -BUFFER_HEIGHT;
	private int bufferRowIndex = 0;
	private int rowIndex = -1;


	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size 0.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param types
	 * 		the types of the columns to construct
	 */
	ComplexRowWriter(List<String> columnLabels, List<ColumnType<?>> types) {
		int numberOfColumns = columnLabels.size();
		columns = new ComplexWriter[numberOfColumns];
		classes = new Class<?>[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			ColumnType<?> type = Objects.requireNonNull(types.get(i), "column type must not be null");
			columns[i] = getObjectBufferForType(type);
			classes[i] = type.elementType();
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
	ComplexRowWriter(List<String> columnLabels, List<ColumnType<?>> types, int expectedRows) {
		int numberOfColumns = columnLabels.size();
		columns = new ComplexWriter[numberOfColumns];
		classes = new Class<?>[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			ColumnType<?> type = Objects.requireNonNull(types.get(i), "column type must not be null");
			columns[i] = getObjectBufferForType(type, expectedRows);
			classes[i] = type.elementType();
		}
		bufferWidth = numberOfColumns;
	}

	private ComplexWriter getObjectBufferForType(ColumnType<?> columnType) {
		if (ColumnTypes.DATETIME.equals(columnType)) {
			return new NanosecondsDateTimeWriter();
		} else if (ColumnTypes.TIME.equals(columnType)) {
			return new TimeColumnWriter();
		} else if (columnType.category() == Column.Category.CATEGORICAL) {
			return new Int32CategoricalWriter<>(columnType);
		} else if (columnType.category() == Column.Category.OBJECT) {
			return new ObjectWriter<>(columnType);
		} else {
			throw new IllegalArgumentException("Numeric types not supported for object row writer.");
		}
	}

	private ComplexWriter getObjectBufferForType(ColumnType<?> columnType, int length) {
		if (ColumnTypes.DATETIME.equals(columnType)) {
			return new NanosecondsDateTimeWriter(length);
		} else if (ColumnTypes.TIME.equals(columnType)) {
			return new TimeColumnWriter(length);
		} else if (columnType.category() == Column.Category.CATEGORICAL) {
			return new Int32CategoricalWriter<>(columnType, length);
		} else if (columnType.category() == Column.Category.OBJECT) {
			return new ObjectWriter<>(columnType, length);
		} else {
			throw new IllegalArgumentException("Numeric types not supported for object row writer.");
		}
	}


	/**
	 * Moves the reader to the next row.
	 */
	public void move() {
		if (bufferRowIndex >= buffer.length - bufferWidth) {
			if (buffer == PLACEHOLDER_BUFFER) {
				buffer = new Object[bufferWidth * BUFFER_HEIGHT];
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
	public void set(int index, Object value) {
		buffer[bufferRowIndex + index] = classes[index].cast(value);
	}

	/**
	 * Creates a {@link Table} from the data inside the row writer. The row writer cannot be changed afterwards.
	 *
	 * @return a new table
	 * @throws IllegalArgumentException
	 * 		if the stored column labels are not valid to create a table
	 * @throws IllegalStateException
	 * 		if this method has been called before
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
		return "Object row writer (" + (rowIndex + 1) + "x" + bufferWidth + ")";
	}
}
