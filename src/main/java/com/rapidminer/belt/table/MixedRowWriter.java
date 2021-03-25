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

package com.rapidminer.belt.table;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.reader.NumericReader;


/**
 * Writer to create a {@link Table} from row-wise data.
 *
 * @author Gisa Meier, Kevin Majchrzak
 * @see Buffers
 */
public final class MixedRowWriter {

	/**
	 * Placeholder buffer used before initialization.
	 */
	private static final double[] PLACEHOLDER_BUFFER = new double[0];

	/**
	 * The height of the internal buffer.
	 */
	private static final int BUFFER_HEIGHT = NumericReader.SMALL_BUFFER_SIZE;

	private final NumericColumnWriter[] columns;
	private final ComplexWriter[] objectColumns;
	private final Class<?>[] classes;
	private final String[] columnLabels;
	private final int bufferWidth;
	private final boolean initialize;

	/**
	 * If this row number is reached, the columns will be checked for sparsity. The row number will be mulitplied with 2
	 * after every check so that the checks occur in exponential steps.
	 */
	private int checkForSparsityRow;

	private double[] buffer = PLACEHOLDER_BUFFER;
	private Object[] objectBuffer = new Object[0];
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
	MixedRowWriter(List<String> columnLabels, List<TypeId> types) {
		this(columnLabels, types, false);
	}

	/**
	 * Creates a row writer to create a {@link Table} with the given column labels and starting row size 0.
	 *
	 * @param columnLabels
	 * 		the names for the columns to construct
	 * @param types
	 * 		the types of the columns to construct
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@link false} values
	 * 		for	indices that are not explicitly set are undetermined
	 */
	MixedRowWriter(List<String> columnLabels, List<TypeId> types, boolean initialize) {
		int numberOfColumns = columnLabels.size();
		checkForSparsityRow = Math.max(BUFFER_HEIGHT + 1, Math.min(NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW,
				NumericRowWriter.MAX_CHECK_FOR_SPARSITY_OVERALL_ROWS / numberOfColumns));
		columns = new NumericColumnWriter[numberOfColumns];
		objectColumns = new ComplexWriter[numberOfColumns];
		classes = new Class<?>[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			TypeId typeId = Objects.requireNonNull(types.get(i), "column type must not be null");
			ColumnType<?> type = ColumnType.forId(typeId);
			columns[i] = getBufferForType(typeId);
			objectColumns[i] = getObjectBufferForType(type);
			classes[i] = type.elementType();
		}
		bufferWidth = numberOfColumns;
		this.initialize = initialize;
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
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@link false} values
	 * 		for	indices that are not explicitly set are undetermined
	 */
	MixedRowWriter(List<String> columnLabels, List<TypeId> types, int expectedRows, boolean initialize) {
		int numberOfColumns = columnLabels.size();
		checkForSparsityRow = Math.max(BUFFER_HEIGHT + 1, Math.min(NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW,
				NumericRowWriter.MAX_CHECK_FOR_SPARSITY_OVERALL_ROWS / numberOfColumns));
		columns = new NumericColumnWriter[numberOfColumns];
		objectColumns = new ComplexWriter[numberOfColumns];
		classes = new Class<?>[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			TypeId typeId = Objects.requireNonNull(types.get(i), "column type must not be null");
			ColumnType<?> type = ColumnType.forId(typeId);
			columns[i] = getBufferForType(typeId, expectedRows);
			objectColumns[i] = getObjectBufferForType(type, expectedRows);
			classes[i] = type.elementType();
		}
		bufferWidth = numberOfColumns;
		this.initialize = initialize;
	}

	/**
	 * Moves the reader to the next row.
	 */
	public void move() {
		if (bufferRowIndex >= buffer.length - bufferWidth) {
			if (buffer == PLACEHOLDER_BUFFER) {
				buffer = new double[bufferWidth * BUFFER_HEIGHT];
				objectBuffer = new Object[bufferWidth * BUFFER_HEIGHT];
			} else {
				writeBuffer();
			}
			if (initialize) {
				Arrays.fill(buffer, Double.NaN);
				Arrays.fill(objectBuffer, null);
			}
			bufferOffset += BUFFER_HEIGHT;
			bufferRowIndex = 0;
		} else {
			bufferRowIndex += bufferWidth;
		}
		rowIndex++;
		if (rowIndex == checkForSparsityRow) {
			checkForSparsity();
			// check again in exponential steps
			checkForSparsityRow *= 2;
		}
	}

	/**
	 * Sets the value at the given column index for a numeric column. This method is well-defined for indices zero
	 * (including) to {@link #width()} (excluding). For non-numeric columns use {@link #set(int, Object)} instead.
	 *
	 * <p>This method does not perform any range checks. Nor does it ever advance the current row. Before invoking this
	 * method, you will have to call {@link #move()} at least once.
	 *
	 * @param index
	 * 		the column index of a numeric column
	 * @param value
	 * 		the value to set
	 */
	public void set(int index, double value) {
		buffer[bufferRowIndex + index] = value;
	}

	/**
	 * Sets the value at the given column index for a non-numeric column. This method is well-defined for indices zero
	 * (including) to {@link #width()} (excluding). For numeric columns use {@link #set(int, double)} instead.
	 *
	 * <p>This method does not perform any range checks. Nor does it ever advance the current row. Before invoking this
	 * method, you will have to call {@link #move()} at least once.
	 *
	 * @param index
	 * 		the column index of a non-numeric column
	 * @param value
	 * 		the value to set
	 */
	public void set(int index, Object value) {
		objectBuffer[bufferRowIndex + index] = classes[index].cast(value);
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
			NumericColumnWriter column = columns[i];
			if (column != null) {
				finalColumns[i] = column.toColumn();
			} else {
				ComplexWriter objectColumn = objectColumns[i];
				if (objectColumn != null) {
					finalColumns[i] = objectColumn.toColumn();
				}
			}
		}
		return new Table(finalColumns, columnLabels);
	}

	/**
	 * @return the number of values per row
	 */
	public int width() {
		return columns.length;
	}


	@Override
	public String toString() {
		return "General row writer (" + (rowIndex + 1) + "x" + bufferWidth + ")";
	}

	/**
	 * Checks the column writers for sparsity. If a dense column writer contains sufficiently sparse data it is
	 * converted into a sparse column writer.
	 * <p> Package private for testing.
	 */
	void checkForSparsity() {
		ComplexRowWriter.checkForSparsity(objectColumns, bufferOffset);
		NumericRowWriter.checkForSparsity(columns, bufferOffset);
	}

	/**
	 * Used for testing.
	 */
	ComplexWriter[] getObjectColumns() {
		return objectColumns;
	}

	/**
	 * Used for testing.
	 */
	NumericColumnWriter[] getNumericColumns() {
		return columns;
	}

	/**
	 * Writes the writer buffer to the column buffers.
	 */
	private void writeBuffer() {
		for (int i = 0; i < columns.length; i++) {
			NumericColumnWriter column = columns[i];
			if (column != null) {
				column.fill(buffer, bufferOffset, i, bufferWidth, rowIndex + 1);
			}
			ComplexWriter objectColumn = objectColumns[i];
			if (objectColumn != null) {
				objectColumn.fill(objectBuffer, bufferOffset, i, bufferWidth, rowIndex + 1);
			}
		}
	}

	private ComplexWriter getObjectBufferForType(ColumnType<?> columnType) {
		if (ColumnType.DATETIME.equals(columnType)) {
			return new NanosecondsDateTimeWriter();
		} else if (ColumnType.TIME.equals(columnType)) {
			return new TimeColumnWriter();
		} else if (columnType.category() == Column.Category.CATEGORICAL) {
			if (!columnType.elementType().equals(String.class)) {
				throw new AssertionError("non-String categorical column");
			}
			//cast is safe since element type was checked above
			@SuppressWarnings("unchecked")
			ColumnType<String> stringColumnType = (ColumnType<String>) columnType;
			return new Int32CategoricalWriter(stringColumnType);
		} else if (columnType.category() == Column.Category.OBJECT) {
			return new ObjectWriter<>(columnType);
		} else {
			return null;
		}
	}

	/**
	 * Returns an integer or real growing column buffer without set length.
	 */
	private NumericColumnWriter getBufferForType(TypeId typeId) {
		if (typeId == TypeId.INTEGER_53_BIT) {
			return new Integer53BitColumnWriter();
		} else if (typeId == TypeId.REAL) {
			return new RealColumnWriter();
		} else {
			return null;
		}
	}

	private ComplexWriter getObjectBufferForType(ColumnType<?> columnType, int expectedRows) {
		if (ColumnType.DATETIME.equals(columnType)) {
			return new NanosecondsDateTimeWriter(expectedRows);
		} else if (ColumnType.TIME.equals(columnType)) {
			return new TimeColumnWriter(expectedRows);
		} else if (columnType.category() == Column.Category.CATEGORICAL) {
			if (!columnType.elementType().equals(String.class)) {
				throw new AssertionError("non-String categorical column");
			}
			//cast is safe since element type was checked above
			@SuppressWarnings("unchecked")
			ColumnType<String> stringColumnType = (ColumnType<String>) columnType;
			return new Int32CategoricalWriter(stringColumnType, expectedRows);
		} else if (columnType.category() == Column.Category.OBJECT) {
			return new ObjectWriter<>(columnType, expectedRows);
		} else {
			return null;
		}
	}

	/**
	 * Returns an integer or real growing column buffer with the given length.
	 */
	private NumericColumnWriter getBufferForType(TypeId typeId, int expectedRows) {
		if (typeId == TypeId.INTEGER_53_BIT) {
			return new Integer53BitColumnWriter(expectedRows);
		} else if (typeId == TypeId.REAL) {
			return new RealColumnWriter(expectedRows);
		} else {
			return null;
		}
	}
}
