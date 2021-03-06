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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SplittableRandom;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnUtils;
import com.rapidminer.belt.reader.NumericReader;


/**
 * Writer to create a {@link Table} from row-wise non-numeric data.
 *
 * @author Gisa Meier, Kevin Majchrzak
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

	/**
	 * If this row number is reached, the columns will be checked for sparsity. The row number will be mulitplied with 2
	 * after every check so that the checks occur in exponential steps.
	 */
	private int checkForSparsityRow;

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
	 * 		the type ids of the columns to construct
	 */
	ComplexRowWriter(List<String> columnLabels, List<TypeId> types) {
		int numberOfColumns = columnLabels.size();
		checkForSparsityRow = Math.max(BUFFER_HEIGHT + 1, Math.min(NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW,
				NumericRowWriter.MAX_CHECK_FOR_SPARSITY_OVERALL_ROWS / numberOfColumns));
		columns = new ComplexWriter[numberOfColumns];
		classes = new Class<?>[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			TypeId typeId = Objects.requireNonNull(types.get(i), "column type must not be null");
			ColumnType<?> type = ColumnType.forId(typeId);
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
	 * 		the type ids of the columns to construct
	 * @param expectedRows
	 * 		the expected number of rows
	 */
	ComplexRowWriter(List<String> columnLabels, List<TypeId> types, int expectedRows) {
		int numberOfColumns = columnLabels.size();
		checkForSparsityRow = Math.max(BUFFER_HEIGHT + 1, Math.min(NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW,
				NumericRowWriter.MAX_CHECK_FOR_SPARSITY_OVERALL_ROWS / numberOfColumns));
		columns = new ComplexWriter[numberOfColumns];
		classes = new Class<?>[numberOfColumns];
		this.columnLabels = columnLabels.toArray(new String[0]);
		for (int i = 0; i < numberOfColumns; i++) {
			TypeId typeId = Objects.requireNonNull(types.get(i), "column type must not be null");
			ColumnType<?> type = ColumnType.forId(typeId);
			columns[i] = getObjectBufferForType(type, expectedRows);
			classes[i] = type.elementType();
		}
		bufferWidth = numberOfColumns;
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
		if (rowIndex == checkForSparsityRow) {
			checkForSparsity();
			// check again in exponential steps
			checkForSparsityRow *= 2;
		}
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
	 * @return the number of values per row
	 */
	public int width() {
		return columns.length;
	}


	@Override
	public String toString() {
		return "Object row writer (" + (rowIndex + 1) + "x" + bufferWidth + ")";
	}

	/**
	 * Used for testing.
	 */
	ComplexWriter[] getColumns() {
		return columns;
	}

	/**
	 * Used for testing.
	 */
	void checkForSparsity(){
		checkForSparsity(columns, bufferOffset);
	}

	/**
	 * Writes the writer buffer to the column buffers.
	 */
	private void writeBuffer() {
		for (int i = 0; i < columns.length; i++) {
			columns[i].fill(buffer, bufferOffset, i, bufferWidth, rowIndex + 1);
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
			throw new IllegalArgumentException("Numeric types not supported for object row writer.");
		}
	}

	private ComplexWriter getObjectBufferForType(ColumnType<?> columnType, int length) {
		if (ColumnType.DATETIME.equals(columnType)) {
			return new NanosecondsDateTimeWriter(length);
		} else if (ColumnType.TIME.equals(columnType)) {
			return new TimeColumnWriter(length);
		} else if (columnType.category() == Column.Category.CATEGORICAL) {
			if (!columnType.elementType().equals(String.class)) {
				throw new AssertionError("non-String categorical column");
			}
			//cast is safe since element type was checked above
			@SuppressWarnings("unchecked")
			ColumnType<String> stringColumnType = (ColumnType<String>) columnType;
			return new Int32CategoricalWriter(stringColumnType, length);
		} else if (columnType.category() == Column.Category.OBJECT) {
			return new ObjectWriter<>(columnType, length);
		} else {
			throw new IllegalArgumentException("Numeric types not supported for object row writer.");
		}
	}

	/**
	 * Checks the column writers for sparsity. If a dense column writer contains sufficiently sparse data it is
	 * converted into a sparse column writer.
	 *
	 * @param columns
	 * 		The column writers to check and convert.
	 * @param length
	 * 		The length of the data stored in the column writers (the max row index that has been filled so far).
	 */
	static void checkForSparsity(ComplexWriter[] columns, int length) {
		for (int i = 0; i < columns.length; i++) {
			ComplexWriter column = columns[i];
			if (column instanceof TimeColumnWriter) {
				TimeColumnWriter writer = (TimeColumnWriter) column;
				long[] data = writer.getData();
				Optional<Long> defaultValue = estimateDefaultValue(data, length);
				if (defaultValue.isPresent()) {
					// replace the dense column writer by its sparse version
					column = TimeColumnWriterSparse.ofNanos(defaultValue.get(), data, length);
					columns[i] = column;
				}
			} else if (column instanceof NanosecondsDateTimeWriter) {
				NanosecondsDateTimeWriter writer = (NanosecondsDateTimeWriter) column;
				long[] seconds = writer.getSeconds();
				Optional<Long> defaultValue = estimateDefaultValue(seconds, length);
				if (defaultValue.isPresent()) {
					// replace the dense column writer by its sparse version
					column = NanoDateTimeWriterSparse.ofSecondsAndNanos(defaultValue.get(),
							seconds, writer.getNanos(), length);
					columns[i] = column;
				}
			} else if (column instanceof Int32CategoricalWriter) {
				Int32CategoricalWriter writer = (Int32CategoricalWriter) column;
				int[] indexData = writer.getData();
				Optional<Integer> defaultValue = estimateDefaultValue(indexData, length);
				if (defaultValue.isPresent()) {
					// replace the dense column writer by its sparse version
					// unchecked is no danger since we copy from a previously checked writer
					@SuppressWarnings("unchecked")
					ComplexWriter newColumn = Int32CategoricalWriterSparse.ofRawIndices(writer.getColumnType(), defaultValue.get(),
							writer.getIndexLookup(), writer.getValueLookup(), indexData, length);
					columns[i] = newColumn;
				}
			} // else: object column without sparse representation or already sparse
		}
	}

	/**
	 * Estimates the default value for the given data inside of the range [0, length].
	 */
	private static Optional<Long> estimateDefaultValue(long[] data, int length) {
		long[] sample = new long[NumericRowWriter.SPARSITY_SAMPLE_SIZE];
		SplittableRandom random = new SplittableRandom();
		for (int j = 0; j < NumericRowWriter.SPARSITY_SAMPLE_SIZE; j++) {
			sample[j] = data[random.nextInt(length)];
		}
		return ColumnUtils.estimateDefaultValue(sample, NumericRowWriter.MIN_SPARSITY);
	}

	/**
	 * Estimates the default value for the given data inside of the range [0, length].
	 */
	private static Optional<Integer> estimateDefaultValue(int[] data, int length) {
		int[] sample = new int[NumericRowWriter.SPARSITY_SAMPLE_SIZE];
		SplittableRandom random = new SplittableRandom();
		for (int j = 0; j < NumericRowWriter.SPARSITY_SAMPLE_SIZE; j++) {
			sample[j] = data[random.nextInt(length)];
		}
		return ColumnUtils.estimateDefaultValue(sample, NumericRowWriter.MIN_SPARSITY);
	}
}
