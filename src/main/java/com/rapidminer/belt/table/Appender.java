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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleConsumer;

import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionUtils;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * Methods to append {@link Column}s or {@link Table}s.
 *
 * @author Gisa Meier
 */
public final class Appender {

	/**
	 * Message for exception when encountering a null column.
	 */
	private static final String MESSAGE_COLUMN_NULL = "column must not be null";

	/**
	 * Appends the given columns list into one column if possible. The result column has the given totalLength,
	 * containing only the first columns values or with missing values at the end if the sizes do not match. Only
	 * columns with the same types can be appended with the exception of integer and real columns which will produce a
	 * real column if mixed.
	 *
	 * @param columns
	 * 		the columns to append into a new column
	 * @param totalLength
	 * 		the length of the new column
	 * @param progressCallback
	 * 		a callback which is called with the progress of the append, can be null
	 * @param context
	 * 		the context to use
	 * @return a new column built by appending the input columns
	 * @throws NullPointerException
	 * 		if the columns list is or contains {@code null} or the context is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the columns list is empty or the total length is negative
	 * @throws IncompatibleTypesException
	 * 		if the columns are not compatible
	 */
	public static Column append(List<Column> columns, int totalLength,
								DoubleConsumer progressCallback, Context context) {
		Objects.requireNonNull(columns, "columns list must not be null");
		Objects.requireNonNull(context, "context must not be null");
		if (columns.isEmpty()) {
			//we do not know the type the new column should have
			throw new IllegalArgumentException("columns list must not be empty");
		}
		if (totalLength < 0) {
			throw new IllegalArgumentException("total length must not be negative");
		}
		if (progressCallback == null) {
			progressCallback = v -> {};
		}
		if (columns.size() == 1 && columns.get(0).size() == totalLength) {
			progressCallback.accept(1);
			return columns.get(0);
		}
		Column column = columns.get(0);
		if (column == null) {
			throw new NullPointerException(MESSAGE_COLUMN_NULL);
		}

		DoubleConsumer finalProgressCallback = progressCallback;
		return ExecutionUtils.run(() -> {
			ColumnType<?> type = column.type();
			switch (type.category()) {
				case NUMERIC:
					return appendNumeric(columns, totalLength, type.id(), finalProgressCallback);
				case CATEGORICAL:
					if (!type.elementType().equals(String.class)) {
						throw new AssertionError("non-String categorical column");
					}
					//cast is safe since element type was checked above
					@SuppressWarnings("unchecked")
					ColumnType<String> stringColumnType = (ColumnType<String>) type;
					return appendCategorical(columns, totalLength, column, stringColumnType, finalProgressCallback);
				case OBJECT:
				default:
					if (type.id() == Column.TypeId.DATE_TIME) {
						return dateTimeMerge(columns, totalLength, finalProgressCallback);
					} else if (type.id() == Column.TypeId.TIME) {
						return timeMerge(columns, totalLength, finalProgressCallback);
					} else {
						return objectMerge(columns, totalLength, type, finalProgressCallback);
					}
			}
		}, context);
	}

	/**
	 * Appends the given tables into a new table if possible. Only tables with the same labels and compatible column
	 * types can be appended. The order of the columns is not required to be the same in different tables. The result
	 * table has the column oder of the first input table. The column meta data is taken from the first table.
	 *
	 * @param tables
	 * 		the tables to append
	 * @param progressCallback
	 * 		a callback which is called with the progress of the append
	 * @param context
	 * 		the context to use
	 * @return a table built by appending the input tables
	 * @throws NullPointerException
	 * 		if the tables list is or contains {@code null} or the context is {@code null}
	 * @throws IncompatibleTableWidthException
	 * 		if the input tables have different widths
	 * @throws IncompatibleColumnsException
	 * 		if the input tables have different column labels
	 * @throws IncompatibleTypesException
	 * 		if the column types are not compatible
	 * @throws TableTooLongException
	 * 		if the appended table would have more rows than allowed
	 */
	public static Table append(List<Table> tables, DoubleConsumer progressCallback, Context context) {
		if (tables == null) {
			throw new NullPointerException("tables must not be null!");
		}
		if (tables.isEmpty()) {
			//return empty table with height 0
			return new Table(0);
		}

		// checks if all tables have the same column names
		checkForCompatibility(tables);


		long sizeSum = 0;
		for (Table table : tables) {
			sizeSum += table.height();
		}

		if (sizeSum > Integer.MAX_VALUE) {
			throw new TableTooLongException();
		}

		int finalSize = (int) sizeSum;

		// create new table
		Table firstTable = tables.get(0);
		int width = firstTable.width();

		if (width == 0) {
			return new Table(finalSize);
		}

		String[] labels = firstTable.labelArray();
		Column[] newColumns = new Column[labels.length];

		DoubleConsumer intermediateConsumer;
		if (progressCallback == null) {
			intermediateConsumer = d -> {};
		} else {
			intermediateConsumer = new DoubleConsumer() {

				private double previousValue = 0;

				@Override
				public void accept(double value) {
					// without synchronization some updates in this method will get lost, but progress good enough
					previousValue += value;
					double sendValue = previousValue;
					if (sendValue <= 1) {
						// due to double imprecision, values slightly bigger than 1 can happen
						progressCallback.accept(sendValue);
					}
				}
			};
		}

		ExecutionUtils.parallel(0, labels.length, index -> {
			String label = labels[index];
			List<Column> columns = new ArrayList<>(width);
			for (Table table : tables) {
				columns.add(table.column(label));
			}
			try {
				newColumns[index] =
						append(columns, finalSize, progressCallback == null ? intermediateConsumer :
										new DoubleConsumer() {

											private double previousValue = 0;

											@Override
											public void accept(double value) {
												intermediateConsumer.accept((value - previousValue) / width);
												previousValue = value;
											}
										}
								, context);
			} catch (IncompatibleTypesException e) {
				e.setColumnName(label);
				throw e;
			}
		}, context);
		if (progressCallback != null) {
			progressCallback.accept(1);
		}
		return new Table(newColumns, labels, firstTable.getMetaData());

	}

	/**
	 * Appends categorical columns. Checks the mapping size of the result and if it can be boolean.
	 *
	 * @throws NullPointerException
	 * 		if the columns list contains {@code null}
	 * @throws IncompatibleTypesException
	 * 		if not all columns are of the same type
	 */
	private static Column appendCategorical(List<Column> columns, int totalLength, Column column,
												ColumnType<String> type, DoubleConsumer progressCallback) {
		Dictionary dictionary = column.getDictionary();
		Set<String> values = new HashSet<>(ColumnAccessor.get().getDictionaryList(dictionary));
		boolean isBoolean = dictionary.isBoolean();
		String[] positiveNegativeValue = getPositiveNegativeValue(dictionary, isBoolean);

		for (int i = 1; i < columns.size(); i++) {
			Column otherColumn = columns.get(i);
			if (otherColumn == null) {
				throw new NullPointerException(MESSAGE_COLUMN_NULL);
			}
			if (!type.equals(otherColumn.type())) {
				throw new IncompatibleTypesException(type, otherColumn.type(), i);
			}
			Dictionary otherDictionary = otherColumn.getDictionary();
			values.addAll(ColumnAccessor.get().getDictionaryList(otherDictionary));
			if (isBoolean) {
				isBoolean = handleBooleanProperties(positiveNegativeValue, otherDictionary);
			}
		}
		int newDiffValues = values.size() - 1;
		return appendCategorical(columns, totalLength, newDiffValues, type, isBoolean, positiveNegativeValue == null ?
						null : positiveNegativeValue[0],
				progressCallback);
	}


	/**
	 * Checks if the other column is boolean with the same positive value.
	 */
	private static boolean handleBooleanProperties(Object[] positiveNegativeValue, Dictionary dictionary) {
		if (!dictionary.isBoolean()) {
			return false;
		} else {
			if (dictionary.hasPositive()) {
				if (positiveNegativeValue[0] == null) {
					positiveNegativeValue[0] = dictionary.get(dictionary.getPositiveIndex());
				} else if (!positiveNegativeValue[0].equals(dictionary.get(dictionary.getPositiveIndex()))) {
					return false;
				}
			}
			if (dictionary.hasNegative()) {
				if (positiveNegativeValue[1] == null) {
					positiveNegativeValue[1] = dictionary.get(dictionary.getNegativeIndex());
				} else return positiveNegativeValue[1].equals(dictionary.get(dictionary.getNegativeIndex()));
			}
		}
		return true;
	}

	/**
	 * Finds the positive value.
	 */
	private static String[] getPositiveNegativeValue(Dictionary dictionary, boolean isBoolean) {
		String[] posNeg = new String[2];
		if (isBoolean) {
			if (dictionary.hasPositive()) {
				posNeg[0] = dictionary.get(dictionary.getPositiveIndex());
			}
			if (dictionary.hasNegative()) {
				posNeg[1] = dictionary.get(dictionary.getNegativeIndex());
			}
			return posNeg;
		}
		return null;
	}

	/**
	 * Appends the given categorical columns, making the result boolean if desired.
	 */
	private static Column appendCategorical(List<Column> columns, int finalSize, int categories,
												ColumnType<String> type, boolean isBoolean, String positiveValue,
												DoubleConsumer progressCallback) {
		NominalBuffer buffer;
		IntegerFormats.Format minimal = IntegerFormats.Format.findMinimal(categories);
		switch (minimal) {
			case UNSIGNED_INT2:
				buffer = BufferAccessor.get().newUInt2Buffer(type, finalSize);
				break;
			case UNSIGNED_INT4:
				buffer = BufferAccessor.get().newUInt4Buffer(type, finalSize);
				break;
			case UNSIGNED_INT8:
				buffer = BufferAccessor.get().newUInt8Buffer(type, finalSize);
				break;
			case UNSIGNED_INT16:
				buffer = BufferAccessor.get().newUInt16Buffer(type, finalSize);
				break;
			case SIGNED_INT32:
			default:
				buffer = BufferAccessor.get().newInt32Buffer(type, finalSize);
		}
		if (isBoolean && finalSize > 0) {
			//ensure that positive value is in mapping even if it is not in the data
			buffer.set(0, positiveValue);
		}
		int rowIndex = 0;
		for (Column column : columns) {
			ObjectReader<String> reader = Readers.objectReader(column, type.elementType());
			while (reader.hasRemaining() && rowIndex < finalSize) {
				buffer.set(rowIndex++, reader.read());
			}
			progressCallback.accept(Math.min(rowIndex, finalSize) / (double) finalSize);
		}
		progressCallback.accept(1.0);
		if (isBoolean && finalSize > 0) {
			return buffer.toBooleanColumn(positiveValue);
		}
		return buffer.toColumn();
	}

	/**
	 * Appends the given numeric columns. If one of the columns is a real column the result is real.
	 *
	 * @throws NullPointerException
	 * 		if the columns list contains {@code null}
	 * @throws IncompatibleTypesException
	 * 		if the columns list contains a non-numeric column
	 */
	private static Column appendNumeric(List<Column> columns, int totalLength, Column.TypeId typeId,
										DoubleConsumer progressCallback) {
		boolean hasReal = typeId == Column.TypeId.REAL;
		for (int i = 1; i < columns.size(); i++) {
			Column otherColumn = columns.get(i);

			if (otherColumn == null) {
				throw new NullPointerException(MESSAGE_COLUMN_NULL);
			}
			// At least one non-numerical -> throw
			if (otherColumn.type().category() != Column.Category.NUMERIC) {
				throw new IncompatibleTypesException(ColumnType.REAL, otherColumn.type(), i);
			}

			hasReal |= otherColumn.type().id() == Column.TypeId.REAL;
		}
		double[] data = new double[totalLength];
		int index = 0;
		int start = 0;
		while (start < totalLength && index < columns.size()) {
			Column col = columns.get(index++);
			col.fill(data, 0, start, 1);
			start += col.size();
			progressCallback.accept(Math.min(start, totalLength) / (double) totalLength);
		}

		if (start < totalLength) {
			Arrays.fill(data, start, totalLength, Double.NaN);
			progressCallback.accept(1.0);
		}
		return ColumnAccessor.get().newNumericColumn(hasReal ? Column.TypeId.REAL : Column.TypeId.INTEGER_53_BIT, data);
	}

	/**
	 * Appends the given time columns.
	 *
	 * @throws NullPointerException
	 * 		if columns contains {@code null}
	 * @throws IncompatibleTypesException
	 * 		if not all columns are time columns
	 */
	private static Column timeMerge(List<Column> columns, int totalLength, DoubleConsumer progressCallback) {
		long[] data = new long[totalLength];
		int index = 0;
		int start = 0;
		while (start < totalLength && index < columns.size()) {
			Column col = columns.get(index++);
			if (col == null) {
				throw new NullPointerException(MESSAGE_COLUMN_NULL);
			}
			if (col.type().id() != Column.TypeId.TIME) {
				throw new IncompatibleTypesException(ColumnType.TIME, col.type(), index - 1);
			}

			if (col instanceof TimeColumn) {
				TimeColumn mappedTimeColumn = (TimeColumn) col;
				mappedTimeColumn.fillNanosIntoArray(data, start);
				start += col.size();
			} else {
				throw new AssertionError("Unknown implementation of time column.");
			}
			progressCallback.accept(Math.min(start, totalLength) / (double) totalLength);
		}
		if (start < totalLength) {
			Arrays.fill(data, start, totalLength, TimeColumn.MISSING_VALUE);
			progressCallback.accept(1.0);
		}
		return ColumnAccessor.get().newTimeColumn(data);
	}

	/**
	 * Appends the given date-time columns.
	 *
	 * @throws NullPointerException
	 * 		if columns contains {@code null}
	 * @throws IncompatibleTypesException
	 * 		if not all columns are date-time columns
	 */
	private static Column dateTimeMerge(List<Column> columns, int totalLength, DoubleConsumer progressCallback) {
		long[] seconds = new long[totalLength];
		int[] nanos = null;
		int index = 0;
		int start = 0;
		while (start < totalLength && index < columns.size()) {
			Column col = columns.get(index++);
			if (col == null) {
				throw new NullPointerException(MESSAGE_COLUMN_NULL);
			}
			if (col.type().id() != Column.TypeId.DATE_TIME) {
				throw new IncompatibleTypesException(ColumnType.DATETIME, col.type(), index - 1);
			}

			nanos = copyDateTimeData(col, totalLength, seconds, nanos, start);
			start += col.size();
			progressCallback.accept(Math.min(start, totalLength) / (double) totalLength);
		}
		if (start < totalLength) {
			Arrays.fill(seconds, start, totalLength, DateTimeColumn.MISSING_VALUE);
			progressCallback.accept(1.0);
		}

		return ColumnAccessor.get().newDateTimeColumn(seconds, nanos);

	}

	/**
	 * Copies the data from the given date-time column.
	 */
	private static int[] copyDateTimeData(Column column, int totalLength, long[] seconds, int[] nanos, int start) {
		if (column instanceof DateTimeColumn) {
			DateTimeColumn dateTimeColumn = (DateTimeColumn) column;
			dateTimeColumn.fillSecondsIntoArray(seconds, start);
			if (dateTimeColumn.hasSubSecondPrecision()) {
				if (nanos == null) {
					nanos = new int[totalLength];
				}
				dateTimeColumn.fillNanosIntoArray(nanos, start);
			}
		} else {
			throw new AssertionError("Unknown implementation of time column.");
		}
		return nanos;
	}

	/**
	 * Appends generic object columns that are not time or date-time columns.
	 *
	 * @throws IncompatibleTypesException
	 * 		if the columns have different types
	 */
	private static <T> Column objectMerge(List<Column> columns, int totalLength, ColumnType<T> type,
										  DoubleConsumer progressCallback) {
		Object[] data = new Object[totalLength];
		int index = 0;
		int start = 0;
		while (start < totalLength && index < columns.size()) {
			Column col = columns.get(index++);
			if (col == null) {
				throw new NullPointerException(MESSAGE_COLUMN_NULL);
			}
			if (col.type() != type) {
				throw new IncompatibleTypesException(type, col.type(), index - 1);
			}
			col.fill(data, 0, start, 1);
			start += col.size();
			progressCallback.accept(Math.min(start, totalLength) / (double) totalLength);
		}
		progressCallback.accept(1.0);
		return ColumnAccessor.get().newObjectColumn(type, data);
	}


	/**
	 * Checks if the tables are compatible.
	 *
	 * @param tables
	 * 		the tables to check
	 * @throws NullPointerException
	 * 		if the tables list contains {@code null}
	 * @throws IncompatibleTableWidthException
	 * 		if the tables have different width
	 * @throws IncompatibleColumnsException
	 * 		if the column labels of the first table are not present in all other tables
	 */
	private static void checkForCompatibility(List<Table> tables) {
		Iterator<Table> i = tables.iterator();
		Table first = i.next();
		if (first == null) {
			throw new NullPointerException("tables list must not contain null");
		}
		int index = 1;
		while (i.hasNext()) {
			Table second = i.next();
			if (second == null) {
				throw new NullPointerException("tables list must not contain null");
			}
			if (first.width() != second.width()) {
				throw new IncompatibleTableWidthException(index);
			}

			for (String label : first.labels()) {
				if (!second.contains(label)) {
					throw new IncompatibleColumnsException(index, label);
				}
			}
			index++;
		}
	}


	private Appender() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

	/**
	 * Exception for the case that the column with index out of a list of columns has the actual type that does not
	 * match the desired type. The column name is stored if possible.
	 */
	public static final class IncompatibleTypesException extends IllegalArgumentException {

		private final String desiredType;
		private final String actualType;
		private final int index;
		private String columnName;


		private IncompatibleTypesException(ColumnType<?> desiredType, ColumnType<?> actualType, int index) {
			super("Columns have incompatible types");
			this.desiredType = desiredType.toString();
			this.actualType = actualType.toString();
			this.index = index;
		}

		public String getDesiredType() {
			return desiredType;
		}

		public String getActualType() {
			return actualType;
		}

		public int getIndex() {
			return index;
		}

		private void setColumnName(String columnName) {
			this.columnName = columnName;
		}

		public String getColumnName() {
			return columnName;
		}
	}

	/**
	 * Exception for the case that the table with tableIndex out of a list of tables has incompatible width.
	 */
	public static final class IncompatibleTableWidthException extends IllegalArgumentException {

		private final int tableIndex;

		private IncompatibleTableWidthException(int tableIndex) {
			super("Table number " + (tableIndex + 1) + " has incompatible width");
			this.tableIndex = tableIndex;
		}

		public int getTableIndex() {
			return tableIndex;
		}

	}

	/**
	 * Exception for the case that the table with tableIndex out of a list of tables does not contain a certain column
	 * name.
	 */
	public static final class IncompatibleColumnsException extends IllegalArgumentException {

		private final int tableIndex;
		private final String columnName;

		private IncompatibleColumnsException(int tableIndex, String columnName) {
			super("Table number " + (tableIndex + 1) + " is missing the column with name " + columnName);
			this.tableIndex = tableIndex;
			this.columnName = columnName;
		}

		public int getTableIndex() {
			return tableIndex;
		}

		public String getColumnName() {
			return columnName;
		}

	}

	/**
	 * Exception for appending more than {@link Integer#MAX_VALUE} rows.
	 */
	public static final class TableTooLongException extends IllegalArgumentException {

		private TableTooLongException() {
			super("Total table length is longer than the maximal allowed length");
		}
	}
}
