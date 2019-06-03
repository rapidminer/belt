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

package com.rapidminer.belt.reader;


import java.util.Arrays;
import java.util.List;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.table.Table;


/**
 * Utility methods for reading one or more {@link Column}s.
 *
 * <p>Given the column-oriented design of Belt, the readers for single columns perform better than their row-oriented
 * equivalents. For example, using {@link NumericReader}s is preferred over using a {@link NumericRowReader}.
 *
 * @author Gisa Meier, Michael Knopf
 */
public final class Readers {

	/**
	 * The position of a row reader before the first invocation of {@code move()}. Calling {@code
	 * setPosition(BEFORE_FIRST_ROW)} will reset any row reader.
	 */
	public static final int BEFORE_FIRST_ROW = -1;

	private static final String MSG_NULL_TABLE = "Table must not be null";
	private static final String MSG_NULL_COLUMN = "Column must not be null";
	private static final String MSG_NULL_TYPE = "Type must not be null";
	private static final String MSG_NULL_LIST = "Column list must not be null";
	private static final String MSG_NULL_LIST_ELEMENT = "Column list must not contain null columns";
	private static final String MSG_NULL_OTHER_COLUMNS = "Other columns must not be null";

	/**
	 * Creates a new numeric column reader.
	 *
	 * @param column
	 * 		the column to read, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @return a reader for a numeric-readable column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 */
	public static NumericReader numericReader(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		return new NumericReader(column, NumericReader.DEFAULT_BUFFER_SIZE, column.size());
	}

	/**
	 * Creates a new numerical column reader.
	 *
	 * @param column
	 * 		the column to read, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @param length
	 * 		the number of elements allowed to be read from the start of the column
	 * @return a reader for a numeric-readable column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 */
	public static NumericReader numericReader(Column column, int length) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		return new NumericReader(column, NumericReader.DEFAULT_BUFFER_SIZE, length);
	}

	/**
	 * Creates a new categorical column reader.
	 *
	 * @param column
	 * 		the column to read, must be of column category {@link Column.Category#CATEGORICAL}
	 * @return a reader for a categorical column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 */
	public static CategoricalReader categoricalReader(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		return new CategoricalReader(column, NumericReader.DEFAULT_BUFFER_SIZE, column.size());
	}

	/**
	 * Creates a new categorical column reader.
	 *
	 * @param column
	 * 		the column to read,  must be of column category {@link Column.Category#CATEGORICAL}
	 * @param length
	 * 		the number of elements allowed to be read from the start of the column
	 * @return a reader for a categorical column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 */
	public static CategoricalReader categoricalReader(Column column, int length) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		return new CategoricalReader(column, NumericReader.DEFAULT_BUFFER_SIZE, length);
	}

	/**
	 * Creates a new object column reader of the given type.
	 *
	 * @param column
	 * 		the column to read, must be {@link Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the type as which to read the column
	 * @return a reader for an object-readable column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if type is not a super type of the column element type
	 */
	public static <T> ObjectReader<T> objectReader(Column column, Class<T> type) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		return new ObjectReader<>(column, type, NumericReader.DEFAULT_BUFFER_SIZE, column.size());
	}

	/**
	 * Creates a new object column reader of the given type.
	 *
	 * @param column
	 * 		the column to read, must be {@link Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the type as which to read the column
	 * @param length
	 * 		the number of elements allowed to be read from the start of the column
	 * @return a reader for an object-readable column
	 * @throws NullPointerException
	 * 		if the given column or type is {@code null}
	 * @throws IllegalArgumentException
	 * 		if type is not a super type of the column element type
	 */
	public static <T> ObjectReader<T> objectReader(Column column, Class<T> type, int length) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}

		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		return new ObjectReader<>(column, type, NumericReader.DEFAULT_BUFFER_SIZE, length);
	}

	/**
	 * Creates a new reader for row-wise numeric reads of all the columns in the given table. All columns must be
	 * {@link Column.Capability#NUMERIC_READABLE}.
	 *
	 * <p>Given the column-oriented design of Belt, {@link NumericRowReader}s do not perform as well as {@link
	 * NumericReader}s. Thus, whenever possible the use of {@link NumericReader}s is preferred.
	 *
	 * @param table
	 * 		the table to read
	 * @return a reader for numeric-readable columns
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public static NumericRowReader numericRowReader(Table table) {
		if (table == null) {
			throw new NullPointerException(MSG_NULL_TABLE);
		}
		return new NumericRowReader(table.columnList());
	}

	/**
	 * Creates a new reader for row-wise numeric reads of the given columns. All columns must be
	 * {@link Column.Capability#NUMERIC_READABLE}.
	 *
	 * <p>Given the column-oriented design of Belt, {@link NumericRowReader}s do not perform as well as {@link
	 * NumericReader}s. Thus, whenever possible the use of {@link NumericReader}s is preferred.
	 *
	 * @param first
	 * 		the first column to read
	 * @param second
	 * 		the second column to read
	 * @param other
	 * 		further columns to read
	 * @return a reader for numeric-readable columns
	 * @throws NullPointerException
	 * 		if any of the given columns is {@code null}
	 */
	public static NumericRowReader numericRowReader(Column first, Column second, Column... other) {
		if (other == null) {
			throw new NullPointerException(MSG_NULL_OTHER_COLUMNS);
		}
		return new NumericRowReader(createColumnList(first, second, other));
	}

	/**
	 * Creates a new reader for row-wise numeric reads of the given columns. All columns must be
	 * {@link Column.Capability#NUMERIC_READABLE}.
	 *
	 * <p>Given the column-oriented design of Belt, {@link NumericRowReader}s do not perform as well as {@link
	 * NumericReader}s. Thus, whenever possible the use of {@link NumericReader}s is preferred.
	 *
	 * @param columns
	 * 		the columns to read
	 * @return a reader for numeric-readable columns
	 * @throws NullPointerException
	 * 		if the source column array is {@code null} or contains {@code null} columns
	 */
	public static NumericRowReader numericRowReader(List<Column> columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_LIST);
		}
		for (Column column : columns) {
			if (column == null) {
				throw new NullPointerException(MSG_NULL_LIST_ELEMENT);
			}
		}
		return new NumericRowReader(columns);
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link MixedRowReader}s do not perform as well as column readers
	 * such as the {@link NumericReader}.
	 *
	 * @param table
	 * 		the table to read
	 * @return a reader for arbitrary columns
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public static MixedRowReader mixedRowReader(Table table) {
		if (table == null) {
			throw new NullPointerException(MSG_NULL_TABLE);
		}
		return new MixedRowReader(table.columnList());
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link MixedRowReader}s do not perform as well as column readers
	 * such as the {@link NumericReader}.
	 *
	 * @param first
	 * 		the first column to read
	 * @param second
	 * 		the second column to read
	 * @param other
	 * 		further columns to read
	 * @return a reader for arbitrary columns
	 * @throws NullPointerException
	 * 		if any of the given columns is {@code null}
	 */
	public static MixedRowReader mixedRowReader(Column first, Column second, Column... other) {
		if (other == null) {
			throw new NullPointerException(MSG_NULL_OTHER_COLUMNS);
		}
		return new MixedRowReader(createColumnList(first, second, other));
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link MixedRowReader}s do not perform as well as column readers
	 * such as the {@link NumericReader}.
	 *
	 * @param columns
	 * 		the columns to read
	 * @return a reader for arbitrary columns
	 * @throws NullPointerException
	 * 		if the source column array is {@code null} or contains {@code null} columns
	 */
	public static MixedRowReader mixedRowReader(List<Column> columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_LIST);
		}
		for (Column column: columns) {
			if (column == null) {
				throw new NullPointerException(MSG_NULL_LIST_ELEMENT);
			}
		}
		return new MixedRowReader(columns);
	}

	/**
	 * Creates a new reader for row-wise categorical reads of the given categorical columns. All columns must be
	 * {@link Column.Category#CATEGORICAL}.
	 *
	 * <p>Given the column-oriented design of Belt, {@link CategoricalRowReader}s do not perform as well as column
	 * readers such as the {@link CategoricalReader}.
	 *
	 * @param columns
	 * 		the columns to read
	 * @return a reader for categorical columns
	 * @throws NullPointerException
	 * 		if the source column array is {@code null} or contains {@code null} columns
	 */
	public static CategoricalRowReader categoricalRowReader(List<Column> columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_LIST);
		}
		for (Column column : columns) {
			if (column == null) {
				throw new NullPointerException(MSG_NULL_LIST_ELEMENT);
			}
		}
		return new CategoricalRowReader(columns);
	}

	/**
	 * Creates a new reader for row-wise object reads of the given categorical columns. All columns must be
	 * {@link Column.Capability#OBJECT_READABLE}.
	 *
	 * <p>Given the column-oriented design of Belt, {@link ObjectRowReader}s do not perform as well as column
	 * readers such as the {@link ObjectReader}.
	 *
	 * @param columns
	 * 		the columns to read
	 * @param type
	 * 		the type to read the columns, must be a super type of all column element types
	 * @return a reader for object columns
	 * @throws NullPointerException
	 * 		if the source column array is {@code null} or contains {@code null} columns
	 */
	public static <T> ObjectRowReader<T> objectRowReader(List<Column> columns, Class<T> type) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_LIST);
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		for (Column column : columns) {
			if (column == null) {
				throw new NullPointerException(MSG_NULL_LIST_ELEMENT);
			}
		}
		return new ObjectRowReader<>(columns, type);
	}

	/**
	 * Creates a new reader for row-wise reads of a single or a handful of rows of all the columns in the given table.
	 * All columns must be {@link Column.Capability#NUMERIC_READABLE}.
	 *
	 * <p>This is only intended for a few reads and will be much slower than {@link #numericRowReader(Table)} in most
	 * cases.
	 *
	 * @param table
	 * 		the table to read
	 * @return a reader for numeric-readable columns
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public static NumericRowReader unbufferedNumericRowReader(Table table) {
		if (table == null) {
			throw new NullPointerException(MSG_NULL_TABLE);
		}
		return new NumericRowReader(table.columnList(), NumericReader.MIN_BUFFER_SIZE);
	}

	/**
	 * Creates a new reader for row-wise numeric reads of a single or a handful of rows of all the columns in the given
	 * table.
	 *
	 * <p>This is only intended for a few reads and will be much slower than {@link #numericRowReader(Table)} in most
	 * cases.
	 *
	 * @param table
	 * 		the table to read
	 * @return a reader for arbitrary columns
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public static MixedRowReader unbufferedMixedRowReader(Table table) {
		if (table == null) {
			throw new NullPointerException(MSG_NULL_TABLE);
		}
		return new MixedRowReader(table.columnList(), NumericReader.MIN_BUFFER_SIZE);
	}

	/**
	 * Creates one column array from the given input, checks for {@code null} columns.
	 */
	private static List<Column> createColumnList(Column first, Column second, Column[] other) {
		if (first == null) {
			throw new NullPointerException("First column must not be null");
		}
		if (second == null) {
			throw new NullPointerException("Second Column must not be null");
		}
		if (other == null || other.length == 0) {
			return Arrays.asList(first, second);
		} else {
			for (Column column : other) {
				if (column == null) {
					throw new NullPointerException(MSG_NULL_OTHER_COLUMNS);
				}
			}
			Column[] allColumns = new Column[other.length + 2];
			System.arraycopy(other, 0, allColumns, 2, other.length);
			allColumns[0] = first;
			allColumns[1] = second;
			return Arrays.asList(allColumns);
		}
	}

	private Readers() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

}
