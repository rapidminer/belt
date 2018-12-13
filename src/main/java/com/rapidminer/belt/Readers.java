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
	private static final String MSG_NULL_ARRAY = "Column array must not be null";
	private static final String MSG_NULL_LIST = "Column list must not be null";
	private static final String MSG_NULL_ARRAY_ELEMENT = "Column array must not contain null columns";
	private static final String MSG_NULL_OTHER_COLUMNS = "Other columns must not be null";

	/**
	 * Creates a new numerical column reader.
	 *
	 * @param column
	 * 		the column to read
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
	 * 		the column to read
	 * @param length
	 * 		the number of elements allowed to be read from the start of the column
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
	 * 		the column to read, must be of column category {@link Column.Category#CATEGORICAL}
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
	 * 		the column to read,  must be of column category {@link Column.Category#CATEGORICAL}
	 * @param length
	 * 		the number of elements allowed to be read from the start of the column
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
	 * Creates a new reader for row-wise reads of all the columns in the given table.
	 *
	 * <p>Given the column-oriented design of Belt, {@link NumericRowReader}s do not perform as well as {@link
	 * NumericReader}s. Thus, whenever possible the use of {@link NumericReader}s is preferred.
	 *
	 * @param table
	 * 		the table to read
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public static NumericRowReader numericRowReader(Table table) {
		if (table == null) {
			throw new NullPointerException(MSG_NULL_TABLE);
		}
		return new NumericRowReader(table.getColumns());
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
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
	 * @throws NullPointerException
	 * 		if any of the given columns is {@code null}
	 */
	public static NumericRowReader numericRowReader(Column first, Column second, Column... other) {
		if (other == null) {
			throw new NullPointerException(MSG_NULL_OTHER_COLUMNS);
		}
		return new NumericRowReader(createColumnArray(first, second, other));
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link NumericRowReader}s do not perform as well as {@link
	 * NumericReader}s. Thus, whenever possible the use of {@link NumericReader}s is preferred.
	 *
	 * @param columns
	 * 		the columns to read
	 * @throws NullPointerException
	 * 		if the source column array is {@code null} or contains {@code null} columns
	 */
	public static NumericRowReader numericRowReader(Column[] columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_ARRAY);
		}
		for (Column column : columns) {
			if (column == null) {
				throw new NullPointerException(MSG_NULL_ARRAY_ELEMENT);
			}
		}
		return new NumericRowReader(columns);
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link NumericRowReader}s do not perform as well as {@link
	 * NumericReader}s. Thus, whenever possible the use of {@link NumericReader}s is preferred.
	 *
	 * @param columns
	 * 		the columns to read
	 * @throws NullPointerException
	 * 		if the column list is {@code null} or contains {@code null} columns
	 */
	public static NumericRowReader numericRowReader(List<Column> columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_LIST);
		}
		return numericRowReader(columns.toArray(new Column[0]));
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link MixedRowReader}s do not perform as well as column readers
	 * such as the {@link NumericReader}.
	 *
	 * @param table
	 * 		the table to read
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	public static MixedRowReader mixedRowReader(Table table) {
		if (table == null) {
			throw new NullPointerException(MSG_NULL_TABLE);
		}
		return new MixedRowReader(table.getColumns());
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
	 * @throws NullPointerException
	 * 		if any of the given columns is {@code null}
	 */
	public static MixedRowReader mixedRowReader(Column first, Column second, Column... other) {
		if (other == null) {
			throw new NullPointerException(MSG_NULL_OTHER_COLUMNS);
		}
		return new MixedRowReader(createColumnArray(first, second, other));
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link MixedRowReader}s do not perform as well as column readers
	 * such as the {@link NumericReader}.
	 *
	 * @param columns
	 * 		the columns to read
	 * @throws NullPointerException
	 * 		if the source column array is {@code null} or contains {@code null} columns
	 */
	public static MixedRowReader mixedRowReader(Column[] columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_ARRAY);
		}
		for (Column column: columns) {
			if (column == null) {
				throw new NullPointerException(MSG_NULL_ARRAY_ELEMENT);
			}
		}
		return new MixedRowReader(columns);
	}

	/**
	 * Creates a new reader for row-wise reads of the given columns.
	 *
	 * <p>Given the column-oriented design of Belt, {@link MixedRowReader}s do not perform as well as column readers
	 * such as the {@link NumericReader}.
	 *
	 * @param columns
	 * 		the columns to read
	 * @throws NullPointerException
	 * 		if the column list is {@code null} or contains {@code null} columns
	 */
	public static MixedRowReader mixedRowReader(List<Column> columns) {
		if (columns == null) {
			throw new NullPointerException(MSG_NULL_LIST);
		}
		return mixedRowReader(columns.toArray(new Column[0]));
	}

	/**
	 * Creates one column array from the given input, checks for {@code null} columns.
	 */
	private static Column[] createColumnArray(Column first, Column second, Column[] other) {
		if (first == null) {
			throw new NullPointerException("First column must not be null");
		}
		if (second == null) {
			throw new NullPointerException("Second Column must not be null");
		}
		if (other == null || other.length == 0) {
			return new Column[]{first, second};
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
			return allColumns;
		}
	}

	private Readers() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

}
