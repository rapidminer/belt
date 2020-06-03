/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2020 RapidMiner GmbH
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


import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.table.Table;


/**
 * Utility methods for reading only small parts of one or more {@link Column}s.
 *
 * <p>The methods in this class should only be used if there are special reasons that the methods from {@link Readers}
 * cannot be used instead. When reading a whole column or table the {@link Readers} methods are more performant.
 *
 * @author Gisa Meier
 */
public final class SmallReaders {

	private static final String MSG_NULL_TABLE = "Table must not be null";
	private static final String MSG_NULL_COLUMN = "Column must not be null";
	private static final String MSG_NULL_TYPE = "Type must not be null";

	/**
	 * Creates a new reader for row-wise reads of a single or a handful of rows of all the columns in the given table.
	 * All columns must be {@link Column.Capability#NUMERIC_READABLE}.
	 *
	 * <p>This is only intended for a few reads and will be much slower than {@link Readers#numericRowReader(Table)}
	 * in most cases.
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
	 * <p>This is only intended for a few reads and will be much slower than {@link Readers#numericRowReader(Table)}
	 * in most cases.
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
	 * Creates a new numeric column reader with a small internal buffer. Use this if you need to create several {@link
	 * NumericReader} at the same time and you cannot use a {@link NumericRowReader} via {@link
	 * Readers#numericRowReader}, which should be preferred.
	 *
	 * @param column
	 * 		the column to read, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @return a reader for a numeric-readable column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 */
	public static NumericReader smallNumericReader(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		return new NumericReader(column, NumericReader.SMALL_BUFFER_SIZE, column.size());
	}

	/**
	 * Creates a new numeric column reader with a minimal internal buffer. Use this if you need to use multiple
	 * calls to
	 * {@link NumericReader#setPosition(int)} with positions that are not known beforehand. If the positions are known
	 * in advance, calling {@link Table#rows(int[], Context)} with the expected rows first and then using {@link
	 * Readers#numericReader(Column)} should be preferred.
	 *
	 * @param column
	 * 		the column to read, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @return a reader for a numeric-readable column
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 */
	public static NumericReader unbufferedNumericReader(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		return new NumericReader(column, NumericReader.MIN_BUFFER_SIZE, column.size());
	}

	/**
	 * Creates a new object column reader of the given type with a small internal buffer. Use this if you need to
	 * create
	 * several {@link ObjectReader} at the same time and you cannot use a row reader, which should be preferred.
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
	public static <T> ObjectReader<T> smallObjectReader(Column column, Class<T> type) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		return new ObjectReader<>(column, type, NumericReader.SMALL_BUFFER_SIZE, column.size());
	}

	/**
	 * Creates a new object column reader of the given type with a minimal internal buffer. Use this if you need to use
	 * multiple calls to {@link ObjectReader#setPosition(int)} with positions that are not known beforehand. If the
	 * positions are known in advance, calling {@link Table#rows(int[], Context)} with the expected rows first and then
	 * using {@link Readers#numericReader(Column)} should be preferred.
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
	public static <T> ObjectReader<T> unbufferedObjectReader(Column column, Class<T> type) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		return new ObjectReader<>(column, type, NumericReader.MIN_BUFFER_SIZE, column.size());
	}

	private SmallReaders() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

}
