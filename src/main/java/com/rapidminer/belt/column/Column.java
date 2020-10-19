/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
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

package com.rapidminer.belt.column;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.rapidminer.belt.column.type.StringList;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.util.Order;


/**
 * An immutable column for use in a {@link Table}.
 *
 * @author Michael Knopf, Gisa Meier
 */
public abstract class Column {

	/**
	 * Types of columns. Can be used to identify and create columns, e.g., via
	 * {@link com.rapidminer.belt.table.Writers#mixedRowWriter(List, List, boolean)}.
	 */
	public enum TypeId {

		/**
		 * 64 bit double values. Data can be accessed, for example, via {@link Readers#numericReader(Column)}.
		 */
		REAL("Real"),

		/**
		 * 64 bit double values with no fractional digits. Data can be accessed, for example, via {@link
		 * Readers#numericReader(Column)}.
		 * <p>
		 * Please note: The maximum / minimum value that can be stored without loss of information is {@code +/-
		 * 2^53-1}. Therefore, casting this column's values to {@code int} leads to loss of information for values
		 * smaller / larger than {@link Integer#MIN_VALUE} / {@link Integer#MAX_VALUE}.
		 */
		INTEGER_53_BIT("Integer"),

		/**
		 * 64 bit UTC timestamps (seconds), and 32 bit fraction of a second (nanoseconds, optional). Data can be read as
		 * {@link java.time.Instant}. This can be done, for example, via {@link Readers#objectReader(Column, Class)}.
		 */
		DATE_TIME("Date-Time"),

		/**
		 * Nanoseconds of a day. Data can be read numeric or as {@link java.time.LocalTime}. This can be done, for
		 * example, via {@link Readers#numericReader(Column)} or {@link Readers#objectReader(Column, Class)}.
		 */
		TIME("Time"),

		/**
		 * Categorical strings. Data can be read categorical or as {@link String}. This can be done, for example, via
		 * {@link Readers#categoricalReader(Column)} or {@link Readers#objectReader(Column, Class)}.
		 */
		NOMINAL("Nominal"),

		/**
		 * Non-categorical strings. Data can be read as {@link String}. This can be done, for example, via {@link
		 * Readers#objectReader(Column, Class)}.
		 */
		TEXT("Text"),

		/**
		 * Sets of strings. Data can be read as {@link StringSet}. This can be done, for example, via {@link
		 * Readers#objectReader(Column, Class)}.
		 */
		TEXT_SET("Text-Set"),

		/**
		 * List of strings. Data can be read as {@link StringList}. This can be done, for example, via {@link
		 * Readers#objectReader(Column, Class)}.
		 */
		TEXT_LIST("Text-List");

		private final String displayName;

		TypeId(String name) {
			this.displayName = name;
		}

		@Override
		public String toString() {
			return displayName;
		}
	}

	public enum Category {

		/**
		 * Columns of non-unique integer indices paired with an index mapping to a complex type. Columns of this
		 * category are always numeric-readable (indices interpreted as double), category-readable (integer indices that
		 * can be translated to objects via the mapping) and object-readable (indices resolved to objects via the
		 * mapping). For example, Nominal columns are categorical mapping integer indices to arbitrary strings.
		 */
		CATEGORICAL,

		/**
		 * Columns of numeric data, such as real or integer. Columns of this category are always numeric-readable but
		 * not object-readable.
		 */
		NUMERIC,


		/**
		 * Columns of complex types such as instants of time. Columns of this category are always object-readable but
		 * usually not numeric-readable.
		 */
		OBJECT
	}

	public enum Capability {

		/**
		 * Can be read as double values, via row or column reader.
		 */
		NUMERIC_READABLE,

		/**
		 * Can be read as objects, via row or column reader.
		 */
		OBJECT_READABLE,

		/**
		 * The column can be sorted.
		 */
		SORTABLE;

	}

	private final int size;

	Column(int size) {
		this.size = size;
	}

	/**
	 * Returns the size.
	 *
	 * @return the capacity of the column
	 */
	public final int size() {
		return size;
	}

	/**
	 * Returns the {@link ColumnType} of this column.
	 *
	 * @return the column type
	 */
	public abstract ColumnType<?> type();


	/**
	 * Creates a new column with rows reordered according to the given row selection. If the row selection contains
	 * invalid indices (i.e., values outside of the range {@code [0, size())}), the new column will contain missing
	 * value at that place. In particular, sub- and supersets, as well as duplicate indices are supported.
	 *
	 * @param rows
	 * 		the rows to select
	 * @param view
	 * 		if this is {@code true} the data will not be copied, only a view will be attached. Otherwise a heuristic is
	 * 		applied that might decide to copy the rows into a new column.
	 * @return a column consisting only of those rows selected by the rows parameter
	 * @throws NullPointerException
	 * 		if rows it {@code null}
	 */
	public Column rows(int[] rows, boolean view) {
		Objects.requireNonNull(rows, "Row index array must not be null");
		return map(Arrays.copyOf(rows, rows.length), view);
	}

	/**
	 * Creates a new column with values reordered according to the given index mapping. If the mapping contains invalid
	 * indices (i.e., values outside of the range {@code [0, size())}), the new column will contain missing value at
	 * that place. In particular, sub- and supersets, as well as duplicate indices are supported.
	 *
	 * @param mapping
	 * 		the index mapping
	 * @param preferView
	 * 		whether a view is preferred over a deep copy
	 * @return a mapped column
	 */
	abstract Column map(int[] mapping, boolean preferView);

	/**
	 * Fills the given array with column data starting with the given row index. Throws an {@link
	 * UnsupportedOperationException} if this column has not the capability {@link Capability#NUMERIC_READABLE}.
	 * <p>
	 * Please note that accessing rows outside of the columns bounds will lead to undefined results.
	 *
	 * @param array
	 * 		the target array
	 * @param rowIndex
	 * 		the row index
	 * @throws NullPointerException
	 * 		if the array is {@code null}
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the row index is negative
	 */
	public void fill(double[] array, int rowIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given array with column data starting with the given row index and using the given array offset and
	 * step size. For instance, given the row index {@code 256}, the array offset {@code 4} and a step size of {@code
	 * 8}, the method writes the values at index {@code 256, 257, 258, ...} to array positions {@code array[4],
	 * array[12], array[20], ...} respectively. Throws an {@link UnsupportedOperationException} if this column has not
	 * the capability {@link Capability#NUMERIC_READABLE}.
	 * <p>
	 * Please note that accessing rows outside of the columns bounds will lead to undefined results.
	 *
	 * @param array
	 * 		the target array
	 * @param rowIndex
	 * 		the row index
	 * @param arrayOffset
	 * 		the offset in the array
	 * @param arrayStepSize
	 * 		the positive step size in between values
	 * @throws NullPointerException
	 * 		if the array is {@code null}
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the row index or the array offset is negative
	 * @throws IllegalArgumentException
	 * 		if the array step size is smaller than {@code 1}
	 */
	public void fill(double[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given array with column data starting with the given row index. Throws an {@link
	 * UnsupportedOperationException} if the category of this column is not {@link Category#CATEGORICAL}.
	 * <p>
	 * Please note that accessing rows outside of the columns bounds will lead to undefined results.
	 *
	 * @param array
	 * 		the target array
	 * @param rowIndex
	 * 		the row index
	 * @throws NullPointerException
	 * 		if the array is {@code null}
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the row index is negative
	 */
	public void fill(int[] array, int rowIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given array with column data starting with the given row index and using the given array offset and
	 * step size. For instance, given the row index {@code 256}, the array offset {@code 4} and a step size of {@code
	 * 8}, the method writes the values at index {@code 256, 257, 268, ...} to array positions {@code array[4],
	 * array[12], array[20], ...} respectively. Throws an {@link UnsupportedOperationException} if the category of this
	 * column is not {@link Category#CATEGORICAL}.
	 * <p>
	 * Please note that accessing rows outside of the columns bounds will lead to undefined results.
	 *
	 * @param array
	 * 		the target array
	 * @param rowIndex
	 * 		the row index
	 * @param arrayOffset
	 * 		the offset in the array
	 * @param arrayStepSize
	 * 		the step size in between values
	 * @throws NullPointerException
	 * 		if the array is {@code null}
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the row index or the array offset is negative
	 * @throws IllegalArgumentException
	 * 		if the array step size is smaller than {@code 1}
	 */
	public void fill(int[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given array with column data starting with the given row index. Throws an {@link
	 * UnsupportedOperationException} if this column has not the capability {@link Capability#OBJECT_READABLE}.
	 *
	 * @param array
	 * 		the target array
	 * @param rowIndex
	 * 		the row index
	 * @throws NullPointerException
	 * 		if the array is {@code null}
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the row index is negative
	 */
	public void fill(Object[] array, int rowIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given array with column data starting with the given row index and using the given array offset and
	 * step size. For instance, given the row index {@code 256}, the array offset {@code 4} and a step size of {@code
	 * 8}, the method writes the values at index {@code 256, 257, 268, ...} to array positions {@code array[4],
	 * array[12], array[20], ...} respectively. Throws an {@link UnsupportedOperationException} if this column has not
	 * the capability {@link Capability#OBJECT_READABLE}.
	 *
	 * @param array
	 * 		the target array
	 * @param rowIndex
	 * 		the row index
	 * @param arrayOffset
	 * 		the offset in the array
	 * @param arrayStepSize
	 * 		the step size in between values
	 * @throws NullPointerException
	 * 		if the array is {@code null}
	 * @throws ArrayIndexOutOfBoundsException
	 * 		if the row index or the array offset is negative
	 * @throws IllegalArgumentException
	 * 		if the array step size is smaller than {@code 1}
	 */
	public void fill(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Returns the dictionary of the categorical column. Throws an {@link UnsupportedOperationException} except if the
	 * category of this column is {@link Category#CATEGORICAL}.
	 *
	 * @return the dictionary
	 */
	public Dictionary getDictionary() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Sorts the column indirectly using the given sorting {@link Order}. This method does not modify the column
	 * itself.
	 * Instead, it computes and returns a row index reordering that if applied to the column results in a sorted
	 * sequence.
	 * Throws an {@link UnsupportedOperationException} if the column has not the capability {@link Capability#SORTABLE}.
	 *
	 * @param order
	 * 		the sorting order
	 * @return the the index mapping resulting in a sorted sequence
	 * @throws NullPointerException
	 * 		if order is {@code null}
	 */
	public int[] sort(Order order) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Strips the data from the column so that it has length 0 and no mapping or remapping.
	 *
	 * @return a column of the same type, with the same dictionary but with length 0
	 */
	public abstract Column stripData();

}
