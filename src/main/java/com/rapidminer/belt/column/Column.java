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

package com.rapidminer.belt.column;

import java.util.Arrays;
import java.util.Objects;

import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.util.Order;


/**
 * An immutable column for use in a {@link Table}.
 *
 * @author Michael Knopf, Gisa Meier
 */
public abstract class Column {

	/**
	 * Types of columns
	 */
	public enum TypeId {

		/**
		 * Double values.
		 */
		REAL("Real"),

		/**
		 * Double values with no fractional digits.
		 */
		INTEGER("Integer"),

		/**
		 * 64Bit UTC timestamps (seconds), and 32Bit fraction of a second (nanoseconds, optional).
		 */
		DATE_TIME("Date-Time"),

		/**
		 * 64Bit nanoseconds of a day.
		 */
		TIME("Time"),

		/**
		 * Categorical strings.
		 */
		NOMINAL("Nominal"),

		/**
		 * Not builtin id.
		 */
		CUSTOM("Custom");

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
	 * 8}, the method writes the values at index {@code 256, 257, 268, ...} to array positions {@code array[4],
	 * array[12], array[20], ...} respectively. Throws an {@link UnsupportedOperationException} if this column has not
	 * the capability {@link Capability#NUMERIC_READABLE}.
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
	 * array[12], array[20], ...} respectively. Throws an {@link UnsupportedOperationException} if the category of
	 * this column is not {@link Category#CATEGORICAL}.
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
	 * @param elementType
	 * 		the desired element type, must be a super type of the element type of this column
	 * @param <T>
	 * 		the type of the returned dictionary
	 * @return the dictionary
	 */
	public <T> Dictionary<T> getDictionary(Class<T> elementType) {
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
