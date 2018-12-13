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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Set;

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
		SORTABLE,

		/**
		 * The column can associate {@code true} and {@code false} to its values (and {@code false} to missing values).
		 */
		BOOLEAN;

	}

	private final Set<Capability> capabilities;
	private final int size;

	Column(Set<Capability> capabilities, int size) {
		this.capabilities = capabilities;
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
	 * Returns the category of this column.
	 *
	 * @param capability the capability to check
	 * @return {@code true} if the column has the given category, {@code false} otherwise
	 */
	public final boolean hasCapability(Capability capability) {
		return capabilities.contains(capability);
	}


	/**
	 * Creates a new column with values reordered according to the given index mapping. The mapping must contain valid
	 * indices only (i.e., values in the range {@code [0, size())}), but is otherwise without restrictions. In
	 * particular, sub- and supersets, as well as duplicate indices are supported.
	 *
	 * @param mapping
	 * 		the index mapping
	 * @param preferView
	 * 		whether a view is preferred over a deep copy
	 * @return a mapped column
	 */
	abstract Column map(int[] mapping, boolean preferView);

	/**
	 * Writes the data into the channel starting at the given position. Returns the next start position.
	 *
	 * @param channel
	 * 		the channel into which to write
	 * @param startPosition
	 * 		where to start writing into the channel
	 * @return the position for writing next
	 * @throws IOException
	 * 		if writing to the channel fails
	 */
	abstract long writeToChannel(FileChannel channel, long startPosition) throws IOException;

	/**
	 * Fills the given buffer with column data starting with the given row index. Throws an {@link
	 * UnsupportedOperationException} if this column has not the capability {@link Capability#NUMERIC_READABLE}.
	 *
	 * @param buffer
	 * 		the target buffer
	 * @param rowIndex
	 * 		the row index
	 */
	void fill(double[] buffer, int rowIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given buffer with column data starting with the given row index and using the given buffer offset and
	 * step size. For instance, given the row index {@code 256}, the buffer offset {@code 4} and a step size of {@code
	 * 8}, the method writes the values at index {@code 256, 257, 268, ...} to buffer positions {@code buffer[4],
	 * buffer[12], buffer[20], ...} respectively. Throws an {@link UnsupportedOperationException} if this column has not
	 * the capability {@link Capability#NUMERIC_READABLE}.
	 *
	 * @param buffer
	 * 		the target buffer
	 * @param rowIndex
	 * 		the row index
	 * @param bufferOffset
	 * 		the offset in the buffer
	 * @param bufferStepSize
	 * 		the step size in between values
	 */
	void fill(double[] buffer, int rowIndex, int bufferOffset, int bufferStepSize) {
		throw new UnsupportedOperationException();
	}


	/**
	 * Returns an immutable list containing the dictionary of the categorical column. Throws an {@link
	 * UnsupportedOperationException} except if the category of this column is {@link Category#CATEGORICAL}.
	 *
	 * @param elementType
	 * 		the desired element type, must be a super type of the element type of this column
	 * @param <T>
	 * 		the type of the returned list
	 * @return the dictionary
	 */
	public <T> List<T> getDictionary(Class<T> elementType) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given buffer with column data starting with the given row index. Throws an {@link
	 * UnsupportedOperationException} if the category of this column is not {@link Category#CATEGORICAL}.
	 *
	 * @param buffer
	 * 		the target buffer
	 * @param rowIndex
	 * 		the row index
	 */
	void fill(int[] buffer, int rowIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given buffer with column data starting with the given row index and using the given buffer offset and
	 * step size. For instance, given the row index {@code 256}, the buffer offset {@code 4} and a step size of {@code
	 * 8}, the method writes the values at index {@code 256, 257, 268, ...} to buffer positions {@code buffer[4],
	 * buffer[12], buffer[20], ...} respectively. Throws an {@link UnsupportedOperationException} if the category of this
	 * column is not {@link Category#CATEGORICAL}.
	 *
	 * @param buffer
	 * 		the target buffer
	 * @param rowIndex
	 * 		the row index
	 * @param bufferOffset
	 * 		the offset in the buffer
	 * @param bufferStepSize
	 * 		the step size in between values
	 */
	void fill(int[] buffer, int rowIndex, int bufferOffset, int bufferStepSize) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given buffer with column data starting with the given row index. Throws an {@link
	 * UnsupportedOperationException} if this column has not the capability {@link Capability#OBJECT_READABLE}.
	 *
	 * @param buffer
	 * 		the target buffer
	 * @param rowIndex
	 * 		the row index
	 */
	void fill(Object[] buffer, int rowIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Fills the given buffer with column data starting with the given row index and using the given buffer offset and
	 * step size. For instance, given the row index {@code 256}, the buffer offset {@code 4} and a step size of {@code
	 * 8}, the method writes the values at index {@code 256, 257, 268, ...} to buffer positions {@code buffer[4],
	 * buffer[12], buffer[20], ...} respectively. Throws an {@link UnsupportedOperationException} if this column has not
	 * the capability {@link Capability#OBJECT_READABLE}.
	 *
	 * @param buffer
	 * 		the target buffer
	 * @param rowIndex
	 * 		the row index
	 * @param bufferOffset
	 * 		the offset in the buffer
	 * @param bufferStepSize
	 * 		the step size in between values
	 */
	void fill(Object[] buffer, int rowIndex, int bufferOffset, int bufferStepSize) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Sorts the column indirectly using the given sorting {@link Order}. This method does not modify the column itself.
	 * Instead, it computes and returns an index mapping that if applied to the column results in a sorted sequence.
	 * Throws an {@link UnsupportedOperationException} except if the column has not the capability {@link
	 * Capability#SORTABLE}.
	 *
	 * @param order
	 * 		the sorting order
	 * @return the the index mapping resulting in a sorted sequence
	 */
	int[] sort(Order order) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Converts the given index to a boolean.
	 *
	 * @param categoryIndex
	 * 		the category index (of a categorical column)
	 * @return {@code true} or {@code false} for non-null category indices, {@code false} for 0
	 */
	public boolean toBoolean(int categoryIndex) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Converts the given column entry value to a boolean.
	 *
	 * @param value
	 * 		the value to check
	 * @return {@code true} or {@code false} for non-missing values, {@code false} for {@code Double.NaN}
	 */
	public boolean toBoolean(double value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Converts the given column entry object to a boolean.
	 *
	 * @param value
	 * 		the object value to check
	 * @return {@code true} or {@code false} for non-null objects, {@code false} for {@code null}
	 */
	public boolean toBoolean(Object value) {
		throw new UnsupportedOperationException();
	}

}
