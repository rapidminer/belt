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

package com.rapidminer.belt.buffer;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.Capability;
import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Utility methods for creating mutable temporary buffers. These buffers can then be used to create immutable
 * {@link Column}s.
 *
 * @author Gisa Meier, Michael Knopf
 */
public final class Buffers {

	static {
		InternalBuffers internalBuffers = new InternalBuffers() {

			@Override
			public <T> UInt2CategoricalBuffer<T> newUInt2Buffer(int length) {
				return new UInt2CategoricalBuffer<>(length);
			}

			@Override
			public <T> UInt4CategoricalBuffer<T> newUInt4Buffer(int length) {
				return new UInt4CategoricalBuffer<>(length);
			}

			@Override
			public <T> UInt8CategoricalBuffer<T> newUInt8Buffer(int length) {
				return new UInt8CategoricalBuffer<>(length);
			}

			@Override
			public <T> UInt16CategoricalBuffer<T> newUInt16Buffer(int length) {
				return new UInt16CategoricalBuffer<>(length);
			}

			@Override
			public <T> Int32CategoricalBuffer<T> newInt32Buffer(int length) {
				return new Int32CategoricalBuffer<>(length);
			}
		};
		com.rapidminer.belt.table.BufferAccessor.set(internalBuffers);
		com.rapidminer.belt.transform.BufferAccessor.set(internalBuffers);
	}

	private static final String MSG_NULL_COLUMN = "Column must not be null";
	private static final String MSG_NULL_TYPE = "Type must not be null";
	private static final String MSG_NOT_NUMERIC_READABLE = "Column must be numeric readable";
	private static final String MSG_ILLEGAL_CAPACITY = "Illegal capacity: ";
	private static final String MSG_TYPE_INCOMPATIBLE_WITH = "Given type is incompatible with column of type: ";

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#REAL}. The buffer is
	 * initially filled with {@link Double#NaN}. If the buffer is completely overwritten anyway {@link #realBuffer(int,
	 * boolean)} can be used with initialize {@code false} instead.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NumericBuffer realBuffer(int length) {
		return realBuffer(length, true);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#REAL}. The buffer is
	 * initially filled with {@link Double#NaN} if initialize is {@code true}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param initialize
	 * 		if {@code true} the buffer is initially filled with missing values
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NumericBuffer realBuffer(int length, boolean initialize) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new RealBuffer(length, initialize);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#REAL} by copying the data from the given
	 * column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if he given column does not have the capability {@link Capability#NUMERIC_READABLE}
	 */
	public static NumericBuffer realBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.NUMERIC_READABLE)) {
			throw new IllegalArgumentException(MSG_NOT_NUMERIC_READABLE);
		}
		return new RealBuffer(column);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#INTEGER}. The buffer is
	 * initially filled with {@link Double#NaN}. If the buffer is completely overwritten anyway {@link #realBuffer(int,
	 * boolean)} can be used with initialize {@code false} instead.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NumericBuffer integerBuffer(int length) {
		return integerBuffer(length, true);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#INTEGER}. The buffer is
	 * initially filled with {@link Double#NaN} is initialize is {@code true}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param initialize
	 * 		if {@code true} the buffer is initially filled with missing values
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NumericBuffer integerBuffer(int length, boolean initialize) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new IntegerBuffer(length, initialize);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#INTEGER} by copying the data from the given
	 * column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the given column does not have the capability {@link Capability#NUMERIC_READABLE}
	 */
	public static NumericBuffer integerBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.NUMERIC_READABLE)) {
			throw new IllegalArgumentException(MSG_NOT_NUMERIC_READABLE);
		}
		return new IntegerBuffer(column);
	}

	/**
	 * Creates a buffer of the given length to create a column of category {@link Category#OBJECT}. The buffer is
	 * initially filled with {@code null} (missing value).
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param <T>
	 * 		the element type
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public static <T> ObjectBuffer<T> objectBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new ObjectBuffer<>(length);
	}

	/**
	 * Creates a buffer to create a {@link Column} of category {@link Category#OBJECT} by copying the data from the given
	 * column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @param type
	 * 		the element type
	 * @param <T>
	 * 		the element type
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the given column does not have the capability {@link Capability#OBJECT_READABLE} or its elements cannot
	 * 		be cast to the given type
	 */
	public static <T> ObjectBuffer<T> objectBuffer(Column column, Class<T> type) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.OBJECT_READABLE)) {
			throw new IllegalArgumentException("Column must be object readable");
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		if (!type.isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		return new ObjectBuffer<>(type, column);
	}

	/**
	 * Creates a buffer of the given size to create a {@link Column} of type id {@link TypeId#DATE_TIME}. The buffer is
	 * initially filled with missing values. If the buffer is completely overwritten anyway {@link #realBuffer(int,
	 * boolean)} can be used with initialize {@code false} instead.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param subSecondPrecision
	 * 		whether the buffer should have sub-second precision (nanoseconds of the day)
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static DateTimeBuffer dateTimeBuffer(int length, boolean subSecondPrecision) {
		return dateTimeBuffer(length, subSecondPrecision, true);
	}

	/**
	 * Creates a buffer of the given size to create a {@link Column} of type id {@link TypeId#DATE_TIME}. The buffer is
	 * initially filled with missing values if initialize is {@code true}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param subSecondPrecision
	 * 		whether the buffer should have sub-second precision (nanoseconds of the day)
	 * @param initialize
	 * 		if {@code true} the buffer is initially filled with missing values
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static DateTimeBuffer dateTimeBuffer(int length, boolean subSecondPrecision, boolean initialize) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		if (subSecondPrecision) {
			return new NanosecondDateTimeBuffer(length, initialize);
		} else {
			return new SecondDateTimeBuffer(length, initialize);
		}
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#DATE_TIME} by copying the data from the given
	 * column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column does not contain date-time data
	 */
	public static DateTimeBuffer dateTimeBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (column.type().id() != TypeId.DATE_TIME) {
			throw new IllegalArgumentException("Column must be of type date-time");
		}
		if (column instanceof DateTimeColumn) {
			DateTimeColumn dateTime = (DateTimeColumn) column;
			if (dateTime.hasSubSecondPrecision()) {
				return new NanosecondDateTimeBuffer(column);
			} else {
				return new SecondDateTimeBuffer(column);
			}
		}
		throw new AssertionError();
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#TIME}. The buffer is
	 * initially filled with missing values. If the buffer is completely overwritten anyway {@link #realBuffer(int,
	 * boolean)} can be used with initialize {@code false} instead.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static TimeBuffer timeBuffer(int length) {
		return timeBuffer(length, true);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#TIME}. The buffer is
	 * initially filled with missing values if initialize is {@code true}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param initialize
	 * 	 	if {@code true} the buffer is initially filled with missing values
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static TimeBuffer timeBuffer(int length, boolean initialize) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new TimeBuffer(length, initialize);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#TIME} by copying the data from the given
	 * column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not contain time data
	 */
	public static TimeBuffer timeBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (column.type().id() != TypeId.TIME) {
			throw new IllegalArgumentException("Column must be of type time");
		}
		return new TimeBuffer(column);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of category {@link Category#CATEGORICAL}. The
	 * buffer supports the maximum number of categories supported by Belt ({@value Integer#MAX_VALUE}). The buffer is
	 * initially filled with {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * <p>Please note, that is recommended to use {@link #categoricalBuffer(int, int)} instead whenever the number of
	 * categories is known in advance.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param <T>
	 * 		the element type
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static <T> CategoricalBuffer<T> categoricalBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new Int32CategoricalBuffer<>(length);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of category {@link Category#CATEGORICAL} with at
	 * most the given number of categories. Trying to use the buffer with more than the specified number of categories
	 * might result in an error. The buffer is initially filled with {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * @param length
	 * 		the size of the buffer
	 * @param categories
	 * 		the number of categories
	 * @param <T>
	 * 		the element type
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size or number of categories is negative
	 */
	public static <T> CategoricalBuffer<T> categoricalBuffer(int length, int categories) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		if (categories < 0) {
			throw new IllegalArgumentException("Illegal number of categories: " + categories);
		}
		Format minimal = Format.findMinimal(categories);
		switch (minimal) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				// Do not use packed buffers (smaller that 8Bit) here, since the corresponding buffers are only
				// partially thread-safe.
				return new UInt8CategoricalBuffer<>(length, minimal);
			case UNSIGNED_INT16:
				return new UInt16CategoricalBuffer<>(length);
			case SIGNED_INT32:
			default:
				return new Int32CategoricalBuffer<>(length);
		}
	}

	/**
	 * Creates a buffer to create a {@link Column} of category {@link Category#CATEGORICAL} by copying the data from the
	 * given column. The buffer supports the maximum number of categories supported by Belt
	 * ({@value Integer#MAX_VALUE}). The buffer is initially filled with {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * <p>Please note, that is recommended to use {@link #categoricalBuffer(Column, Class, int)} instead whenever the
	 * number of categories is known in advance.
	 *
	 * @param column
	 * 		the column to derive the buffer from
	 * @param type
	 * 		the element type
	 * @param <T>
	 * 		the element type
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical or if the specified type is incompatible with the given column
	 */
	public static <T> CategoricalBuffer<T> categoricalBuffer(Column column, Class<T> type) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!Category.CATEGORICAL.equals(column.type().category())) {
			throw new IllegalArgumentException("Column must be categorical");
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		if (!type.isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		return new Int32CategoricalBuffer<>(column, type);
	}

	/**
	 * Creates a buffer to create a {@link Column} of category {@link Category#CATEGORICAL} with at most the given
	 * number of categories by copying the data from the given column. Trying to use the buffer with more than the
	 * specified number of categories might result in an error. The buffer is initially filled with
	 * {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * @param column
	 * 		the column to derive the buffer from
	 * @param type
	 * 		the element type
	 * @param categories
	 * 		the number of categories
	 * @param <T>
	 * 		the element type
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical, if the specified type is incompatible with the given column, or if the
	 * 		given number of categories is negative
	 */
	public static <T> CategoricalBuffer<T> categoricalBuffer(Column column, Class<T> type, int categories) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!Category.CATEGORICAL.equals(column.type().category())) {
			throw new IllegalArgumentException("Column must be categorical");
		}
		if (type == null) {
			throw new NullPointerException(MSG_NULL_TYPE);
		}
		if (!type.isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		if (categories < 0) {
			throw new IllegalArgumentException("Illegal number of categories: " + categories);
		}
		int existingCategories = column.getDictionary(type).maximalIndex() + 1;
		Format minimal = Format.findMinimal(Integer.max(existingCategories, categories));
		switch (minimal) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				// Do not use packed buffers (smaller that 8Bit) here, since the corresponding buffers are only
				// partially thread-safe.
				return new UInt8CategoricalBuffer<>(column, type, minimal);
			case UNSIGNED_INT16:
				return new UInt16CategoricalBuffer<>(column, type);
			case SIGNED_INT32:
			default:
				return new Int32CategoricalBuffer<>(column, type);
		}
	}

	private Buffers() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}


	/**
	 * Buffer creation methods for internal access.
	 */
	public abstract static class InternalBuffers {
		/**
		 * Creates a new uint2 categorical buffer of the given length.
		 */
		public abstract <T> UInt2CategoricalBuffer<T> newUInt2Buffer(int length);

		/**
		 * Creates a new uint4 categorical buffer of the given length.
		 */
		public abstract <T> UInt4CategoricalBuffer<T> newUInt4Buffer(int length);

		/**
		 * Creates a new uint8 categorical buffer of the given length.
		 */
		public abstract <T> UInt8CategoricalBuffer<T> newUInt8Buffer(int length);

		/**
		 * Creates a new uint16 categorical buffer of the given length.
		 */
		public abstract <T> UInt16CategoricalBuffer<T> newUInt16Buffer(int length);

		/**
		 * Creates a new int32 categorical buffer of the given length.
		 */
		public abstract <T> Int32CategoricalBuffer<T> newInt32Buffer(int length);

		private InternalBuffers(){}
	}

}
