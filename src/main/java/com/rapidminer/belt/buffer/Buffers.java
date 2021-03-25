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

package com.rapidminer.belt.buffer;

import java.time.Instant;
import java.time.LocalTime;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.Capability;
import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.type.StringList;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Utility methods for creating mutable temporary buffers. These buffers can then be used to create immutable {@link
 * Column}s.
 *
 * @author Gisa Meier, Michael Knopf, Kevin Majchrzak
 */
public final class Buffers {

	private static final String ILLEGAL_NUMBER_OF_CATEGORIES = "Illegal number of categories: ";

	static {
		InternalBuffers internalBuffers = new InternalBuffers() {

			@Override
			public UInt2NominalBuffer newUInt2Buffer(ColumnType<String> type, int length) {
				return new UInt2NominalBuffer(type, length);
			}

			@Override
			public UInt4NominalBuffer newUInt4Buffer(ColumnType<String> type, int length) {
				return new UInt4NominalBuffer(type, length);
			}

			@Override
			public UInt8NominalBuffer newUInt8Buffer(ColumnType<String> type, int length, Format targetFormat) {
				return new UInt8NominalBuffer(type, length, targetFormat);
			}

			@Override
			public UInt8NominalBuffer newUInt8Buffer(ColumnType<String> type, int length) {
				return new UInt8NominalBuffer(type, length);
			}

			@Override
			public UInt16NominalBuffer newUInt16Buffer(ColumnType<String> type, int length) {
				return new UInt16NominalBuffer(type, length);
			}

			@Override
			public Int32NominalBuffer newInt32Buffer(ColumnType<String> type, int length) {
				return new Int32NominalBuffer(type, length);
			}

			@Override
			public <T> ObjectBuffer<T> newObjectBuffer(ColumnType<T> type, int length) {
				return new ObjectBuffer<>(type, length);
			}

		};
		com.rapidminer.belt.table.BufferAccessor.set(internalBuffers);
		com.rapidminer.belt.transform.BufferAccessor.set(internalBuffers);
	}

	private static final String MSG_NULL_COLUMN = "Column must not be null";
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
	 * Creates a sparse buffer of the given length to create a memory efficient sparse {@link Column} of type {@link
	 * TypeId#REAL}. The buffer is initially filled with the given default value.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for data with a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static RealBufferSparse sparseRealBuffer(double defaultValue, int length) {
		return new RealBufferSparse(defaultValue, length);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#INTEGER_53_BIT}. The buffer is
	 * initially filled with {@link Double#NaN}. If the buffer is completely overwritten anyway {@link #realBuffer(int,
	 * boolean)} can be used with initialize {@code false} instead.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NumericBuffer integer53BitBuffer(int length) {
		return integer53BitBuffer(length, true);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#INTEGER_53_BIT}. The buffer is
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
	public static NumericBuffer integer53BitBuffer(int length, boolean initialize) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new Integer53BitBuffer(length, initialize);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#INTEGER_53_BIT} by copying the data from the given
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
	public static NumericBuffer integer53BitBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.NUMERIC_READABLE)) {
			throw new IllegalArgumentException(MSG_NOT_NUMERIC_READABLE);
		}
		return new Integer53BitBuffer(column);
	}

	/**
	 * Creates a memory efficient sparse buffer of the given length to create a sparse {@link Column} of type {@link
	 * TypeId#INTEGER_53_BIT}. The buffer is initially filled with the given default value.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for data with a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static Integer53BitBufferSparse sparseInteger53BitBuffer(double defaultValue, int length) {
		return new Integer53BitBufferSparse(defaultValue, length);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#TEXT}. The buffer is
	 * initially filled with {@code null} (missing value).
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public static ObjectBuffer<String> textBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new ObjectBuffer<>(ColumnType.TEXT, length);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#TEXT} by copying the data from the
	 * given column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the given column does not have the capability {@link Capability#OBJECT_READABLE} or its elements cannot be
	 * 		cast to {@link String}
	 */
	public static ObjectBuffer<String> textBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.OBJECT_READABLE)) {
			throw new IllegalArgumentException("Column must be object readable");
		}
		if (!ColumnType.TEXT.elementType().isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		return new ObjectBuffer<>(ColumnType.TEXT, column);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#TEXT_SET}. The buffer is
	 * initially filled with {@code null} (missing value).
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public static ObjectBuffer<StringSet> textsetBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new ObjectBuffer<>(ColumnType.TEXTSET, length);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#TEXT_SET} by copying the data from the
	 * given column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the given column does not have the capability {@link Capability#OBJECT_READABLE} or its elements cannot be
	 * 		cast to {@link StringSet}
	 */
	public static ObjectBuffer<StringSet> textsetBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.OBJECT_READABLE)) {
			throw new IllegalArgumentException("Column must be object readable");
		}
		if (!ColumnType.TEXTSET.elementType().isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		return new ObjectBuffer<>(ColumnType.TEXTSET, column);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#TEXT_LIST}. The buffer is
	 * initially filled with {@code null} (missing value).
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public static ObjectBuffer<StringList> textlistBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new ObjectBuffer<>(ColumnType.TEXTLIST, length);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#TEXT_LIST} by copying the data from the
	 * given column.
	 *
	 * @param column
	 * 		the column to create a copy of
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if the given column is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the given column does not have the capability {@link Capability#OBJECT_READABLE} or its elements cannot be
	 * 		cast to {@link StringList}
	 */
	public static ObjectBuffer<StringList> textlistBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!column.type().hasCapability(Capability.OBJECT_READABLE)) {
			throw new IllegalArgumentException("Column must be object readable");
		}
		if (!ColumnType.TEXTLIST.elementType().isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		return new ObjectBuffer<>(ColumnType.TEXTLIST, column);
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
	 * Creates a memory efficient sparse buffer of the given size to create a sparse {@link Column} of type id {@link
	 * TypeId#DATE_TIME}. The buffer is initially filled with the default seconds value. Nanoseconds are initialized to
	 * zero. Iff subSecondPrecision is false nanosecond inputs will be ignored by the buffer.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for data with a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultSeconds
	 * 		the (usually most common) default second value in the data.
	 * @param length
	 * 		the length of the buffer
	 * @param subSecondPrecision
	 *        {@code true} when the buffer should have sub-second precision (nanoseconds of the day) and false otherwise.
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static DateTimeBufferSparse sparseDateTimeBuffer(long defaultSeconds, int length, boolean subSecondPrecision) {
		if (subSecondPrecision) {
			return new NanoDateTimeBufferSparse(defaultSeconds, length);
		} else {
			return new SecondDateTimeBufferSparse(defaultSeconds, length);
		}
	}

	/**
	 * Creates a memory efficient sparse buffer of the given size to create a sparse {@link Column} of type id {@link
	 * TypeId#DATE_TIME}. The buffer is initially filled with the default seconds value. Nanoseconds are initialized to
	 * zero. Iff subSecondPrecision is false nanosecond inputs will be ignored by the buffer.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for data with a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultSeconds
	 * 		the (usually most common) default second value in the data represented via an {@link Instant}. The nanoseconds
	 * 		will be ignored as they are not stored sparsely.
	 * @param length
	 * 		the length of the buffer
	 * @param subSecondPrecision
	 *        {@code true} when the buffer should have sub-second precision (nanoseconds of the day) and false otherwise.
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static DateTimeBufferSparse sparseDateTimeBuffer(Instant defaultSeconds, int length, boolean subSecondPrecision) {
		if (subSecondPrecision) {
			return new NanoDateTimeBufferSparse(defaultSeconds, length);
		} else {
			return new SecondDateTimeBufferSparse(defaultSeconds, length);
		}
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
	 * 		if {@code true} the buffer is initially filled with missing values
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
	 * Creates a memory efficient sparse buffer of the given length to create a sparse {@link Column} of type id {@link
	 * TypeId#TIME}. The buffer is initially filled with the given default value.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for data with a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static TimeBufferSparse sparseTimeBuffer(long defaultValue, int length) {
		return new TimeBufferSparse(defaultValue, length);
	}

	/**
	 * Creates a memory efficient sparse buffer of the given length to create a {@link Column} of type id {@link
	 * TypeId#TIME}. The buffer is initially filled with the given default value.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for data with a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static TimeBufferSparse sparseTimeBuffer(LocalTime defaultValue, int length) {
		return new TimeBufferSparse(defaultValue, length);
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#NOMINAL}. The
	 * buffer supports the maximum number of categories supported by Belt ({@value Integer#MAX_VALUE}). The buffer is
	 * initially filled with {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * <p>Please note, that it is recommended to use {@link #nominalBuffer(int, int)} instead whenever the number
	 * of categories is known in advance.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NominalBuffer nominalBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		return new Int32NominalBuffer(ColumnType.NOMINAL, length);
	}

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type {@link TypeId#NOMINAL}.
	 * The buffer supports the maximum number of categories supported by Belt ({@value Integer#MAX_VALUE}). The buffer
	 * is initially filled with the given default value.
	 *
	 * <p>Please note, that it is recommended to use {@link #nominalBuffer(int, int)} instead whenever the number
	 * of categories is known in advance.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient
	 * for a sparsity of {@code >= 50%}.
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultValue
	 * 		the buffer's (usually most common) default value.
	 * @param length
	 * 		the length of the buffer
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size is negative
	 */
	public static NominalBufferSparse sparseNominalBuffer(String defaultValue, int length) {
		return new Int32NominalBufferSparse(ColumnType.NOMINAL, defaultValue, length);
	}

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type {@link TypeId#NOMINAL}
	 * with at most the given number of categories. Trying to use the buffer with more than the
	 * specified number of categories will result in an error. The buffer is initially filled with the given default
	 * value.
	 * <p>
	 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
	 * <p>
	 * The buffer's efficiency will depend on the data's sparsity and number of different categories. The buffer will
	 * usually be memory and time efficient for
	 * <ul>
	 * <li> {@code > 65535} categories (32 bit data) and a sparsity of {@code >= 50%}.
	 * <li> {@code > 255} and {@code <= 65535} categories (16 bit data) and a sparsity of {@code >= 66%}.
	 * <li> {@code <= 255} categories (8 bit data) and a sparsity of {@code >= 80%}.
	 * </ul>
	 * <p> The sparse buffer comes with a constant memory overhead so that it is
	 * recommended not to use it for very small data ({@code <1024} data points).
	 *
	 * @param defaultValue
	 * 		the buffer's (usually most common) default value.
	 * @param length
	 * 		the size of the buffer
	 * @param categories
	 * 		the number of categories
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size or number of categories is negative
	 */
	public static NominalBufferSparse sparseNominalBuffer(String defaultValue, int length, int categories) {
		if (categories < 0) {
			throw new IllegalArgumentException(ILLEGAL_NUMBER_OF_CATEGORIES + categories);
		}
		Format minimal = Format.findMinimal(categories);
		switch (minimal) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				return new UInt8NominalBufferSparse(ColumnType.NOMINAL, defaultValue, length);
			case UNSIGNED_INT16:
				return new UInt16NominalBufferSparse(ColumnType.NOMINAL, defaultValue, length);
			case SIGNED_INT32:
			default:
				return new Int32NominalBufferSparse(ColumnType.NOMINAL, defaultValue, length);
		}
	}

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type {@link TypeId#NOMINAL} with at
	 * most the given number of categories. Trying to use the buffer with more than the specified number of categories
	 * might result in an error. The buffer is initially filled with {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * @param length
	 * 		the size of the buffer
	 * @param categories
	 * 		the number of categories
	 * @return the new buffer
	 * @throws IllegalArgumentException
	 * 		if the given size or number of categories is negative
	 */
	public static NominalBuffer nominalBuffer(int length, int categories) {
		if (length < 0) {
			throw new IllegalArgumentException(MSG_ILLEGAL_CAPACITY + length);
		}
		if (categories < 0) {
			throw new IllegalArgumentException(ILLEGAL_NUMBER_OF_CATEGORIES + categories);
		}
		Format minimal = Format.findMinimal(categories);
		switch (minimal) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				// Do not use packed buffers (smaller that 8Bit) here, since the corresponding buffers are only
				// partially thread-safe.
				return new UInt8NominalBuffer(ColumnType.NOMINAL, length, minimal);
			case UNSIGNED_INT16:
				return new UInt16NominalBuffer(ColumnType.NOMINAL, length);
			case SIGNED_INT32:
			default:
				return new Int32NominalBuffer(ColumnType.NOMINAL, length);
		}
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#NOMINAL} by copying the data from the
	 * given column. The buffer supports the maximum number of categories supported by Belt ({@value
	 * Integer#MAX_VALUE}). The buffer is initially filled with {@link CategoricalReader#MISSING_CATEGORY}.
	 *
	 * <p>Please note, that it is recommended to use {@link #nominalBuffer(Column, int)} instead whenever
	 * the number of categories is known in advance.
	 *
	 * @param column
	 * 		the column to derive the buffer from
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical or if the String type is incompatible with the given column
	 */
	public static NominalBuffer nominalBuffer(Column column) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!Category.CATEGORICAL.equals(column.type().category())) {
			throw new IllegalArgumentException("Column must be categorical");
		}
		if (!ColumnType.NOMINAL.elementType().isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		return new Int32NominalBuffer(column, ColumnType.NOMINAL);
	}

	/**
	 * Creates a buffer to create a {@link Column} of type {@link TypeId#NOMINAL} with at most the given
	 * number of categories by copying the data from the given column. Trying to use the buffer with more than the
	 * specified number of categories might result in an error. The buffer is initially filled with {@link
	 * CategoricalReader#MISSING_CATEGORY}.
	 *
	 * @param column
	 * 		the column to derive the buffer from
	 * @param categories
	 * 		the number of categories
	 * @return the new buffer
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical, if the specified type is incompatible with the given column, or if the given
	 * 		number of categories is negative
	 */
	public static NominalBuffer nominalBuffer(Column column, int categories) {
		if (column == null) {
			throw new NullPointerException(MSG_NULL_COLUMN);
		}
		if (!Category.CATEGORICAL.equals(column.type().category())) {
			throw new IllegalArgumentException("Column must be categorical");
		}
		if (!ColumnType.NOMINAL.elementType().isAssignableFrom(column.type().elementType())) {
			throw new IllegalArgumentException(MSG_TYPE_INCOMPATIBLE_WITH + column.type().elementType());
		}
		if (categories < 0) {
			throw new IllegalArgumentException(ILLEGAL_NUMBER_OF_CATEGORIES + categories);
		}
		int existingCategories = column.getDictionary().maximalIndex() + 1;
		Format minimal = Format.findMinimal(Integer.max(existingCategories, categories));
		switch (minimal) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				// Do not use packed buffers (smaller that 8Bit) here, since the corresponding buffers are only
				// partially thread-safe.
				return new UInt8NominalBuffer(column, ColumnType.NOMINAL, minimal);
			case UNSIGNED_INT16:
				return new UInt16NominalBuffer(column, ColumnType.NOMINAL);
			case SIGNED_INT32:
			default:
				return new Int32NominalBuffer(column, ColumnType.NOMINAL);
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
		public abstract UInt2NominalBuffer newUInt2Buffer(ColumnType<String> type, int length);

		/**
		 * Creates a new uint4 categorical buffer of the given length.
		 */
		public abstract UInt4NominalBuffer newUInt4Buffer(ColumnType<String> type, int length);

		/**
		 * Creates a new uint8 categorical buffer of the given length.
		 */
		public abstract UInt8NominalBuffer newUInt8Buffer(ColumnType<String> type, int length);

		/**
		 * Creates a new uint8 categorical buffer of the given length with the given target format.
		 */
		public abstract UInt8NominalBuffer newUInt8Buffer(ColumnType<String> type, int length,
														  Format targetFormat);

		/**
		 * Creates a new uint16 categorical buffer of the given length.
		 */
		public abstract UInt16NominalBuffer newUInt16Buffer(ColumnType<String> type, int length);

		/**
		 * Creates a new int32 categorical buffer of the given length.
		 */
		public abstract Int32NominalBuffer newInt32Buffer(ColumnType<String> type, int length);

		/**
		 * Creates a new object buffer of the given length.
		 */
		public abstract <T> ObjectBuffer<T> newObjectBuffer(ColumnType<T> type, int length);

		private InternalBuffers() {
		}
	}

}
