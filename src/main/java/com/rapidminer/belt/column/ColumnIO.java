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

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.LinkedHashSet;

import com.rapidminer.belt.column.io.DateTimeColumnBuilder;
import com.rapidminer.belt.column.io.NominalColumnBuilder;
import com.rapidminer.belt.column.io.NumericColumnBuilder;
import com.rapidminer.belt.column.io.TimeColumnBuilder;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * Allows to put column data into {@link ByteBuffer}s and create columns from {@link ByteBuffer} data.
 *
 * @author Michael Knopf, Gisa Meier
 */
public final class ColumnIO {

	private static final int SHIFT_FOR_8_BYTE_NUMBER = 3;
	private static final int SHIFT_FOR_4_BYTE_NUMBER = 2;
	private static final int SHIFT_FOR_2_BYTE_NUMBER = 1;

	private static final int READER_BUFFER_SIZE = 1024;

	private static final String MSG_COLUMN_NOT_CATEGORICAL = "Column not categorical";

	/**
	 * Puts the values of the numeric-readable column as double values into the buffer.
	 *
	 * @param column
	 * 		the column to write as double values into the buffer, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not numeric readable
	 */
	public static int putNumericDoubles(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().hasCapability(Column.Capability.NUMERIC_READABLE)) {
			if (column instanceof DoubleArrayColumn) {
				return writeDoubleArrayColumn((DoubleArrayColumn) column, buffer, offset);
			} else {
				return writeGenericNumericColumn(column, buffer, offset);
			}
		} else {
			throw new IllegalArgumentException("Column is not numeric-readable");
		}
	}

	/**
	 * Puts the values of the time column as long values representing the nanoseconds of day into the buffer. Missing
	 * values are represented by {@link TimeColumn#MISSING_VALUE} ({@link Long#MAX_VALUE}).
	 *
	 * @param column
	 * 		the column to write as long values into the buffer, must be a {@link Column.TypeId#TIME} column
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not a time column
	 */
	public static int putTimeLongs(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().id() == Column.TypeId.TIME) {
			if (column instanceof SimpleTimeColumn) {
				return writeLongArrayColumn(((SimpleTimeColumn) column).array(), buffer, offset);
			} else {
				return writeGenericTimeColumn((TimeColumn) column, buffer, offset);
			}
		} else {
			throw new IllegalArgumentException("not a time column");
		}
	}

	/**
	 * Puts the seconds since epoch of the date-time column as long values into the buffer. Missing values are
	 * represented by {@link DateTimeColumn#MISSING_VALUE} ({@link Long#MAX_VALUE}). For date-time columns with
	 * nanosecond precision ({@link DateTimeColumn#hasSubSecondPrecision()}), use
	 * {@link #putDateTimeNanoInts(Column, int, ByteBuffer)} additionally.
	 *
	 * @param column
	 * 		the column to write as long values into the buffer, must be a {@link Column.TypeId#DATE_TIME} column
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not a date-time column
	 */
	public static int putDateTimeLongs(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().id() == Column.TypeId.DATE_TIME) {
			if (column instanceof SimpleDateTimeColumn) {
				return writeLongArrayColumn(((SimpleDateTimeColumn) column).secondsArray(), buffer, offset);
			} else {
				return writeGenericDateTimeColumn((DateTimeColumn) column, buffer, offset);
			}
		} else {
			throw new IllegalArgumentException("not a date time column");
		}
	}

	/**
	 * Puts the subsecond part of the date-time column as nanosecond int values into the buffer. Should be used
	 * together with {@link #putDateTimeLongs(Column, int, ByteBuffer)}. If the date-time column has no subseconds
	 * ({@link DateTimeColumn#hasSubSecondPrecision()} then {@code 0}s are written.
	 *
	 * @param column
	 * 		the column of which to write the nanoseconds as int values into the buffer, must be a
	 *        {@link Column.TypeId#DATE_TIME} column
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not a date-time column
	 */
	public static int putDateTimeNanoInts(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().id() == Column.TypeId.DATE_TIME) {
			if (!((DateTimeColumn) column).hasSubSecondPrecision()) {
				//write 0s
				int length = Math.min(buffer.remaining() >>> SHIFT_FOR_4_BYTE_NUMBER, column.size() - offset);
				for (int i = 0; i < length; i++) {
					buffer.putInt(0);
				}
				return length;
			} else {
				if (column instanceof SimpleDateTimeColumn) {
					return writeIntegerArray(((SimpleDateTimeColumn) column).nanoArray(), buffer, offset);
				} else if (column instanceof DateTimeHighPrecisionSparseColumn) {
					return writeIntegerArray(((DateTimeHighPrecisionSparseColumn) column).nanoArray(), buffer, offset);
				} else if (column instanceof MappedDateTimeColumn) {
					MappedDateTimeColumn mappedColumn = (MappedDateTimeColumn) column;
					return writeMappedIntegerArray(mappedColumn.getMapping(), mappedColumn.nanoArray(), buffer,
							offset);
				} else {
					throw new AssertionError("unknown date-time implementation");
				}
			}
		} else {
			throw new IllegalArgumentException("not a date time column");
		}
	}

	/**
	 * Puts the category indices of the categorical column as int values into the buffer. Use
	 * {@link #putCategoricalShorts(Column, int, ByteBuffer)} or {@link #putCategoricalBytes(Column, int, ByteBuffer)}
	 * if you want to save space and the dictionary is small enough to allow a representation by {@link short}s or
	 * {@link byte}s.
	 *
	 * @param column
	 * 		the column to write as int values into the buffer, must be a {@link Column.Category#CATEGORICAL} column
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not a categorical column
	 */
	public static int putCategoricalIntegers(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().category() == Column.Category.CATEGORICAL) {
			if (column instanceof SimpleCategoricalColumn && ((SimpleCategoricalColumn) column).getFormat()
					== IntegerFormats.Format.SIGNED_INT32) {
				return writeIntegerArray(((SimpleCategoricalColumn) column).getIntData(), buffer, offset);
			} else if (column instanceof MappedCategoricalColumn && ((MappedCategoricalColumn) column).getFormat()
					== IntegerFormats.Format.SIGNED_INT32) {
				return writeMappedIntegerArray(((MappedCategoricalColumn) column).getRowMapping(),
						((MappedCategoricalColumn) column).getIntData(), buffer, offset);
			} else {
				return writeGenericIntegerColumn(column, buffer, offset);
			}
		} else {
			throw new IllegalArgumentException(MSG_COLUMN_NOT_CATEGORICAL);
		}
	}

	/**
	 * Puts the category indices of the categorical column as (signed) short values into the buffer. Use
	 * {@link #putCategoricalBytes(Column, int, ByteBuffer)} if you want to save space and the dictionary is small
	 * enough to allow a representation by {@link byte}s. If there are too many dictionary entries to represent the
	 * category indices by {@link short}s, use {@link #putCategoricalIntegers(Column, int, ByteBuffer)} instead.
	 *
	 * @param column
	 * 		the column to write as short values into the buffer, must be a {@link Column.Category#CATEGORICAL} column
	 * 		and the {@link Dictionary#maximalIndex()} must be at most {@link Short#MAX_VALUE}
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not a categorical column or the dictionary has a maximal index bigger than {@link Short#MAX_VALUE}
	 */
	public static int putCategoricalShorts(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().category() == Column.Category.CATEGORICAL) {
			if(column.getDictionary().maximalIndex() > Short.MAX_VALUE){
				throw new IllegalArgumentException("value range does not fit in signed short");
			}
			if (column instanceof SimpleCategoricalColumn && ((SimpleCategoricalColumn) column).getFormat() == IntegerFormats.Format.UNSIGNED_INT16) {
				return writeSimpleShortArrayColumn((SimpleCategoricalColumn) column, buffer, offset);
			} else {
				return writeGenericShortColumn(column, buffer, offset);
			}
		} else {
			throw new IllegalArgumentException(MSG_COLUMN_NOT_CATEGORICAL);
		}
	}

	/**
	 * Puts the category indices of the categorical column as (signed) byte values into the buffer. Use
	 * {@link #putCategoricalShorts(Column, int, ByteBuffer)} or
	 * {@link #putCategoricalIntegers(Column, int, ByteBuffer)} instead if there are too many dictionary entries to
	 * represent the category indices by {@link byte}s.
	 *
	 * @param column
	 * 		the column to write as short values into the buffer, must be a {@link Column.Category#CATEGORICAL} column
	 * 		and the {@link Dictionary#maximalIndex()} must be at most {@link Byte#MAX_VALUE}
	 * @param offset
	 * 		the row in the column to start from
	 * @param buffer
	 * 		the buffer to write into
	 * @return the number of rows written into the buffer
	 * @throws NullPointerException
	 * 		if the buffer or the column is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the offset is negative or bigger than the column size
	 * @throws IllegalArgumentException
	 * 		if the column is not a categorical column or the dictionary has a maximal index bigger than {@link Byte#MAX_VALUE}
	 */
	public static int putCategoricalBytes(Column column, int offset, ByteBuffer buffer) {
		checkInput(column, offset, buffer);
		if (column.type().category() == Column.Category.CATEGORICAL) {
			if(column.getDictionary().maximalIndex() > Byte.MAX_VALUE){
				throw new IllegalArgumentException("value range does not fit in signed byte");
			}
			if (column instanceof SimpleCategoricalColumn && ((SimpleCategoricalColumn) column).getFormat() == IntegerFormats.Format.UNSIGNED_INT8) {
				return writeSimpleByteArrayColumn((SimpleCategoricalColumn) column, buffer, offset);
			} else {
				return writeGenericByteColumn(column, buffer, offset);
			}
		} else {
			throw new IllegalArgumentException(MSG_COLUMN_NOT_CATEGORICAL);
		}
	}

	/**
	 * Creates a column builder for a {@link Column.TypeId#REAL} column of the given length where the data can be put
	 * via {@link ByteBuffer}s containing double values.
	 *
	 * @param length
	 * 		the length of the column to construct
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the length is negative
	 */
	public static NumericColumnBuilder readReal(int length) {
		return new NumericColumnBuilder(length, Column.TypeId.REAL);
	}

	/**
	 * Creates a column builder for a {@link Column.TypeId#INTEGER_53_BIT} column of the given length where the data can
	 * be put via {@link ByteBuffer}s containing double values.
	 *
	 * @param length
	 * 		the length of the column to construct
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the length is negative
	 */
	public static NumericColumnBuilder readInteger53Bit(int length) {
		return new NumericColumnBuilder(length, Column.TypeId.INTEGER_53_BIT);
	}

	/**
	 * Creates a column builder for a {@link Column.TypeId#TIME} column of the given length where the data can be put
	 * via {@link ByteBuffer}s containing long values representing nanoseconds of the day.
	 *
	 * @param length
	 * 		the length of the column to construct
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the length is negative
	 */
	public static TimeColumnBuilder readTime(int length) {
		return new TimeColumnBuilder(length);
	}

	/**
	 * Creates a column builder for a {@link Column.TypeId#DATE_TIME} column of the given length. The second data can
	 * be put via {@link ByteBuffer}s containing long values representing seconds since epoch. Optionally, the
	 * nanosecond data can be put via {@link ByteBuffer}s containing int values representing the subsecond part.
	 *
	 * @param length
	 * 		the length of the column to construct
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the length is negative
	 */
	public static DateTimeColumnBuilder readDateTime(int length) {
		return new DateTimeColumnBuilder(length);
	}

	/**
	 * Creates a column builder for a {@link Column.TypeId#NOMINAL} column of the given length. The dictionary of the
	 * nominal column is defined via the dictionary values ordered set and the category indices can be put via {@link
	 * ByteBuffer}s containing int, (signed) short or (signed) byte values.
	 *
	 * @param dictionaryValues
	 * 		the dictionary values matching the category indices to be read, must start with {@code null}
	 * @param length
	 * 		the length of the column to construct
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the length is negative, the dictionary values are empty or do not start with {@code null}
	 * @throws NullPointerException
	 * 		if the dictionary values are {@code null}
	 */
	public static NominalColumnBuilder readNominal(LinkedHashSet<String> dictionaryValues, int length) {
		return new NominalColumnBuilder(dictionaryValues, length);
	}

	/**
	 * Checks column and buffer for {@code null} and the offset for the column size.
	 */
	private static void checkInput(Column column, int offset, ByteBuffer buffer) {
		if (column == null) {
			throw new NullPointerException("Column must not be null");
		}
		if (offset < 0 || offset > column.size()) {
			throw new IndexOutOfBoundsException("Offset: " + offset);
		}
		if (buffer == null) {
			throw new NullPointerException("Buffer must not be null");
		}
	}

	/**
	 * Writes the int array into the buffer starting from the row index.
	 */
	private static int writeIntegerArray(int[] data, ByteBuffer buffer, int rowIndex) {
		IntBuffer intBuffer = buffer.asIntBuffer();
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(intBuffer.remaining(), data.length - start);
		intBuffer.put(data, start, length);
		int byteLength = length << SHIFT_FOR_4_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the data array twisted by the mapping into the buffer starting from the row index.
	 */
	private static int writeMappedIntegerArray(int[] mapping, int[] data, ByteBuffer buffer, int rowIndex) {
		int columnLength = mapping.length;
		int start = Math.min(columnLength, rowIndex);
		int length = Math.min(buffer.remaining() >>> SHIFT_FOR_4_BYTE_NUMBER, columnLength - start);
		for (int i = 0; i < length; i++) {
			int position = mapping[start + i];
			if (position < 0 || position > data.length) {
				buffer.putInt(0);
			} else {
				buffer.putInt(data[position]);
			}
		}
		return length;
	}

	/**
	 * Writes the short array behind the column into the buffer starting from the row index.
	 */
	private static int writeSimpleShortArrayColumn(SimpleCategoricalColumn column, ByteBuffer buffer, int rowIndex) {
		short[] data = column.getShortData();
		ShortBuffer shortBuffer = buffer.asShortBuffer();
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(shortBuffer.remaining(), data.length - start);
		shortBuffer.put(data, start, length);
		int byteLength = length << SHIFT_FOR_2_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the byte array behind the column into the buffer starting from the row index.
	 */
	private static int writeSimpleByteArrayColumn(SimpleCategoricalColumn column, ByteBuffer buffer,
												  int rowIndex) {
		byte[] data = column.getByteData().data();
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(buffer.remaining(), data.length - start);
		buffer.put(data, start, length);
		return length;
	}

	/**
	 * Writes the category indices as integers into the buffer starting at the row index.
	 */
	private static int writeGenericIntegerColumn(Column column, ByteBuffer buffer, int rowIndex) {
		IntBuffer intBuffer = buffer.asIntBuffer();
		int start = Math.min(column.size(), rowIndex);
		int length = Math.min(intBuffer.remaining(), column.size() - start);

		int bufferSize = Math.min(length, READER_BUFFER_SIZE); //as much as necessary or buffer size in categorical reader
		int[] intArray = new int[bufferSize];
		int written = 0;
		while (written < length) {
			column.fill(intArray, rowIndex + written);
			intBuffer.put(intArray, 0, Math.min(bufferSize, length - written));
			written += bufferSize;
		}

		int byteLength = length << SHIFT_FOR_4_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the category indices as shorts into the buffer starting at the row index.
	 */
	private static int writeGenericShortColumn(Column column, ByteBuffer buffer, int rowIndex) {
		ShortBuffer shortBuffer = buffer.asShortBuffer();
		int start = Math.min(column.size(), rowIndex);
		int length = Math.min(shortBuffer.remaining(), column.size() - start);

		int bufferSize = Math.min(length, READER_BUFFER_SIZE); //as much as necessary or buffer size in categorical reader
		int[] intArray = new int[bufferSize];
		int written = 0;
		while (written < length) {
			column.fill(intArray, rowIndex + written);
			for (int i = 0; i < Math.min(bufferSize, length - written); i++) {
				shortBuffer.put((short)intArray[i]);
			}
			written += bufferSize;
		}

		int byteLength = length << SHIFT_FOR_2_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the category indices as bytes into the buffer starting at the row index.
	 */
	private static int writeGenericByteColumn(Column column, ByteBuffer buffer, int rowIndex) {
		int start = Math.min(column.size(), rowIndex);
		int length = Math.min(buffer.remaining(), column.size() - start);

		int bufferSize = Math.min(length, READER_BUFFER_SIZE); //as much as necessary or buffer size in categorical reader
		int[] intArray = new int[bufferSize];
		int written = 0;
		while (written < length) {
			column.fill(intArray, rowIndex + written);
			for (int i = 0; i < Math.min(bufferSize, length - written); i++) {
				buffer.put((byte) intArray[i]);
			}
			written += bufferSize;
		}
		return length;
	}

	/**
	 * Writes the double array behind the column into the buffer starting from the row index.
	 */
	private static int writeDoubleArrayColumn(DoubleArrayColumn column, ByteBuffer buffer, int rowIndex) {
		double[] data = column.array();
		DoubleBuffer doubleBuffer = buffer.asDoubleBuffer();
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(doubleBuffer.remaining(), data.length - start);
		doubleBuffer.put(data, start, length);
		int byteLength = length << SHIFT_FOR_8_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the long array into the buffer starting from the row index.
	 */
	private static int writeLongArrayColumn(long[] data, ByteBuffer buffer, int rowIndex) {
		LongBuffer longBuffer = buffer.asLongBuffer();
		int start = Math.min(data.length, rowIndex);
		int length = Math.min(longBuffer.remaining(), data.length - start);
		longBuffer.put(data, start, length);
		int byteLength = length << SHIFT_FOR_8_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the numeric column as double values into the buffer starting with the row index.
	 */
	private static int writeGenericNumericColumn(Column column, ByteBuffer buffer, int rowIndex) {
		DoubleBuffer doubleBuffer = buffer.asDoubleBuffer();
		int start = Math.min(column.size(), rowIndex);
		int length = Math.min(doubleBuffer.remaining(), column.size() - start);

		int bufferSize = Math.min(length, READER_BUFFER_SIZE); //as much as necessary or buffer size in numeric reader
		double[] intArray = new double[bufferSize];
		int written = 0;
		while (written < length) {
			column.fill(intArray, rowIndex + written);
			doubleBuffer.put(intArray, 0, Math.min(bufferSize, length - written));
			written += bufferSize;
		}

		int byteLength = length << SHIFT_FOR_8_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the time column as long values into the buffer starting with the row index.
	 */
	private static int writeGenericTimeColumn(TimeColumn column, ByteBuffer buffer, int rowIndex) {
		LongBuffer longBuffer = buffer.asLongBuffer();
		int start = Math.min(column.size(), rowIndex);
		int length = Math.min(longBuffer.remaining(), column.size() - start);

		int bufferSize = Math.min(length, READER_BUFFER_SIZE); //as much as necessary or buffer size in numeric reader
		long[] bufferArray = new long[bufferSize];
		int written = 0;
		while (written < length) {
			column.fill(bufferArray, rowIndex + written);
			longBuffer.put(bufferArray, 0, Math.min(bufferSize, length - written));
			written += bufferSize;
		}

		int byteLength = length << SHIFT_FOR_8_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	/**
	 * Writes the second data of the date-time column as long values into the buffer starting with the row index.
	 */
	private static int writeGenericDateTimeColumn(DateTimeColumn column, ByteBuffer buffer, int rowIndex) {
		LongBuffer longBuffer = buffer.asLongBuffer();
		int start = Math.min(column.size(), rowIndex);
		int length = Math.min(longBuffer.remaining(), column.size() - start);

		int bufferSize = Math.min(length, READER_BUFFER_SIZE); //as much as necessary or buffer size in numeric reader
		long[] bufferArray = new long[bufferSize];
		int written = 0;
		while (written < length) {
			column.fillSeconds(bufferArray, rowIndex + written);
			longBuffer.put(bufferArray, 0, Math.min(bufferSize, length - written));
			written += bufferSize;
		}

		int byteLength = length << SHIFT_FOR_8_BYTE_NUMBER;
		buffer.position(buffer.position() + byteLength);
		return length;
	}

	private ColumnIO() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

}