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

package com.rapidminer.belt.column.io;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * Builder for nominal columns out of {@link ByteBuffer}s.
 *
 * @author Gisa Meier
 */
public final class NominalColumnBuilder {

	private static final String MESSAGE_OUT_OF_DICT_RANGE = "Category index %s not in dictionary range";
	private static final String MSG_NULL_BUFFER = "Buffer must not be null";
	private static final int SHIFT_FOR_4_BYTE_NUMBER = 2;
	private static final int SHIFT_FOR_2_BYTE_NUMBER = 1;

	private int[] intData;
	private short[] shortData;
	private byte[] byteData;
	private int position;
	private final List<String> dictionary;
	private final IntegerFormats.Format format;
	private final int size;

	/**
	 * Creates a column builder for a {@link Column.TypeId#NOMINAL} column of the given length. The dictionary of the
	 * nominal column is defined via the dictionary values ordered set and the category indices can be put via {@link
	 * ByteBuffer}s containing int, (signed) short or (signed) byte values.
	 *
	 * @param dictionaryValues
	 * 		the dictionary values matching the category indices to be read, must start with {@code null}
	 * @param size
	 * 		the length of the column to construct
	 * @throws IllegalArgumentException
	 * 		if the size is negative, the dictionary values are empty or do not start with {@code null}
	 * @throws NullPointerException
	 * 		if the dictionary values are {@code null}
	 */
	public NominalColumnBuilder(LinkedHashSet<String> dictionaryValues, int size) {
		if (size < 0) {
			throw new IllegalArgumentException("Number of elements must be positive");
		}
		if (Objects.requireNonNull(dictionaryValues, "Dictionary values must not be null").isEmpty()) {
			throw new IllegalArgumentException("Dictionary must not be empty");
		}
		dictionary = new ArrayList<>(dictionaryValues);
		if (dictionary.get(0) != null) {
			throw new IllegalArgumentException("Dictionary must start with null");
		}
		format = IntegerFormats.Format.findMinimal(dictionary.size() - 1);
		initializeDataForFormat(size);
		this.position = 0;
		this.size = size;
	}

	/**
	 * Returns the current position until which the data has been written.
	 *
	 * @return the row index that is written next
	 */
	public int position() {
		return position;
	}

	/**
	 * Puts the integer values in the buffer as category indices into the column starting at the current
	 * {@link #position()}.
	 *
	 * @param buffer
	 * 		a buffer containing category indices as {@link int}s
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if a value in the buffer is not in the range specified by the dictionary
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 */
	public NominalColumnBuilder putIntegers(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException(MSG_NULL_BUFFER);
		}
		IntBuffer wrapper = buffer.asIntBuffer();
		int length = Math.min(wrapper.remaining(), size - position);
		switch (format) {
			case SIGNED_INT32:
				wrapper.get(intData, position, length);
				checkIntArray(intData, position, position + length);
				break;
			case UNSIGNED_INT16:
				copyIntegerToShort(wrapper, position, length);
				break;
			case UNSIGNED_INT8:
				copyIntegerToByte(wrapper, position, length);
				break;
			case UNSIGNED_INT4:
				copyIntegerToByte4(wrapper, position, length);
				break;
			case UNSIGNED_INT2:
				copyIntegerToByte2(wrapper, position, length);
				break;
		}
		buffer.position(buffer.position() + (wrapper.position() << SHIFT_FOR_4_BYTE_NUMBER));
		position += length;
		return this;
	}

	/**
	 * Puts the (signed) short values in the buffer as category indices into the column starting at the current {@link
	 * #position()}. This is only supported if it makes sense for the range defined by the dictionary, i.e., if
	 * the maximal dictionary index and thus the maximal category index is bigger than {@link Short#MAX_VALUE}, then
	 * the values must be written via {@link #putIntegers(ByteBuffer)} and not with this method.
	 *
	 * @param buffer
	 * 		a buffer containing category indices as (signed) {@link short}s
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if a value in the buffer is not in the range specified by the dictionary
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 * @throws AssertionError
	 * 		if the maximal dictionary index is too big to allow storing category indices as {@link short}s
	 */
	public NominalColumnBuilder putShorts(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException(MSG_NULL_BUFFER);
		}
		ShortBuffer wrapper = buffer.asShortBuffer();
		int length = Math.min(wrapper.remaining(), size - position);
		switch (format) {
			case SIGNED_INT32:
				throw new AssertionError("Cannot put values bigger than max short with shorts");
			case UNSIGNED_INT16:
				wrapper.get(shortData, position, length);
				checkShortArray(shortData, position, position + length);
				break;
			case UNSIGNED_INT8:
				copyShortToByte(wrapper, position, length);
				break;
			case UNSIGNED_INT4:
				copyShortToByte4(wrapper, position, length);
				break;
			case UNSIGNED_INT2:
				copyShortToByte2(wrapper, position, length);
				break;
		}
		buffer.position(buffer.position() + (wrapper.position() << SHIFT_FOR_2_BYTE_NUMBER));
		position += length;
		return this;
	}

	/**
	 * Puts the (signed) byte values in the buffer as category indices into the column starting at the current {@link
	 * #position()}. This is only supported if it makes sense for the range defined by the dictionary, i.e., if
	 * the maximal dictionary index and thus the maximal category index is bigger than {@link Byte#MAX_VALUE}, then
	 * the values must be written via {@link #putIntegers(ByteBuffer)} or {@link #putShorts(ByteBuffer)} and not with
	 * this method.
	 *
	 * @param buffer
	 * 		a buffer containing category indices as (signed) {@link short}s
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if a value in the buffer is not in the range specified by the dictionary
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 * @throws AssertionError
	 * 		if the maximal dictionary index is too big to allow storing category indices as {@link short}s
	 */
	public NominalColumnBuilder putBytes(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException(MSG_NULL_BUFFER);
		}
		int length = Math.min(buffer.remaining(), size - position);
		switch (format) {
			case SIGNED_INT32:
			case UNSIGNED_INT16:
				throw new AssertionError("Cannot put values bigger than max byte with bytes");
			case UNSIGNED_INT8:
				buffer.get(byteData, position, length);
				checkByteArray(byteData, position, position + length);
				break;
			case UNSIGNED_INT4:
				copyByteToByte4(buffer, position, length);
				break;
			case UNSIGNED_INT2:
				copyByteToByte2(buffer, position, length);
				break;
		}
		position += length;
		return this;
	}

	/**
	 * Creates a new column from the builder. If the current {@link #position()} is smaller than the originally
	 * defined column size, the remaining values will be missing values.
	 *
	 * @return the column defined by the builder
	 */
	public Column toColumn() {
		switch (format) {
			case SIGNED_INT32:
				return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, intData, dictionary);
			case UNSIGNED_INT16:
				return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, shortData, dictionary);
			default:
				return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL,
						new IntegerFormats.PackedIntegers(byteData, format, size), dictionary);
		}
	}

	/**
	 * Creates a new boolean column with the given positive index from the builder. If the current {@link #position()}
	 * is smaller than the originally defined column size, the remaining values will be missing values.
	 *
	 * @return the boolean column defined by the builder
	 * @throws IllegalArgumentException
	 * 		if the dictionary has more than two values
	 */
	public Column toBooleanColumn(int positiveIndex) {
		if (dictionary.size() > BooleanDictionary.MAXIMAL_RAW_SIZE) {
			throw new IllegalArgumentException("Boolean column must have 2 values or less");
		}
		//with max two values, the format can only be a byte format
		return ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL,
				new IntegerFormats.PackedIntegers(byteData, format, size), dictionary, positiveIndex);
	}

	/**
	 * Copies int values from the wrapper into the short array.
	 */
	private void copyIntegerToShort(IntBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			int value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			shortData[position + i] = (short) value;
		}
	}

	/**
	 * Copies int values from the wrapper into the byte array.
	 */
	private void copyIntegerToByte(IntBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			int value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			byteData[position + i] = (byte) value;
		}
	}

	/**
	 * Copies int values from the wrapper into the byte array for {@link IntegerFormats.Format#UNSIGNED_INT4}.
	 */
	private void copyIntegerToByte4(IntBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			int value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			IntegerFormats.writeUInt4(byteData, position + i, value);
		}
	}

	/**
	 * Copies int values from the wrapper into the byte array for {@link IntegerFormats.Format#UNSIGNED_INT2}.
	 */
	private void copyIntegerToByte2(IntBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			int value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			IntegerFormats.writeUInt2(byteData, position + i, value);
		}
	}

	/**
	 * Copies short values from the wrapper into the byte array.
	 */
	private void copyShortToByte(ShortBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			short value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			byteData[position + i] = (byte) value;
		}
	}

	/**
	 * Copies short values from the wrapper into the byte array for {@link IntegerFormats.Format#UNSIGNED_INT4}.
	 */
	private void copyShortToByte4(ShortBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			short value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			IntegerFormats.writeUInt4(byteData, position + i, value);
		}
	}

	/**
	 * Copies short values from the wrapper into the byte array for {@link IntegerFormats.Format#UNSIGNED_INT2}.
	 */
	private void copyShortToByte2(ShortBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			short value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			IntegerFormats.writeUInt2(byteData, position + i, value);
		}
	}

	/**
	 * Copies byte values from the wrapper into the byte array for {@link IntegerFormats.Format#UNSIGNED_INT4}.
	 */
	private void copyByteToByte4(ByteBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			byte value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			IntegerFormats.writeUInt4(byteData, position + i, value);
		}
	}

	/**
	 * Copies byte values from the wrapper into the byte array for {@link IntegerFormats.Format#UNSIGNED_INT2}.
	 */
	private void copyByteToByte2(ByteBuffer wrapper, int position, int length) {
		int maxValue = dictionary.size() - 1;
		for (int i = 0; i < length; i++) {
			byte value = wrapper.get();
			if (value < 0 || value > maxValue) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
			IntegerFormats.writeUInt2(byteData, position + i, value);
		}
	}

	/**
	 * Checks if the int-values in the data array between from and to are in the dictionary range.
	 */
	private void checkIntArray(int[] data, int from, int to) {
		int maxValue = dictionary.size() - 1;
		for (int i = from; i < to; i++) {
			int value = data[i];
			if ((value < 0 || value > maxValue)) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
		}
	}

	/**
	 * Checks if the short-values in the data array between from and to are in the dictionary range.
	 */
	private void checkShortArray(short[] data, int from, int to) {
		int maxValue = dictionary.size() - 1;
		for (int i = from; i < to; i++) {
			short value = data[i];
			if ((value < 0 || value > maxValue)) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
		}
	}

	/**
	 * Checks if the byte-values in the data array between from and to are in the dictionary range.
	 */
	private void checkByteArray(byte[] data, int from, int to) {
		int maxValue = dictionary.size() - 1;
		for (int i = from; i < to; i++) {
			byte value = data[i];
			if ((value < 0 || value > maxValue)) {
				throw new IllegalArgumentException(String.format(MESSAGE_OUT_OF_DICT_RANGE, value));
			}
		}
	}

	/**
	 * Initializes the data arrays for the given size according to the selected format.
	 */
	private void initializeDataForFormat(int size) {
		switch (format) {
			case SIGNED_INT32:
				intData = new int[size];
				return;
			case UNSIGNED_INT16:
				shortData = new short[size];
				return;
			case UNSIGNED_INT8:
				byteData = new byte[size];
				return;
			case UNSIGNED_INT4:
				byteData = new byte[size / 2 + size % 2];
				return;
			case UNSIGNED_INT2:
				byteData = new byte[size % 4 == 0 ? size / 4 : size / 4 + 1];
		}
	}
}
