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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Implementation of a {@link CategoricalBuffer} with category index format {@link Format#UNSIGNED_INT8} that can
 * hold {@code 255} different categories. The category indices are stored as {@code byte}.
 *
 * @author Gisa Meier
 */
public class UInt8CategoricalBuffer<T> extends CategoricalBuffer<T> {


	private final byte[] data;
	private final Format targetFormat;
	private final int maxCategories;
	private boolean frozen = false;
	private final Map<T, Byte> indexLookup = new ConcurrentHashMap<>();
	private final List<T> valueLookup = new ArrayList<>();

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	UInt8CategoricalBuffer(int length) {
		this(length, Format.UNSIGNED_INT8);
	}

	/**
	 * Copies the data of the given column into a buffer. Throws an {@link UnsupportedOperationException} if the column
	 * category is not {@link Category#CATEGORICAL} or if the compression format of the column has more values
	 * than supported by this buffer.
	 *
	 * @param column
	 * 		the column to convert to a buffer
	 * @param elementType
	 * 		the desired type of the buffer, must be a super type of the column type
	 */
	UInt8CategoricalBuffer(Column column, Class<T> elementType) {
		this(column, elementType, Format.UNSIGNED_INT8);
	}

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param targetFormat
	 * 		the format of the final column
	 */
	UInt8CategoricalBuffer(int length, Format targetFormat) {
		this.data = new byte[length];
		this.targetFormat = targetFormat;
		this.maxCategories = Integer.min(targetFormat.maxValue(), Format.UNSIGNED_INT8.maxValue());
		this.valueLookup.add(null); //position 0 stands for missing value, i.e. null
	}

	/**
	 * Copies the data of the given column into a buffer. Throws an {@link UnsupportedOperationException} if the column
	 * category is not {@link Column.Category#CATEGORICAL} or if the compression format of the column has more values
	 * than supported by this buffer.
	 *
	 * @param column
	 * 		the column to convert to a buffer
	 * @param elementType
	 * 		the desired type of the buffer, must be a super type of the column type
	 * @param targetFormat
	 * 		the format of the final column
	 */
	UInt8CategoricalBuffer(Column column, Class<T> elementType, Format targetFormat) {
		if (column instanceof CategoricalColumn) {
			this.targetFormat = targetFormat;
			this.maxCategories = Integer.min(targetFormat.maxValue(), Format.UNSIGNED_INT8.maxValue());
			CategoricalColumn<?> categoricalColumn = (CategoricalColumn) column;
			Dictionary<T> dictionary = categoricalColumn.getDictionary(elementType);
			if (dictionary.maximalIndex() < maxCategories) {
				if (categoricalColumn.getFormat() == Format.UNSIGNED_INT8) {
					// Same format: directly copy the data
					data = ColumnAccessor.get().getByteDataCopy(categoricalColumn);
				} else {
					// Different format: go via column reader
					data = new byte[column.size()];
					CategoricalReader reader = Readers.categoricalReader(column);
					for (int i = 0; i < data.length; i++) {
						data[i] = (byte) reader.read();
					}
				}
			} else {
				throw new UnsupportedOperationException("Column contains to many categories for this buffer format");
			}
			fillStructures(dictionary);
		} else {
			throw new UnsupportedOperationException("Column is not categorical");
		}
	}

	private void fillStructures(Dictionary<T> values) {
		valueLookup.add(null);
		for (int i = 1; i <= values.maximalIndex(); i++) {
			T value = values.get(i);
			valueLookup.add(value);
			indexLookup.put(value, (byte) i);
		}
	}

	@Override
	public T get(int index) {
		int valueIndex = Byte.toUnsignedInt(data[index]);
		synchronized (valueLookup) {
			return valueLookup.get(valueIndex);
		}
	}

	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public void set(int index, T value) {
		if (!setSave(index, value)) {
			throw new IllegalArgumentException("More than " + maxCategories + " different values.");
		}
	}

	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public boolean setSave(int index, T value) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (value == null) {
			//set NaN
			data[index] = 0;
		} else {
			Byte mappingIndex = indexLookup.get(value);
			if (mappingIndex != null) {
				data[index] = mappingIndex;
			} else {
				byte newByteIndex;
				synchronized (valueLookup) {
					//double check that it was not added in parallel
					Byte mappingIndexAgain = indexLookup.get(value);
					if (mappingIndexAgain == null) {
						int newMappingIndex = valueLookup.size();
						if (newMappingIndex > maxCategories) {
							return false;
						}
						valueLookup.add(value);
						newByteIndex = (byte) newMappingIndex;
						indexLookup.put(value, newByteIndex);
					} else {
						newByteIndex = mappingIndexAgain;
					}
				}
				data[index] = newByteIndex;
			}
		}
		return true;
	}


	@Override
	public int size() {
		return data.length;
	}

	@Override
	public Format indexFormat() {
		return Format.UNSIGNED_INT8;
	}

	@Override
	public int differentValues() {
		synchronized (valueLookup) {
			return valueLookup.size() - 1;
		}
	}

	/**
	 * @return the underlying packed integers
	 */
	PackedIntegers getData() {
		return new PackedIntegers(data, indexFormat(), data.length);
	}


	@Override
	List<T> getMapping() {
		return valueLookup;
	}

	@Override
	void freeze() {
		frozen = true;
	}

	@Override
	public CategoricalColumn<T> toColumn(ColumnType<T> type) {
		freeze();
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		PackedIntegers packed;
		switch (targetFormat) {
			case UNSIGNED_INT2:
				packed = packAsUInt2();
				break;
			case UNSIGNED_INT4:
				packed = packAsUInt4();
				break;
			case UNSIGNED_INT16:
			default:
				packed = new PackedIntegers(data, Format.UNSIGNED_INT8, data.length);
				break;
		}
		return ColumnAccessor.get().newCategoricalColumn(type, packed, valueLookup);
	}

	@Override
	public CategoricalColumn<T> toBooleanColumn(ColumnType<T> type, T positiveValue) {
		freeze();
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		if (valueLookup.size() > BooleanDictionary.MAXIMAL_RAW_SIZE) {
			throw new IllegalArgumentException("Boolean column must have 2 values or less");
		}
		int positiveIndex = BooleanDictionary.NO_ENTRY;
		if (positiveValue != null) {
			Byte index = indexLookup.get(positiveValue);
			if (index == null) {
				throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue)
						+ "\" not in dictionary.");
			}
			positiveIndex = Byte.toUnsignedInt(index);
		}
		PackedIntegers packed;
		switch (targetFormat) {
			case UNSIGNED_INT2:
				packed = packAsUInt2();
				break;
			case UNSIGNED_INT4:
				packed = packAsUInt4();
				break;
			case UNSIGNED_INT16:
			default:
				packed = new PackedIntegers(data, Format.UNSIGNED_INT8, data.length);
				break;
		}
		return ColumnAccessor.get().newCategoricalColumn(type, packed, valueLookup, positiveIndex);
	}

	private PackedIntegers packAsUInt2() {
		byte[] uInt2Data = new byte[data.length / 4 + data.length % 4];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(uInt2Data, i, data[i]);
		}
		return new PackedIntegers(uInt2Data, Format.UNSIGNED_INT2, data.length);
	}

	private PackedIntegers packAsUInt4() {
		byte[] uInt4Data = new byte[data.length / 2 + data.length % 2];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(uInt4Data, i, data[i]);
		}
		return new PackedIntegers(uInt4Data, Format.UNSIGNED_INT4, data.length);
	}

}