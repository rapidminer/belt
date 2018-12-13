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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.Column.Category;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Implementation of a {@link CategoricalBuffer} with category index format {@link Format#UNSIGNED_INT16} that can
 * hold {@code 65535} different categories. The category indices are stored as {@code short}.
 *
 * @author Gisa Meier
 */
public class UInt16CategoricalBuffer<T> extends CategoricalBuffer<T> {


	private final short[] data;
	private boolean frozen = false;
	private final Map<T, Short> indexLookup = new ConcurrentHashMap<>();
	private final List<T> valueLookup = new ArrayList<>();

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	UInt16CategoricalBuffer(int length) {
		data = new short[length];
		valueLookup.add(null); //position 0 stands for missing value, i.e. null
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
	 */
	UInt16CategoricalBuffer(Column column, Class<T> elementType) {
		if (column instanceof CategoricalColumn) {
			CategoricalColumn<?> categoricalColumn = (CategoricalColumn) column;
			List<T> dictionary = categoricalColumn.getDictionary(elementType);
			if (categoricalColumn.getFormat() == indexFormat()) {
				// Same format: directly copy the data
				short[] originalData = categoricalColumn.getShortData();
				data = Arrays.copyOf(originalData, originalData.length);
			} else if (dictionary.size() <= indexFormat().maxValue()) {
				// Different format: go via column reader
				data = new short[column.size()];
				CategoricalReader reader = Readers.categoricalReader(column);
				for (int i = 0; i < data.length; i++) {
					data[i] = (short) reader.read();
				}
			} else {
				throw new UnsupportedOperationException("Column contains to many categories for this buffer format");
			}
			fillStructures(dictionary);
		} else {
			throw new UnsupportedOperationException("Column is not categorical");
		}
	}

	private void fillStructures(List<T> values) {
		valueLookup.add(null);
		for (int i = 1; i < values.size(); i++) {
			T value = values.get(i);
			valueLookup.add(value);
			indexLookup.put(value, (short) i);
		}
	}

	@Override
	public T get(int index) {
		int valueIndex = Short.toUnsignedInt(data[index]);
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
			throw new IllegalArgumentException("More than " + indexFormat().maxValue() + " different values.");
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
			Short mappingIndex = indexLookup.get(value);
			if (mappingIndex != null) {
				data[index] = mappingIndex;
			} else {
				short newShortIndex;

				synchronized (valueLookup) {
					//double check that it was not added in parallel
					Short mappingIndexAgain = indexLookup.get(value);
					if (mappingIndexAgain == null) {
						int newMappingIndex = valueLookup.size();
						if (newMappingIndex > indexFormat().maxValue()) {
							return false;
						}
						valueLookup.add(value);
						newShortIndex = (short) newMappingIndex;
						indexLookup.put(value, newShortIndex);
					} else {
						newShortIndex = mappingIndexAgain;
					}
				}
				data[index] = newShortIndex;
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
		return Format.UNSIGNED_INT16;
	}

	@Override
	public int differentValues() {
		synchronized (valueLookup) {
			return valueLookup.size() - 1;
		}
	}

	/**
	 * @return the underlying data array
	 */
	short[] getData() {
		return data;
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
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		freeze();
		return new SimpleCategoricalColumn<>(type, data, valueLookup);
	}

	@Override
	public CategoricalColumn<T> toBooleanColumn(ColumnType<T> type, T positiveValue) {
		freeze();
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		int positiveIndex = CategoricalColumn.NO_POSITIVE_ENTRY;
		if (positiveValue != null) {
			Short index = indexLookup.get(positiveValue);
			if (index == null) {
				throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue)
						+ "\" not in dictionary.");
			}
			positiveIndex = Short.toUnsignedInt(index);
		}
		return new SimpleCategoricalColumn<>(type, data, valueLookup, positiveIndex);
	}

}