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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.Column.Category;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Implementation of a {@link CategoricalBuffer} with category index format {@link Format#SIGNED_INT32} that can
 * hold {@link Integer#MAX_VALUE} many different categories. The category indices are stored as {@code int}.
 *
 * @author Gisa Meier
 */
public class Int32CategoricalBuffer<T> extends CategoricalBuffer<T> {


	private final int[] data;
	private boolean frozen = false;
	private final Map<T, Integer> indexLookup = new ConcurrentHashMap<>();
	private final List<T> valueLookup = new ArrayList<>();

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	Int32CategoricalBuffer(int length) {
		data = new int[length];
		valueLookup.add(null); //position 0 stands for missing value, i.e. null
	}

	/**
	 * Copies the data of the given column into a buffer. Throws an {@link UnsupportedOperationException} if the column
	 * category is not {@link Column.Category#CATEGORICAL}.
	 *
	 * @param column
	 * 		the column to convert to a buffer
	 * @param elementType
	 * 		the desired type of the buffer, must be a super type of the column type
	 */
	Int32CategoricalBuffer(Column column, Class<T> elementType) {
		data = new int[column.size()];
		column.fill(data, 0);
		fillStructures(column.getDictionary(elementType));
	}

	private void fillStructures(List<T> values) {
		valueLookup.add(null);
		for (int i = 1; i < values.size(); i++) {
			T value = values.get(i);
			valueLookup.add(value);
			indexLookup.put(value, i);
		}
	}

	@Override
	public T get(int index) {
		int valueIndex = data[index];
		synchronized (valueLookup) {
			return valueLookup.get(valueIndex);
		}
	}

	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public void set(int index, T value) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		if (value == null) {
			//set NaN
			data[index] = 0;
		} else {
			Integer mappingIndex = indexLookup.get(value);
			if (mappingIndex != null) {
				data[index] = mappingIndex;
			} else {
				int newMappingIndex;
				synchronized (valueLookup) {
					//double check that it was not added in parallel
					Integer mappingIndexAgain = indexLookup.get(value);
					if (mappingIndexAgain == null) {
						newMappingIndex = valueLookup.size();
						valueLookup.add(value);
						indexLookup.put(value, newMappingIndex);
					} else {
						newMappingIndex = mappingIndexAgain;
					}
				}

				data[index] = newMappingIndex;
			}
		}
	}

	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public boolean setSave(int index, T value) {
		set(index, value);
		return true;
	}


	@Override
	public int size() {
		return data.length;
	}

	@Override
	public IntegerFormats.Format indexFormat() {
		return Format.SIGNED_INT32;
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
	int[] getData() {
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
			Integer index = indexLookup.get(positiveValue);
			if (index == null) {
				throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue)
						+ "\" not in dictionary.");
			}
			positiveIndex = index;
		}
		return new SimpleCategoricalColumn<>(type, data, valueLookup, positiveIndex);
	}

}