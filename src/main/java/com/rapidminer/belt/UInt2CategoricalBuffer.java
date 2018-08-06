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

import static com.rapidminer.belt.util.IntegerFormats.readUInt2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.Column.Category;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Implementation of a {@link CategoricalColumnBuffer} with category index format {@link Format#UNSIGNED_INT2} that can
 * hold {@code 3} different categories. Four category indices are stored in a single {@code byte}.
 *
 * @author Gisa Meier
 */
public class UInt2CategoricalBuffer<T> extends AbstractCategoricalColumnBuffer<T> {

	private final PackedIntegers bytes;
	private boolean frozen = false;
	private final Map<T, Byte> indexLookup = new ConcurrentHashMap<>();
	private final List<T> valueLookup = new ArrayList<>();

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public UInt2CategoricalBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		bytes = new PackedIntegers(new byte[length % 4 == 0 ? length / 4 : length / 4 + 1], indexFormat(), length);
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
	public UInt2CategoricalBuffer(Column column, Class<T> elementType) {
		if (column instanceof CategoricalColumn) {
			CategoricalColumn<?> categoricalColumn = (CategoricalColumn) column;
			if (categoricalColumn.getFormat() == indexFormat()) {
				//same format: directly copy the data
				PackedIntegers originalData = categoricalColumn.getByteData();
				bytes = new PackedIntegers(Arrays.copyOf(originalData.data(), originalData.data().length),
						indexFormat(), originalData.size());
			} else {
				//bigger format
				throw new UnsupportedOperationException("Column format incompatible with buffer format");
			}
		} else {
			throw new UnsupportedOperationException("Column is not categorical");
		}
		fillStructures(column.getDictionary(elementType));
	}

	private void fillStructures(List<T> values) {
		valueLookup.add(null);
		for (byte i = 1; i < values.size(); i++) {
			T value = values.get(i);
			valueLookup.add(value);
			indexLookup.put(value, i);
		}
	}

	@Override
	public T get(int index) {
		int valueIndex = readUInt2(bytes.data(), index);
		synchronized (valueLookup) {
			return valueLookup.get(valueIndex);
		}
	}

	/**
	 * {@inheritDoc} <p>Due to the compressed integer format, this method is only thread-safe for indices with different
	 * results when divided by 4. Values can be lost when writing at indices {@code index1} and {@code index2} with
	 * {@code index1/4 == index2/4} from different threads. So when writing in batches of indices {@code [m,n)} from
	 * different threads, you must ensure that m and n are divisible by 4.
	 */
	@Override
	public void set(int index, T value) {
		if (!setSave(index, value)) {
			throw new IllegalArgumentException("More than " + indexFormat().maxValue() + " different values.");
		}
	}

	/**
	 * {@inheritDoc} <p>Due to the compressed integer format, this method is only thread-safe for indices with different
	 * results when divided by 4. Values can be lost when writing at indices {@code index1} and {@code index2} with
	 * {@code index1/4 == index2/4} from different threads. So when writing in batches of indices {@code [m,n)} from
	 * different threads, you must ensure that m and n are divisible by 4.
	 */
	@Override
	public boolean setSave(int index, T value) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		if (value == null) {
			//set NaN
			IntegerFormats.writeUInt2(bytes.data(), index, 0);
		} else {
			Byte mappingIndex = indexLookup.get(value);
			if (mappingIndex != null) {
				IntegerFormats.writeUInt2(bytes.data(), index, mappingIndex);
			} else {
				int newMappingIndex;
				synchronized (valueLookup) {
					//double check that it was not added in parallel
					Byte mappingIndexAgain = indexLookup.get(value);
					if (mappingIndexAgain == null) {
						newMappingIndex = valueLookup.size();
						if (newMappingIndex > indexFormat().maxValue()) {
							return false;
						}
						valueLookup.add(value);
						indexLookup.put(value, (byte) newMappingIndex);
					} else {
						newMappingIndex = mappingIndexAgain;
					}
				}

				IntegerFormats.writeUInt2(bytes.data(), index, newMappingIndex);
			}
		}
		return true;
	}


	@Override
	public int size() {
		return bytes.size();
	}

	@Override
	public Format indexFormat() {
		return Format.UNSIGNED_INT2;
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
		return bytes;
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
		return new SimpleCategoricalColumn<>(type, bytes, valueLookup);
	}
	/**
	 * Creates a boolean column with the given positive value if the buffer has at most two {@link #differentValues()}.
	 * The other value is automatically negative.
	 *
	 * @param type
	 * 		the column type of the column
	 * @param positiveValue
	 * 		the positive value
	 * @return a categorical column with {@link Column.Capability#BOOLEAN}
	 * @throws IllegalArgumentException
	 * 		if there are more than two different values or if the given positive value is not one of the values
	 */
	public CategoricalColumn<T> toBooleanColumn(ColumnType<T> type, T positiveValue) {
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		freeze();
		if (differentValues() > 2) {
			throw new IllegalArgumentException("More than 2 different values");
		}
		int size = valueLookup.size();
		int positiveIndex;
		if (size > 0 && valueLookup.get(1).equals(positiveValue)) {
			positiveIndex = 1;
		} else if (size > 1 && valueLookup.get(2).equals(positiveValue)) {
			positiveIndex = 2;
		} else {
			throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue) + "\" not in dictionary.");
		}
		return new SimpleCategoricalColumn<>(type, bytes, valueLookup, positiveIndex);
	}
}