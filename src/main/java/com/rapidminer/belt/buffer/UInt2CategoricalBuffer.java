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

import static com.rapidminer.belt.util.IntegerFormats.readUInt2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Implementation of a {@link CategoricalBuffer} with category index format {@link Format#UNSIGNED_INT2} that can
 * hold {@code 3} different categories. Four category indices are stored in a single {@code byte}.
 *
 * @author Gisa Meier
 */
public class UInt2CategoricalBuffer<T> extends CategoricalBuffer<T> {

	private final PackedIntegers bytes;
	private boolean frozen = false;
	private final Map<T, Byte> indexLookup = new ConcurrentHashMap<>();
	private final List<T> valueLookup = new ArrayList<>(4);

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	UInt2CategoricalBuffer(int length) {
		bytes = new PackedIntegers(new byte[length % 4 == 0 ? length / 4 : length / 4 + 1], indexFormat(), length);
		valueLookup.add(null); //position 0 stands for missing value, i.e. null
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
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
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
		return ColumnAccessor.get().newCategoricalColumn(type, bytes, valueLookup);
	}

	@Override
	public CategoricalColumn<T> toBooleanColumn(ColumnType<T> type, T positiveValue) {
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		if (valueLookup.size() > BooleanDictionary.MAXIMAL_RAW_SIZE) {
			throw new IllegalArgumentException("Boolean column must have 2 values or less");
		}
		freeze();
		int positiveIndex = BooleanDictionary.NO_ENTRY;
		if (positiveValue != null) {
			Byte index = indexLookup.get(positiveValue);
			if (index == null) {
				throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue)
						+ "\" not in dictionary.");
			}
			positiveIndex = Byte.toUnsignedInt(index);
		}
		return ColumnAccessor.get().newCategoricalColumn(type, bytes, valueLookup, positiveIndex);
	}

}