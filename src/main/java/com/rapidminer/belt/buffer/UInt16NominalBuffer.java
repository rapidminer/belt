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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * Implementation of a {@link NominalBuffer} with category index format {@link Format#UNSIGNED_INT16} that can
 * hold {@code 65535} different categories. The category indices are stored as {@code short}.
 *
 * @author Gisa Meier
 */
public class UInt16NominalBuffer extends NominalBuffer {


	private final short[] data;
	private boolean frozen = false;
	private final Map<String, Short> indexLookup = new ConcurrentHashMap<>();
	private final List<String> valueLookup = new ArrayList<>();

	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 */
	UInt16NominalBuffer(ColumnType<String> type, int length) {
		super(type);
		data = new short[length];
		valueLookup.add(null); //position 0 stands for missing value, i.e. null
	}

	/**
	 * Copies the data of the given column into a buffer. Throws an {@link UnsupportedOperationException} if the column
	 * category is not {@link Category#CATEGORICAL} or if the compression format of the column has more values
	 * than supported by this buffer.
	 *
	 * @param column
	 * 		the column to convert to a buffer
	 * @param type
	 * 		the desired type of the buffer, must be a super type of the column type
	 */
	UInt16NominalBuffer(Column column, ColumnType<String> type) {
		super(type);
		if (column instanceof CategoricalColumn) {
			CategoricalColumn categoricalColumn = (CategoricalColumn) column;
			Dictionary dictionary = categoricalColumn.getDictionary();
			if (categoricalColumn.getFormat() == indexFormat()) {
				// Same format: directly copy the data
				data = ColumnAccessor.get().getShortDataCopy(categoricalColumn);
			} else if (dictionary.maximalIndex() < indexFormat().maxValue()) {
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

	private void fillStructures(Dictionary values) {
		valueLookup.add(null);
		for (int i = 1; i <= values.maximalIndex(); i++) {
			String value = values.get(i);
			valueLookup.add(value);
			indexLookup.put(value, (short) i);
		}
	}

	@Override
	public String get(int index) {
		int valueIndex = Short.toUnsignedInt(data[index]);
		synchronized (valueLookup) {
			return valueLookup.get(valueIndex);
		}
	}

	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public void set(int index, String value) {
		if (!setSave(index, value)) {
			throw new IllegalArgumentException("More than " + indexFormat().maxValue() + " different values.");
		}
	}

	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public boolean setSave(int index, String value) {
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
	List<String> getMapping() {
		return valueLookup;
	}

	@Override
	void freeze() {
		frozen = true;
	}

	@Override
	public CategoricalColumn toColumn() {
		freeze();
		return ColumnAccessor.get().newCategoricalColumn(type, data, valueLookup);
	}

	@Override
	public CategoricalColumn toBooleanColumn(String positiveValue) {
		freeze();
		if (valueLookup.size() > BooleanDictionary.MAXIMAL_RAW_SIZE) {
			throw new IllegalArgumentException("Boolean column must have 2 values or less");
		}
		int positiveIndex = BooleanDictionary.NO_ENTRY;
		if (positiveValue != null) {
			Short index = indexLookup.get(positiveValue);
			if (index == null) {
				throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue)
						+ "\" not in dictionary.");
			}
			positiveIndex = Short.toUnsignedInt(index);
		}
		return ColumnAccessor.get().newCategoricalColumn(type, data, valueLookup, positiveIndex);
	}

}