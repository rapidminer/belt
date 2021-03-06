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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.ArrayBuilderConfiguration;
import com.rapidminer.belt.util.IntegerArrayBuilder;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * Implementation of a {@link NominalBufferSparse} with category index format {@link
 * IntegerFormats.Format#SIGNED_INT32} that can hold {@link Integer#MAX_VALUE} many different categories. The category
 * indices are stored as {@code int}.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 *
 * @author Kevin Majchrzak
 * @see NominalBufferSparse
 * @see Buffers
 */
public final class Int32NominalBufferSparse extends NominalBufferSparse {

	/**
	 * The maximum size a chunk can have relative to the overall buffer size. This means that a chunk will never be
	 * larger than {@code MAX_RELATIVE_CHUNK_SIZE * size + 1}.
	 */
	private static final double MAX_RELATIVE_CHUNK_SIZE = 0.01;

	/**
	 * The buffer's logical size.
	 */
	private final int size;

	/**
	 * The buffer's (usually most common) default value.
	 */
	private final int defaultValueAsUnsignedInt;

	/**
	 * The buffer's (usually most common) default value's object representation.
	 */
	private final String defaultValueAsObject;

	/**
	 * Maps the given categorical value to its corresponding index.
	 */
	private final Map<String, Integer> indexLookup;

	/**
	 * List of distinct values stored at their corresponding index positions.
	 */
	private final List<String> valueLookup;

	/**
	 * {@code true} if the buffer cannot be modified anymore.
	 */
	private boolean frozen = false;

	/**
	 * pool of chunks used for memory efficient representation of the sparse buffer
	 */
	private IntegerArrayBuilder nonDefaultIndices;

	/**
	 * pool of chunks used for memory efficient representation of the sparse buffer
	 */
	private IntegerArrayBuilder nonDefaultValues;

	/**
	 * The next free index in the buffer.
	 */
	private int nextLogicalIndex;

	/**
	 * The final non-default indices array or {@code null} if the buffer has not been frozen yet.
	 */
	private int[] finalNonDefaultIndices;

	/**
	 * The final non-default values array or {@code null} if the buffer has not been frozen yet.
	 */
	private int[] finalNonDefaultValues;

	/**
	 * Creates a sparse buffer of the given length used to define a {@link CategoricalColumn}.
	 *
	 * @param defaultValue
	 * 		the data's most common default value.
	 * @param length
	 * 		the length of the buffer
	 */
	Int32NominalBufferSparse(ColumnType<String> type, String defaultValue, int length) {
		super(type);
		if (length < 0) {
			throw new IllegalArgumentException("Sparse categorical buffer cannot have negative length: " + length);
		}
		this.size = length;
		indexLookup = new HashMap<>();
		valueLookup = new ArrayList<>();
		valueLookup.add(null);
		this.defaultValueAsObject = defaultValue;
		if (defaultValueAsObject == null) {
			this.defaultValueAsUnsignedInt = 0;
		} else {
			this.defaultValueAsUnsignedInt = valueLookup.size();
			valueLookup.add(defaultValueAsObject);
			indexLookup.put(defaultValueAsObject, defaultValueAsUnsignedInt);
		}
		nextLogicalIndex = 0;
		ArrayBuilderConfiguration config = new ArrayBuilderConfiguration(null, null,
				(int) (MAX_RELATIVE_CHUNK_SIZE * size + 1));
		nonDefaultIndices = new IntegerArrayBuilder(config);
		nonDefaultValues = new IntegerArrayBuilder(config);
	}

	@Override
	public void setNext(String value) {
		setNext(nextLogicalIndex, value);
	}

	@Override
	public synchronized void setNext(int index, String value) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		checkIndexRange(index);
		if (!Objects.equals(defaultValueAsObject, value)) {
			nonDefaultIndices.setNext(index);
			if (value == null) {
				nonDefaultValues.setNext(0);
			} else {
				Integer mappingIndex = indexLookup.get(value);
				if (mappingIndex != null) {
					nonDefaultValues.setNext(mappingIndex);
				} else {
					int newMappingIndex = valueLookup.size();
					valueLookup.add(value);
					indexLookup.put(value, newMappingIndex);
					nonDefaultValues.setNext(newMappingIndex);
				}
			}
		}
		nextLogicalIndex = index + 1;
	}

	@Override
	public boolean setNextSave(int index, String value) {
		setNext(index, value);
		return true;
	}

	@Override
	public synchronized int differentValues() {
		return valueLookup.size() - 1;
	}

	@Override
	public IntegerFormats.Format indexFormat() {
		return IntegerFormats.Format.SIGNED_INT32;
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return "Sparse 32 bit categorical buffer of length " + size + " with default value " + defaultValueAsObject;
	}

	@Override
	List<String> getMapping() {
		return valueLookup;
	}

	@Override
	public synchronized CategoricalColumn toColumn() {
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		if (!frozen) {
			frozen = true;
			finalizeDataAndFreeChunks();
		}
		return ColumnAccessor.get().newSparseCategoricalColumn(type, finalNonDefaultIndices, finalNonDefaultValues,
				valueLookup, defaultValueAsUnsignedInt, size);
	}

	@Override
	public CategoricalColumn toBooleanColumn(String positiveValue) {
		Objects.requireNonNull(type, "Column type must not be null");
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column type must be categorical");
		}
		if (valueLookup.size() > BooleanDictionary.MAXIMAL_RAW_SIZE) {
			throw new IllegalArgumentException("Boolean column must have 2 values or less");
		}
		int positiveIndex = BooleanDictionary.NO_ENTRY;
		if (positiveValue != null) {
			Integer index = indexLookup.get(positiveValue);
			if (index == null) {
				throw new IllegalArgumentException("Positive value \"" + Objects.toString(positiveValue)
						+ "\" not in dictionary.");
			}
			positiveIndex = index;
		}
		if (!frozen) {
			frozen = true;
			finalizeDataAndFreeChunks();
		}
		return ColumnAccessor.get().newSparseCategoricalColumn(type, finalNonDefaultIndices, finalNonDefaultValues,
				valueLookup, defaultValueAsUnsignedInt, size, positiveIndex);
	}

	@Override
	int getNumberOfNonDefaults() {
		return nonDefaultValues.size();
	}

	/**
	 * Throws an exception if the specified index is out of the buffer's bounds.
	 *
	 * @throws IndexOutOfBoundsException
	 * 		if the index is out of bounds.
	 */
	private void checkIndexRange(int index) {
		if (index < 0 || index >= size) {
			throw new ArrayIndexOutOfBoundsException(index);
		}
		if (index < nextLogicalIndex) {
			throw new IllegalArgumentException("Cannot modify index " + index
					+ ". An equal or larger index has already been set.");
		}
	}

	/**
	 * Generates the final sparse data format resulting from the buffer after it has been frozen. Furthermore, the
	 * method frees the memory used by the buffer's chunks, as the chunks are not needed anymore.
	 */
	private void finalizeDataAndFreeChunks() {
		finalNonDefaultIndices = nonDefaultIndices.getData();
		// free memory for index chunks
		nonDefaultIndices = null;
		finalNonDefaultValues = nonDefaultValues.getData();
		// free memory for value chunks
		nonDefaultValues = null;
	}

}
