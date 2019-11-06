/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General
 * Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License aint with this program. If not, see
 * https://www.gnu.org/licenses/.
 */

package com.rapidminer.belt.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SplittableRandom;

import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnUtils;
import com.rapidminer.belt.util.IntegerArrayBuilder;


/**
 * A sparse {@link ComplexWriter} that grows its size to fit the given data. It stores 32 bit integer values
 * representing categories in a memory efficient sparse format.
 *
 * @author Kevin Majchrzak
 */
final class Int32CategoricalWriterSparse<T> implements ComplexWriter {

	/**
	 * The buffer's (usually most common) default value's object representation.
	 */
	private final T defaultValueAsObject;

	/**
	 * {@code true} if the buffer cannot be modified anymore.
	 */
	private boolean frozen = false;

	/**
	 * The column type used to create columns.
	 */
	private final ColumnType<T> columnType;

	/**
	 * The buffer's (usually most common) default value.
	 */
	private int defaultValue;

	/**
	 * Maps the given categorical value to its corresponding index.
	 */
	private Map<T, Integer> indexLookup;

	/**
	 * List of distinct values stored at their corresponding index positions.
	 */
	private List<T> valueLookup;

	/**
	 * The column that has been created by the buffer or {@code null} if the buffer has not been frozen yet.
	 */
	private Column column;

	/**
	 * The buffer's next logical index.
	 */
	private int nextLogicalIndex;

	/**
	 * pool of chunks used for memory efficient representation of the sparse buffer
	 */
	private IntegerArrayBuilder nonDefaultIndices;

	/**
	 * pool of chunks used for memory efficient representation of the sparse buffer
	 */
	private IntegerArrayBuilder nonDefaultValues;

	/**
	 * Creates a new {@link Int32CategoricalWriterSparse} for the given default value and fills the given categorical
	 * indices (indices[0] to indices[height]) into the writer. For internal use.
	 *
	 * @param type
	 * 		the column type
	 * @param defaultValue
	 * 		the default categorical index value for the new writer
	 * @param indexLookup
	 * 		the index lookup to be used in the column writer (will not be copied but directly used and modified)
	 * @param valueLookup
	 * 		the value lookup to be used in the column writer (will not be copied but directly used and modified)
	 * @param indices
	 * 		The categorical indices to add to the new writer. Only the first 'height' values will be used and the rest will
	 * 		be ignored.
	 * @param height
	 * 		only the first 'height' values of the indices array will be used and the rest will be ignored.
	 */
	static <T> Int32CategoricalWriterSparse<T> ofRawIndices(ColumnType<T> type, int defaultValue,
															Map<T, Integer> indexLookup, List<T> valueLookup,
															int[] indices, int height) {
		return new Int32CategoricalWriterSparse<>(type, defaultValue, indexLookup, valueLookup, indices, height);
	}

	/**
	 * For internal use only.
	 */
	private Int32CategoricalWriterSparse(ColumnType<T> type, int defaultValue,
										 Map<T, Integer> indexLookup, List<T> valueLookup,
										 int[] indices, int height) {
		this.columnType = type;
		this.indexLookup = indexLookup;
		this.valueLookup = valueLookup;
		this.defaultValueAsObject = valueLookup.get(defaultValue);
		this.defaultValue = defaultValue;
		nonDefaultIndices = new IntegerArrayBuilder();
		nonDefaultValues = new IntegerArrayBuilder();
		for (int i = 0; i < height; i++) {
			int value = indices[i];
			if (value != defaultValue) {
				nonDefaultIndices.setNext(i);
				nonDefaultValues.setNext(value);
			}
		}
		nextLogicalIndex = height;
	}

	/**
	 * Creates a sparse column writer which can be used to create a sparse categorical {@link Column}.
	 *
	 * @param type
	 * 		The column type used to define a categorical column.
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 */
	Int32CategoricalWriterSparse(ColumnType<T> type, T defaultValue) {
		this.columnType = type;
		indexLookup = new HashMap<>();
		valueLookup = new ArrayList<>();
		valueLookup.add(null);
		this.defaultValueAsObject = defaultValue;
		if (defaultValueAsObject == null) {
			this.defaultValue = 0;
		} else {
			this.defaultValue = valueLookup.size();
			valueLookup.add(defaultValueAsObject);
			indexLookup.put(defaultValueAsObject, this.defaultValue);
		}
		nextLogicalIndex = 0;
		nonDefaultIndices = new IntegerArrayBuilder();
		nonDefaultValues = new IntegerArrayBuilder();
	}

	@Override
	public void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(nextLogicalIndex + buffer.length / bufferStepSize + remainder, height);
		int bufferIndex = bufferOffset;
		while (nextLogicalIndex < max) {
			// the cast is safe because we check the data that is put into the buffer
			@SuppressWarnings("unchecked")
			T value = (T) buffer[bufferIndex];
			setNext(value);
			bufferIndex += bufferStepSize;
		}
	}

	/**
	 * Returns a column constructed from this buffer. The buffer will be frozen and it becomes read-only. In contrast to
	 * constructing a new buffer from a column this does not copy the data.
	 *
	 * @return a column with the data from this buffer
	 */
	@Override
	public Column toColumn() {
		if (!frozen) {
			frozen = true;
			generateColumnAndFreeChunks();
		}
		return column;
	}

	/**
	 * Sets the next logical index to the given value.
	 */
	protected void setNext(T value) {
		if (!Objects.equals(defaultValueAsObject, value)) {
			nonDefaultIndices.setNext(nextLogicalIndex);
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
		nextLogicalIndex++;
	}

	/**
	 * Returns the buffer's default value (usually but not necessarily the most common value). Used for testing.
	 */
	T getDefaultValue() {
		return defaultValueAsObject;
	}

	/**
	 * Used for testing.
	 */
	List<T> getValueLookup() {
		return valueLookup;
	}

	/**
	 * Generates the final column resulting from the buffer after it has been frozen. Furthermore, the method frees the
	 * memory used by the buffer's chunks, as the chunks are not needed anymore.
	 * <p> The data is checked for sparsity once more to decide whether a sparse or dense column should be created.
	 */
	private void generateColumnAndFreeChunks() {
		int[] finalNonDefaultIndices = nonDefaultIndices.getData();
		// free memory for index chunks
		nonDefaultIndices = null;
		int[] finalNonDefaultValues = nonDefaultValues.getData();
		// free memory for value chunks
		nonDefaultValues = null;
		boolean stillSparseWithSameDefault =
				finalNonDefaultIndices.length / (double) nextLogicalIndex <= ColumnUtils.MAX_DENSITY_DOUBLE_SPARSE_COLUMN;
		if (stillSparseWithSameDefault) {
			column = ColumnAccessor.get().newSparseCategoricalColumn(columnType, finalNonDefaultIndices,
					finalNonDefaultValues, valueLookup, defaultValue, nextLogicalIndex);
		} else {
			Optional<Integer> newDefaultValue = ColumnUtils.estimateDefaultValue(NumericRowWriter.SPARSITY_SAMPLE_SIZE,
					NumericRowWriter.MIN_SPARSITY * nextLogicalIndex / finalNonDefaultIndices.length,
					finalNonDefaultValues, new SplittableRandom());
			boolean sparseWithOtherDefault = newDefaultValue.isPresent();
			if (sparseWithOtherDefault) {
				column = generateSparseColumnWithNewDefault(defaultValue, finalNonDefaultIndices, finalNonDefaultValues,
						nextLogicalIndex, newDefaultValue.get());
			} else {
				column = generateDenseColumn(finalNonDefaultIndices, finalNonDefaultValues);
			}
		}
	}

	/**
	 * Takes a sparse data format defined by the old default value, the old non-default indices, old non-default values
	 * and size. It then transforms the old format to a new sparse data format for the given new default value.
	 *
	 * @param oldDefaultValue
	 * 		the old default value
	 * @param oldNonDefaultIndices
	 * 		the old non-default indices
	 * @param oldNonDefaultValues
	 * 		the old non-default values
	 * @param size
	 * 		the columns size
	 * @param newDefaultValue
	 * 		the new default value
	 */
	private Column generateSparseColumnWithNewDefault(int oldDefaultValue, int[] oldNonDefaultIndices,
													  int[] oldNonDefaultValues, int size, int newDefaultValue) {
		int newNumberOfNonDefaults = size - count(oldNonDefaultValues, newDefaultValue);
		int[] newNonDefaultIndices = new int[newNumberOfNonDefaults];
		int[] newNonDefaultValues = new int[newNumberOfNonDefaults];
		int oldDataIndex = 0;
		int newDataIndex = 0;
		for (int i = 0; i < size; i++) {
			if (i != oldNonDefaultIndices[oldDataIndex]) {
				// Every old default index is now a non-default
				// index with the old default value now being
				// the corresponding non-default value
				newNonDefaultIndices[newDataIndex] = i;
				newNonDefaultValues[newDataIndex] = oldDefaultValue;
				newDataIndex++;
			} else {
				int value = oldNonDefaultValues[oldDataIndex];
				if (newDefaultValue != value) {
					// Every old non-default index with the corresponding
					// non-default value not being the new default value
					// is still a non-default index
					newNonDefaultIndices[newDataIndex] = i;
					newNonDefaultValues[newDataIndex] = value;
					newDataIndex++;
				} // else: new default value -> nothing to do
				oldDataIndex++;
			}
		}
		return ColumnAccessor.get().newSparseCategoricalColumn(columnType, newNonDefaultIndices,
				newNonDefaultValues, valueLookup, newDefaultValue, size);
	}

	/**
	 * Creates a dense column from the given sparse data format.
	 */
	private Column generateDenseColumn(int[] finalNonDefaultIndices, int[] finalNonDefaultValues) {
		Column tmp = ColumnAccessor.get().newSparseCategoricalColumn(columnType, finalNonDefaultIndices,
				finalNonDefaultValues, valueLookup, defaultValue, nextLogicalIndex);
		int[] denseData = new int[nextLogicalIndex];
		tmp.fill(denseData, 0);
		return ColumnAccessor.get().newCategoricalColumn(columnType, denseData, valueLookup);
	}

	/**
	 * Counts all occurrences of element in the given data.
	 */
	private int count(int[] data, int element) {
		int result = 0;
		for (int i = 0; i < data.length; i++) {
			if (data[i] == element) {
				result++;
			}
		}
		return result;
	}

}
