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
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see
 * https://www.gnu.org/licenses/.
 */

package com.rapidminer.belt.table;

import java.util.Optional;
import java.util.SplittableRandom;

import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnUtils;
import com.rapidminer.belt.util.DoubleArrayBuilder;
import com.rapidminer.belt.util.IntegerArrayBuilder;


/**
 * A sparse {@link NumericColumnWriter} that grows its size to fit the given data. It stores double values in a memory
 * efficient sparse format.
 *
 * @author Kevin Majchrzak
 */
class RealColumnWriterSparse implements NumericColumnWriter {

	/**
	 * The buffer's (usually most common) default value.
	 */
	private final double defaultValue;

	/**
	 * {@code true} iff the buffer's default value is {@link Double#NaN}.
	 */
	private final boolean defaultIsNan;

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
	private DoubleArrayBuilder nonDefaultValues;

	/**
	 * The column that has been created by the buffer or {@code null} if the buffer has not been frozen yet.
	 */
	private Column column;

	/**
	 * The buffer's next logical index.
	 */
	private int nextLogicalIndex;

	/**
	 * Creates a sparse column writer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#REAL}.
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 */
	RealColumnWriterSparse(double defaultValue) {
		defaultIsNan = Double.isNaN(defaultValue);
		this.defaultValue = defaultValue;
		nextLogicalIndex = 0;
		nonDefaultIndices = new IntegerArrayBuilder();
		nonDefaultValues = new DoubleArrayBuilder();
	}

	@Override
	public void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height) {
		if (frozen) {
			throw new IllegalStateException(NumericBuffer.BUFFER_FROZEN_MESSAGE);
		}
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(nextLogicalIndex + buffer.length / bufferStepSize + remainder, height);
		int bufferIndex = bufferOffset;
		while (nextLogicalIndex < max) {
			setNext(buffer[bufferIndex]);
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
	protected void setNext(double value) {
		if (!isDefault(value)) {
			nonDefaultIndices.setNext(nextLogicalIndex);
			nonDefaultValues.setNext(value);
		}
		nextLogicalIndex++;
	}

	/**
	 * Returns the buffer's {@link Column.TypeId}.
	 *
	 * @return {@link Column.TypeId#REAL}.
	 */
	protected Column.TypeId type() {
		return Column.TypeId.REAL;
	}

	/**
	 * Returns the buffer's default value (usually but not necessarily the most common value). Used for testing.
	 */
	double getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Checks if the specified value is this buffer's default value.
	 */
	private boolean isDefault(double value) {
		if (defaultIsNan) {
			return Double.isNaN(value);
		}
		return defaultValue == value;
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
		double[] finalNonDefaultValues = nonDefaultValues.getData();
		// free memory for value chunks
		nonDefaultValues = null;
		boolean stillSparseWithSameDefault =
				finalNonDefaultIndices.length / (double) nextLogicalIndex <= ColumnUtils.MAX_DENSITY_DOUBLE_SPARSE_COLUMN;
		if (stillSparseWithSameDefault) {
			column = ColumnAccessor.get().newSparseNumericColumn(type(), defaultValue, finalNonDefaultIndices,
					finalNonDefaultValues, nextLogicalIndex);
		} else {
			Optional<Double> newDefaultValue = ColumnUtils.estimateDefaultValue(NumericRowWriter.SPARSITY_SAMPLE_SIZE,
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
	private Column generateSparseColumnWithNewDefault(double oldDefaultValue, int[] oldNonDefaultIndices,
													  double[] oldNonDefaultValues, int size, double newDefaultValue) {
		boolean newDefaultIsNan = Double.isNaN(newDefaultValue);
		int newNumberOfNonDefaults = size - count(oldNonDefaultValues, newDefaultValue);
		int[] newNonDefaultIndices = new int[newNumberOfNonDefaults];
		double[] newNonDefaultValues = new double[newNumberOfNonDefaults];
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
				double value = oldNonDefaultValues[oldDataIndex];
				if (newDefaultIsNan ? !Double.isNaN(value) : newDefaultValue != value) {
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
		return ColumnAccessor.get().newSparseNumericColumn(type(), newDefaultValue, newNonDefaultIndices,
				newNonDefaultValues, size);
	}

	/**
	 * Creates a dense column from the given sparse data format.
	 */
	private Column generateDenseColumn(int[] finalNonDefaultIndices, double[] finalNonDefaultValues) {
		Column tmp = ColumnAccessor.get().newSparseNumericColumn(type(), defaultValue, finalNonDefaultIndices,
				finalNonDefaultValues, nextLogicalIndex);
		double[] denseData = new double[nextLogicalIndex];
		tmp.fill(denseData, 0);
		return ColumnAccessor.get().newNumericColumn(type(), denseData);
	}

	/**
	 * Counts all occurrences of element in the given data.
	 */
	private int count(double[] data, double element) {
		int result = 0;
		if (Double.isNaN(element)) {
			for (int i = 0; i < data.length; i++) {
				if (Double.isNaN(data[i])) {
					result++;
				}
			}
		} else {
			for (int i = 0; i < data.length; i++) {
				if (data[i] == element) {
					result++;
				}
			}
		}
		return result;
	}

}
