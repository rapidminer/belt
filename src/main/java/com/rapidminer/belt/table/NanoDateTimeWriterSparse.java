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

package com.rapidminer.belt.table;

import java.time.Instant;
import java.util.Optional;
import java.util.SplittableRandom;

import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnUtils;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.util.IntegerArrayBuilder;
import com.rapidminer.belt.util.LongArrayBuilder;


/**
 * A sparse {@link ComplexWriter} that grows its size to fit the given data. It stores long values representing seconds
 * of epoch and integer values representing nanoseconds in a memory efficient sparse format.
 *
 * @author Kevin Majchrzak
 */
final class NanoDateTimeWriterSparse implements ComplexWriter {

	/**
	 * The buffer's (usually most common) default value.
	 */
	private final long defaultValue;

	/**
	 * {@code true} if the buffer cannot be modified anymore.
	 */
	private boolean frozen = false;

	/**
	 * The column that has been created by the buffer or {@code null} if the buffer has not been frozen yet.
	 */
	private Column column;

	/**
	 * The buffer's next logical index.
	 */
	private int nextLogicalIndex;

	/**
	 * pool of chunks used for memory efficient representation of the sparse non-default indices
	 */
	private IntegerArrayBuilder nonDefaultIndices;

	/**
	 * pool of chunks used for memory efficient representation of the sparse non-default values
	 */
	private LongArrayBuilder nonDefaultValues;

	/**
	 * pool of chunks used for memory efficient representation of the dense nanosecond data
	 */
	private IntegerArrayBuilder nanos;

	/**
	 * Creates a new {@link NanoDateTimeWriterSparse} with the given default value and fills it with the given second
	 * and nanosecond data (seconds[0] to seconds[height] and nanos[0] to nanos[height]). For internal use.
	 *
	 * @param defaultValue
	 * 		the default value for the new writer
	 * @param seconds
	 * 		The seconds to add to the new writer. Only the first 'height' values will be used and the rest will be
	 * 		ignored.
	 * @param nanos
	 * 		The nanoseconds to add to the new writer. Only the first 'height' values will be used and the rest will be
	 * 		ignored.
	 * @param height
	 * 		only the first 'height' values of the seconds and nanos arrays will be used and the rest will be ignored.
	 */
	static NanoDateTimeWriterSparse ofSecondsAndNanos(long defaultValue, long[] seconds, int[] nanos, int height) {
		NanoDateTimeWriterSparse newWriter = new NanoDateTimeWriterSparse(defaultValue);
		for (int i = 0; i < height; i++) {
			newWriter.setNext(seconds[i], nanos[i]);
		}
		return newWriter;
	}

	/**
	 * Creates a sparse column writer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#TIME}.
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 */
	private NanoDateTimeWriterSparse(long defaultValue) {
		this.defaultValue = defaultValue;
		nextLogicalIndex = 0;
		nonDefaultIndices = new IntegerArrayBuilder();
		nonDefaultValues = new LongArrayBuilder();
		nanos = new IntegerArrayBuilder();
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
			Instant instant = (Instant) buffer[bufferIndex];
			if (instant == null) {
				setNext(DateTimeColumn.MISSING_VALUE, 0);
			} else {
				setNext(instant.getEpochSecond(), instant.getNano());
			}
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
	private void setNext(long secondsOfEpoch, int nanoSeconds) {
		if (secondsOfEpoch != defaultValue) {
			nonDefaultIndices.setNext(nextLogicalIndex);
			nonDefaultValues.setNext(secondsOfEpoch);
		}
		nanos.setNext(nanoSeconds);
		nextLogicalIndex++;
	}

	/**
	 * Returns the buffer's default value (usually but not necessarily the most common value). Used for testing.
	 */
	long getDefaultValue() {
		return defaultValue;
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
		long[] finalNonDefaultValues = nonDefaultValues.getData();
		// free memory for value chunks
		nonDefaultValues = null;
		int[] finalNanos = nanos.getData();
		// free memory for nano chunks
		nanos = null;
		boolean stillSparseWithSameDefault =
				finalNonDefaultIndices.length / (double) nextLogicalIndex <= ColumnUtils.MAX_DENSITY_DOUBLE_SPARSE_COLUMN;
		if (stillSparseWithSameDefault) {
			column = ColumnAccessor.get().newSparseDateTimeColumn(defaultValue, finalNonDefaultIndices,
					finalNonDefaultValues, finalNanos, nextLogicalIndex);
		} else {
			Optional<Long> newDefaultValue = ColumnUtils.estimateDefaultValue(NumericRowWriter.SPARSITY_SAMPLE_SIZE,
					NumericRowWriter.MIN_SPARSITY * nextLogicalIndex / finalNonDefaultIndices.length,
					finalNonDefaultValues, new SplittableRandom());
			boolean sparseWithOtherDefault = newDefaultValue.isPresent();
			if (sparseWithOtherDefault) {
				column = generateSparseColumnWithNewDefault(defaultValue, finalNonDefaultIndices, finalNonDefaultValues,
						finalNanos, nextLogicalIndex, newDefaultValue.get());
			} else {
				column = generateDenseColumn(finalNonDefaultIndices, finalNonDefaultValues, finalNanos);
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
	private Column generateSparseColumnWithNewDefault(long oldDefaultValue, int[] oldNonDefaultIndices,
													  long[] oldNonDefaultValues, int[] nanos, int size,
													  long newDefaultValue) {
		int newNumberOfNonDefaults = size - count(oldNonDefaultValues, newDefaultValue);
		int[] newNonDefaultIndices = new int[newNumberOfNonDefaults];
		long[] newNonDefaultValues = new long[newNumberOfNonDefaults];
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
				long value = oldNonDefaultValues[oldDataIndex];
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
		return ColumnAccessor.get().newSparseDateTimeColumn(newDefaultValue, newNonDefaultIndices,
				newNonDefaultValues, nanos, size);
	}

	/**
	 * Creates a dense column from the given sparse data format.
	 */
	private Column generateDenseColumn(int[] finalNonDefaultIndices, long[] finalNonDefaultValues, int[] nanos) {
		DateTimeColumn tmp = ColumnAccessor.get().newSparseDateTimeColumn(defaultValue, finalNonDefaultIndices,
				finalNonDefaultValues, nanos, nextLogicalIndex);
		long[] denseSeconds = new long[nextLogicalIndex];
		tmp.fillSecondsIntoArray(denseSeconds, 0);
		return ColumnAccessor.get().newDateTimeColumn(denseSeconds, nanos);
	}

	/**
	 * Counts all occurrences of element in the given data.
	 */
	private int count(long[] data, long element) {
		int result = 0;
		for (int i = 0; i < data.length; i++) {
			if (data[i] == element) {
				result++;
			}
		}
		return result;
	}

}
