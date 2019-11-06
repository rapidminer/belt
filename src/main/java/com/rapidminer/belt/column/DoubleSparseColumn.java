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

package com.rapidminer.belt.column;

import java.util.Arrays;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Sparse implementation for double / integer columns. It uses a more memory efficient representation of the data than
 * the default dense columns while still being comparably fast or faster on sparse data.
 *
 * @author Kevin Majchrzak
 */
class DoubleSparseColumn extends NumericColumn {

	private final ColumnType<Void> columnType;

	/**
	 * The indices that correspond to values that do not equal the default (usually most common) value of the column.
	 * These indices need to be sorted in ascending order.
	 */
	private final int[] nonDefaultIndices;

	/**
	 * The values of the column that do not equal the default value. They need to be specified in the same order as
	 * their corresponding indices in the non-default indices array so that column[nonDefaultIndices[i]] =
	 * nonDefaultValues[i] for all i in [0,nonDefaultIndices.length-1].
	 */
	private final double[] nonDefaultValues;
	private final double defaultValue;

	/**
	 * The number of data rows stored by this column (logical column size).
	 */
	private final int size;

	/**
	 * Creates a new column with the given type id, default value, non-default indices and values. The non-default
	 * indices and values need to be in the same order so that the index stored at the i-th position of the non-default
	 * indices array corresponds to the i-th value in the non-default values array. Furthermore, please note that the
	 * non-default indices need to be sorted in ascending order.
	 *
	 * @param type
	 * 		The columns {@link Column.TypeId}.
	 * @param defaultValue
	 * 		The default value. Usually the most common value is used as default value as this leads to the least memory
	 * 		consumption.
	 * @param nonDefaultIndices
	 * 		Sorted array of non-default indices.
	 * @param nonDefaultValues
	 * 		Array of non-default values corresponding to the non-default indices.
	 * @param size
	 * 		Number of data rows in the original data (logical size of the column).
	 */
	DoubleSparseColumn(TypeId type, double defaultValue, int[] nonDefaultIndices, double[] nonDefaultValues, int size) {
		super(size);
		this.size = size;
		columnType = type == TypeId.INTEGER ? ColumnTypes.INTEGER : ColumnTypes.REAL;
		this.defaultValue = defaultValue;
		this.nonDefaultIndices = nonDefaultIndices;
		this.nonDefaultValues = nonDefaultValues;
	}

	/**
	 * Creates a new column with the given type id, default value and data.
	 *
	 * @param type
	 * 		The columns {@link Column.TypeId}.
	 * @param defaultValue
	 * 		The default value. Usually the most common value is used as default value as this leads to the least memory
	 * 		consumption.
	 * @param data
	 * 		The data that this column should hold. Every entry in the data array will be used as one row in the resulting
	 * 		column.
	 */
	DoubleSparseColumn(TypeId type, double defaultValue, double[] data) {
		super(data.length);
		this.size = data.length;
		columnType = type == TypeId.INTEGER ? ColumnTypes.INTEGER : ColumnTypes.REAL;
		this.defaultValue = defaultValue;
		boolean defaultIsNan = Double.isNaN(defaultValue);
		int numberOfNonDefaults = 0;
		for (double value : data) {
			if (!isDefault(value, defaultIsNan)) {
				numberOfNonDefaults++;
			}
		}
		nonDefaultIndices = new int[numberOfNonDefaults];
		nonDefaultValues = new double[numberOfNonDefaults];
		int index = 0;
		for (int i = 0; i < data.length; i++) {
			if (!isDefault(data[i], defaultIsNan)) {
				nonDefaultIndices[index] = i;
				nonDefaultValues[index++] = data[i];
			}
		}
	}

	@Override
	public ColumnType<Void> type() {
		return columnType;
	}

	@Override
	public Column map(int[] mapping, boolean preferView) {
		if (mapping.length == 0) {
			return stripData();
		}

		SparseBitmap bitMap = new SparseBitmap(Double.isNaN(defaultValue), nonDefaultIndices, size);

		int numberOfNonDefaults = bitMap.countNonDefaultIndices(mapping);
		if (mapping.length * ColumnUtils.MAX_DENSITY_DOUBLE_SPARSE_COLUMN < numberOfNonDefaults) {
			// column is not sparse enough anymore
			return makeDenseColumn(mapping);
		}

		int[] newNonDefaultIndices = new int[numberOfNonDefaults];
		double[] newNonDefaultValues = new double[numberOfNonDefaults];
		int nonDefaultIndex = 0;
		for (int i = 0; i < mapping.length; i++) {
			int bitMapIndex = bitMap.get(mapping[i]);
			if (bitMapIndex != SparseBitmap.DEFAULT_INDEX) {
				newNonDefaultIndices[nonDefaultIndex] = i;
				if (bitMapIndex == SparseBitmap.OUT_OF_BOUNDS_INDEX) {
					newNonDefaultValues[nonDefaultIndex++] = Double.NaN;
				} else {
					newNonDefaultValues[nonDefaultIndex++] = nonDefaultValues[bitMapIndex];
				}
			}
		}
		return new DoubleSparseColumn(type().id(), defaultValue, newNonDefaultIndices, newNonDefaultValues,
				mapping.length);
	}

	@Override
	public void fill(double[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValue);
		if (nonDefaultIndices.length == 0){
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = nonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	public void fill(double[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException("step size must not be smaller than 1");
		}
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValue;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] = nonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	public int[] sort(Order order) {
		// 1) calc sorted mapping for the non-default values and 1x the default value
		// as the default value does not have an index yet, we need to add an artificial index for it
		int newIndexForDefault = nonDefaultIndices.length;
		double[] toSort = Arrays.copyOf(nonDefaultValues, newIndexForDefault + 1);
		toSort[newIndexForDefault] = defaultValue;
		int[] sortedMapping = Sorting.sort(toSort, order);
		// find where default value index is in sorted result
		int defaultPosition = 0;
		for (int i = 0; i < sortedMapping.length; i++) {
			if (sortedMapping[i] == newIndexForDefault) {
				defaultPosition = i;
				break;
			}
		}
		// 2) map the non-default indices using the sorted mapping
		// (This will lead to a 0 at the defaultPosition because newIndexForDefault is out of range for the
		// nonDefaultIndices array. But that is no problem as we are not using this value.)
		int[] sortedNonDefaultIndices = Mapping.apply(nonDefaultIndices, sortedMapping);

		// 3) add the mapped non-default indices with values < the default value to the result
		int[] sortResult = new int[size];
		System.arraycopy(sortedNonDefaultIndices, 0, sortResult, 0, defaultPosition);

		// 4) add the default indices after that
		int index = defaultPosition;
		int last = -1;
		for (int i : nonDefaultIndices) {
			for (int j = last + 1; j < i; j++) {
				sortResult[index++] = j;
			}
			last = i;
		}
		// add the default indices after last non-default until size
		for (int j = last + 1; j < size; j++) {
			sortResult[index++] = j;
		}

		// 5) add the rest of the non-default indices after that
		if (defaultPosition < sortedNonDefaultIndices.length - 1) {
			System.arraycopy(sortedNonDefaultIndices, defaultPosition + 1, sortResult, index,
					sortResult.length - index);
		}

		return sortResult;
	}

	/**
	 * Returns the columns default value (usually but not necessarily the most common value).
	 *
	 * @return the columns default value.
	 */
	double getDefaultValue() {
		return defaultValue;
	}

	/**
	 * Checks if the specified value is this columns default value.
	 */
	private boolean isDefault(double value, boolean defaultIsNan) {
		if (defaultIsNan) {
			return Double.isNaN(value);
		}
		return defaultValue == value;
	}

	/**
	 * Creates a {@link DoubleArrayColumn} by applying the given mapping to this column.
	 */
	private Column makeDenseColumn(int[] mapping) {
		double[] data = new double[size];
		fill(data, 0);
		return new DoubleArrayColumn(columnType.id(), Mapping.apply(data, mapping));
	}
}
