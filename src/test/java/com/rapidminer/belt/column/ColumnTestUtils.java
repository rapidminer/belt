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

import java.util.List;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * @author Gisa Meier
 */
public class ColumnTestUtils {

	public static Column getSparseDoubleColumn(Column.TypeId typeId, double defaultValue, double[] data) {
		return new DoubleSparseColumn(typeId, defaultValue, data);
	}

	public static Column getMappedTimeColumn(long[] data, int[] mapping) {
		return new MappedTimeColumn(data, mapping);
	}

	public static Column getMappedDateTimeColumn(long[] data, int[] nanos, int[] mapping) {
		if (nanos == null) {
			return new MappedDateTimeColumn(data, mapping);
		}
		return new MappedDateTimeColumn(data, nanos, mapping);
	}

	public static <T> Column getMappedObjectColumn(ColumnType<T> type, Object[] data, int[] mapping) {
		return new MappedObjectColumn<>(type, data, mapping);
	}

	public static <T> Column getObjectColumn(ColumnType<T> type, Object[] data) {
		return new SimpleObjectColumn<>(type, data);
	}

	public static <T> Column getRemappedCategoricalColumn(ColumnType<T> type, int[] data, List<T> dictionary, int[] remapping) {
		return new RemappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), remapping);
	}

	public static <T> Column getRemappedCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> dictionary, int[] remapping) {
		return new RemappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), remapping);
	}

	public static <T> Column getRemappedCategoricalColumn(ColumnType<T> type, short[] data, List<T> dictionary, int[] remapping) {
		return new RemappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), remapping);
	}

	public static <T> Column getMappedCategoricalColumn(ColumnType<T> type, int[] data, List<T> dictionary, int[] mapping) {
		return new MappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), mapping);
	}

	public static <T> Column getMappedCategoricalColumn(ColumnType<T> type, short[] data, List<T> dictionary, int[] mapping) {
		return new MappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), mapping);
	}

	public static <T> Column getMappedCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> dictionary, int[] mapping) {
		return new MappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), mapping);
	}

	public static <T> Column getRemappedMappedCategoricalColumn(ColumnType<T> type, int[] data, List<T> dictionary, int[] remapping, int[] mapping) {
		return new RemappedMappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary),remapping, mapping);
	}

	public static <T> Column getRemappedMappedCategoricalColumn(ColumnType<T> type, short[] data, List<T> dictionary, int[] remapping, int[] mapping) {
		return new RemappedMappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary),remapping, mapping);
	}

	public static <T> Column getRemappedMappedCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> dictionary, int[] remapping, int[] mapping) {
		return new RemappedMappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary),remapping, mapping);
	}

	public static <T> CategoricalColumn <T> getCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data,
												  List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	public static <T> CategoricalColumn <T> getCategoricalColumn(ColumnType<T> type, short[] data, List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	public static <T> CategoricalColumn <T> getCategoricalColumn(ColumnType<T> type, int[] data, List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	public static Column getMappedNumericColumn(Column.TypeId type, double[] mappedData, int[] mapping) {
		return new MappedDoubleArrayColumn(type, mappedData, mapping);
	}

	public static Column getNumericColumn(Column.TypeId type, double[] data) {
		return new DoubleArrayColumn(type, data);
	}

	public static int[] getIntData(CategoricalColumn column){
		return column.getIntData();
	}

	public static Column getObjectNotReadableColumn(){
		return new Column(3) {
			@Override
			public ColumnType<?> type() {
				return ColumnTypes.NOMINAL;
			}

			@Override
			Column map(int[] mapping, boolean preferView) {
				return null;
			}

			@Override
			public Column stripData() {
				return null;
			}
		};
	}

	public static Column getDoubleObjectTestColumn(){
		return new ObjectColumn<Double>(ColumnTypes.objectType(
				"com.rapidminer.belt.column.test.bigdoublecolumn", Double.class, null), 10) {
			@Override
			public void fill(Object[] array, int rowIndex) {
			}

			@Override
			public void fill(Object[] array, int startIndex, int bufferOffset, int bufferStepSize) {
				int remainder = array.length % bufferStepSize > bufferOffset ? 1 : 0;
				int max = Math.min(startIndex + array.length / bufferStepSize + remainder, 10);
				int rowIndex = startIndex;
				int bufferIndex = bufferOffset;
				while (rowIndex < max) {
					array[bufferIndex] = 1.0;
					bufferIndex += bufferStepSize;
					rowIndex++;
				}
			}

			@Override
			Column map(int[] mapping, boolean preferView) {
				return null;
			}

		};
	}

	public static double getMostFrequentValue(double[] data, double orElse) {
		if (data == null || data.length == 0) {
			return orElse;
		}
		return ColumnUtils.getMostFrequentValue(data)[0];
	}
}
