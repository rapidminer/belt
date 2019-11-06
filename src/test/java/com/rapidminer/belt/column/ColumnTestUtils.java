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
import java.util.List;
import java.util.SplittableRandom;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Some methods used for testing the different column implementations.
 *
 * @author Gisa Meier, Kevin Majchrzak
 */
public class ColumnTestUtils {

	public static Column getSparseDoubleColumn(Column.TypeId typeId, double defaultValue, double[] data) {
		return new DoubleSparseColumn(typeId, defaultValue, data);
	}

	/**
	 * Returns the sparse column's most common default value. Throws an error if the column is not sparse.
	 */
	public static double getDefaultValue(Column sparseColumn) {
		if (sparseColumn instanceof DoubleSparseColumn) {
			return ((DoubleSparseColumn) sparseColumn).getDefaultValue();
		} else if (sparseColumn instanceof TimeSparseColumn) {
			return ((TimeSparseColumn) sparseColumn).getDefaultValue();
		} else if (sparseColumn instanceof DateTimeLowPrecisionSparseColumn) {
			return ((DateTimeLowPrecisionSparseColumn) sparseColumn).getDefaultValue();
		} else if (sparseColumn instanceof DateTimeHighPrecisionSparseColumn) {
			return ((DateTimeHighPrecisionSparseColumn) sparseColumn).getDefaultValue();
		} else if (sparseColumn instanceof CategoricalSparseColumn){
			return ((CategoricalSparseColumn) sparseColumn).getDefaultValue();
		} else if (sparseColumn instanceof RemappedCategoricalSparseColumn){
			return ((RemappedCategoricalSparseColumn) sparseColumn).getDefaultValue();
		} else {
			throw new IllegalArgumentException("Column needs to be sparse to have a default value.");
		}
	}

	/**
	 * Creates a sparse time column.
	 */
	public static TimeColumn getSparseTimeColumn(long[] nanoOfDay) {
		return new TimeSparseColumn(ColumnUtils.getMostFrequentValue(nanoOfDay, true)[0], nanoOfDay);
	}

	/**
	 * Creates a dense time column.
	 */
	public static TimeColumn getDenseTimeColumn(long[] nanoOfDay) {
		return new SimpleTimeColumn(nanoOfDay);
	}

	/**
	 * Creates a dense high precision date time column using the specified seconds of epoch and random nanos.
	 */
	public static DateTimeColumn getDenseHighPrecDateTimeColumn(long[] secondsOfEpoch, long seed) {
		SplittableRandom random = new SplittableRandom(seed);
		int[] nanos = new int[secondsOfEpoch.length];
		Arrays.setAll(nanos, i -> random.nextInt());
		return new SimpleDateTimeColumn(secondsOfEpoch, nanos);
	}

	/**
	 * Creates a dense high precision date time column using the specified seconds of epoch and nanos.
	 */
	public static DateTimeColumn getDenseHighPrecDateTimeColumn(long[] secondsOfEpoch, int[] nanos) {
		return new SimpleDateTimeColumn(secondsOfEpoch, nanos);
	}

	/**
	 * Creates a dense low precision date time column using the specified seconds of epoch.
	 */
	public static DateTimeColumn getDenseLowPrecDateTimeColumn(long[] secondsOfEpoch) {
		return new SimpleDateTimeColumn(secondsOfEpoch, null);
	}

	/**
	 * Creates a sparse high precision date time column using the specified seconds of epoch and random nanos.
	 */
	public static DateTimeColumn getSparseHighPrecDateTimeColumn(long[] secondsOfEpoch, long seed) {
		SplittableRandom random = new SplittableRandom(seed);
		int[] nanos = new int[secondsOfEpoch.length];
		Arrays.setAll(nanos, i -> random.nextInt());
		return new DateTimeHighPrecisionSparseColumn(ColumnUtils.getMostFrequentValue(secondsOfEpoch, true)[0],
				secondsOfEpoch, nanos);
	}

	/**
	 * Creates a sparse high precision date time column using the specified seconds of epoch and random nanos.
	 */
	public static DateTimeColumn getSparseHighPrecDateTimeColumn(long[] secondsOfEpoch, int[] nanos) {
		return new DateTimeHighPrecisionSparseColumn(ColumnUtils.getMostFrequentValue(secondsOfEpoch, true)[0],
				secondsOfEpoch, nanos);
	}

	/**
	 * Creates a sparse low precision date time column using the specified seconds of epoch and random nanos.
	 */
	public static DateTimeColumn getSparseLowPrecDateTimeColumn(long[] secondsOfEpoch) {
		return new DateTimeLowPrecisionSparseColumn(ColumnUtils.getMostFrequentValue(secondsOfEpoch, true)[0],
				secondsOfEpoch);
	}

	/**
	 * Creates a time column (that might be sparse if the data is sparse).
	 *
	 * @param seed
	 * 		This is used for the auto-sparsity heuristic. The method will always return the same sparse / dense column for
	 * 		the same seed. This might be important for testing.
	 * @param data
	 * 		The data that will be used for column creation.
	 * @return a new time column
	 */
	public static TimeColumn getTimeColumn(long seed, double[] data) {
		long[] longData = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			double datum = data[i];
			if (Double.isNaN(datum)) {
				longData[i] = TimeColumn.MISSING_VALUE;
			} else {
				longData[i] = (long) datum;
			}
		}
		return new InternalColumnsImpl().createTimeColumn(longData, new SplittableRandom(seed));
	}

	/**
	 * Creates a categorical column (that might be sparse if the data is sparse).
	 *
	 * @param seed
	 * 		This is used for the auto-sparsity heuristic. The method will always return the same sparse / dense column for
	 * 		the same seed. This might be important for testing.
	 * @param data
	 * 		The data that will be used for column creation.
	 * @param mapping
	 * 		the categorical mapping
	 * @return a new categorical column
	 */
	public static <T> CategoricalColumn<T> getCategoricalColumn(long seed, ColumnType<T> type, short[] data, List<T> mapping) {
		return new InternalColumnsImpl().createCategoricalColumn(type, data, new Dictionary<T>(mapping), new SplittableRandom(seed));
	}

	/**
	 * Creates a sparse categorical column (regardless of the data's actual sparsity).
	 *
	 * @param data
	 * 		The data that will be used for column creation.
	 * @param mapping
	 * 		the categorical mapping
	 * @return a new sparse categorical column
	 */
	public static <T> CategoricalColumn<T> getSparseCategoricalColumn(ColumnType<T> type, short[] data, List<T> mapping, short defaultValue) {
		return new CategoricalSparseColumn<>(type, data, new Dictionary<T>(mapping), defaultValue);
	}

	/**
	 * Creates a sparse categorical column (regardless of the data's actual sparsity).
	 *
	 * @param data
	 * 		The data that will be used for column creation.
	 * @param mapping
	 * 		the categorical mapping
	 * @return a new sparse categorical column
	 */
	public static <T> CategoricalColumn<T> getSparseCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> mapping, byte defaultValue) {
		return new CategoricalSparseColumn<>(type, data, new Dictionary<T>(mapping), defaultValue);
	}

	/**
	 * Creates a sparse categorical column (regardless of the data's actual sparsity).
	 *
	 * @param data
	 * 		The data that will be used for column creation.
	 * @param mapping
	 * 		the categorical mapping
	 * @return a new sparse categorical column
	 */
	public static <T> CategoricalColumn<T> getSparseCategoricalColumn(ColumnType<T> type, int[] data, List<T> mapping, int defaultValue) {
		return new CategoricalSparseColumn<>(type, data, new Dictionary<T>(mapping), defaultValue);
	}

	/**
	 * Creates a categorical column (that might be sparse if the data is sparse).
	 *
	 * @param seed
	 * 		This is used for the auto-sparsity heuristic. The method will always return the same sparse / dense column for
	 * 		the same seed. This might be important for testing.
	 * @param data
	 * 		The data that will be used for column creation.
	 * @param mapping
	 * 		the categorical mapping
	 * @return a new categorical column
	 */
	public static <T> CategoricalColumn<T> getCategoricalColumn(long seed, ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> mapping) {
		return new InternalColumnsImpl().createCategoricalColumn(type, data, new Dictionary<T>(mapping), new SplittableRandom(seed));
	}

	/**
	 * Creates a categorical column (that might be sparse if the data is sparse).
	 *
	 * @param seed
	 * 		This is used for the auto-sparsity heuristic. The method will always return the same sparse / dense column for
	 * 		the same seed. This might be important for testing.
	 * @param data
	 * 		The data that will be used for column creation.
	 * @param mapping
	 * 		the categorical mapping
	 * @return a new categorical column
	 */
	public static <T> CategoricalColumn<T> getCategoricalColumn(long seed, ColumnType<T> type, int[] data, List<T> mapping) {
		return new InternalColumnsImpl().createCategoricalColumn(type, data, new Dictionary<T>(mapping), new SplittableRandom(seed));
	}

	/**
	 * Creates a date time column (that might be sparse if the data is sparse).
	 *
	 * @param seed
	 * 		This is used for the auto-sparsity heuristic. The method will always return the same sparse / dense column for
	 * 		the same seed. This might be important for testing.
	 * @param seconds
	 * 		The seconds that will be used for date time column creation.
	 * @param nanos
	 * 		The nanos that will be used for date time column creation.
	 * @return a new time column
	 */
	public static DateTimeColumn getDateTimeColumn(long seed, long[] seconds, int[] nanos) {
		return new InternalColumnsImpl().createDateTimeColumn(seconds, nanos, new SplittableRandom(seed));
	}

	/**
	 * Creates a time column (that might be sparse if the data is sparse).
	 *
	 * @param seed
	 * 		This is used for the auto-sparsity heuristic. The method will always return the same sparse / dense column for
	 * 		the same seed. This might be important for testing.
	 * @param data
	 * 		The data that will be used for column creation.
	 * @return a new time column
	 */
	public static TimeColumn getTimeColumn(long seed, long[] data) {
		return new InternalColumnsImpl().createTimeColumn(data, new SplittableRandom(seed));
	}

	public static boolean isSparse(TimeColumn column) {
		return column instanceof TimeSparseColumn;
	}

	public static boolean isSparse(CategoricalColumn column) {
		return column instanceof CategoricalSparseColumn || column instanceof RemappedCategoricalSparseColumn;
	}

	public static boolean isSparseLowPrecisionDateTime(DateTimeColumn column) {
		return column instanceof DateTimeLowPrecisionSparseColumn;
	}

	public static boolean isSparse(DateTimeColumn column) {
		return column instanceof DateTimeLowPrecisionSparseColumn || column instanceof DateTimeHighPrecisionSparseColumn;
	}

	public static boolean isSparseHighPrecisionDateTime(DateTimeColumn column) {
		return column instanceof DateTimeHighPrecisionSparseColumn;
	}

	public static boolean isSparse(Column column) {
		return column instanceof DoubleSparseColumn || column instanceof TimeSparseColumn
				|| column instanceof DateTimeLowPrecisionSparseColumn ||
				column instanceof DateTimeHighPrecisionSparseColumn ||
				column instanceof CategoricalSparseColumn || column instanceof RemappedCategoricalSparseColumn;
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

	public static <T> Column getRemappedCategoricalSparseColumn(ColumnType<T> type, int[] data, List<T> dictionary, int[] remapping, int defaultValue) {
		return new RemappedCategoricalSparseColumn<>(type, data, new Dictionary<>(dictionary), remapping, defaultValue);
	}

	public static <T> Column getRemappedCategoricalSparseColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> dictionary, int[] remapping, byte defaultValue) {
		return new RemappedCategoricalSparseColumn<>(type, data, new Dictionary<>(dictionary), remapping, defaultValue);
	}

	public static <T> Column getRemappedCategoricalSparseColumn(ColumnType<T> type, short[] data, List<T> dictionary, int[] remapping, short defaultValue) {
		return new RemappedCategoricalSparseColumn<>(type, data, new Dictionary<>(dictionary), remapping, defaultValue);
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
		return new RemappedMappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), remapping, mapping);
	}

	public static <T> Column getRemappedMappedCategoricalColumn(ColumnType<T> type, short[] data, List<T> dictionary, int[] remapping, int[] mapping) {
		return new RemappedMappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), remapping, mapping);
	}

	public static <T> Column getRemappedMappedCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data, List<T> dictionary, int[] remapping, int[] mapping) {
		return new RemappedMappedCategoricalColumn<>(type, data, new Dictionary<>(dictionary), remapping, mapping);
	}

	public static <T> CategoricalColumn<T> getSimpleCategoricalColumn(ColumnType<T> type, IntegerFormats.PackedIntegers data,
																	  List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	public static <T> CategoricalColumn<T> getSimpleCategoricalColumn(ColumnType<T> type, short[] data, List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	public static <T> CategoricalColumn<T> getSimpleCategoricalColumn(ColumnType<T> type, int[] data, List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	public static Column getMappedNumericColumn(Column.TypeId type, double[] mappedData, int[] mapping) {
		return new MappedDoubleArrayColumn(type, mappedData, mapping);
	}

	public static Column getNumericColumn(Column.TypeId type, double[] data) {
		return new DoubleArrayColumn(type, data);
	}

	public static int[] getIntData(CategoricalColumn column) {
		return column.getIntData();
	}

	public static Column getObjectNotReadableColumn() {
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

	public static Column getDoubleObjectTestColumn() {
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

	public static int getMostFrequentValue(int[] data, int orElse) {
		if (data == null || data.length == 0) {
			return orElse;
		}
		return ColumnUtils.getMostFrequentValue(data)[0];
	}

	public static short getMostFrequentValue(short[] data, short orElse) {
		if (data == null || data.length == 0) {
			return orElse;
		}
		return (short) ColumnUtils.getMostFrequentValue(data)[0];
	}

	public static byte getMostFrequentValue(byte[] data, byte orElse) {
		if (data == null || data.length == 0) {
			return orElse;
		}
		return (byte) ColumnUtils.getMostFrequentValue(data)[0];
	}

	public static long getMostFrequentValue(long[] data, long orElse) {
		if (data == null || data.length == 0) {
			return orElse;
		}
		return ColumnUtils.getMostFrequentValue(data)[0];
	}


	/**
	 * Randomly creates sparse long data.
	 *
	 * @param rng
	 * 		Random number generator.
	 * @param bound
	 * 		The upper bound for the random longs.
	 * @param n
	 * 		The number of random longs (length of the resulting array).
	 * @param defaultValue
	 * 		The most common default value in the generated data.
	 * @param sparsity
	 * 		The required (approximate) frequency of the default value in the generated data.
	 * @return n random longs with the approximately the given sparsity.
	 */
	public static long[] sparseRandomLongs(SplittableRandom rng, long bound, int n, long defaultValue, double sparsity) {
		long[] data = new long[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < sparsity ? defaultValue : rng.nextLong(bound);
		}
		return data;
	}

	/**
	 * Randomly creates sparse int data.
	 *
	 * @param rng
	 * 		Random number generator.
	 * @param bound
	 * 		The upper bound for the random ints.
	 * @param n
	 * 		The number of random ints (length of the resulting array).
	 * @param defaultValue
	 * 		The most common default value in the generated data.
	 * @param sparsity
	 * 		The required (approximate) frequency of the default value in the generated data.
	 * @return n random ints with the approximately the given sparsity.
	 */
	public static int[] sparseRandomInts(SplittableRandom rng, int bound, int n, int defaultValue, double sparsity) {
		int[] data = new int[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < sparsity ? defaultValue : rng.nextInt(bound);
		}
		return data;
	}

	/**
	 * Randomly creates sparse long data (represented by a double array).
	 *
	 * @param rng
	 * 		Random number generator.
	 * @param bound
	 * 		The upper bound for the random longs.
	 * @param n
	 * 		The number of random longs (length of the resulting array).
	 * @param defaultValue
	 * 		The most common default value in the generated data.
	 * @param sparsity
	 * 		The required (approximate) frequency of the default value in the generated data.
	 * @return n random longs (represented by doubles) with the approximately the given sparsity.
	 */
	public static double[] sparseRandomLongs(SplittableRandom rng, long bound, int n, double defaultValue, double sparsity) {
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < sparsity ? defaultValue : rng.nextLong(bound);
		}
		return data;
	}

	/**
	 * Similar to {@link #sparseRandomLongs(SplittableRandom, long, int, long, double)} but adds an offset to the
	 * results.
	 */
	public static double[] sparseRandomLongs(SplittableRandom rng, long bound, int n, double defaultValue, double sparsity, long offset) {
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < sparsity ? defaultValue : rng.nextLong(bound);
			data[i] += offset;
		}
		return data;
	}
}
