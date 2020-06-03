/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
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
import java.util.Optional;
import java.util.SplittableRandom;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Implementation for internal column access.
 *
 * @author Gisa Meier, Kevin Majchrzak
 */
final class InternalColumnsImpl extends Columns.InternalColumns {

	/**
	 * Columns with a size below this value will always be represented via dense columns. See the implementation of
	 * {@link #createNumericColumn(Column.TypeId, double[], SplittableRandom)} for details on its usage.
	 */
	private static final int MIN_SPARSE_COLUMN_SIZE = 1024;

	/**
	 * Number of samples used to estimate if a data set is sufficiently sparse to be efficiently represented via a
	 * sparse column implementation. See the implementation of {@link #createNumericColumn(Column.TypeId, double[],
	 * SplittableRandom)} for details on its usage.
	 */
	private static final int SPARSITY_SAMPLE_SIZE = 1024;

	/**
	 * This is a heuristic value used as threshold when predicting if a data set is sufficiently sparse to be
	 * efficiently represented via a sparse column implementation. We need different thresholds for different data
	 * representations (64, 32, 16 or 8 bit) because the memory efficiency depends on these data representations. See
	 * the implementation of {@link #createNumericColumn(Column.TypeId, double[], SplittableRandom)} for details on its
	 * usage.
	 */
	static final double MIN_SPARSITY_64BIT = 0.625d;

	/**
	 * This is a heuristic value used as threshold when predicting if a data set is sufficiently sparse to be
	 * efficiently represented via a sparse column implementation. We need different thresholds for different data
	 * representations (64, 32, 16 or 8 bit) because the memory efficiency depends on these data representations. See
	 * the implementation of {@link #createCategoricalColumn(ColumnType, int[], Dictionary, SplittableRandom)} for
	 * details on its usage.
	 */
	static final double MIN_SPARSITY_32BIT = 0.625d;

	/**
	 * This is a heuristic value used as threshold when predicting if a data set is sufficiently sparse to be
	 * efficiently represented via a sparse column implementation. We need different thresholds for different data
	 * representations (64, 32, 16 or 8 bit) because the memory efficiency depends on these data representations. See
	 * the implementation of {@link #createCategoricalColumn(ColumnType, short[], Dictionary, SplittableRandom)} for
	 * details on its usage.
	 */
	static final double MIN_SPARSITY_16BIT = 0.72d;

	/**
	 * This is a heuristic value used as threshold when predicting if a data set is sufficiently sparse to be
	 * efficiently represented via a sparse column implementation. We need different thresholds for different data
	 * representations (64, 32, 16 or 8 bit) because the memory efficiency depends on these data representations. See
	 * the implementation of {@link #createCategoricalColumn(ColumnType, IntegerFormats.PackedIntegers, Dictionary,
	 * SplittableRandom)} for details on its usage.
	 */
	static final double MIN_SPARSITY_8BIT = 0.85d;


	@Override
	public CategoricalColumn newCategoricalColumn(ColumnType<String> type, int[] data,
												  List<String> dictionary) {
		return createCategoricalColumn(type, data, new Dictionary(dictionary), new SplittableRandom());
	}

	@Override
	public CategoricalColumn newCategoricalColumn(ColumnType<String> type, short[] data,
												  List<String> dictionary) {
		return createCategoricalColumn(type, data, new Dictionary(dictionary), new SplittableRandom());
	}

	@Override
	public CategoricalColumn newCategoricalColumn(ColumnType<String> type,
												  IntegerFormats.PackedIntegers bytes,
												  List<String> dictionary) {
		return createCategoricalColumn(type, bytes, new Dictionary(dictionary), new SplittableRandom());
	}

	@Override
	public CategoricalColumn newCategoricalColumn(ColumnType<String> type, int[] data,
												  List<String> dictionary, int positiveIndex) {
		return createCategoricalColumn(type, data, new BooleanDictionary(dictionary, positiveIndex),
				new SplittableRandom());
	}

	@Override
	public CategoricalColumn newCategoricalColumn(ColumnType<String> type, short[] data,
												  List<String> dictionary, int positiveIndex) {
		return createCategoricalColumn(type, data, new BooleanDictionary(dictionary, positiveIndex),
				new SplittableRandom());
	}

	@Override
	public CategoricalColumn newCategoricalColumn(ColumnType<String> type,
												  IntegerFormats.PackedIntegers bytes,
												  List<String> dictionary, int positiveIndex) {
		return createCategoricalColumn(type, bytes, new BooleanDictionary(dictionary, positiveIndex),
				new SplittableRandom());
	}

	@Override
	public CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
														int[] nonDefaultValues,
														List<String> dictionary, int defaultValue, int size) {
		return new CategoricalSparseColumn(type, nonDefaultIndices, nonDefaultValues, new Dictionary(dictionary),
				defaultValue, size);
	}

	@Override
	public CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
														short[] nonDefaultValues,
														List<String> dictionary, short defaultValue, int size) {
		return new CategoricalSparseColumn(type, nonDefaultIndices, nonDefaultValues, new Dictionary(dictionary),
				defaultValue, size);
	}

	@Override
	public CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
														byte[] nonDefaultValues,
														List<String> dictionary, byte defaultValue, int size) {
		return new CategoricalSparseColumn(type, nonDefaultIndices, nonDefaultValues, new Dictionary(dictionary),
				defaultValue, size);
	}

	@Override
	public CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
														int[] nonDefaultValues,
														List<String> dictionary, int defaultValue, int size,
														int positiveIndex) {
		return new CategoricalSparseColumn(type, nonDefaultIndices, nonDefaultValues,
				new BooleanDictionary(dictionary, positiveIndex), defaultValue, size);
	}

	@Override
	public CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
														short[] nonDefaultValues,
														List<String> dictionary, short defaultValue, int size,
														int positiveIndex) {
		return new CategoricalSparseColumn(type, nonDefaultIndices, nonDefaultValues,
				new BooleanDictionary(dictionary, positiveIndex), defaultValue, size);
	}

	@Override
	public CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
														byte[] nonDefaultValues,
														List<String> dictionary, byte defaultValue, int size,
														int positiveIndex) {
		return new CategoricalSparseColumn(type, nonDefaultIndices, nonDefaultValues,
				new BooleanDictionary(dictionary, positiveIndex), defaultValue, size);
	}

	@Override
	public TimeColumn newTimeColumn(long[] data) {
		return createTimeColumn(data, new SplittableRandom());
	}

	@Override
	public <T> SimpleObjectColumn<T> newObjectColumn(ColumnType<T> type, Object[] data) {
		return new SimpleObjectColumn<>(type, data);
	}

	@Override
	public NumericColumn newNumericColumn(Column.TypeId type, double[] src) {
		return createNumericColumn(type, src, new SplittableRandom());
	}

	@Override
	public Column newSparseNumericColumn(Column.TypeId type, double defaultValue, int[] nonDefaultIndices,
										 double[] nonDefaultValues, int size) {
		return new DoubleSparseColumn(type, defaultValue, nonDefaultIndices, nonDefaultValues, size);
	}

	@Override
	public DateTimeColumn newDateTimeColumn(long[] seconds, int[] nanos) {
		return createDateTimeColumn(seconds, nanos, new SplittableRandom());
	}

	@Override
	public Column newSparseTimeColumn(long defaultValue, int[] nonDefaultIndices, long[] nonDefaultValues, int size) {
		return new TimeSparseColumn(defaultValue, nonDefaultIndices, nonDefaultValues, size);
	}

	@Override
	public DateTimeColumn newSparseDateTimeColumn(long defaultValue, int[] nonDefaultIndices, long[] nonDefaultSeconds,
												  int[] nanos, int size) {
		if (nanos == null || nanos.length == 0 || allNull(nanos)) {
			return new DateTimeLowPrecisionSparseColumn(defaultValue, nonDefaultIndices, nonDefaultSeconds, size);
		} else {
			return new DateTimeHighPrecisionSparseColumn(defaultValue, nonDefaultIndices, nonDefaultSeconds, size,
					nanos);
		}
	}

	@Override
	public CategoricalColumn newSingleValueCategoricalColumn(ColumnType<String> type, String value, int size) {
		return new CategoricalSparseColumn(type, new int[]{}, new byte[]{},
				new Dictionary(Arrays.asList(null, value)), (byte) 1, size);
	}

	@Override
	public byte[] getByteDataCopy(CategoricalColumn column) {
		byte[] originalData = column.getByteData().data();
		return Arrays.copyOf(originalData, originalData.length);
	}

	@Override
	public short[] getShortDataCopy(CategoricalColumn column) {
		short[] originalData = column.getShortData();
		return Arrays.copyOf(originalData, originalData.length);
	}

	@Override
	public List<String> getDictionaryList(Dictionary dictionary) {
		return dictionary.getValueList();
	}

	@Override
	public Column map(Column column, int[] mapping, boolean preferView) {
		return column.map(mapping, preferView);
	}

	/**
	 * Numeric columns that have less than {@link #MIN_SPARSE_COLUMN_SIZE} rows are represented via dense columns.
	 * Otherwise an estimate of the columns sparsity is calculated via {@link ColumnUtils#estimateDefaultValue(int,
	 * double, double[], SplittableRandom)} with {@code sampleSize = } {@link #SPARSITY_SAMPLE_SIZE}. If the estimated
	 * sparsity is {@code >=} {@link #MIN_SPARSITY_64BIT}, the column will be represented by a sparse column
	 * implementation that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	NumericColumn createNumericColumn(Column.TypeId type, double[] src, SplittableRandom random) {
		if (src.length < MIN_SPARSE_COLUMN_SIZE) {
			return new DoubleArrayColumn(type, src);
		}
		Optional<Double> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE, MIN_SPARSITY_64BIT, src, random);
		if (defaultValue.isPresent()) {
			return new DoubleSparseColumn(type, defaultValue.get(), src);
		} else {
			return new DoubleArrayColumn(type, src);
		}
	}

	/**
	 * Time columns that have less than {@link #MIN_SPARSE_COLUMN_SIZE} rows are represented via dense columns.
	 * Otherwise an estimate of the columns sparsity is calculated via {@link ColumnUtils#estimateDefaultValue(int,
	 * double, long[], SplittableRandom)} with {@code sampleSize = } {@link #SPARSITY_SAMPLE_SIZE}. If the estimated
	 * sparsity is {@code >=} {@link #MIN_SPARSITY_64BIT}, the column will be represented by a sparse column
	 * implementation that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	TimeColumn createTimeColumn(long[] src, SplittableRandom random) {
		if (src.length < MIN_SPARSE_COLUMN_SIZE) {
			return new SimpleTimeColumn(src);
		}
		Optional<Long> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE, MIN_SPARSITY_64BIT, src, random);
		if (defaultValue.isPresent()) {
			return new TimeSparseColumn(defaultValue.get(), src);
		} else {
			return new SimpleTimeColumn(src);
		}
	}

	/**
	 * Date time columns that have less than {@link #MIN_SPARSE_COLUMN_SIZE} rows are represented via dense columns.
	 * Otherwise an estimate of the columns sparsity is calculated via {@link ColumnUtils#estimateDefaultValue(int,
	 * double, long[], SplittableRandom)} with {@code sampleSize = } {@link #SPARSITY_SAMPLE_SIZE}. If the estimated
	 * sparsity is {@code >=} {@link #MIN_SPARSITY_64BIT}, the column will be represented by a sparse column
	 * implementation that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	DateTimeColumn createDateTimeColumn(long[] seconds, int[] nanos, SplittableRandom random) {
		if (seconds.length < MIN_SPARSE_COLUMN_SIZE) {
			return new SimpleDateTimeColumn(seconds, nanos);
		}
		Optional<Long> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE, MIN_SPARSITY_64BIT, seconds, random);
		if (defaultValue.isPresent()) {
			if (nanos == null || nanos.length == 0 || allNull(nanos)) {
				return new DateTimeLowPrecisionSparseColumn(defaultValue.get(), seconds);
			} else {
				return new DateTimeHighPrecisionSparseColumn(defaultValue.get(), seconds, nanos);
			}
		} else {
			return new SimpleDateTimeColumn(seconds, nanos);
		}
	}

	/**
	 * Categorical columns that have less than {@link #MIN_SPARSE_COLUMN_SIZE} rows are represented via dense columns.
	 * Otherwise an estimate of the columns sparsity is calculated via {@link ColumnUtils#estimateDefaultValue(int,
	 * double, int[], SplittableRandom)} with {@code sampleSize = } {@link #SPARSITY_SAMPLE_SIZE}. If the estimated
	 * sparsity is {@code >=} {@link #MIN_SPARSITY_32BIT}, the column will be represented by a sparse column
	 * implementation that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	CategoricalColumn createCategoricalColumn(ColumnType<String> type, int[] data, Dictionary dictionary,
													 SplittableRandom random) {
		if (data.length < MIN_SPARSE_COLUMN_SIZE) {
			return new SimpleCategoricalColumn(type, data, dictionary);
		}
		Optional<Integer> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE,
				MIN_SPARSITY_32BIT, data, random);
		if (defaultValue.isPresent()) {
			return new CategoricalSparseColumn(type, data, dictionary, defaultValue.get());
		} else {
			return new SimpleCategoricalColumn(type, data, dictionary);
		}
	}

	/**
	 * Categorical columns that have less than {@link #MIN_SPARSE_COLUMN_SIZE} rows are represented via dense columns.
	 * Otherwise an estimate of the columns sparsity is calculated via {@link ColumnUtils#estimateDefaultValue(int,
	 * double, int[], SplittableRandom)} with {@code sampleSize = } {@link #SPARSITY_SAMPLE_SIZE}. If the estimated
	 * sparsity is {@code >=} {@link #MIN_SPARSITY_16BIT}, the column will be represented by a sparse column
	 * implementation that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	CategoricalColumn createCategoricalColumn(ColumnType<String> type, short[] data, Dictionary dictionary,
													 SplittableRandom random) {
		if (data.length < MIN_SPARSE_COLUMN_SIZE) {
			return new SimpleCategoricalColumn(type, data, dictionary);
		}
		Optional<Short> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE,
				MIN_SPARSITY_16BIT, data, random);
		if (defaultValue.isPresent()) {
			return new CategoricalSparseColumn(type, data, dictionary, defaultValue.get());
		} else {
			return new SimpleCategoricalColumn(type, data, dictionary);
		}
	}

	/**
	 * Categorical columns that have less than {@link #MIN_SPARSE_COLUMN_SIZE} rows are represented via dense columns.
	 * Otherwise an estimate of the columns sparsity is calculated via {@link ColumnUtils#estimateDefaultValue(int,
	 * double, int[], SplittableRandom)} with {@code sampleSize = } {@link #SPARSITY_SAMPLE_SIZE}. If the estimated
	 * sparsity is {@code >=} {@link #MIN_SPARSITY_8BIT}, the column will be represented by a sparse column
	 * implementation that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	CategoricalColumn createCategoricalColumn(ColumnType<String> type, IntegerFormats.PackedIntegers bytes,
													 Dictionary dictionary, SplittableRandom random) {
		byte[] data = bytes.data();
		if (data.length < MIN_SPARSE_COLUMN_SIZE || IntegerFormats.Format.UNSIGNED_INT2.equals(bytes.format()) ||
				IntegerFormats.Format.UNSIGNED_INT4.equals(bytes.format())) {
			return new SimpleCategoricalColumn(type, bytes, dictionary);
		}
		Optional<Byte> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE,
				MIN_SPARSITY_8BIT, data, random);
		if (defaultValue.isPresent()) {
			return new CategoricalSparseColumn(type, bytes, dictionary, defaultValue.get());
		} else {
			return new SimpleCategoricalColumn(type, bytes, dictionary);
		}
	}

	/**
	 * Checks if all values in the given array are equal to {@code 0}.
	 */
	private static boolean allNull(int[] nanos) {
		for (int i = 0, len = nanos.length; i < len; i++) {
			if (nanos[i] != 0) {
				return false;
			}
		}
		return true;
	}

}
