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
import java.util.Optional;
import java.util.SplittableRandom;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Implementation for internal column access.
 *
 * @author Gisa Meier
 */
final class InternalColumnsImpl extends Columns.InternalColumns {

	/**
	 * See {@link #createNumericColumn(Column.TypeId, double[], SplittableRandom)}.
	 */
	static final int MIN_SPARSE_COLUMN_SIZE = 1024;

	/**
	 * See {@link #createNumericColumn(Column.TypeId, double[], SplittableRandom)}.
	 */
	static final int SPARSITY_SAMPLE_SIZE = 1024;

	/**
	 * See {@link #createNumericColumn(Column.TypeId, double[], SplittableRandom)}.
	 */
	static final double MIN_SPARSITY = 0.625d;

	@Override
	public <T> SimpleCategoricalColumn<T> newCategoricalColumn(ColumnType<T> type, int[] data,
															   List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	@Override
	public <T> SimpleCategoricalColumn<T> newCategoricalColumn(ColumnType<T> type, short[] data,
															   List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, data, new Dictionary<>(dictionary));
	}

	@Override
	public <T> SimpleCategoricalColumn<T> newCategoricalColumn(ColumnType<T> type,
															   IntegerFormats.PackedIntegers bytes,
															   List<T> dictionary) {
		return new SimpleCategoricalColumn<>(type, bytes, new Dictionary<>(dictionary));
	}

	@Override
	public <T> SimpleCategoricalColumn<T> newCategoricalColumn(ColumnType<T> type, int[] data,
															   List<T> dictionary, int positiveIndex) {
		return new SimpleCategoricalColumn<>(type, data, new BooleanDictionary<>(dictionary, positiveIndex));
	}

	@Override
	public <T> SimpleCategoricalColumn<T> newCategoricalColumn(ColumnType<T> type, short[] data,
															   List<T> dictionary, int positiveIndex) {
		return new SimpleCategoricalColumn<>(type, data, new BooleanDictionary<>(dictionary, positiveIndex));
	}

	@Override
	public <T> SimpleCategoricalColumn<T> newCategoricalColumn(ColumnType<T> type,
															   IntegerFormats.PackedIntegers bytes,
															   List<T> dictionary, int positiveIndex) {
		return new SimpleCategoricalColumn<>(type, bytes, new BooleanDictionary<>(dictionary, positiveIndex));
	}

	@Override
	public TimeColumn newTimeColumn(long[] data) {
		return new SimpleTimeColumn(data);
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
	public DateTimeColumn newDateTimeColumn(long[] seconds, int[] nanos) {
		return new SimpleDateTimeColumn(seconds, nanos);
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
	public <T> List<T> getDictionaryList(Dictionary<T> dictionary) {
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
	 * sparsity is {@code >=} {@link #MIN_SPARSITY}, the column will be represented by a sparse column implementation
	 * that is optimized for sparse data. Otherwise it will be represented by a dense column.
	 */
	NumericColumn createNumericColumn(Column.TypeId type, double[] src, SplittableRandom random) {
		if (src.length < MIN_SPARSE_COLUMN_SIZE) {
			return new DoubleArrayColumn(type, src);
		}
		Optional<Double> defaultValue = ColumnUtils.estimateDefaultValue(SPARSITY_SAMPLE_SIZE, MIN_SPARSITY, src, random);
		if (defaultValue.isPresent()) {
			return new DoubleSparseColumn(type, defaultValue.get(), src);
		} else {
			return new DoubleArrayColumn(type, src);
		}
	}

}
