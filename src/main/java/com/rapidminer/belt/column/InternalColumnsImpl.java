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

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Implementation for internal column access.
 *
 * @author Gisa Meier
 */
final class InternalColumnsImpl extends Columns.InternalColumns {


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
	public DoubleArrayColumn newNumericColumn(Column.TypeId type, double[] src) {
		return new DoubleArrayColumn(type, src);
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

}
