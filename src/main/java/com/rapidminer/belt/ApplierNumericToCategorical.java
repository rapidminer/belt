/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2018 RapidMiner GmbH
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

package com.rapidminer.belt;


import java.util.function.DoubleFunction;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Maps a {@link Column.Capability#NUMERIC_READABLE} {@link Column} to a {@link CategoricalColumnBuffer} using a given
 * mapping operator.
 *
 * @author Gisa Meier
 */
final class ApplierNumericToCategorical<T> implements ParallelExecutor.Calculator<CategoricalColumnBuffer<T>> {


	private CategoricalColumnBuffer<T> target;
	private final Column source;
	private final DoubleFunction<T> operator;
	private final IntegerFormats.Format format;

	ApplierNumericToCategorical(Column source, DoubleFunction<T> operator, IntegerFormats.Format format) {
		this.source = source;
		this.operator = operator;
		this.format = format;
	}


	@Override
	public void init(int numberOfBatches) {
		switch (format) {
			case UNSIGNED_INT2:
				target = new UInt2CategoricalBuffer<>(source.size());
				break;
			case UNSIGNED_INT4:
				target = new UInt4CategoricalBuffer<>(source.size());
				break;
			case UNSIGNED_INT8:
				target = new UInt8CategoricalBuffer<>(source.size());
				break;
			case UNSIGNED_INT16:
				target = new UInt16CategoricalBuffer<>(source.size());
				break;
			case SIGNED_INT32:
			default:
				target = new Int32CategoricalBuffer<>(source.size());
		}
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		switch (format) {
			case UNSIGNED_INT2:
				mapPart(source, operator, (UInt2CategoricalBuffer<T>) target, from, to);
				break;
			case UNSIGNED_INT4:
				mapPart(source, operator, (UInt4CategoricalBuffer<T>) target, from, to);
				break;
			case UNSIGNED_INT8:
				mapPart(source, operator, (UInt8CategoricalBuffer<T>) target, from, to);
				break;
			case UNSIGNED_INT16:
				mapPart(source, operator, (UInt16CategoricalBuffer<T>) target, from, to);
				break;
			case SIGNED_INT32:
			default:
				mapPart(source, operator, (Int32CategoricalBuffer<T>) target, from, to);
		}
	}

	@Override
	public CategoricalColumnBuffer<T> getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, DoubleFunction<T> operator, UInt2CategoricalBuffer<T> target, int from, int to) {
		final ColumnReader reader = new ColumnReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, DoubleFunction<T> operator, UInt4CategoricalBuffer<T> target, int from, int to) {
		final ColumnReader reader = new ColumnReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, DoubleFunction<T> operator, UInt8CategoricalBuffer<T> target, int from, int to) {
		final ColumnReader reader = new ColumnReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, DoubleFunction<T> operator, UInt16CategoricalBuffer<T> target, int from, int to) {
		final ColumnReader reader = new ColumnReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, DoubleFunction<T> operator, Int32CategoricalBuffer<T> target, int from, int to) {
		final ColumnReader reader = new ColumnReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}


}