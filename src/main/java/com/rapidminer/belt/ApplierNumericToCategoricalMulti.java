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


import java.util.function.Function;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Maps {@link Column.Capability#NUMERIC_READABLE} {@link Column}s to a {@link CategoricalColumnBuffer} using a given
 * mapping operator.
 *
 * @author Gisa Meier
 */
final class ApplierNumericToCategoricalMulti<T> implements ParallelExecutor.Calculator<CategoricalColumnBuffer<T>> {


	private CategoricalColumnBuffer<T> target;
	private final Column[] sources;
	private final Function<Row, T> operator;
	private final IntegerFormats.Format format;

	ApplierNumericToCategoricalMulti(Column[] sources, Function<Row, T> operator, IntegerFormats.Format format) {
		this.sources = sources;
		this.operator = operator;
		this.format = format;
	}


	@Override
	public void init(int numberOfBatches) {
		switch (format) {
			case UNSIGNED_INT2:
				target = new UInt2CategoricalBuffer<>(sources[0].size());
				break;
			case UNSIGNED_INT4:
				target = new UInt4CategoricalBuffer<>(sources[0].size());
				break;
			case UNSIGNED_INT8:
				target = new UInt8CategoricalBuffer<>(sources[0].size());
				break;
			case UNSIGNED_INT16:
				target = new UInt16CategoricalBuffer<>(sources[0].size());
				break;
			case SIGNED_INT32:
			default:
				target = new Int32CategoricalBuffer<>(sources[0].size());
		}
	}

	@Override
	public int getNumberOfOperations() {
		return sources[0].size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		switch (format) {
			case UNSIGNED_INT2:
				mapPart(sources, operator, (UInt2CategoricalBuffer<T>) target, from, to);
				break;
			case UNSIGNED_INT4:
				mapPart(sources, operator, (UInt4CategoricalBuffer<T>) target, from, to);
				break;
			case UNSIGNED_INT8:
				mapPart(sources, operator, (UInt8CategoricalBuffer<T>) target, from, to);
				break;
			case UNSIGNED_INT16:
				mapPart(sources, operator, (UInt16CategoricalBuffer<T>) target, from, to);
				break;
			case SIGNED_INT32:
			default:
				mapPart(sources, operator, (Int32CategoricalBuffer<T>) target, from, to);
		}
	}

	@Override
	public CategoricalColumnBuffer<T> getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source columns using the operator and stores
	 * the result in target in format {@link IntegerFormats.Format#UNSIGNED_INT2}.
	 */
	private static <T> void mapPart(Column[] sources, Function<Row, T> operator, Int32CategoricalBuffer<T> target,
									int from, int to) {
		final RowReader reader = new RowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source columns using the operator and stores
	 * the result in target in format {@link IntegerFormats.Format#UNSIGNED_INT4}.
	 */
	private static <T> void mapPart(Column[] sources, Function<Row, T> operator, UInt16CategoricalBuffer<T> target,
									int from, int to) {
		final RowReader reader = new RowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source columns using the operator and stores
	 * the result in target in format {@link IntegerFormats.Format#UNSIGNED_INT8}.
	 */
	private static <T> void mapPart(Column[] sources, Function<Row, T> operator, UInt8CategoricalBuffer<T> target,
									int from, int to) {
		final RowReader reader = new RowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source columns using the operator and stores
	 * the result in target in format {@link IntegerFormats.Format#UNSIGNED_INT16}.
	 */
	private static <T> void mapPart(Column[] sources, Function<Row, T> operator, UInt4CategoricalBuffer<T> target,
									int from, int to) {
		final RowReader reader = new RowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source columns using the operator and stores
	 * the result in target in format {@link IntegerFormats.Format#SIGNED_INT32}.
	 */
	private static <T> void mapPart(Column[] sources, Function<Row, T> operator, UInt2CategoricalBuffer<T> target,
									int from, int to) {
		final RowReader reader = new RowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}


}