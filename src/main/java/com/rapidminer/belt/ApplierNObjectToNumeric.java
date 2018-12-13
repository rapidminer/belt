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


import java.util.function.ToDoubleFunction;


/**
 * Maps {@link Column.Capability#OBJECT_READABLE} {@link Column}s to a {@link NumericBuffer} using a given mapping
 * operator.
 *
 * @author Gisa Meier
 */
final class ApplierNObjectToNumeric<T> implements ParallelExecutor.Calculator<NumericBuffer> {


	private NumericBuffer target;
	private final Column[] sources;
	private final ToDoubleFunction<ObjectRow<T>> operator;
	private final Class<T> type;
	private final boolean round;

	ApplierNObjectToNumeric(Column[] sources, Class<T> type, ToDoubleFunction<ObjectRow<T>> operator, boolean round) {
		this.sources = sources;
		this.operator = operator;
		this.type = type;
		this.round = round;
	}


	@Override
	public void init(int numberOfBatches) {
		target = round ? new IntegerBuffer(sources[0].size(), false) : new RealBuffer(sources[0].size(), false);
	}

	@Override
	public int getNumberOfOperations() {
		return sources[0].size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(sources, operator, type, target, from, to);
	}

	@Override
	public NumericBuffer getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the sources columns using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(final Column[] sources, final ToDoubleFunction<ObjectRow<T>> operator,
									final Class<T> type, final NumericBuffer target, int from, int to) {
		final ObjectRowReader<T> reader = new ObjectRowReader<>(sources, type);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			double value = operator.applyAsDouble(reader);
			target.set(i, value);
		}
	}


}