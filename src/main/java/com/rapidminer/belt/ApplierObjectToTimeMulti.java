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


import java.time.LocalTime;
import java.util.function.Function;


/**
 * Maps {@link Column.Capability#OBJECT_READABLE} {@link Column}s to a {@link TimeColumnBuffer} using a given mapping
 * operator.
 *
 * @author Gisa Meier
 */
final class ApplierObjectToTimeMulti<R> implements ParallelExecutor.Calculator<TimeColumnBuffer> {


	private TimeColumnBuffer target;
	private final Column[] sources;
	private final Function<ObjectRow<R>, LocalTime> operator;
	private final Class<R> type;

	ApplierObjectToTimeMulti(Column[] sources, Class<R> type, Function<ObjectRow<R>, LocalTime> operator) {
		this.sources = sources;
		this.operator = operator;
		this.type = type;
	}


	@Override
	public void init(int numberOfBatches) {
		target = new TimeColumnBuffer(sources[0].size());
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
	public TimeColumnBuffer getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the sources columns using the operator and stores
	 * the result in target.
	 */
	private static <R> void mapPart(final Column[] sources, final Function<ObjectRow<R>, LocalTime> operator,
									   final Class<R> type, final TimeColumnBuffer target, int from, int to) {
		final ObjectRowReader<R> reader = new ObjectRowReader<>(sources, type);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			target.set(i, operator.apply(reader));
		}
	}


}