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
 * Maps a {@link Column.Capability#OBJECT_READABLE} {@link Column} to a {@link TimeColumnBuffer} using a given mapping
 * operator.
 *
 * @author Gisa Meier
 */
final class ApplierObjectToTime<R> implements ParallelExecutor.Calculator<TimeColumnBuffer> {

	private TimeColumnBuffer target;
	private final Column source;
	private final Class<R> sourceType;
	private final Function<R, LocalTime> operator;

	ApplierObjectToTime(Column source, Class<R> sourceType, Function<R, LocalTime> operator) {
		this.source = source;
		this.operator = operator;
		this.sourceType = sourceType;
	}


	@Override
	public void init(int numberOfBatches) {
		target = new TimeColumnBuffer(source.size());
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(source, sourceType, operator, target, from, to);
	}

	@Override
	public TimeColumnBuffer getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <R> void mapPart(Column source, Class<R> sourceType, Function<R, LocalTime> operator,
									TimeColumnBuffer target, int from, int to) {
		final ObjectColumnReader<R> reader = new ObjectColumnReader<>(source, sourceType, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			R value = reader.read();
			target.set(i, operator.apply(value));
		}
	}


}