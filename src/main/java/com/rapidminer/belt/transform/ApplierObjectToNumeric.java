/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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

package com.rapidminer.belt.transform;


import java.util.function.ToDoubleFunction;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Maps a {@link Column.Capability#OBJECT_READABLE} {@link Column} to a {@link NumericBuffer} using a given mapping
 * operator.
 *
 * @author Gisa Meier
 */
final class ApplierObjectToNumeric<T> implements Calculator<NumericBuffer> {


	private NumericBuffer target;
	private final Column source;
	private final ToDoubleFunction<T> operator;
	private final Class<T> type;
	private final boolean round;

	ApplierObjectToNumeric(Column source, Class<T> type, ToDoubleFunction<T> operator, boolean round) {
		this.source = source;
		this.operator = operator;
		this.type = type;
		this.round = round;
	}


	@Override
	public void init(int numberOfBatches) {
		target = round ? Buffers.integerBuffer(source.size(), false) : Buffers.realBuffer(source.size(), false);
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(source, operator, type, target, from, to);
	}

	@Override
	public NumericBuffer getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, ToDoubleFunction<T> operator, Class<T> type, NumericBuffer target, int from, int to) {
		final ObjectReader<T> reader = Readers.objectReader(source, type, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			T value = reader.read();
			target.set(i, operator.applyAsDouble(value));
		}
	}


}