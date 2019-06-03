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


import java.util.List;
import java.util.function.Function;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.ObjectRow;
import com.rapidminer.belt.reader.ObjectRowReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Maps {@link Column.Capability#OBJECT_READABLE} {@link Column}s to a {@link ObjectBuffer} using a given mapping
 * operator.
 *
 * @author Gisa Meier
 */
final class ApplierNObjectToObject<R, T> implements Calculator<ObjectBuffer<T>> {


	private ObjectBuffer<T> target;
	private final List<Column> sources;
	private final Function<ObjectRow<R>, T> operator;
	private final Class<R> type;

	ApplierNObjectToObject(List<Column> sources, Class<R> type, Function<ObjectRow<R>, T> operator) {
		this.sources = sources;
		this.operator = operator;
		this.type = type;
	}


	@Override
	public void init(int numberOfBatches) {
		target = Buffers.objectBuffer(sources.get(0).size());
	}

	@Override
	public int getNumberOfOperations() {
		return sources.get(0).size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(sources, operator, type, target, from, to);
	}

	@Override
	public ObjectBuffer<T> getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the sources columns using the operator and stores
	 * the result in target.
	 */
	private static <R, T> void mapPart(final List<Column> sources, final Function<ObjectRow<R>, T> operator,
									   final Class<R> type, final ObjectBuffer<T> target, int from, int to) {
		final ObjectRowReader<R> reader = Readers.objectRowReader(sources, type);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}


}