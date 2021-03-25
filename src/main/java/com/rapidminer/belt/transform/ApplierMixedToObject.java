/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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

import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.reader.MixedRow;
import com.rapidminer.belt.reader.MixedRowReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Maps arbitrary {@link Column}s to a {@link ObjectBuffer} using a given mapping operator.
 *
 * @author Gisa Meier
 */
final class ApplierMixedToObject<T> implements Calculator<ObjectBuffer<T>> {


	private ObjectBuffer<T> target;
	private final List<Column> sources;
	private final Function<MixedRow, T> operator;
	private final ColumnType<T> targetType;

	ApplierMixedToObject(List<Column> sources, Function<MixedRow, T> operator, ColumnType<T> targetType) {
		this.sources = sources;
		this.operator = operator;
		this.targetType = targetType;
	}


	@Override
	public void init(int numberOfBatches) {
		target = BufferAccessor.get().newObjectBuffer(targetType, sources.get(0).size());
	}

	@Override
	public int getNumberOfOperations() {
		return sources.get(0).size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(sources, operator, target, from, to);
	}

	@Override
	public ObjectBuffer<T> getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the sources columns using the operator and
	 * stores the result in target.
	 */
	private static <T> void mapPart(final List<Column> sources, final Function<MixedRow, T> operator,
									final ObjectBuffer<T> target,
									int from, int to) {
		final MixedRowReader reader = Readers.mixedRowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			T value = operator.apply(reader);
			target.set(i, value);
		}
	}


}