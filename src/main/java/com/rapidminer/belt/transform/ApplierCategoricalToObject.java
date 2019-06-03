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


import java.util.function.IntFunction;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Maps a {@link Column.Category#CATEGORICAL} {@link Column} to a {@link ObjectBuffer} using a given mapping operator.
 *
 * @author Gisa Meier
 */
final class ApplierCategoricalToObject<T> implements Calculator<ObjectBuffer<T>> {


	private ObjectBuffer<T> target;
	private final Column source;
	private final IntFunction<T> operator;

	ApplierCategoricalToObject(Column source, IntFunction<T> operator) {
		this.source = source;
		this.operator = operator;
	}


	@Override
	public void init(int numberOfBatches) {
		target = Buffers.objectBuffer(source.size());
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(source, operator, target, from, to);
	}

	@Override
	public ObjectBuffer<T> getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static <T> void mapPart(Column source, IntFunction<T> operator, ObjectBuffer<T> target, int from, int to) {
		final CategoricalReader reader = Readers.categoricalReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			int value = reader.read();
			target.set(i, operator.apply(value));
		}
	}


}