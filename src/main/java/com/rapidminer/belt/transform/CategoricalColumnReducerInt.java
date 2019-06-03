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


import java.util.function.IntBinaryOperator;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Reduces a {@link Column.Category#CATEGORICAL} {@link Column} using a given reduction information to a int value.
 *
 * @author Gisa Meier
 */
final class CategoricalColumnReducerInt implements Calculator<Integer> {


	private final Column source;
	private final int identity;
	private final IntBinaryOperator reducer;
	private final IntBinaryOperator combiner;

	private int[] partResults;

	CategoricalColumnReducerInt(Column source, int identity, IntBinaryOperator reducer) {
		this.source = source;
		this.identity = identity;
		this.reducer = reducer;
		this.combiner = reducer;
	}

	CategoricalColumnReducerInt(Column source, int identity, IntBinaryOperator reducer, IntBinaryOperator combiner) {
		this.source = source;
		this.identity = identity;
		this.reducer = reducer;
		this.combiner = combiner;
	}


	@Override
	public void init(int numberOfBatches) {
		partResults = new int[numberOfBatches];
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		int res = reducePart(source, identity, reducer, from, to);
		partResults[batchIndex] = res;
	}


	@Override
	public Integer getResult() {
		int end = identity;
		for (int res : partResults) {
			end = combiner.applyAsInt(end, res);
		}
		return end;
	}

	/**
	 * Calls the reducer for every column value between from (inclusive) and to (exclusive).
	 */
	private static int reducePart(Column source, int identity, IntBinaryOperator reducer, int from, int to) {
		int container = identity;
		final CategoricalReader reader = Readers.categoricalReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			container = reducer.applyAsInt(container, reader.read());
		}
		return container;
	}

}