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


import java.util.function.DoubleBinaryOperator;


/**
 * Reduces a {@link Column} using a given reduction information to a double value.
 *
 * @author Gisa Meier
 */
final class ColumnReducerDouble implements ParallelExecutor.Calculator<Double> {


	private final Column source;
	private final double identity;
	private final DoubleBinaryOperator reducer;
	private final DoubleBinaryOperator combiner;

	private double[] partResults;

	ColumnReducerDouble(Column source, double identity, DoubleBinaryOperator reducer) {
		this.source = source;
		this.identity = identity;
		this.reducer = reducer;
		this.combiner = reducer;
	}

	ColumnReducerDouble(Column source, double identity, DoubleBinaryOperator reducer, DoubleBinaryOperator combiner) {
		this.source = source;
		this.identity = identity;
		this.reducer = reducer;
		this.combiner = combiner;
	}


	@Override
	public void init(int numberOfBatches) {
		partResults = new double[numberOfBatches];
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		double res = reducePart(source, identity, reducer, from, to);
		partResults[batchIndex] = res;
	}


	@Override
	public Double getResult() {
		double end = identity;
		for (double res : partResults) {
			end = combiner.applyAsDouble(end, res);
		}
		return end;
	}

	/**
	 * Calls the reducer for every column value between from (inclusive) and to (exclusive).
	 */
	private static double reducePart(Column source, double identity, DoubleBinaryOperator reducer, int from, int to) {
		double container = identity;
		final NumericReader reader = new NumericReader(source, NumericReader.DEFAULT_BUFFER_SIZE, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			container = reducer.applyAsDouble(container, reader.read());
		}
		return container;
	}

}