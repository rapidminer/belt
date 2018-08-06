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


import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;


/**
 * Reduces several {@link Column}s using a given reduction information.
 *
 * @author Gisa Meier
 */
final class ColumnsReducer<T> implements ParallelExecutor.Calculator<T> {


	private final Column[] sources;
	private final Supplier<T> supplier;
	private final BiConsumer<T, Row> reducer;
	private final BiConsumer<T, T> combiner;
	private ColumnReducer.CombineTree<T> combineTree;

	ColumnsReducer(Column[] sources, Supplier<T> supplier, BiConsumer<T, Row> reducer, BiConsumer<T, T> combiner) {
		this.sources = sources;
		this.supplier = supplier;
		this.reducer = reducer;
		this.combiner = combiner;
	}


	@Override
	public void init(int numberOfBatches) {
		combineTree = new ColumnReducer.CombineTree<>(numberOfBatches);
	}

	@Override
	public int getNumberOfOperations() {
		return sources[0].size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		T supplied = Objects.requireNonNull(supplier.get(), "Supplier must not return null");
		reducePart(sources, supplied, reducer, from, to);
		combineTree.combine(supplied, batchIndex, combiner);
	}

	@Override
	public T getResult() {
		return combineTree.getRoot();
	}

	/**
	 * Calls the reducer for every row between from (inclusive) and to (exclusive).
	 */
	private static <T> void reducePart(Column[] sources, T container, BiConsumer<T, Row> reducer, int from, int to) {
		final RowReader reader = new RowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			reducer.accept(container, reader);
		}
	}


}