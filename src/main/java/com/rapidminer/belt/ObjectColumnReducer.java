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
 * Reduces a {@link Column.Capability#OBJECT_READABLE} {@link Column} with objects a subtype of R using a given
 * reduction information.
 *
 * @param <T>
 * 		The resulting class of the reduction
 * @param <R>
 * 		The class the column should be read as
 * @author Gisa Meier
 */
final class ObjectColumnReducer<T, R> implements ParallelExecutor.Calculator<T> {


	private final Column source;
	private final Supplier<T> supplier;
	private final BiConsumer<T, R> reducer;
	private final BiConsumer<T, T> combiner;
	private final Class<R> type;
	private ColumnReducer.CombineTree<T> combineTree;

	ObjectColumnReducer(Column source, Class<R> type, Supplier<T> supplier, BiConsumer<T, R> reducer, BiConsumer<T, T> combiner) {
		this.source = source;
		this.supplier = supplier;
		this.reducer = reducer;
		this.combiner = combiner;
		this.type = type;
	}


	@Override
	public void init(int numberOfBatches) {
		combineTree = new ColumnReducer.CombineTree<>(numberOfBatches);
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		T supplied = Objects.requireNonNull(supplier.get(), "Supplier must not return null");
		reducePart(source, type, supplied, reducer, from, to);
		combineTree.combine(supplied, batchIndex, combiner);
	}


	@Override
	public T getResult() {
		return combineTree.getRoot();
	}

	/**
	 * Calls the reducer for every column value between from (inclusive) and to (exclusive).
	 */
	private static <T, R> void reducePart(Column source, Class<R> type, T container, BiConsumer<T, R> reducer, int from, int to) {
		final ObjectReader<R> reader = new ObjectReader<>(source, type, NumericReader.DEFAULT_BUFFER_SIZE, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reducer.accept(container, reader.read());
		}
	}

}