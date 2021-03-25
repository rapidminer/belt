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
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.ObjectRow;
import com.rapidminer.belt.reader.ObjectRowReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Reduces several {@link Column.Capability#OBJECT_READABLE} {@link Column}s with objects a subtype of R using a given
 * reduction information.
 *
 * @param <T>
 * 		The resulting class of the reduction
 * @param <R>
 * 		The class the column should be read as
 * @author Gisa Meier
 */
final class ObjectColumnsReducer<T, R> implements Calculator<T> {


	private final List<Column> sources;
	private final Supplier<T> supplier;
	private final BiConsumer<T, ObjectRow<R>> reducer;
	private final BiConsumer<T, T> combiner;
	private final Class<R> type;
	private NumericColumnReducer.CombineTree<T> combineTree;

	ObjectColumnsReducer(List<Column> sources, Class<R> type, Supplier<T> supplier, BiConsumer<T, ObjectRow<R>> reducer,
						 BiConsumer<T, T> combiner) {
		this.sources = sources;
		this.supplier = supplier;
		this.reducer = reducer;
		this.combiner = combiner;
		this.type = type;
	}


	@Override
	public void init(int numberOfBatches) {
		combineTree = new NumericColumnReducer.CombineTree<>(numberOfBatches);
	}

	@Override
	public int getNumberOfOperations() {
		return sources.get(0).size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		T supplied = Objects.requireNonNull(supplier.get(), "Supplier must not return null");
		reducePart(sources, type, supplied, reducer, from, to);
		combineTree.combine(supplied, batchIndex, combiner);
	}

	@Override
	public T getResult() {
		return combineTree.getRoot();
	}

	/**
	 * Calls the reducer for every row between from (inclusive) and to (exclusive).
	 */
	private static <T,R> void reducePart(List<Column> sources, Class<R> type, T container,
										 BiConsumer<T, ObjectRow<R>> reducer, int from, int to) {
		final ObjectRowReader<R> reader = Readers.objectRowReader(sources, type);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			reducer.accept(container, reader);
		}
	}


}