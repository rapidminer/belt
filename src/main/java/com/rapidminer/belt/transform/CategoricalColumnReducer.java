/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
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


import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Reduces a {@link Column.Category#CATEGORICAL} {@link Column} using a given reduction information.
 *
 * @author Gisa Meier
 */
final class CategoricalColumnReducer<T> implements Calculator<T> {


	private final Column source;
	private final Supplier<T> supplier;
	private final ObjIntConsumer<T> reducer;
	private final BiConsumer<T, T> combiner;
	private NumericColumnReducer.CombineTree<T> combineTree;

	CategoricalColumnReducer(Column source, Supplier<T> supplier, ObjIntConsumer<T> reducer, BiConsumer<T, T> combiner) {
		this.source = source;
		this.supplier = supplier;
		this.reducer = reducer;
		this.combiner = combiner;
	}


	@Override
	public void init(int numberOfBatches) {
		combineTree = new NumericColumnReducer.CombineTree<>(numberOfBatches);
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		T supplied = Objects.requireNonNull(supplier.get(), "Supplier must not return null");
		reducePart(source, supplied, reducer, from, to);
		combineTree.combine(supplied, batchIndex, combiner);
	}


	@Override
	public T getResult() {
		return combineTree.getRoot();
	}

	/**
	 * Calls the reducer for every column value between from (inclusive) and to (exclusive).
	 */
	private static <T> void reducePart(Column source, T container, ObjIntConsumer<T> reducer, int from, int to) {
		final CategoricalReader reader = Readers.categoricalReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reducer.accept(container, reader.read());
		}
	}
}