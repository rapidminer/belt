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
import java.util.function.ObjDoubleConsumer;
import java.util.function.Supplier;


/**
 * Reduces a {@link Column} using a given reduction information.
 *
 * @author Gisa Meier
 */
final class ColumnReducer<T> implements ParallelExecutor.Calculator<T> {


	private final Column source;
	private final Supplier<T> supplier;
	private final ObjDoubleConsumer<T> reducer;
	private final BiConsumer<T, T> combiner;
	private CombineTree<T> combineTree;

	ColumnReducer(Column source, Supplier<T> supplier, ObjDoubleConsumer<T> reducer, BiConsumer<T, T> combiner) {
		this.source = source;
		this.supplier = supplier;
		this.reducer = reducer;
		this.combiner = combiner;
	}


	@Override
	public void init(int numberOfBatches) {
		combineTree = new CombineTree<>(numberOfBatches);
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
	private static <T> void reducePart(Column source, T container, ObjDoubleConsumer<T> reducer, int from, int to) {
		final ColumnReader reader = new ColumnReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reducer.accept(container, reader.read());
		}
	}


	/**
	 * A tree backed by an array with as many leaves as expected results. Knows how to combine results following the
	 * tree structure.
	 *
	 * @param <T>
	 * 		the type of the stored results
	 */
	static final class CombineTree<T> {

		private final T[] array;
		private final int numberOfLeaves;
		private final int nodesInLastRow;

		CombineTree(int numberOfLeaves) {
			this.numberOfLeaves = numberOfLeaves;
			int high = Integer.highestOneBit(numberOfLeaves);
			nodesInLastRow = 2 * (numberOfLeaves - high);
			int numberOfNodes = 2 * high - 1 + nodesInLastRow;

			// only T objects are put in
			@SuppressWarnings("unchecked")
			T[] shortArr = (T[]) new Object[numberOfNodes];
			array = shortArr;
		}

		/**
		 * Combines the result at a leaf of the heap array with the neighboring result, if it already exists. Goes on
		 * combining following the tree structure, if the other results have been already calculated.
		 *
		 * @param leafResult
		 * 		the calculated result for the leaf
		 * @param leafIndex
		 * 		the index of the leaf
		 * @param combiner
		 * 		the combiner to combine two neighboring results
		 */
		void combine(T leafResult, int leafIndex, BiConsumer<T, T> combiner) {
			T result = leafResult;
			int index = getArrayIndexForLeaf(leafIndex);
			while (index > 0) {
				boolean left = isLeftIndex(index);
				int neighbourIndex = getNeighbourIndex(index, left);
				T neighbourResult = getOrStoreAt(neighbourIndex, index, result);
				if (neighbourResult == null) {
					return;
				} else {
					if (left) {
						combiner.accept(result, neighbourResult);
					} else {
						combiner.accept(neighbourResult, result);
						result = neighbourResult;
					}
					index = getParentIndex(index);
				}
			}
			setRoot(result);
		}

		/**
		 * Retrieves the value at the root.
		 *
		 * @return the value at the root of the tree
		 */
		synchronized T getRoot() {
			return array[0];
		}

		/**
		 * Sets the value for the root.
		 */
		private synchronized void setRoot(T value) {
			array[0] = value;
		}

		/**
		 * Returns the array index associated to the leaf index.
		 */
		private int getArrayIndexForLeaf(int leafIndex) {
			if (leafIndex < nodesInLastRow) {
				return array.length - nodesInLastRow + leafIndex;
			} else {
				return array.length - numberOfLeaves + leafIndex - nodesInLastRow;
			}
		}

		/**
		 * Returns the value at getIndex if it is present. Otherwise stores the storeValue at the storeIndex and returns
		 * {@code null}.
		 */
		private synchronized T getOrStoreAt(int getIndex, int storeIndex, T storeValue) {
			T getValue = array[getIndex];
			if (getValue != null) {
				array[getIndex] = null;
				return getValue;
			} else {
				array[storeIndex] = storeValue;
				return null;
			}
		}

		/**
		 * Returns {@code true} if the given index is the left one of two that have the same parent.
		 */
		private boolean isLeftIndex(int index) {
			return index % 2 == 1;
		}

		/**
		 * Calculates the other index with the same parent, depending on whether the index is a left child.
		 */
		private int getNeighbourIndex(int index, boolean isLeftIndex) {
			return isLeftIndex ? index + 1 : index - 1;
		}

		/**
		 * Returns the parent index
		 */
		private int getParentIndex(int index) {
			return (index - 1) / 2;
		}


	}
}