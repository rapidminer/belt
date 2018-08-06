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


import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;


/**
 * Filters by several {@link Column.Category#CATEGORICAL} {@link Column}s using a given filter operator.
 *
 * @author Gisa Meier
 */
final class CategoricalColumnsFilterer implements ParallelExecutor.Calculator<int[]> {

	private final Column[] sources;
	private final Predicate<CategoricalRow> operator;
	private boolean[] target;
	private AtomicInteger found = new AtomicInteger();

	CategoricalColumnsFilterer(Column[] sources, Predicate<CategoricalRow> operator) {
		this.sources = sources;
		this.operator = operator;
	}


	@Override
	public void init(int numberOfBatches) {
		target = new boolean[sources[0].size()];
	}

	@Override
	public int getNumberOfOperations() {
		return sources[0].size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		int filtered = filterPart(sources, target, operator, from, to);
		found.addAndGet(filtered);
	}

	@Override
	public int[] getResult() {
		return ColumnFilterer.getMapping(found.get(), target);
	}

	/**
	 * Writes the result of the filter into the target array for every index between from (inclusive) and to (exclusive)
	 * of the source column using the operator.
	 */
	private static int filterPart(Column[] sources, boolean[] target, Predicate<CategoricalRow> operator, int from, int to) {
		int found = 0;
		final CategoricalRowReader reader = new CategoricalRowReader(sources);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			reader.move();
			boolean testResult = operator.test(reader);
			target[i] = testResult;
			if (testResult) {
				found++;
			}
		}
		return found;
	}


}