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


import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Filters a {@link Column.Category#CATEGORICAL} {@link Column} using a given filter operator.
 *
 * @author Gisa Meier
 */
final class CategoricalColumnFilterer implements Calculator<int[]> {


	private final Column source;
	private final IntPredicate operator;
	private boolean[] target;
	private AtomicInteger found = new AtomicInteger();

	CategoricalColumnFilterer(Column source, IntPredicate operator) {
		this.source = source;
		this.operator = operator;
	}


	@Override
	public void init(int numberOfBatches) {
		target = new boolean[source.size()];
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		int filtered = filterPart(source, target, operator, from, to);
		found.addAndGet(filtered);
	}

	@Override
	public int[] getResult() {
		return getMapping(found.get(), target);
	}

	static int[] getMapping(int found, boolean[] testResults) {
		int[] mapping = new int[found];
		int mappingIndex = 0;
		for (int i = 0; i < testResults.length; i++) {
			if (testResults[i]) {
				mapping[mappingIndex++] = i;
			}
		}
		return mapping;
	}

	/**
	 * Writes the result of the filter into the target array for every index between from (inclusive) and to (exclusive)
	 * of the source column using the operator.
	 */
	private static int filterPart(Column source, boolean[] target, IntPredicate operator, int from, int to) {
		int found = 0;
		final CategoricalReader reader = Readers.categoricalReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			boolean testResult = operator.test(reader.read());
			target[i] = testResult;
			if (testResult) {
				found++;
			}
		}
		return found;
	}


}