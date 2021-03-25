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
import java.util.function.Predicate;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Filters a {@link Column.Capability#OBJECT_READABLE} {@link Column} using a given filter operator.
 *
 * @author Gisa Meier
 */
final class ObjectColumnFilterer<T> implements Calculator<int[]> {


	private final Column source;
	private final Class<T> sourceType;
	private final Predicate<T> operator;
	private boolean[] target;
	private AtomicInteger found = new AtomicInteger();

	ObjectColumnFilterer(Column source, Class<T> sourceType, Predicate<T> operator) {
		this.source = source;
		this.operator = operator;
		this.sourceType = sourceType;
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
		int filtered = filterPart(source, sourceType, target, operator, from, to);
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
	private static <T> int filterPart(Column source, Class<T> type, boolean[] target, Predicate<T> operator, int from, int to) {
		int found = 0;
		final ObjectReader<T> reader = Readers.objectReader(source, type, to);
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