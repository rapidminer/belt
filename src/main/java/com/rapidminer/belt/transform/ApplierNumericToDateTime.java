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


import java.time.Instant;
import java.util.function.DoubleFunction;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.DateTimeBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Maps a {@link Column.Capability#NUMERIC_READABLE} {@link Column} to a {@link DateTimeBuffer} using a
 * given mapping operator.
 *
 * @author Gisa Meier
 */
final class ApplierNumericToDateTime implements Calculator<DateTimeBuffer> {


	private DateTimeBuffer target;
	private final Column source;
	private final DoubleFunction<Instant> operator;

	ApplierNumericToDateTime(Column source, DoubleFunction<Instant> operator) {
		this.source = source;
		this.operator = operator;
	}


	@Override
	public void init(int numberOfBatches) {
		target = Buffers.dateTimeBuffer(source.size(), true, false);
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		mapPart(source, operator, target, from, to);
	}

	@Override
	public DateTimeBuffer getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static void mapPart(Column source, DoubleFunction<Instant> operator, DateTimeBuffer target,
								int from, int to) {
		final NumericReader reader = Readers.numericReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}


}