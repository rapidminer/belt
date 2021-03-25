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


import java.util.function.DoubleFunction;

import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.buffer.Int32NominalBuffer;
import com.rapidminer.belt.buffer.UInt16NominalBuffer;
import com.rapidminer.belt.buffer.UInt8NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * Maps a {@link Column.Capability#NUMERIC_READABLE} {@link Column} to a {@link NominalBuffer} using a given
 * mapping operator.
 *
 * @author Gisa Meier
 */
final class ApplierNumericToCategorical implements Calculator<NominalBuffer> {


	private NominalBuffer target;
	private final Column source;
	private final DoubleFunction<String> operator;
	private final IntegerFormats.Format format;
	private final ColumnType<String> targetType;

	ApplierNumericToCategorical(Column source, DoubleFunction<String> operator, IntegerFormats.Format format,
								ColumnType<String> targetType) {
		this.source = source;
		this.operator = operator;
		this.format = format;
		this.targetType = targetType;
	}


	@Override
	public void init(int numberOfBatches) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				target = BufferAccessor.get().newUInt8Buffer(targetType, source.size(), format);
				break;
			case UNSIGNED_INT16:
				target = BufferAccessor.get().newUInt16Buffer(targetType, source.size());
				break;
			case SIGNED_INT32:
			default:
				target = BufferAccessor.get().newInt32Buffer(targetType, source.size());
		}
	}

	@Override
	public int getNumberOfOperations() {
		return source.size();
	}

	@Override
	public void doPart(int from, int to, int batchIndex) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				mapPart(source, operator, (UInt8NominalBuffer) target, from, to);
				break;
			case UNSIGNED_INT16:
				mapPart(source, operator, (UInt16NominalBuffer) target, from, to);
				break;
			case SIGNED_INT32:
			default:
				mapPart(source, operator, (Int32NominalBuffer) target, from, to);
		}
	}

	@Override
	public NominalBuffer getResult() {
		return target;
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static void mapPart(Column source, DoubleFunction<String> operator, UInt8NominalBuffer target, int from, int to) {
		final NumericReader reader = Readers.numericReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static void mapPart(Column source, DoubleFunction<String> operator, UInt16NominalBuffer target, int from, int to) {
		final NumericReader reader = Readers.numericReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}

	/**
	 * Maps every index between from (inclusive) and to (exclusive) of the source column using the operator and stores
	 * the result in target.
	 */
	private static void mapPart(Column source, DoubleFunction<String> operator, Int32NominalBuffer target, int from, int to) {
		final NumericReader reader = Readers.numericReader(source, to);
		reader.setPosition(from - 1);
		for (int i = from; i < to; i++) {
			double value = reader.read();
			target.set(i, operator.apply(value));
		}
	}


}