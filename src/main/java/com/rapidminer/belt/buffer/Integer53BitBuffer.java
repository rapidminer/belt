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

package com.rapidminer.belt.buffer;

import java.util.Arrays;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;


/**
 * A {@link NumericBuffer} with length fixed from the start storing rounded double values. Finite double values are
 * rounded via {@link Math#round(double)}. Whole numbers between {@code +/- 2^53-1} can be stored without loss of
 * information.
 *
 * @author Gisa Meier
 */
final class Integer53BitBuffer extends NumericBuffer {

	private final double[] data;
	private boolean frozen = false;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link Column.TypeId#INTEGER_53_BIT}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @param initialize
	 * 		if {@code true} all values are set to {@link Double#NaN}
	 */
	Integer53BitBuffer(int length, boolean initialize) {
		data = new double[length];
		if (initialize) {
			Arrays.fill(data, Double.NaN);
		}
	}

	/**
	 * Creates a buffer by copying the data from the given column. Throws a {@link UnsupportedOperationException} if the
	 * category has not the capability {@link Column.Capability#NUMERIC_READABLE}.
	 * <p> Finite values are rounded via {@link Math#round(double)} and stored as {@code double} values. Whole numbers
	 * between {@code +/- 2^53-1} can be stored without loss of information.
	 *
	 * @param column
	 * 		the Column to copy into the buffer
	 */
	Integer53BitBuffer(Column column) {
		data = new double[column.size()];
		column.fill(data, 0);
		ColumnType<?> type = column.type();
		if (type.id() != TypeId.INTEGER_53_BIT && type.id() != TypeId.TIME && type
				.category() != Column.Category.CATEGORICAL) {
			// must round if underlying data was not rounded already
			for (int i = 0; i < data.length; i++) {
				double value = data[i];
				if (Double.isFinite(value)) {
					data[i] = Math.round(value);
				}
			}
		}
	}

	@Override
	public double get(int index) {
		return data[index];
	}


	/**
	 * {@inheritDoc} <p> Finite values are rounded via {@link Math#round(double)} and stored as {@code double} values.
	 * Whole numbers between {@code +/- 2^53-1} can be stored without loss of information.
	 */
	@Override
	public void set(int index, double value) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		double integerValue = value;
		if (Double.isFinite(integerValue)) {
			//round values that are not NaN, +- infinity
			integerValue = Math.round(integerValue);
		}
		data[index] = integerValue;
	}


	@Override
	public int size() {
		return data.length;
	}

	@Override
	public TypeId type() {
		return TypeId.INTEGER_53_BIT;
	}

	@Override
	protected double[] getData() {
		return data;
	}


	@Override
	protected void freeze() {
		frozen = true;
	}

	@Override
	public String toString() {
		return BufferPrinter.print(this);
	}

}
