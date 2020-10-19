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

package com.rapidminer.belt.column.io;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.Arrays;

import com.rapidminer.belt.column.Column;


/**
 * Builder for numeric columns out of {@link ByteBuffer}s.
 *
 * @author Michael Knopf, Gisa Meier
 */
public final class NumericColumnBuilder {

	private static final String MSG_NULL_BUFFER = "Buffer must not be null";
	private static final int SHIFT_FOR_8_BYTE_NUMBER = 3;

	private final double[] data;
	private final Column.TypeId type;

	private int position;

	/**
	 * Creates a column builder for a {@link Column.TypeId#REAL} or {@link Column.TypeId#INTEGER_53_BIT} column of the
	 * given length where the data can be put via {@link ByteBuffer}s containing double values.
	 *
	 * @param size
	 * 		the length of the column to construct
	 * @param type
	 * 		the column type, either real or integer
	 * @throws IllegalArgumentException
	 * 		if the size is negative or the type neither real nor integer
	 * @throws NullPointerException
	 * 		if the type is {@code null}
	 */
	public NumericColumnBuilder(int size, Column.TypeId type) {
		if (size < 0) {
			throw new IllegalArgumentException("Number of elements must be positive");
		}
		if (type == null) {
			throw new NullPointerException("Column type must not be null");
		}
		if (Column.TypeId.REAL != type && Column.TypeId.INTEGER_53_BIT != type) {
			throw new IllegalArgumentException("Incompatible column type");
		}
		this.data = new double[size];
		this.type = type;
		this.position = 0;
	}

	/**
	 * Returns the current position until which the second data has been written.
	 *
	 * @return the row index that is written next
	 */
	public int position() {
		return position;
	}

	/**
	 * Puts the double values in the buffer into the column starting at the current {@link #position()}. Values for
	 * {@link Column.TypeId#INTEGER_53_BIT} columns are rounded.
	 *
	 * @param buffer
	 * 		a buffer containing double values
	 * @return the builder
	 * @throws NullPointerException
	 * 		if the buffer is {@code null}
	 */
	public NumericColumnBuilder put(ByteBuffer buffer) {
		if (buffer == null) {
			throw new NullPointerException(MSG_NULL_BUFFER);
		}
		DoubleBuffer wrapper = buffer.asDoubleBuffer();
		int length = Math.min(wrapper.remaining(), data.length - position);
		wrapper.get(data, position, length);
		if (Column.TypeId.INTEGER_53_BIT == type) {
			roundArray(data, position, position + length);
		}
		buffer.position(wrapper.position() << SHIFT_FOR_8_BYTE_NUMBER);
		position += length;
		return this;
	}

	/**
	 * Creates a column as defined by the builder. If the current {@link #position()} is smaller than the originally
	 * defined column size, the remaining values will be missing values.
	 *
	 * @return a new column
	 */
	public Column toColumn() {
		if (position < data.length) {
			Arrays.fill(data, position, data.length, Double.NaN);
			position = data.length;
		}
		return ColumnAccessor.get().newNumericColumn(type, data);
	}

	/**
	 * Rounds the values in the data array between from and to.
	 */
	private void roundArray(double[] data, int from, int to) {
		for (int i = from; i < to; i++) {
			double value = data[i];
			if (Double.isFinite(value)) {
				data[i] = Math.round(value);
			}
		}
	}

}
