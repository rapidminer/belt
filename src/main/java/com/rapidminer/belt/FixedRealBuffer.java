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

import com.rapidminer.belt.Column.TypeId;

/**
 * A {@link ColumnBuffer} with length fixed from the start storing double values.
 *
 * @author Gisa Schaefer
 */
public final class FixedRealBuffer extends ColumnBuffer {

	/**
	 * Error message when trying to change frozen buffer
	 */
	private static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";

	private final double[] data;
	private boolean frozen = false;

	/**
	 * Creates a buffer of the given length to create a {@link Column} of type id {@link TypeId#REAL}.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	public FixedRealBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		data = new double[length];
	}

	/**
	 * Creates a buffer by copying the data from the given column. Throws a {@link UnsupportedOperationException} if the
	 * category has not the capability {@link Column.Capability#NUMERIC_READABLE}.
	 *
	 * @param column
	 * 		the Column to copy into the buffer
	 */
	public FixedRealBuffer(Column column) {
		data = new double[column.size()];
		column.fill(data, 0);
	}

	@Override
	public double get(int index) {
		return data[index];
	}


	@Override
	public void set(int index, double value) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		data[index] = value;
	}


	@Override
	public int size() {
		return data.length;
	}

	@Override
	public TypeId type() {
		return TypeId.REAL;
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
		return PrettyPrinter.print(this);
	}

}
