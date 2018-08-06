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

import java.util.Arrays;

import com.rapidminer.belt.Column.TypeId;


/**
 * A {@link ColumnBuffer} that can change its size and contains double values. If the size is fixed, a {@link
 * FixedRealBuffer} should be used instead.
 *
 * @author Gisa Meier
 */
public final class GrowingRealBuffer extends GrowingColumnBuffer {

	/**
	 * Error message when trying to change frozen buffer
	 */
	private static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";
	private static final double[] PLACEHOLDER_BUFFER = new double[0];
	private static final int MIN_NON_EMPTY_SIZE = 8;


	private double[] data;
	private boolean frozen = false;
	private int size;


	/**
	 * Creates a buffer of the given length.
	 *
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if the given length is negative
	 */
	GrowingRealBuffer(int length) {
		if (length < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + length);
		}
		data = new double[length];
		size = length;
	}


	/**
	 * Creates a buffer with starting size zero.
	 */
	GrowingRealBuffer() {
		data = PLACEHOLDER_BUFFER;
		size = 0;
	}


	@Override
	public double get(int index) {
		return data[index];
	}


	@Override
	public synchronized void set(int index, double value) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		data[index] = value;
	}


	@Override
	public synchronized int size() {
		return size;
	}

	@Override
	public TypeId type() {
		return Column.TypeId.REAL;
	}

	/**
	 * Resizes the buffer. The underlying data structure might be bigger than the given length to prevent copies on
	 * every resize.
	 *
	 * @param length
	 * 		the new length of the buffer.
	 */
	public synchronized void resize(int length) {
		if (frozen) {
			throw new IllegalStateException(BUFFER_FROZEN_MESSAGE);
		}
		size = length;
		if (length <= data.length) {
			return;
		}
		int oldLength = data.length;
		int newLength = Math.max(Math.max(MIN_NON_EMPTY_SIZE, length), oldLength + (oldLength >> 1));
		data = Arrays.copyOf(data, newLength);
	}


	@Override
	void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height) {
		resize(height);
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, height);
		int copyIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (copyIndex < max) {
			data[copyIndex] = buffer[bufferIndex];
			bufferIndex += bufferStepSize;
			copyIndex++;
		}
	}


	@Override
	protected synchronized double[] getData() {
		return data;
	}


	@Override
	protected synchronized void freeze() {
		frozen = true;
		if (data.length > size) {
			data = Arrays.copyOf(data, size);
		}
	}

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}

}
