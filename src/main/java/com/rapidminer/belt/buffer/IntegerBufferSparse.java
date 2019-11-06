/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;


/**
 * Sparse integer buffer implementation with fixed length, internally storing double values. The buffer is write only.
 * The {@link #setNext(double)} method can be used to set the next free position in the buffer to the given value. The
 * {@link #setNext(int, double)} method can be used to set a specific position to the given value. In this case all
 * values at smaller indices (that have not already been set) will be set to the buffer's default value. This is more
 * efficient than calling {@link #setNext(double)} for every default value. Please note that values, once they are set,
 * cannot be modified.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 * <p>
 * The buffer's efficiency will depend on the data's sparsity. The buffer will usually be memory and time efficient for
 * data with a sparsity of {@code >= 50%}. The sparse buffer comes with a constant memory overhead so that it is
 * recommended not to use it for very small data ({@code <1024} data points).
 *
 * @author Kevin Majchrzak
 * @see IntegerBuffer
 * @see Buffers
 */
public final class IntegerBufferSparse extends RealBufferSparse {

	/**
	 * Creates a sparse buffer of the given length to create a sparse {@link Column} of type id {@link TypeId#INTEGER}.
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 * @param length
	 * 		the length of the buffer
	 */
	IntegerBufferSparse(double defaultValue, int length) {
		super(Double.isFinite(defaultValue) ? Math.round(defaultValue) : defaultValue, length);
	}

	/**
	 * Returns the buffer's {@link TypeId}.
	 *
	 * @return {@link TypeId#INTEGER}.
	 */
	@Override
	public TypeId type() {
		return TypeId.INTEGER;
	}

	@Override
	public synchronized void setNext(int index, double value) {
		super.setNext(index, Double.isFinite(value) ? Math.round(value) : value);
	}

	@Override
	public String toString() {
		return "Sparse integer buffer of length " + size() + " with default value " + defaultValue;
	}
}
