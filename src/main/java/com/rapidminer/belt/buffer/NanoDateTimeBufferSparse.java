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

import java.time.Instant;

import com.rapidminer.belt.column.Column;


/**
 * Sparse high precision date time buffer implementation with fixed length, internally storing long second and integer
 * nanosecond values. This buffer is write only and it cannot be randomly modified. The {@link #setNext(long, int)}
 * method can be used to set the next free index in the buffer to the given value. The {@link #setNext(int, long, int)}
 * method can be used to set the value at the given index to the given value. In this case all values at smaller indices
 * (that have not already been set) will be set to the buffers default second value and {0} nanos. This is more
 * efficient than calling {@link #setNext(long, int)} for every default value. Please note that values, once they are
 * set, cannot be modified.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 *
 * @author Kevin Majchrzak
 * @see NanosecondDateTimeBuffer
 * @see SecondDateTimeBufferSparse
 * @see Buffers
 */
final class NanoDateTimeBufferSparse extends DateTimeBufferSparse {


	/**
	 * Creates a sparse high precision buffer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#DATE_TIME}. The method does not take default nanoseconds as only seconds are stored sparsely and
	 * nanos will default to {@code 0}.
	 *
	 * @param defaultSeconds
	 * 		the data's most common default seconds value.
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if length is negative or defaultSeconds is out of bounds.
	 */
	NanoDateTimeBufferSparse(long defaultSeconds, int length) {
		super(defaultSeconds, length, true);
	}

	/**
	 * Creates a high precision sparse buffer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#DATE_TIME}. The default value's nanoseconds are ignored as only seconds are stored sparsely and
	 * nanos will default to {@code 0}.
	 *
	 * @param defaultSeconds
	 * 		the data's most common default seconds value represented via an {@link Instant}.
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if length is negative or defaultSeconds is out of bounds.
	 */
	NanoDateTimeBufferSparse(Instant defaultSeconds, int length) {
		super(defaultSeconds, length, true);
	}
}
