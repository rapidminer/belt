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
 * Sparse low precision date time buffer implementation with fixed length, internally storing seconds of epoch long
 * values. Nanoseconds will not be saved and ignored. The buffer is write only. The {@link #setNext(long)} method can be
 * used to set the next free index in the buffer to the given value. The {@link #setNext(int, long)} method can be used
 * to set a specific position to the given value. In this case all values at smaller indices (that have not already been
 * set) will be set to the buffer's default value. This is more efficient than calling {@link #setNext(long)} for every
 * default value. Please note that values, once they are set, cannot be modified.
 * <p>
 * Please note that the buffer implementation is thread safe but accessing it from multiple threads will be slow.
 *
 * @author Kevin Majchrzak
 * @see SecondDateTimeBuffer
 * @see NanoDateTimeBufferSparse
 * @see Buffers
 */
final class SecondDateTimeBufferSparse extends DateTimeBufferSparse {

	/**
	 * Creates a sparse low precision buffer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#DATE_TIME}. Nanoseconds will not be saved and ignored.
	 *
	 * @param defaultSeconds
	 * 		the data's most common default seconds value.
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if length is negative or defaultSeconds is out of bounds.
	 */
	SecondDateTimeBufferSparse(long defaultSeconds, int length) {
		super(defaultSeconds, length, false);
	}

	/**
	 * Creates a low precision sparse buffer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#DATE_TIME}. Nanoseconds will not be saved and ignored.
	 *
	 * @param defaultSeconds
	 * 		the data's most common default seconds value represented via an {@link Instant}.
	 * @param length
	 * 		the length of the buffer
	 * @throws IllegalArgumentException
	 * 		if length is negative or defaultSeconds is out of bounds.
	 */
	SecondDateTimeBufferSparse(Instant defaultSeconds, int length) {
		super(defaultSeconds, length, false);
	}

}
