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

package com.rapidminer.belt.table;


import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.Buffers.InternalBuffers;


/**
 * Provides access to selected package-private methods from the {@code com.rapidminer.belt.buffer} package. See
 * {@link Buffers} for the initialization code. This code is in place to allow for dividing the code
 * base into meaningful sub-package without exposing sensitive methods (e.g., constructors for internal types). It is
 * a straight-forward implementation of the friend package pattern (Practical API Design, Tulach 2008).
 *
 * @author Gisa Meier
 */
public final class BufferAccessor {

	private static volatile InternalBuffers instance;

	/**
	 * @return an instance that can be used to create columns
	 */
	static InternalBuffers get() {
		InternalBuffers a = instance;
		if (a != null) {
			return a;
		}
		try {
			Class.forName(Buffers.class.getName(), true, Buffers.class.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
		return instance;
	}

	/**
	 * Sets the instance. Is called from a static block in {@link Buffers}.
	 *
	 * @param access the accessor to set
	 */
	public static void set(InternalBuffers access) {
		if (instance != null) {
			throw new IllegalStateException();
		}
		instance = access;
	}

	private BufferAccessor() {}

}
