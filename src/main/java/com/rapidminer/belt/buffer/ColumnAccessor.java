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

package com.rapidminer.belt.buffer;


import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Columns.InternalColumns;


/**
 * Provides access to selected package-private methods from the {@code com.rapidminer.belt.column} package. See
 * {@link Columns} for the initialization code. This code is in place to allow for dividing the code base into
 * meaningful sub-package without exposing sensitive methods (e.g., constructors for internal types). It is
 * a straight-forward implementation of the friend package pattern (Practical API Design, Tulach 2008).
 *
 * @author Gisa Meier
 */
public final class ColumnAccessor {

	private static volatile InternalColumns instance;

	/**
	 * @return an instance that can be used to create columns
	 */
	static InternalColumns get() {
		InternalColumns a = instance;
		if (a != null) {
			return a;
		}
		try {
			Class.forName(Columns.class.getName(), true, Columns.class.getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(e);
		}
		return instance;
	}

	/**
	 * Sets the instance. Is called from a static block in {@link Columns}.
	 *
	 * @param access the access to set
	 */
	public static void set(InternalColumns access) {
		if (instance != null) {
			throw new IllegalStateException();
		}
		instance = access;
	}

	private ColumnAccessor() {
	}

}
