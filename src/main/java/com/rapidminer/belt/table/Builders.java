/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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


/**
 * Utility methods for creating table builders.
 *
 * @author Gisa Meier
 */
public final class Builders {

	private Builders() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

	/**
	 * Creates a builder for a new {@link Table} with the given number of rows.
	 *
	 * @param rows
	 * 		the number of rows
	 * @return a table builder
	 */
	public static TableBuilder newTableBuilder(int rows) {
		return new TableBuilder(rows);
	}

	/**
	 * Creates a builder for a new {@link Table} derived from the given table.
	 *
	 * @param table
	 * 		the source table
	 * @return a table builder
	 */
	public static TableBuilder newTableBuilder(Table table) {
		return new TableBuilder(table);
	}

}
