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

package com.rapidminer.belt.util;

/**
 * Meta data for a single column.
 *
 * @author Michael Knopf
 */
public interface ColumnMetaData {

	/**
	 * The uniqueness level of a column data type.
	 */
	enum Uniqueness {
		/**
		 * No limitations. Multiple instances of a single column meta data type can be attached to a table or even a
		 * single column.
		 */
		NONE,

		/**
		 * Multiple instances of a single column meta data type can be attached to a table but at most one per column.
		 */
		COLUMN,

		/**
		 * At most one instance of the column meta data type can be attached to a table.
		 */
		TABLE
	}

	/**
	 * The unique type id of the column meta data.
	 *
	 * @return the type id
	 */
	String type();

	/**
	 * The uniqueness level of the meta data. Defaults to {@link Uniqueness#NONE} (no restrictions).
	 *
	 * @return the uniqueness level
	 */
	default Uniqueness uniqueness() {
		return Uniqueness.NONE;
	}

}
