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

package com.rapidminer.belt.util;

import java.util.Objects;


/**
 * References another column in the table, e.g., to represent the relation between a prediction and a score column.
 *
 * @author Michael Knopf
 */
public final class ColumnReference implements ColumnMetaData {

	private static final String TYPE_ID = "com.rapidminer.belt.meta.column.reference";

	private final String column;
	private final String value;

	public ColumnReference(String column) {
		this(column, null);
	}

	public ColumnReference(String column, String value) {
		this.column = column;
		this.value = value;
	}

	@Override
	public String type() {
		return TYPE_ID;
	}

	@Override
	public Uniqueness uniqueness() {
		return Uniqueness.COLUMN;
	}

	/**
	 * Returns the label of the referenced column.
	 *
	 * @return the column label
	 */
	public String getColumn() {
		return column;
	}

	/**
	 * Returns an optional referenced value.
	 *
	 * @return the value or {@code null}
	 */
	public String getValue() {
		return value;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ColumnReference other = (ColumnReference) o;
		return Objects.equals(column, other.column) &&
				Objects.equals(value, other.value);
	}

	@Override
	public int hashCode() {
		return Objects.hash(column, value);
	}

	@Override
	public String toString() {
		return "ColumnReference[" + column + ", " + value + "]";
	}

}
