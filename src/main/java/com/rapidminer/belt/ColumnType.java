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

import java.util.Comparator;
import java.util.Objects;

import com.rapidminer.belt.Column.Category;
import com.rapidminer.belt.Column.TypeId;


/**
 * Describes the type of a column but does not hold any data. Can be used to create new columns, e.g.,
 * via {@link CategoricalColumnBuffer#toColumn(ColumnType)}.
 *
 * @param <T>
 * 		the complex type of the column elements (optional)
 * @author Michael Knopf
 * @see ColumnTypes
 */
public final class ColumnType<T> {

	private final Column.TypeId id;
	private final String customTypeId;
	private final Column.Category category;
	private final Class<T> elementType;
	private final Comparator<T> elementComparator;

	ColumnType(TypeId type, String customTypeId, Category category, Class<T> elementType,
			   Comparator<T> elementComparator) {
		this.id = type;
		this.customTypeId = customTypeId;
		this.category = category;
		this.elementType = elementType;
		this.elementComparator = elementComparator;
	}

	/**
	 * Identifies the column type as one of the builtin types, e.g., {@link TypeId#NOMINAL}, or a custom column type
	 * registered by a plugin ({@link TypeId#CUSTOM}).
	 *
	 * @return the type id
	 */
	public TypeId id() {
		return id;
	}

	/**
	 * Returns an additional identifier for column types with id {@link TypeId#CUSTOM}. Return {@code null} for builtin
	 * column types.
	 *
	 * @return the custom id or {@code null}
	 */
	public String customTypeID() {
		return customTypeId;
	}

	/**
	 * Returns the complex type of the column elements. Returns {@link Void} for primitive columns such as
	 * {@link TypeId#REAL} columns.
	 *
	 * @return the element type or {@link Void}
	 */
	public Class<T> elementType() {
		return elementType;
	}

	/**
	 * Returns the category to which this column type bleongs.
	 *
	 * @return the column category
	 */
	public Category category() {
		return category;
	}

	/**
	 * Returns a comparator for complex elements types. Returns {@code null} for primitive columns such as
	 * {@link TypeId#REAL} columns or if no comparator was specified.
	 *
	 * @return a comparator for the element type
	 */
	public Comparator<T> comparator() {
		return elementComparator;
	}

	@Override
	public String toString() {
		if (id == TypeId.CUSTOM) {
			return String.format("Column type %s (%s)", id, customTypeId);
		} else {
			return String.format("Column type %s", id);
		}
	}

	@Override
	public boolean equals(Object other) {
		if (this == other) {
			return true;
		}
		if (other == null || getClass() != other.getClass()) {
			return false;
		}
		ColumnType<?> otherType = (ColumnType<?>) other;
		return id == otherType.id &&
				Objects.equals(customTypeId, otherType.customTypeId) &&
				category == otherType.category &&
				Objects.equals(elementType, otherType.elementType) &&
				Objects.equals(elementComparator, otherType.elementComparator);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, customTypeId, category, elementType, elementComparator);
	}

}
