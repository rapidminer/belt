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

package com.rapidminer.belt.column;

import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.buffer.CategoricalBuffer;


/**
 * Describes the type of a column but does not hold any data. Can be used to create new columns, e.g.,
 * via {@link CategoricalBuffer#toColumn(ColumnType)}.
 *
 * @param <T>
 * 		the complex type of the column elements (optional)
 * @author Michael Knopf
 * @see ColumnTypes
 */
public final class ColumnType<T> {

	private static final EnumSet<Column.Capability> CATEGORICAL = EnumSet.of(Column.Capability.OBJECT_READABLE,
			Column.Capability.NUMERIC_READABLE);

	private static final EnumSet<Column.Capability> NUMERIC = EnumSet.of(Column.Capability.NUMERIC_READABLE);

	private static final EnumSet<Column.Capability> OBJECT = EnumSet.of(Column.Capability.OBJECT_READABLE);

	private static final EnumSet<Column.Capability> CATEGORICAL_SORTABLE =
			EnumSet.of(Column.Capability.OBJECT_READABLE, Column.Capability.NUMERIC_READABLE);

	private static final EnumSet<Column.Capability> NUMERIC_SORTABLE = EnumSet.of(Column.Capability.NUMERIC_READABLE);

	private static final EnumSet<Column.Capability> OBJECT_SORTABLE = EnumSet.of(Column.Capability.OBJECT_READABLE);


	private final Column.TypeId id;
	private final String customTypeId;
	private final Column.Category category;
	private final Class<T> elementType;
	private final Comparator<T> elementComparator;
	private final Set<Column.Capability> capabilities;

	ColumnType(TypeId type, String customTypeId, Category category, Class<T> elementType,
			   Comparator<T> elementComparator) {
		this.id = type;
		this.customTypeId = customTypeId;
		this.category = category;
		this.elementType = elementType;
		this.elementComparator = elementComparator;
		this.capabilities = getStandardCapabilities(category, elementComparator);
	}

	ColumnType(TypeId type, String customTypeId, Category category, Class<T> elementType,
			   Comparator<T> elementComparator, Column.Capability... nonStandardCapabilities) {
		this.id = type;
		this.customTypeId = customTypeId;
		this.category = category;
		this.elementType = elementType;
		this.elementComparator = elementComparator;
		this.capabilities = EnumSet.copyOf(getStandardCapabilities(category, elementComparator));
		this.capabilities.addAll(Arrays.asList(nonStandardCapabilities));
	}

	private Set<Column.Capability> getStandardCapabilities(Category category, Comparator<T> comparator) {
		if (comparator == null) {
			switch (category) {
				case CATEGORICAL:
					return CATEGORICAL;
				case NUMERIC:
					return NUMERIC;
				case OBJECT:
				default:
					return OBJECT;
			}
		} else {
			switch (category) {
				case CATEGORICAL:
					return CATEGORICAL_SORTABLE;
				case NUMERIC:
					return NUMERIC_SORTABLE;
				case OBJECT:
				default:
					return OBJECT_SORTABLE;
			}
		}
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
	 * Returns the category to which this column type belongs.
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

	/**
	 * Returns whether the column to which this column type belongs has the capability.
	 *
	 * @param capability
	 * 		the capability to check
	 * @return {@code true} if the column has the given capability, {@code false} otherwise
	 */
	public final boolean hasCapability(Column.Capability capability) {
		return capabilities.contains(capability);
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
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ColumnType<?> that = (ColumnType<?>) o;
		return id == that.id &&
				Objects.equals(customTypeId, that.customTypeId) &&
				category == that.category &&
				Objects.equals(elementType, that.elementType) &&
				Objects.equals(elementComparator, that.elementComparator) &&
				Objects.equals(capabilities, that.capabilities);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, customTypeId, category, elementType, elementComparator, capabilities);
	}
}
