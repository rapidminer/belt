/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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

import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.rapidminer.belt.column.Column.Category;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.type.StringList;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.reader.Readers;


/**
 * Describes the type of a column but does not hold any data.
 *
 * @param <T>
 * 		the complex type of the column elements (optional)
 * @author Michael Knopf
 */
public final class ColumnType<T> {

	/**
	 * Complex id for columns holding 64 bit double values. Data can be accessed, for example, via {@link
	 * Readers#numericReader(Column)}.
	 */
	public static final ColumnType<Void> REAL;

	/**
	 * Complex id for columns holding 64 bit double values with no fractional digits. Data can be accessed, for example,
	 * via {@link Readers#numericReader(Column)}.
	 * <p>
	 * Please note: The maximum / minimum value that can be stored without loss of information is {@code +/- 2^53-1} ,
	 * or {@code +/- 9,007,199,254,740,991}. Therefore, casting this column's values to {@code int} leads to loss of
	 * information for values smaller / larger than {@link Integer#MIN_VALUE} / {@link Integer#MAX_VALUE}.
	 */
	public static final ColumnType<Void> INTEGER_53_BIT;

	/**
	 * Complex id for columns holding categorical strings. Data can be read categorical or as {@link String}. This can
	 * be done, for example, via {@link Readers#categoricalReader(Column)} or {@link Readers#objectReader(Column,
	 * Class)}.
	 */
	public static final ColumnType<String> NOMINAL;

	/**
	 * Complex id for columns holding nanoseconds of a day. Data can be read numeric or as {@link java.time.LocalTime}.
	 * This can be done, for example, via {@link Readers#numericReader(Column)} or {@link Readers#objectReader(Column,
	 * Class)}.
	 */
	public static final ColumnType<LocalTime> TIME;

	/**
	 * Complex id for columns holding 64 bit UTC timestamps (seconds), and 32Bit fraction of a second (nanoseconds,
	 * optional). Data can be read as {@link java.time.Instant}. This can be done, for example, via {@link
	 * Readers#objectReader(Column, Class)}.
	 */
	public static final ColumnType<Instant> DATETIME;

	/**
	 * Complex id for columns holding non-categorical strings. Data can be read as {@link String}. This can be done, for
	 * example, via {@link Readers#objectReader(Column, Class)}.
	 */
	public static final ColumnType<String> TEXT;

	/**
	 * Complex id for columns holding sets of strings. Data can be read as {@link StringSet}. This can be done, for
	 * example, via {@link Readers#objectReader(Column, Class)}.
	 */
	public static final ColumnType<StringSet> TEXTSET;

	/**
	 * Complex id for columns holding lists of strings. Data can be read as {@link StringList}. This can be done, for
	 * example, via {@link Readers#objectReader(Column, Class)}.
	 */
	public static final ColumnType<StringList> TEXTLIST;

	private static final Map<TypeId, ColumnType<?>> ID_TO_TYPE;

	private static final EnumSet<Column.Capability> CATEGORICAL;
	private static final EnumSet<Column.Capability> NUMERIC;
	private static final EnumSet<Column.Capability> OBJECT;
	private static final EnumSet<Column.Capability> CATEGORICAL_SORTABLE;
	private static final EnumSet<Column.Capability> NUMERIC_SORTABLE;
	private static final EnumSet<Column.Capability> OBJECT_SORTABLE;

	static {
		// init capabilities
		CATEGORICAL = EnumSet.of(Column.Capability.OBJECT_READABLE, Column.Capability.NUMERIC_READABLE);
		NUMERIC = EnumSet.of(Column.Capability.NUMERIC_READABLE);
		OBJECT = EnumSet.of(Column.Capability.OBJECT_READABLE);
		CATEGORICAL_SORTABLE = EnumSet.of(Column.Capability.OBJECT_READABLE, Column.Capability.NUMERIC_READABLE,
				Column.Capability.SORTABLE);
		NUMERIC_SORTABLE = EnumSet.of(Column.Capability.NUMERIC_READABLE, Column.Capability.SORTABLE);
		OBJECT_SORTABLE = EnumSet.of(Column.Capability.OBJECT_READABLE, Column.Capability.SORTABLE);

		// init column types
		NOMINAL = new ColumnType<>(TypeId.NOMINAL, Category.CATEGORICAL, String.class, String::compareTo);
		INTEGER_53_BIT = new ColumnType<>(TypeId.INTEGER_53_BIT, Category.NUMERIC, Void.class, null,
				Column.Capability.SORTABLE);
		REAL = new ColumnType<>(TypeId.REAL, Category.NUMERIC, Void.class, null,
				Column.Capability.SORTABLE);
		TIME = new ColumnType<>(TypeId.TIME, Category.OBJECT, LocalTime.class, LocalTime::compareTo,
				Column.Capability.NUMERIC_READABLE);
		DATETIME = new ColumnType<>(TypeId.DATE_TIME, Category.OBJECT, Instant.class, Instant::compareTo);
		TEXT = new ColumnType<>(TypeId.TEXT, Category.OBJECT, String.class, String::compareTo);
		TEXTSET = new ColumnType<>(TypeId.TEXT_SET, Category.OBJECT, StringSet.class, StringSet::compareTo);
		TEXTLIST = new ColumnType<>(TypeId.TEXT_LIST, Category.OBJECT, StringList.class, StringList::compareTo);

		// init id to type map
		ID_TO_TYPE = new EnumMap<>(TypeId.class);
		for (ColumnType<?> columnType : Arrays.asList(NOMINAL, INTEGER_53_BIT, REAL, TIME, DATETIME, TEXT, TEXTSET, TEXTLIST)) {
			ID_TO_TYPE.put(columnType.id(), columnType);
		}
	}

	private final Column.TypeId id;
	private final Column.Category category;
	private final Class<T> elementType;
	private final Comparator<T> elementComparator;
	private final Set<Column.Capability> capabilities;

	ColumnType(TypeId type, Category category, Class<T> elementType,
			   Comparator<T> elementComparator) {
		this.id = type;
		this.category = category;
		this.elementType = elementType;
		this.elementComparator = elementComparator;
		this.capabilities = getStandardCapabilities(category, elementComparator);
	}

	ColumnType(TypeId type, Category category, Class<T> elementType,
			   Comparator<T> elementComparator, Column.Capability... nonStandardCapabilities) {
		this.id = type;
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
	 * Identifies the column type as one of the builtin types, e.g., {@link TypeId#NOMINAL}.
	 *
	 * @return the type id
	 */
	public TypeId id() {
		return id;
	}

	/**
	 * Returns the complex type of the column elements. Returns {@link Void} for primitive columns such as {@link
	 * TypeId#REAL} columns.
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
	 * Returns a comparator for complex elements types. Returns {@code null} for primitive columns such as {@link
	 * TypeId#REAL} columns or if no comparator was specified.
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
		return String.format("Column type %s", id);
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
				category == that.category &&
				Objects.equals(elementType, that.elementType) &&
				Objects.equals(elementComparator, that.elementComparator) &&
				Objects.equals(capabilities, that.capabilities);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, category, elementType, elementComparator, capabilities);
	}

	/**
	 * Returns the unique {@link ColumnType} with this id.
	 *
	 * @param id
	 * 		the id of the requested column type
	 * @return the type with the given id
	 */
	public static ColumnType<?> forId(TypeId id) {
		return ID_TO_TYPE.get(id);
	}
}
