/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2018 RapidMiner GmbH
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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;

import com.rapidminer.belt.util.ColumnMetaData;


/**
 * Allows to select columns that satisfy certain properties. Example usage:
 * {@code selector.ofCategory(Category.NUMERIC)
 * 				  .withMetaData(ColumnRole.LABEL)
 * 				  .labels()}.
 *
 * <p>This class is not threadsafe. If adding filtering properties from different threads is required, appropriate
 * synchronization must be used.
 *
 * @author Gisa Meier
 */
public final class ColumnSelector {

	private static final String MESSAGE_CAPABILITY_NULL = "Capability must not be null.";
	private static final String MESSAGE_CATEGORY_NULL = "Category must not be null.";
	private static final String MESSAGE_TYPE_ID_NULL = "Type id must not be null.";
	private static final String MESSAGE_METADATA_NULL = "Column metadata must not be null.";
	private static final String MESSAGE_METADATA_CLASS_NULL = "Column metadata type must not be null.";

	private final Table table;

	private BiPredicate<Column, String> selector = null;

	ColumnSelector(Table table) {
		this.table = Objects.requireNonNull(table, "Table must not be null");
	}

	/**
	 * Adds the requirement to the selector that all selected columns must have the given type id. Calling this method
	 * twice with different type ids leads to no results since every column has a unique type id.
	 *
	 * @param typeId
	 * 		the type id to filter by
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if typeId is {@code null}
	 */
	public ColumnSelector ofTypeId(Column.TypeId typeId) {
		Objects.requireNonNull(typeId, MESSAGE_TYPE_ID_NULL);
		setOrAnd((c, s) -> c.type().id() == typeId);
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must not have the given type id. Calling this
	 * method twice with different type ids leads to results where the type id is neither the first nor the second
	 * specified type id.
	 *
	 * @param typeId
	 * 		the type id to filter out
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if typeId is {@code null}
	 */
	public ColumnSelector notOfTypeId(Column.TypeId typeId) {
		Objects.requireNonNull(typeId, MESSAGE_TYPE_ID_NULL);
		setOrAnd((c, s) -> c.type().id() != typeId);
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must have the given category. Calling this
	 * method twice with different categories leads to no results since every column has a unique category.
	 *
	 * @param category
	 * 		the category to filter by
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if category is {@code null}
	 */
	public ColumnSelector ofCategory(Column.Category category) {
		Objects.requireNonNull(category, MESSAGE_CATEGORY_NULL);
		setOrAnd((c, s) -> c.type().category() == category);
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must not have the given category. Note that
	 * calling this method twice with different categories leads to results where the category is neither the first
	 * nor the second specified category.
	 *
	 * @param category
	 * 		the category to filter out
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if category is {@code null}
	 */
	public ColumnSelector notOfCategory(Column.Category category) {
		Objects.requireNonNull(category, MESSAGE_CATEGORY_NULL);
		setOrAnd((c, s) -> c.type().category() != category);
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must have the given capability. Calling this
	 * method twice with different capabilities leads to results with only columns that have both capabilities.
	 *
	 * @param capability
	 * 		the capability to filter by
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if capability is {@code null}
	 */
	public ColumnSelector withCapability(Column.Capability capability) {
		Objects.requireNonNull(capability, MESSAGE_CAPABILITY_NULL);
		setOrAnd((c, s) -> c.hasCapability(capability));
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must not have the given capability. Calling this
	 * method twice with different capabilities leads to results which have neither the first nor the second specified
	 * capability.
	 *
	 * @param capability
	 * 		the capability to filter out
	 * @return the column selector to enable chaining
	 */
	public ColumnSelector withoutCapability(Column.Capability capability) {
		Objects.requireNonNull(capability, MESSAGE_CAPABILITY_NULL);
		setOrAnd((c, s) -> !c.hasCapability(capability));
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must have the given meta data. Calling this
	 * method twice with different meta data leads to results which have both meta data.
	 * If both meta data is of the same type with {@link com.rapidminer.belt.util.ColumnMetaData.Uniqueness#COLUMN}
	 * the result is empty.
	 *
	 * @param metaData
	 * 		the meta data to filter by
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if metaData is {@code null}
	 */
	public ColumnSelector withMetaData(ColumnMetaData metaData) {
		Objects.requireNonNull(metaData, MESSAGE_METADATA_NULL);
		setOrAnd((c, s) -> withMetaData(s, metaData));
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must not have the given meta data. Calling this
	 * method twice with different meta data leads to results where the meta data is neither the first nor  the second
	 * specified meta data.
	 *
	 * @param metaData
	 * 		the meta data to filter out
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if metaData is {@code null}
	 */
	public ColumnSelector withoutMetaData(ColumnMetaData metaData) {
		Objects.requireNonNull(metaData, MESSAGE_METADATA_NULL);
		setOrAnd((c, s) -> withoutMetaData(s, metaData));
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must have the given meta data type. Calling this
	 * method twice with different types leads to results which have meta data of both types.
	 *
	 * @param type
	 * 		the type to filter by
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if type is {@code null}
	 */
	public <T extends ColumnMetaData> ColumnSelector withMetaData(Class<T> type) {
		Objects.requireNonNull(type, MESSAGE_METADATA_CLASS_NULL);
		setOrAnd((c, s) -> withMetaData(s, type));
		return this;
	}

	/**
	 * Adds the requirement to the selector that all selected columns must not have the given meta data type. Calling
	 * this method twice with different types leads to results where the type is neither the first nor the second
	 * specified type.
	 *
	 * @param type
	 * 		the type to filter out
	 * @return the column selector to enable chaining
	 * @throws NullPointerException
	 * 		if type is {@code null}
	 */
	public <T extends ColumnMetaData> ColumnSelector withoutMetaData(Class<T> type) {
		Objects.requireNonNull(type, MESSAGE_METADATA_CLASS_NULL);
		setOrAnd((c, s) -> withoutMetaData(s, type));
		return this;
	}

	/**
	 * Returns the labels of the columns which have the specified properties.
	 *
	 * @return the columns selected by this selector
	 */
	public List<String> labels() {
		if (selector == null) {
			return table.labels();
		}
		String[] labels = table.labelArray();
		Column[] columns = table.getColumns();
		List<String> result = new ArrayList<>();
		for (int i = 0; i < labels.length; i++) {
			if (selector.test(columns[i], labels[i])) {
				result.add(labels[i]);
			}
		}
		return result;
	}

	/**
	 * Returns the columns which have the specified properties.
	 *
	 * @return the columns selected by this selector
	 */
	public List<Column> columns() {
		if (selector == null) {
			return new ArrayList<>(Arrays.asList(table.getColumns()));
		}
		String[] labels = table.labelArray();
		Column[] columns = table.getColumns();
		List<Column> result = new ArrayList<>();
		for (int i = 0; i < labels.length; i++) {
			if (selector.test(columns[i], labels[i])) {
				result.add(columns[i]);
			}
		}
		return result;
	}


	/**
	 * Selects the list of columns associated with the given list of labels from the table.
	 *
	 * @param labels
	 * 		the labels of the columns to select
	 * @param table
	 * 		the table to select the columns from
	 * @return the columns list
	 * @throws NullPointerException
	 * 		if the table is {@code null} or the list is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the list contains labels that are not contained in the table
	 */
	public static List<Column> toColumnList(List<String> labels, Table table) {
		Objects.requireNonNull(table, "Table must not be null");
		Objects.requireNonNull(labels, "Names list must not be null.");
		List<Column> columns = new ArrayList<>(labels.size());
		for (String name : labels) {
			columns.add(table.column(name));
		}
		return columns;
	}

	/**
	 * Checks if the column with the label has the meta data.
	 */
	private boolean withMetaData(String label, ColumnMetaData metaData) {
		List<ColumnMetaData> list = table.getMetaData().get(label);
		if (list != null) {
			for (ColumnMetaData data : list) {
				if (metaData.equals(data)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Checks if the column with the label does not have the meta data.
	 */
	private boolean withoutMetaData(String label, ColumnMetaData metaData) {
		List<ColumnMetaData> list = table.getMetaData().get(label);
		if (list != null) {
			for (ColumnMetaData data : list) {
				if (metaData.equals(data)) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Checks if the column with the label has meta data of the given type.
	 */
	private <T extends ColumnMetaData> boolean withMetaData(String label, Class<T> type) {
		List<ColumnMetaData> list = table.getMetaData().get(label);
		if (list != null) {
			for (ColumnMetaData data : list) {
				if (type.isInstance(data)) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Checks if the column with the label does not have meta data of the given type.
	 */
	private <T extends ColumnMetaData> boolean withoutMetaData(String label, Class<T> type) {
		List<ColumnMetaData> list = table.getMetaData().get(label);
		if (list != null) {
			for (ColumnMetaData data : list) {
				if (type.isInstance(data)) {
					return false;
				}
			}
		}
		return true;
	}

	/**
	 * Sets the bipredicate if none exists already or combines it with the existing one by and.
	 */
	private void setOrAnd(BiPredicate<Column, String> predicate) {
		if (selector == null) {
			selector = predicate;
		} else {
			selector = selector.and(predicate);
		}
	}

}
