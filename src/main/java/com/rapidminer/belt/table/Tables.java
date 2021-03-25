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

package com.rapidminer.belt.table;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.column.Dictionary;


/**
 * Utility methods for {@link Table}s.
 *
 * @author Gisa Meier
 */
public final class Tables {

	/**
	 * Options for the dictionary handling in {@link #adapt(Table, Table, ColumnHandling, DictionaryHandling)}
	 */
	public enum DictionaryHandling {

		/**
		 * Dictionaries of categorical columns are not changed.
		 */
		UNCHANGED,

		/**
		 * If both columns are categorical of the same type, the dictionary is changed to the one in the schema.
		 * {@link Columns#changeDictionary(Column, Column)} is applied for all possible columns with the same label.
		 */
		CHANGE,

		/**
		 * If both columns are categorical of the same type, the dictionaries are merged, starting with the one of the
		 * schema. {@link Columns#mergeDictionary(Column, Column)} is applied for all possible columns with the same
		 * label.
		 */
		MERGE;
	}

	/**
	 * Options for the column handling in {@link #adapt(Table, Table, ColumnHandling, DictionaryHandling)}
	 */
	public enum ColumnHandling {

		/**
		 * The column labels stay the same.
		 */
		UNCHANGED,

		/**
		 * The columns are reordered in the order of their appearance in the schema. Columns that are not part of the
		 * schema are dropped.
		 */
		REORDER,

		/**
		 * The columns are reordered in the order of their appearance in the schema. Columns that are not part of the
		 * schema are added after the ones that are in the order of their appearance in the table.
		 */
		REORDER_KEEP_ADDITIONAL;
	}

	/**
	 * Incompatibility description in the return from {@link #findIncompatible(Table, Table, ColumnSetRequirement,
	 * TypeRequirement...)}
	 */
	public enum Incompatibility {

		/**
		 * The type of the column in the table does not match the type of the reference column in the schema.
		 */
		TYPE_MISMATCH,

		/**
		 * The dictionary of the table column is not a subset of the dictionary of the schema column.
		 */
		NOT_SUB_DICTIONARY,

		/**
		 * The column is in the schema but missing from the table.
		 */
		MISSING_COLUMN,

		/**
		 * The column is in the table but not in the schema.
		 */
		WRONG_COLUMN_PRESENT
	}

	/**
	 * Options for the type requirements in {@link #findIncompatible(Table, Table, ColumnSetRequirement,
	 * TypeRequirement...)}
	 */
	public enum TypeRequirement {

		/**
		 * The types of the columns in the table must be the same as the columns with the same name in the schema.
		 */
		REQUIRE_MATCHING_TYPES,

		/**
		 * If the reference column in the schema is real the table column may be integer.
		 */
		ALLOW_INT_FOR_REAL,

		/**
		 * The dictionaries of the nominal columns in the table must be a subset of the reference columns in the
		 * schema.
		 */
		REQUIRE_SUB_DICTIONARIES
	}

	/**
	 * Options for the column set requirement in {@link #findIncompatible(Table, Table, ColumnSetRequirement,
	 * TypeRequirement...)}
	 */
	public enum ColumnSetRequirement {

		/**
		 * The column set of the table must be equal to the one of the schema.
		 */
		EQUAL,

		/**
		 * The column set of the table must be a subset of the one of the schema.
		 */
		SUBSET,

		/**
		 * The column set of the table must be a superset of the one of the schema.
		 */
		SUPERSET
	}


	/**
	 * Adapts the table to the schema. The ordering and selection of the new columns is defined by the {@link
	 * ColumnHandling} and the dictionary adjustments for associated categorical column by the {@link
	 * DictionaryHandling} parameter.
	 *
	 * @param table
	 * 		the table to adapt
	 * @param schema
	 * 		the schema to adapt to
	 * @param columnRequirement
	 * 		the desired handling for column ordering and selection
	 * @param dictionaryHandling
	 * 		the desired handling for dictionaries of associated categorical columns
	 * @return the table adjusted to the schema
	 * @throws NullPointerException
	 * 		if any of the input parameters is {@code null}
	 */
	public static Table adapt(Table table, Table schema, ColumnHandling columnRequirement,
							  DictionaryHandling dictionaryHandling) {

		if (table == null) {
			throw new NullPointerException("table must not be null");
		}
		if (schema == null) {
			throw new NullPointerException("schema must not be null");
		}
		if (columnRequirement == null) {
			throw new NullPointerException("column handling must not be null");
		}
		if (dictionaryHandling == null) {
			throw new NullPointerException("dictionary handling must not be null");
		}

		// nothing to do if from has no columns
		if (table.width() == 0) {
			return table;
		}

		List<String> newColumns = extractLabels(table, schema, columnRequirement);

		if (newColumns.isEmpty()) {
			//this case needs extra handling to keep the original height
			return new Table(table.height());
		}
		if (dictionaryHandling == DictionaryHandling.UNCHANGED) {
			return table.columns(newColumns);
		}

		Column[] columnArray = handleColumns(table, schema, dictionaryHandling, newColumns);
		String[] newLabelsArray = newColumns.toArray(new String[0]);
		return new Table(columnArray, newLabelsArray, Table.getMetaDataForColumns(newLabelsArray,
				table.getMetaData()));
	}


	/**
	 * Finds the columns in the table that are incompatible with the schema according to the further parameters.
	 *
	 * @param table
	 * 		the table to check
	 * @param schema
	 * 		the schema to check against
	 * @param columnRequirement
	 * 		requirements for the column set
	 * @param typeRequirements
	 * 		requirements for the types, can be empty for no type check
	 * @return a map from incompatible column name to reason for incompatibility
	 * @throws NullPointerException
	 * 		if any of the input parameters is {@code null}
	 */
	public static Map<String, Incompatibility> findIncompatible(Table table, Table schema,
																ColumnSetRequirement columnRequirement,
																TypeRequirement... typeRequirements) {
		if (table == null) {
			throw new NullPointerException("table must not be null");
		}
		if (schema == null) {
			throw new NullPointerException("schema must not be null");
		}
		if (columnRequirement == null) {
			throw new NullPointerException("column handling must not be null");
		}
		if (typeRequirements == null) {
			throw new NullPointerException("type handlings must not be null");
		}

		EnumSet<TypeRequirement> typeRequirement = EnumSet.noneOf(TypeRequirement.class);
		Collections.addAll(typeRequirement, typeRequirements);
		Map<String, Incompatibility> incompatibles = new HashMap<>();
		if (columnRequirement == ColumnSetRequirement.SUPERSET) {
			checkSuperset(table, schema, typeRequirement, incompatibles);
		} else {
			checkSubsetOrEqual(table, schema, columnRequirement, typeRequirement, incompatibles);
		}

		return incompatibles;
	}

	/**
	 * Checks for the case where the column requirement is subset or equal.
	 */
	private static void checkSubsetOrEqual(Table table, Table schema, ColumnSetRequirement columnRequirement,
										   EnumSet<TypeRequirement> typeRequirement,
										   Map<String, Incompatibility> incompatibles) {
		int wrongFound = 0;
		for (int i = 0; i < table.width(); i++) {
			String label = table.label(i);
			if (!schema.contains(label)) {
				incompatibles.put(label, Incompatibility.WRONG_COLUMN_PRESENT);
				wrongFound++;
			} else if (typeRequirement.contains(TypeRequirement.REQUIRE_MATCHING_TYPES)) {
				Column tableColumn = table.column(i);
				Column schemaColumn = schema.column(label);
				checkTypes(typeRequirement, label, tableColumn, schemaColumn, incompatibles);
			}
		}
		if (columnRequirement == ColumnSetRequirement.EQUAL && table.width() - wrongFound != schema.width()) {
			for (int i = 0; i < schema.width(); i++) {
				String label = schema.label(i);
				if (!table.contains(label)) {
					incompatibles.put(label, Incompatibility.MISSING_COLUMN);
				}
			}
		}
	}

	/**
	 * Checks for the superset column requirement.
	 */
	private static void checkSuperset(Table table, Table schema, EnumSet<TypeRequirement> typeRequirement, Map<String,
			Incompatibility> incompatibles) {
		for (int i = 0; i < schema.width(); i++) {
			String label = schema.label(i);
			if (!table.contains(label)) {
				incompatibles.put(label, Incompatibility.MISSING_COLUMN);
			} else if (typeRequirement.contains(TypeRequirement.REQUIRE_MATCHING_TYPES)) {
				Column tableColumn = table.column(label);
				Column schemaColumn = schema.column(i);
				checkTypes(typeRequirement, label, tableColumn, schemaColumn, incompatibles);
			}
		}
	}

	/**
	 * Checks if the types match and otherwise adds to the incompatibilities.
	 */
	private static void checkTypes(EnumSet<TypeRequirement> typeRequirement, String label, Column tableColumn,
								   Column schemaColumn, Map<String, Incompatibility> incompatibles) {
		boolean typeMatches = tableColumn.type().id() == schemaColumn.type().id();
		if (typeMatches) {
			if (typeRequirement.contains(TypeRequirement.REQUIRE_SUB_DICTIONARIES) && schemaColumn.type().id() == Column.TypeId.NOMINAL) {
				Dictionary tableDictionary = tableColumn.getDictionary();
				Dictionary schemaDictionary = schemaColumn.getDictionary();
				Set<String> schemaSet = schemaDictionary.createInverse().keySet();
				for (int i = 1; i <= tableDictionary.maximalIndex(); i++) {
					String value = tableDictionary.get(i);
					if (!schemaSet.contains(value)) {
						incompatibles.put(label, Incompatibility.NOT_SUB_DICTIONARY);
						break;
					}
				}
			}
		} else {
			boolean intMatchesReal = typeRequirement.contains(TypeRequirement.ALLOW_INT_FOR_REAL)
					&& schemaColumn.type().id() == Column.TypeId.REAL && tableColumn.type().id() == Column.TypeId.INTEGER_53_BIT;
			if(!intMatchesReal) {
				incompatibles.put(label, Incompatibility.TYPE_MISMATCH);
			}
		}
	}

	/**
	 * Create an array of new columns with changed dictionaries if required by the dictionary handling and possible.
	 */
	private static Column[] handleColumns(Table from, Table to, DictionaryHandling dictionaryHandling,
										  List<String> newColumns) {
		Column[] columnArray = new Column[newColumns.size()];
		int newColumnIndex = 0;
		for (String label : newColumns) {
			Column column = from.column(label);

			if (column.type().category() == Column.Category.CATEGORICAL && to.contains(label)) {
				Column description = to.column(label);
				//if both columns are categorical, change dictionary
				if (description.type().category() == Column.Category.CATEGORICAL
						&& column.type().elementType().equals(description.type().elementType())) {
					column = dictionaryHandling == DictionaryHandling.CHANGE ? Columns.changeDictionary(column,
							description) : Columns.mergeDictionary(column, description);
				}
			}

			columnArray[newColumnIndex++] = column;
		}
		return columnArray;
	}

	/**
	 * Extracts a list of labels that are in the to adapt table and fit the header with respect to the column handling.
	 */
	private static List<String> extractLabels(Table toAdjust, Table header, ColumnHandling columnRequirement) {
		if (columnRequirement == ColumnHandling.UNCHANGED) {
			return Arrays.asList(toAdjust.labelArray());
		}
		List<String> newColumns = new ArrayList<>();
		for (String label : header.labelArray()) {
			if (toAdjust.contains(label)) {
				newColumns.add(label);
			}
		}
		if (columnRequirement == ColumnHandling.REORDER_KEEP_ADDITIONAL) {
			Set<String> used = new HashSet<>(newColumns);
			for (String label : toAdjust.labelArray()) {
				if (!used.contains(label)) {
					newColumns.add(label);
				}
			}
		}
		return newColumns;
	}

	private Tables() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}
}
