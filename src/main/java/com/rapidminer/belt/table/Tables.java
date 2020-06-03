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

package com.rapidminer.belt.table;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Columns;


/**
 * Utility methods for {@link Table}s.
 *
 * @author Gisa Meier
 */
public final class Tables {


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
	 * Adapts the table to the schema. The ordering and selection of the new columns is defined by the {@link
	 * ColumnHandling} and the dictionary adjustments for associated categorical column by the {@link
	 * DictionaryHandling} parameter.
	 *
	 * @param table
	 * 		the table to adapt
	 * @param schema
	 * 		the schema to adapt to
	 * @param columnHandling
	 * 		the desired handling for column ordering and selection
	 * @param dictionaryHandling
	 * 		the desired handling for dictionaries of associated categorical columns
	 * @return the table adjusted to the schema
	 * @throws NullPointerException if any of the input parameters is {@code null}
	 */
	public static Table adapt(Table table, Table schema, ColumnHandling columnHandling,
							  DictionaryHandling dictionaryHandling) {

		if (table == null) {
			throw new NullPointerException("table must not be null");
		}
		if (schema == null) {
			throw new NullPointerException("schema must not be null");
		}
		if (columnHandling == null) {
			throw new NullPointerException("column handling must not be null");
		}
		if (dictionaryHandling == null) {
			throw new NullPointerException("dictionary handling must not be null");
		}

		// nothing to do if from has no columns
		if (table.width() == 0) {
			return table;
		}

		List<String> newColumns = extractLabels(table, schema, columnHandling);

		if (newColumns.isEmpty()) {
			//this case needs extra handling to keep the original height
			return new Table(table.height());
		}
		if (dictionaryHandling == DictionaryHandling.UNCHANGED) {
			return table.columns(newColumns);
		}

		Column[] columnArray = handleColumns(table, schema, dictionaryHandling, newColumns);
		String[] newLabelsArray = newColumns.toArray(new String[0]);
		return new Table(columnArray, newLabelsArray, Table.getMetaDataForColumns(newLabelsArray, table.getMetaData()));
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
	 * Extracts a list of labels that are in the to adapt table and fit the header with respect to the column
	 * handling.
	 */
	private static List<String> extractLabels(Table toAdjust, Table header, ColumnHandling columnHandling) {
		if (columnHandling == ColumnHandling.UNCHANGED) {
			return Arrays.asList(toAdjust.labelArray());
		}
		List<String> newColumns = new ArrayList<>();
		for (String label : header.labelArray()) {
			if (toAdjust.contains(label)) {
				newColumns.add(label);
			}
		}
		if (columnHandling == ColumnHandling.REORDER_KEEP_ADDITIONAL) {
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
