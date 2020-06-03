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

package com.rapidminer.belt.column;

import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;

import com.rapidminer.belt.column.Column.TypeId;


/**
 * Utility class providing print methods for columns. Returned string representations are suitable to be used
 * for command line output.
 *
 * @author Michael Knopf, Gisa Meier
 */
final class ColumnPrinter {

	/**
	 * String used to represent a missing value
	 */
	public static final String MISSING_VALUE_STRING = "?";

	/**
	 * Print up to {@value} rows when printing a single column (buffer).
	 */
	private static final int MAX_COLUMN_ROWS = 32;

	/**
	 * Placeholder for cells without a value.
	 */
	private static final String DOTS = "...";

	// Suppress default constructor for noninstantiability
	private ColumnPrinter() {
		throw new AssertionError();
	}


	/**
	 * Prints a single {@link NumericColumn} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param column
	 * 		the column to print
	 * @return the formatted string
	 */
	static String print(NumericColumn column) {
		int size = column.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", column.type().id() + " Column (" + size + ")\n(", ")");

		double[] buffer = new double[rowSubset ? MAX_COLUMN_ROWS - 2 : size];
		column.fill(buffer, 0);
		for (double value : buffer) {
			rowJoiner.add(format(value, column.type().id()));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			column.fill(buffer, size - 1);
			rowJoiner.add(format(buffer[0], column.type().id()));
		}

		return rowJoiner.toString();
	}


	/**
	 * Prints a single {@link CategoricalColumn} or {@link ObjectColumn} showing up to {@value MAX_COLUMN_ROWS} rows
	 * (horizontal layout).
	 *
	 * @param column
	 * 		the column with capability {@link Column.Capability#OBJECT_READABLE} to print
	 * @return the formatted string
	 */
	static String print(Column column) {
		int size = column.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", column.type().id() + " Column (" + size + ")\n(", ")");

		Object[] buffer = new Object[rowSubset ? MAX_COLUMN_ROWS - 2 : size];
		column.fill(buffer, 0);
		for (Object value : buffer) {
				rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			column.fill(buffer, size - 1);
			Object value = buffer[0];
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		return rowJoiner.toString();
	}


	private static String format(double value, TypeId type) {
		if (Double.isNaN(value)) {
			return MISSING_VALUE_STRING;
		}
		if (type == TypeId.INTEGER_53_BIT) {
			return String.format(Locale.ENGLISH, "%.0f", value);
		}
		return String.format(Locale.ENGLISH, "%.3f", value);
	}

}
