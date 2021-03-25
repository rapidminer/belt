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

package com.rapidminer.belt.buffer;

import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;

import com.rapidminer.belt.column.Column.TypeId;


/**
 * Utility class providing print methods for buffers. Returned string representations are suitable to be used
 * for command line output.
 *
 * @author Michael Knopf, Gisa Meier
 */
final class BufferPrinter {

	/**
	 * String used to represent a missing value
	 */
	private static final String MISSING_VALUE_STRING = "?";

	/**
	 * Print up to {@value} rows when printing a single column (buffer).
	 */
	private static final int MAX_COLUMN_ROWS = 32;

	/**
	 * Placeholder for cells without a value.
	 */
	private static final String DOTS = "...";

	// Suppress default constructor for noninstantiability
	private BufferPrinter() {
		throw new AssertionError();
	}

	/**
	 * Prints a single {@link NumericBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(NumericBuffer buffer) {
		int size = buffer.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", buffer.type() + " Buffer (" + size + ")\n(", ")");

		int max = rowSubset ? MAX_COLUMN_ROWS - 2 : size;
		for (int r = 0; r < max; r++) {
			rowJoiner.add(format(buffer.get(r), buffer.type()));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			rowJoiner.add(format(buffer.get(size - 1), buffer.type()));
		}

		return rowJoiner.toString();
	}

	/**
	 * Prints a single {@link NominalBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(NominalBuffer buffer) {
		int size = buffer.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", "Categorical Buffer (" + size + ")\n(", ")");

		int max = rowSubset ? MAX_COLUMN_ROWS - 2 : size;
		for (int r = 0; r < max; r++) {
			Object value = buffer.get(r);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			Object value = buffer.get(size - 1);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		return rowJoiner.toString();
	}

	/**
	 * Prints a single {@link ObjectBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(ObjectBuffer<?> buffer) {
		int size = buffer.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", "Object Buffer (" + size + ")\n(", ")");

		int max = rowSubset ? MAX_COLUMN_ROWS - 2 : size;
		for (int r = 0; r < max; r++) {
			Object value = buffer.get(r);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			Object value = buffer.get(size - 1);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		return rowJoiner.toString();
	}

	/**
	 * Prints a single {@link DateTimeBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(DateTimeBuffer buffer) {
		int size = buffer.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", "Date-Time Buffer (" + size + ")\n(", ")");

		int max = rowSubset ? MAX_COLUMN_ROWS - 2 : size;
		for (int r = 0; r < max; r++) {
			Object value = buffer.get(r);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			Object value = buffer.get(size - 1);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		return rowJoiner.toString();
	}

	/**
	 * Prints a single {@link TimeBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(TimeBuffer buffer) {
		int size = buffer.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", "Time Buffer (" + size + ")\n(", ")");

		int max = rowSubset ? MAX_COLUMN_ROWS - 2 : size;
		for (int r = 0; r < max; r++) {
			Object value = buffer.get(r);
			rowJoiner.add(Objects.toString(value, MISSING_VALUE_STRING));
		}

		if (rowSubset) {
			rowJoiner.add(DOTS);
			Object value = buffer.get(size - 1);
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
