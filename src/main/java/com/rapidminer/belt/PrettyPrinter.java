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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

import com.rapidminer.belt.Column.TypeId;

/**
 * Utility class providing print methods for common Belt types. Returned string representations are suitable to be used
 * for command line output.
 *
 * @author Michael Knopf, Gisa Meier
 */
final class PrettyPrinter {

	/**
	 * String used to represent a missing value
	 */
	public static final String MISSING_VALUE_STRING = "?";

	/**
	 * Print up to {@value} rows when printing a single column (buffer).
	 */
	private static final int MAX_COLUMN_ROWS = 32;

	/**
	 * Print up to {@value} rows when printing a data table.
	 */
	private static final int MAX_TABLE_ROWS = 12;

	/**
	 * Print up to {@value} columns when printing a data table.
	 */
	private static final int MAX_TABLE_COLUMNS = 8;

	/**
	 * Placeholder for cells without a value.
	 */
	private static final String DOTS = "...";

	/**
	 * Delimiter for columns (used for table printing).
	 */
	private static final String COLUMN_DELIMITER = " | ";

	// Suppress default constructor for noninstantiability
	private PrettyPrinter() {
		throw new AssertionError();
	}

	/**
	 * Prints a single {@link ColumnBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(ColumnBuffer buffer) {
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
	 * Prints a single {@link CategoricalColumnBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(AbstractCategoricalColumnBuffer<?> buffer) {
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
	 * Prints a single {@link FreeColumnBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(FreeColumnBuffer<?> buffer) {
		int size = buffer.size();
		boolean rowSubset = size > MAX_COLUMN_ROWS;

		StringJoiner rowJoiner = new StringJoiner(", ", "Free Buffer (" + size + ")\n(", ")");

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
	 * Prints a single {@link AbstractDateTimeBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(AbstractDateTimeBuffer buffer) {
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
	 * Prints a single {@link TimeColumnBuffer} showing up to {@value MAX_COLUMN_ROWS} rows (horizontal layout).
	 *
	 * @param buffer
	 * 		the buffer to print
	 * @return the formatted string
	 */
	static String print(TimeColumnBuffer buffer) {
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
	 * Prints a single {@link CategoricalColumn} or {@link FreeColumn} showing up to {@value MAX_COLUMN_ROWS} rows
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

	/**
	 * Prints the given column names showing up to {@value #MAX_TABLE_COLUMNS} columns.
	 *
	 * @param labels
	 * 		the labels to print
	 * @return the formatted string
	 */
	static String printLabels(String[] labels) {
		int width = labels.length;
		boolean columnSubset = width > MAX_TABLE_COLUMNS;
		int nColumns = columnSubset ? MAX_TABLE_COLUMNS - 1 : width;
		int[] columnIndices = new int[nColumns];
		Arrays.setAll(columnIndices, i -> i);
		if (columnSubset) {
			columnIndices[columnIndices.length - 1] = width - 1;
		}

		String[] selectedLabels = new String[nColumns];
		Arrays.setAll(selectedLabels, i -> labels[columnIndices[i]]);
		StringJoiner labelRow = new StringJoiner(COLUMN_DELIMITER);
		int i = 0;
		for (String label : selectedLabels) {
			labelRow.add(label);
			if (columnSubset && i == selectedLabels.length - 2) {
				labelRow.add(DOTS);
			}
			i++;
		}
		return labelRow.toString();
	}

	/**
	 * Prints a {@link Table} showing up {@value #MAX_TABLE_ROWS} rows and up to {@value #MAX_TABLE_COLUMNS} columns.
	 *
	 * @param table
	 * 		the table to print
	 * @return the formatted string
	 */
	static String print(Table table) {
		int width = table.width();
		int height = table.height();

		// Determine column set
		boolean columnSubset = width > MAX_TABLE_COLUMNS;
		int nColumns = columnSubset ? MAX_TABLE_COLUMNS - 1 : width;
		int[] columnIndices = new int[nColumns];
		Arrays.setAll(columnIndices, i -> i);
		if (columnSubset) {
			columnIndices[columnIndices.length - 1] = width - 1;
		}

		// Look-up labels
		String[] allLabels = table.labelArray();
		String[] labels = new String[nColumns];
		Arrays.setAll(labels, i -> allLabels[columnIndices[i]]);
		Column[] allColumns = table.getColumns();
		TypeId[] types = new TypeId[labels.length];
		Arrays.setAll(types, i -> allColumns[columnIndices[i]].type().id());
		Column[] columns = new Column[labels.length];
		Arrays.setAll(columns, i -> allColumns[columnIndices[i]]);

		// Determine row set
		boolean rowSubset = height > MAX_TABLE_ROWS;
		GeneralRowReader reader = new GeneralRowReader(columns);

		// Determine column formatting (constant width columns)
		int[] widths = getColumnWidths(labels, reader, rowSubset, columns);
		List<Function<GeneralRow, String>> formatters = new ArrayList<>(nColumns);
		int index = 0;
		for (int w : widths) {
			String formatString = "%" + w + "s";
			TypeId type = types[index];
			Column column = columns[index];
			int finalIndex = index;
			if (column.type().category() == Column.Category.NUMERIC) {
				formatters
						.add(v -> String.format(Locale.ENGLISH, formatString, format(v.getNumeric(finalIndex), type)));
			} else {
				formatters.add(v -> String.format(Locale.ENGLISH, formatString, format(v.getObject(finalIndex))));
			}
			index++;
		}

		StringJoiner rowJoiner = new StringJoiner("\n", "Table (" + width + "x" + height + ")\n", "");
		StringJoiner labelRow = new StringJoiner(COLUMN_DELIMITER);
		for (int c = 0; c < formatters.size(); c++) {
			String format = "%-" + widths[c] + "s";
			labelRow.add(String.format(format, labels[c]));
			if (columnSubset && c == formatters.size() - 2) {
				labelRow.add(DOTS);
			}
		}
		rowJoiner.add(labelRow.toString());

		StringJoiner typeRow = new StringJoiner(COLUMN_DELIMITER);
		for (int c = 0; c < formatters.size(); c++) {
			String format = "%-" + widths[c] + "s";
			typeRow.add(String.format(format, types[c].toString()));
			if (columnSubset && c == formatters.size() - 2) {
				typeRow.add(DOTS);
			}
		}
		rowJoiner.add(typeRow.toString());

		int position = 0;
		reader.setPosition(position - 1);
		while (reader.hasRemaining()) {
			reader.move();
			rowJoiner.add(rowToString(formatters, columnSubset, reader));
			if (rowSubset && position == MAX_TABLE_ROWS - 3) {
				rowJoiner.add(rowOfDots(widths, columnSubset));
				position = height - 1;
				reader.setPosition(position - 1);
			} else {
				position++;
			}
		}

		return rowJoiner.toString();
	}

	private static String format(double value, TypeId type) {
		if (Double.isNaN(value)) {
			return MISSING_VALUE_STRING;
		}
		if (type == TypeId.INTEGER) {
			return String.format(Locale.ENGLISH, "%.0f", value);
		}
		return String.format(Locale.ENGLISH, "%.3f", value);
	}

	private static String format(Object object) {
		return Objects.toString(object, MISSING_VALUE_STRING);
	}

	private static int[] getColumnWidths(String[] labels, GeneralRowReader reader, boolean rowSubset,
										 Column[] columns) {
		int[] columnWidths = new int[labels.length];
		Arrays.fill(columnWidths, 5);
		for (int i = 0 ; i < labels.length; i++) {
			columnWidths[i] = Math.max(columnWidths[i], labels[i].length());
		}
		for (int i = 0 ; i < labels.length; i++) {
			columnWidths[i] = Math.max(columnWidths[i], columns[i].type().id().toString().length());
		}
		int position = 0;
		while (reader.hasRemaining()) {
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				columnWidths[i] = Math.max(columnWidths[i], columns[i].type().category() == Column.Category.NUMERIC ?
						format(reader.getNumeric(i), columns[i].type().id()).length() :
						format(reader.getObject(i)).length());
			}
			position++;
			if (position == MAX_TABLE_ROWS - 2 && rowSubset) {
				reader.setPosition(reader.position() + reader.remaining() - 1);
			}
		}
		return columnWidths;
	}

	private static String rowToString(List<Function<GeneralRow, String>> formatters, boolean columnSubset,
									  GeneralRowReader reader) {
		StringJoiner columnJoiner = new StringJoiner(COLUMN_DELIMITER);
		for (int c = 0; c < formatters.size(); c++) {
			columnJoiner.add(formatters.get(c).apply(reader));
			if (columnSubset && c == formatters.size() - 2) {
				columnJoiner.add(DOTS);
			}
		}
		return columnJoiner.toString();
	}

	private static String rowOfDots(int[] widths, boolean columnSubset) {
		StringJoiner columnJoiner = new StringJoiner(COLUMN_DELIMITER);
		for (int i = 0; i < widths.length; i++) {
			String formatString = "%" + widths[i] + "s";
			columnJoiner.add(String.format(formatString, DOTS));
			if (columnSubset && i == widths.length - 2) {
				columnJoiner.add(DOTS);
			}
		}
		return columnJoiner.toString();
	}

}
