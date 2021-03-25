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
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.reader.MixedRow;
import com.rapidminer.belt.reader.MixedRowReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Utility class providing print methods for tables. Returned string representations are suitable to be used
 * for command line output.
 *
 * @author Michael Knopf, Gisa Meier
 */
final class TablePrinter {

	/**
	 * String used to represent a missing value
	 */
	public static final String MISSING_VALUE_STRING = "?";

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
	private TablePrinter() {
		throw new AssertionError();
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
		for (int i = 0; i < columnIndices.length; i++) {
			columnIndices[i] = i;
		}
		if (columnSubset) {
			columnIndices[columnIndices.length - 1] = width - 1;
		}

		String[] selectedLabels = new String[nColumns];
		for (int i = 0; i < selectedLabels.length; i++) {
			selectedLabels[i] = labels[columnIndices[i]];
		}
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
		for (int i = 0; i < columnIndices.length; i++) {
			columnIndices[i] = i;
		}
		if (columnSubset) {
			columnIndices[columnIndices.length - 1] = width - 1;
		}

		// Look-up labels
		String[] allLabels = table.labelArray();
		String[] labels = new String[nColumns];
		for (int i = 0; i < labels.length; i++) {
			labels[i] = allLabels[columnIndices[i]];
		}
		List<Column> allColumns = table.columnList();
		TypeId[] types = new TypeId[labels.length];
		for (int i = 0; i < types.length; i++) {
			types[i] = allColumns.get(columnIndices[i]).type().id();
		}
		Column[] columns = new Column[labels.length];
		for (int i = 0; i < columns.length; i++) {
			columns[i] = allColumns.get(columnIndices[i]);
		}

		// Determine row set
		boolean rowSubset = height > MAX_TABLE_ROWS;
		MixedRowReader reader = Readers.mixedRowReader(Arrays.asList(columns));

		// Determine column formatting (constant width columns)
		int[] widths = getColumnWidths(labels, reader, rowSubset, columns);
		List<Function<MixedRow, String>> formatters = new ArrayList<>(nColumns);
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
		if (type == TypeId.INTEGER_53_BIT) {
			return String.format(Locale.ENGLISH, "%.0f", value);
		}
		return String.format(Locale.ENGLISH, "%.3f", value);
	}

	private static String format(Object object) {
		return Objects.toString(object, MISSING_VALUE_STRING);
	}

	private static int[] getColumnWidths(String[] labels, MixedRowReader reader, boolean rowSubset,
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

	private static String rowToString(List<Function<MixedRow, String>> formatters, boolean columnSubset,
									  MixedRowReader reader) {
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
