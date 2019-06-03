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

package com.rapidminer.belt.table;

import java.util.List;

import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;


/**
 * Utility methods for writing {@link Table}s.
 *
 * @author Gisa Meier, Michael Knopf
 */
public final class Writers {

	private static final String MSG_NULL_COLUMN_LABELS = "column labels must not be null";
	private static final String MSG_COLUMN_LABELS_EMPTY = "column labels must not be empty";
	private static final String MSG_EXPECTED_ROWS_NEGATIVE = "expected rows must not be negative";
	private static final String MSG_COLUMN_TYPES_NULL = "column types must not be null";
	private static final String MSG_COLUMN_LABELS_AND_TYPES_LENGTH =
			"column labels and types must be of the same length";

	/**
	 * Writer for a new {@link Table} consisting of numeric columns with the given column labels and types and an
	 * unknown number of rows. If the number of rows can be estimated, use
	 * {@link #numericRowWriter(List, List, int, boolean)} instead to prevent unnecessary resizing of the data
	 * containers. If unset values should be missing, choose {@code true} for initialize, if there are no unset values
	 * choose {@code false} for performance reasons.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param types
	 * 		the types of the columns, either {@link ColumnTypes#INTEGER} or {@link ColumnTypes#REAL}
	 * @param initialize 
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@code false} values
	 * 		for	indices that are not explicitly set are undetermined
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label or type list is {@code null} or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or not the same size as types list or the types contain a type that is
	 * 		neither	{@link ColumnTypes#INTEGER} nor {@link ColumnTypes#REAL}
	 */
	public static NumericRowWriter numericRowWriter(List<String> columnLabels, List<ColumnType<Void>> types,
													boolean initialize) {
		if (columnLabels == null) {
			throw new NullPointerException(MSG_NULL_COLUMN_LABELS);
		}
		if (types == null) {
			throw new NullPointerException(MSG_COLUMN_TYPES_NULL);
		}
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_EMPTY);
		}
		if (columnLabels.size() != types.size()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_AND_TYPES_LENGTH);
		}
		return new NumericRowWriter(columnLabels, types, initialize);
	}

	/**
	 * Writer for a new {@link Table} consisting of real columns with the given column labels and an unknown number of
	 * rows. If the number of rows can be estimated, use {@link #realRowWriter(List, int, boolean)} instead to prevent
	 * unnecessary resizing of the data containers. If unset values should be missing, choose {@code true} for
	 * initialize, if there are no unset values choose {@code false} for performance reasons.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@code false} values
	 * 		for	indices that are not explicitly set are undetermined
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label list is {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty
	 */
	public static NumericRowWriter realRowWriter(List<String> columnLabels, boolean initialize) {
		if (columnLabels == null) {
			throw new NullPointerException(MSG_NULL_COLUMN_LABELS);
		}
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_EMPTY);
		}
		return new NumericRowWriter(columnLabels, initialize);
	}

	/**
	 * Writer for a new {@link Table} consisting of real columns with the given column labels and the given expected
	 * number of rows. If unset values should be missing, choose {@code true} for initialize, if there are no unset
	 * values choose {@code false} for performance reasons.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param expectedRows
	 * 		an estimate for the number of rows in the final table. A good estimation prevents unnecessary resizing of
	 * 		the	data containers.
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@code false} values
	 * 		for	indices that are not explicitly set are undetermined
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if columnLabels is {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or expected rows are negative
	 */
	public static NumericRowWriter realRowWriter(List<String> columnLabels, int expectedRows, boolean initialize) {
		if (columnLabels == null) {
			throw new NullPointerException(MSG_NULL_COLUMN_LABELS);
		}
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_EMPTY);
		}
		if (expectedRows < 0) {
			throw new IllegalArgumentException(MSG_EXPECTED_ROWS_NEGATIVE);
		}
		return new NumericRowWriter(columnLabels, expectedRows, initialize);
	}

	/**
	 * Writer for a new {@link Table} consisting of numeric columns with the given column labels and the given expected
	 * number of rows. If unset values should be missing, choose {@code true} for initialize, if there are no unset
	 * values choose {@code false} for performance reasons.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param types
	 * 		the types of the columns, either {@link ColumnTypes#INTEGER} or {@link ColumnTypes#REAL}
	 * @param expectedRows
	 * 		an estimate for the number of rows in the final table. A good estimation prevents unnecessary resizing of
	 * 		the	data containers.
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@code false} values
	 * 		for	indices that are not explicitly set are undetermined
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label or type list is {@code null} or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or not the same size as types list or expected rows are negative or the types
	 * 		contain a type that is neither {@link ColumnTypes#INTEGER} nor {@link ColumnTypes#REAL}
	 */
	public static NumericRowWriter numericRowWriter(List<String> columnLabels, List<ColumnType<Void>> types,
													int expectedRows, boolean initialize) {
		if (columnLabels == null) {
			throw new NullPointerException(MSG_NULL_COLUMN_LABELS);
		}
		if (types == null) {
			throw new NullPointerException(MSG_COLUMN_TYPES_NULL);
		}
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_EMPTY);
		}
		if (columnLabels.size() != types.size()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_AND_TYPES_LENGTH);
		}
		if (expectedRows < 0) {
			throw new IllegalArgumentException(MSG_EXPECTED_ROWS_NEGATIVE);
		}
		return new NumericRowWriter(columnLabels, types, expectedRows, initialize);
	}

	/**
	 * Writer for a new {@link Table} consisting of non-numeric columns with the given column labels and types and an
	 * unknown number of rows. If the number of rows can be estimated, use
	 * {@link #mixedRowWriter(List, List, int, boolean)}
	 * instead to prevent unnecessary resizing of the data containers. If all types are numeric use {@link
	 * #numericRowWriter(List, List, boolean)} instead. If unset values should be missing, choose {@code true} for
	 * initialize, if there are no unset values choose {@code false} for performance reasons.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param types
	 * 		the types of the columns
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@code false} values
	 * 		for	indices that are not explicitly set are undetermined
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label or type list is {@code null} or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or not the same size as types list
	 */
	public static MixedRowWriter mixedRowWriter(List<String> columnLabels, List<ColumnType<?>> types,
												boolean initialize) {
		if (columnLabels == null) {
			throw new NullPointerException(MSG_NULL_COLUMN_LABELS);
		}
		if (types == null) {
			throw new NullPointerException(MSG_COLUMN_TYPES_NULL);
		}
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_EMPTY);
		}
		if (columnLabels.size() != types.size()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_AND_TYPES_LENGTH);
		}
		return new MixedRowWriter(columnLabels, types, initialize);
	}

	/**
	 * Writer for a new {@link Table} with the given column labels and the given expected number of rows. If all types
	 * are numeric use {@link #numericRowWriter(List, List, int, boolean)} instead. If unset values should be missing,
	 * choose {@code true} for initialize, if there are no unset values choose {@code false} for performance reasons.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param types
	 * 		the types of the columns
	 * @param expectedRows
	 * 		an estimate for the number of rows in the final table. A good estimation prevents unnecessary resizing of
	 * 		the	data containers.
	 * @param initialize
	 * 		if this is {@code true} every value that is not explicitly set is missing, if this is {@code false} values
	 * 		for	indices that are not explicitly set are undetermined
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label or type list is {@code null} or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or not the same size as types list or expected rows are negative
	 */
	public static MixedRowWriter mixedRowWriter(List<String> columnLabels, List<ColumnType<?>> types,
												int expectedRows, boolean initialize) {
		if (columnLabels == null) {
			throw new NullPointerException(MSG_NULL_COLUMN_LABELS);
		}
		if (types == null) {
			throw new NullPointerException(MSG_COLUMN_TYPES_NULL);
		}
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_EMPTY);
		}
		if (columnLabels.size() != types.size()) {
			throw new IllegalArgumentException(MSG_COLUMN_LABELS_AND_TYPES_LENGTH);
		}
		if (expectedRows < 0) {
			throw new IllegalArgumentException(MSG_EXPECTED_ROWS_NEGATIVE);
		}
		return new MixedRowWriter(columnLabels, types, expectedRows, initialize);
	}

	private Writers() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

}
