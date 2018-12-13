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

/**
 * Utility functions for columns.
 *
 * @author Gisa Meier
 */
public final class Columns {

	/**
	 * Two non-null values dictionary size.
	 */
	private static final int TWO_VALUES_DICTIONARY_SIZE = 3;

	/**
	 * The index of the first non-null category.
	 */
	private static final int FIRST_CATEGORY_INDEX = CategoricalReader.MISSING_CATEGORY + 1;

	/**
	 * The index of the second non-null category.
	 */
	private static final int SECOND_CATEGORY_INDEX = FIRST_CATEGORY_INDEX + 1;

	/**
	 * Checks if the column is categorical with exactly two non-null values.
	 *
	 * @param column
	 * 		the column to check
	 * @return {@code true} if the column is categorical with exactly two values different from {@code null}
	 */
	public static boolean isBicategorical(Column column) {
		return column.type().category() == Column.Category.CATEGORICAL
				&& column.getDictionary(Object.class).size() == TWO_VALUES_DICTIONARY_SIZE;
	}

	/**
	 * Checks if the column is categorical with at most two non-null values.
	 *
	 * @param column
	 * 		the column to check
	 * @return {@code true} if the column is categorical with at most two values different from {@code null}
	 */
	public static boolean isAtMostBicategorical(Column column) {
		return column.type().category() == Column.Category.CATEGORICAL
				&& column.getDictionary(Object.class).size() <= TWO_VALUES_DICTIONARY_SIZE;
	}

	/**
	 * Checks if the column is categorical and boolean with exactly two non-null values, one positive and one negative.
	 *
	 * @param column
	 * 		the column to check
	 * @return {@code true} if the column is categorical has exactly two values with different boolean values associated
	 */
	public static boolean isBicategoricalAndBiboolean(Column column) {
		return isBicategorical(column) && column.hasCapability(Column.Capability.BOOLEAN)
				&& column.toBoolean(FIRST_CATEGORY_INDEX) != column.toBoolean(SECOND_CATEGORY_INDEX);
	}

	private Columns() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}
}
