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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.Workload;
import com.rapidminer.belt.transform.Transformer;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * Utility functions for columns.
 *
 * @author Gisa Meier
 */
public final class Columns {

	static {
		InternalColumns internalColumns = new InternalColumnsImpl();
		com.rapidminer.belt.buffer.ColumnAccessor.set(internalColumns);
		com.rapidminer.belt.table.ColumnAccessor.set(internalColumns);
		com.rapidminer.belt.column.io.ColumnAccessor.set(internalColumns);
	}

	/**
	 * Message in case a column is categorical but not a {@link CategoricalColumn}.
	 */
	private static final String MESSAGE_CATEGORICAL_IMPLEMENTATION = "Unknown categorical column implementation";

	/**
	 * Message in case of null column parameter.
	 */
	private static final String MESSAGE_NULL_COLUMN = "Column must not be null";

	/**
	 * Two non-null values dictionary size.
	 */
	private static final int TWO_VALUES_DICTIONARY_SIZE = 2;

	/**
	 * Checks if the column is categorical with exactly two non-null values.
	 *
	 * @param column
	 * 		the column to check
	 * @return {@code true} if the column is categorical with exactly two values different from {@code null}
	 */
	public static boolean isBicategorical(Column column) {
		return column.type().category() == Column.Category.CATEGORICAL
				&& column.getDictionary().size() == TWO_VALUES_DICTIONARY_SIZE;
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
				&& column.getDictionary().size() <= TWO_VALUES_DICTIONARY_SIZE;
	}

	/**
	 * Checks if the column is categorical and boolean with exactly two non-null values, one positive and one negative.
	 *
	 * @param column
	 * 		the column to check
	 * @return {@code true} if the column is categorical has exactly two values with different boolean values associated
	 */
	public static boolean isBicategoricalAndBoolean(Column column) {
		return isBicategorical(column) && column.getDictionary().isBoolean();
	}

	/**
	 * Sets the positive value for the categorical column if the dictionary has at most two values (including the
	 * positive value). If the positive value is not part of the dictionary yet, it is added to the dictionary. The
	 * other value, if it exists, will be made negative. In case of a dictionary with only one value the positive value
	 * can be {@code null}, making the only value negative.
	 *
	 * @param column
	 * 		the column to adjust
	 * @param positiveValue
	 * 		the positive value to use, can be {@code null} in case of a dictionary of size 1
	 * @return a new categorical column with a boolean dictionary with the given positive value
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical or its dictionary has more than two values (including the given positive
	 * 		value)
	 */
	public static Column toBoolean(Column column, String positiveValue) {
		if (column.type().category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column is not categorical.");
		}
		if (column.getDictionary().size() > TWO_VALUES_DICTIONARY_SIZE) {
			throw new IllegalArgumentException("Dictionary has more than 2 values, cannot be made boolean.");
		}
		return setPositiveValue(column, positiveValue);
	}

	/**
	 * Sets the given positive value if it is compatible with the column type. If the positive value is not part of the
	 * dictionary yet, it is added to the dictionary. Also calls {@link #compactDictionary(Column)} on the column to
	 * make sure that there are no gaps in the dictionary (required for boolean dictionaries).
	 */
	private static Column setPositiveValue(Column column, String positiveValue) {
		if (column instanceof CategoricalColumn) {
			CategoricalColumn categoricalColumn = (CategoricalColumn) compactDictionary(column);
			return categoricalColumn.toBoolean(positiveValue);
		} else {
			throw new AssertionError(MESSAGE_CATEGORICAL_IMPLEMENTATION);
		}
	}

	/**
	 * Creates a column that replaces the from-column's dictionary with the one of the to-column. The categorical
	 * indices are updated to match the replacement dictionary (the dictionary itself remains unchanged). Values not
	 * contained in the replacement dictionary are declared missing.
	 *
	 * If the to-column's dictionary is boolean, this is not changed. Therefore, it might happen that the positive
	 * value does not appear in the data.
	 *
	 * @param from
	 * 		the column for which to change the dictionary
	 * @param to
	 * 		the column with the dictionary to change to
	 * @return a new column with the dictionary of the to-column and the data of the from-column
	 * @throws IllegalArgumentException
	 * 		if the columns have different element types or are not both categorical
	 * @throws NullPointerException
	 * 		if any parameter is {@code null}
	 */
	public static Column changeDictionary(Column from, Column to) {
		Objects.requireNonNull(from, "From column must not be null");
		Objects.requireNonNull(to, "To column must not be null");
		if (!from.type().elementType().equals(to.type().elementType())) {
			throw new IllegalArgumentException("The element types of the to and from column are different.");
		}
		return remap(from, to, false);
	}

	/**
	 * Remaps the from-column with type from-type to the to-column which has the same type.
	 */
	private static Column remap(Column from, Column to, boolean merge) {
		if (from.type().category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("The from column is not categorical.");
		}
		if (to.type().category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("The to column is not categorical.");
		}

		CategoricalColumn typedFrom = (CategoricalColumn) from;
		CategoricalColumn typedTo = (CategoricalColumn) to;
		return merge ? typedFrom.mergeDictionaries(typedTo.getDictionary()) : typedFrom.remap(typedTo.getDictionary());
	}

	/**
	 * Merges the dictionary of the from-column with the one of the to-column. A new column is created which has the
	 * data of the from-column and a dictionary that starts with the dictionary of the to-column. If a value of the
	 * from-column is not part of the dictionary of the to-column it is added to end of the new dictionary.
	 *
	 * If the to-column's dictionary is boolean, has a positive and the merged dictionary size is not bigger than 2,
	 * then the merged dictionary will be boolean with the same positive index.
	 *
	 * @param from
	 * 		the column for which to change the dictionary
	 * @param to
	 * 		the column with the dictionary to start with
	 * @return a new column with a dictionary starting with the dictionary of the to-column and the data of the
	 * from-column
	 * @throws IllegalArgumentException
	 * 		if the columns have different element types or are not both categorical
	 * @throws NullPointerException
	 * 		if any parameter is {@code null}
	 */
	public static Column mergeDictionary(Column from, Column to) {
		Objects.requireNonNull(from, "From column must not be null");
		Objects.requireNonNull(to, "To column must not be null");
		if (!from.type().elementType().equals(to.type().elementType())) {
			throw new IllegalArgumentException("The element types of the to and from column are different.");
		}
		return remap(from, to, true);
	}

	/**
	 * The possible cleanup options for {@link #removeUnusedDictionaryValues(Column, CleanupOption, Context)}
	 */
	public enum CleanupOption {

		/**
		 * Removes unused values from the dictionary so that only used values are part of
		 * {@link Dictionary#iterator()}.
		 * There can be gaps in the used category indices, i.e. {@link Dictionary#get(int)} can return {@code null} for
		 * an input between 1 and {@link Dictionary#maximalIndex()} and not all category indices between 1 and {@link
		 * Dictionary#maximalIndex()} appear in the {@link Dictionary.Entry}s from the {@link Dictionary#iterator()}.
		 * The data of the categorical column remains unchanged.
		 */
		REMOVE,

		/**
		 * Creates a new dictionary containing only the used dictionary values and remaps the data. All category
		 * indices between 1 and {@link Dictionary#maximalIndex()} will be part of the data. The method
		 * {@link Dictionary#get(int)} never returns {@code null} for an input between 1 and
		 * {@link Dictionary#maximalIndex()}. The data is remapped, i.e., a view is added that transforms from original
		 * category index to new category index whenever the data is read.
		 */
		COMPACT;
	}

	/**
	 * Removes unused dictionary values from categorical column following the cleanup option. The input column is
	 * returned if the input column is not categorical or every dictionary value is used.
	 *
	 * @param column
	 * 		the column to cleanup
	 * @param option
	 * 		the {@link CleanupOption}
	 * @param context
	 * 		the context to use
	 * @return the cleaned up column or the input column if no cleanup possible
	 * @throws NullPointerException
	 * 		if one of the inputs is {@code null}
	 */
	public static Column removeUnusedDictionaryValues(Column column, CleanupOption option, Context context) {
		Objects.requireNonNull(column, MESSAGE_NULL_COLUMN);
		if (column.type().category() != Column.Category.CATEGORICAL) {
			return column;
		}
		Objects.requireNonNull(context, "Context must not be null");
		if (column instanceof CategoricalColumn) {
			if (option == CleanupOption.COMPACT) {
				return remapToUsed(column, context);
			} else {
				return replaceUnused(column,  context);
			}
		} else {
			throw new AssertionError(MESSAGE_CATEGORICAL_IMPLEMENTATION);
		}
	}

	/**
	 * Replaces unused dictionary values by {@code null}.
	 */
	private static Column replaceUnused(Column column, Context context) {
		Dictionary oldDictionary = column.getDictionary();
		boolean[] used = findUsed(column, context, oldDictionary);
		Dictionary newDict = replaceByNull(oldDictionary, used);

		if (newDict == null) {
			return column;
		} else {
			//cast is safe since the column is categorical
			@SuppressWarnings("unchecked")
			CategoricalColumn categoricalColumn = (CategoricalColumn) column;
			return categoricalColumn.swapDictionary(newDict);
		}
	}

	/**
	 * Creates a boolean array containing whether an index appears in the data.
	 */
	private static boolean[] findUsed(Column column, Context context, Dictionary oldDictionary) {
		int maxIndex = oldDictionary.maximalIndex();
		return new Transformer(column).workload(Workload.SMALL).reduceCategorical(() -> new boolean[maxIndex + 1],
				(b, i) -> b[i] = true, (b1, b2) -> {
					for (int i = 0; i <= maxIndex; i++) {
						b1[i] |= b2[i];
					}
				}, context);
	}

	/**
	 * Creates a new dictionary with {@code null} values in case they are unused. Returns {@code null} otherwise.
	 */
	private static Dictionary replaceByNull(Dictionary oldDictionary, boolean[] used) {
		List<String> values = oldDictionary.getValueList();

		List<String> newValues = null;
		int lastUsed = -1;
		int unused = 0;

		// go backwards through the values list to find the last used value and copy the list until then if an unused
		// value is found
		for (int i = values.size() - 1; i > 0; i--) {
			if (used[i]) {
				if (lastUsed < 0) {
					//find the biggest value that is used
					lastUsed = i;
				}
			} else {
				if (newValues == null && lastUsed > 0) {
					//create list until the last used
					newValues = new ArrayList<>(values.subList(0, lastUsed + 1));
				}
				if (newValues != null) {
					//set unused entries to null
					newValues.set(i, null);
					unused++;
				}
			}
		}

		if (newValues == null && lastUsed < values.size() - 1) {
			//we are in the case (used,...,used,unused,...,unused) or (unused,...,unused)
			newValues = new ArrayList<>(values.subList(0, Math.max(1, lastUsed + 1)));
		}

		return createDictionary(oldDictionary, used, newValues, unused);
	}

	/**
	 * Creates a new dictionary. If it is boolean, the positive index is only kept if it is used.
	 */
	private static Dictionary createDictionary(Dictionary oldDictionary, boolean[] used, List<String> newValues,
													  int unused) {
		if (newValues != null) {
			if (oldDictionary.isBoolean()) {
				return new BooleanDictionary(newValues,
						(oldDictionary.getPositiveIndex() > 0 && used[oldDictionary.getPositiveIndex()]) ?
								oldDictionary.getPositiveIndex() : BooleanDictionary.NO_ENTRY);
			} else {
				return new Dictionary(newValues, unused);
			}
		} else {
			return null;
		}
	}

	/**
	 * Creates a dictionary of used values and remaps the column to match it.
	 */
	private static Column remapToUsed(Column column, Context context) {
		Dictionary oldDictionary = column.getDictionary();
		boolean[] used = findUsed(column, context, oldDictionary);

		int maxIndex = oldDictionary.maximalIndex();
		int[] remapping = new int[maxIndex + 1];
		int index = 1;
		for (int i = 1; i < used.length; i++) {
			if (used[i]) {
				remapping[i] = index++;
			}
		}
		if (index == maxIndex + 1) {
			//every index is used
			return column;
		}
		List<String> oldValues = oldDictionary.getValueList();
		List<String> newValues = new ArrayList<>(index);
		newValues.add(null);
		for (int i = 1; i < used.length; i++) {
			if (used[i]) {
				newValues.add(oldValues.get(i));
			}
		}

		Dictionary newDictionary;
		if (oldDictionary.isBoolean()) {
			int pos = oldDictionary.getPositiveIndex();
			if (pos > 0) {
				if (!used[pos]) {
					pos = BooleanDictionary.NO_ENTRY;
				} else {
					pos = remapping[pos];
				}
			}
			newDictionary = new BooleanDictionary(newValues, pos);
		} else {
			newDictionary = new Dictionary(newValues);
		}

		CategoricalColumn categoricalColumn = (CategoricalColumn) column;
		return categoricalColumn.remap(newDictionary, remapping);
	}


	/**
	 * Creates a new column with a dictionary that contains the same object values as the input but with continuous
	 * category indices. For the dictionary of the resulting column, the method {@link Dictionary#get(int)} never
	 * returns {@code null} for an input between 1 and {@link Dictionary#maximalIndex()}.
	 *
	 * @param column
	 * 		the column with the dictionary to compact
	 * @return a new column with a compacted dictionary
	 * @throws NullPointerException
	 * 		if the column is {@code null}
	 */
	public static Column compactDictionary(Column column) {
		Objects.requireNonNull(column, MESSAGE_NULL_COLUMN);
		if (column.type().category() != Column.Category.CATEGORICAL) {
			return column;
		}
		Dictionary dictionary = column.getDictionary();
		if (dictionary.size() == dictionary.maximalIndex()) {
			return column;
		}

		if (column instanceof CategoricalColumn) {
			return columnWithoutGaps(column);
		} else {
			throw new AssertionError(MESSAGE_CATEGORICAL_IMPLEMENTATION);
		}
	}

	/**
	 * Removes the gaps from the dictionary by a remapping.
	 */
	private static Column columnWithoutGaps(Column column) {
		Dictionary mapping = column.getDictionary();
		List<String> oldValues = mapping.getValueList();
		int[] remapping = new int[mapping.maximalIndex() + 1];
		List<String> newValues = new ArrayList<>(mapping.size() + 1);
		newValues.add(null);

		int newIndex = 1;
		for (int i = 1; i <= mapping.maximalIndex(); i++) {
			String value = oldValues.get(i);
			if (value != null) {
				remapping[i] = newIndex++;
				newValues.add(value);
			}
		}

		Dictionary dictionary;
		if (mapping.isBoolean()) {
			int newPositive = mapping.getPositiveIndex();
			if (newPositive > 0) {
				newPositive = remapping[newPositive];
			}
			dictionary = new BooleanDictionary(newValues, newPositive);
		} else {
			dictionary = new Dictionary(newValues);
		}

		CategoricalColumn nominalColumn = (CategoricalColumn) column;
		return nominalColumn.remap(dictionary, remapping);
	}


	/**
	 * Creates a new column with the oldValue replaced by the newValue in the dictionary. In case this is not possible
	 * because the new value is already part of the dictionary, a {@link IllegalReplacementException} is thrown. In
	 * that case, the column data needs to be changed, either via a buffer ({@link Buffers#nominalBuffer(Column)}) or
	 * an apply function ({@link Transformer#applyObjectToNominal(Class, Function, Context)}. If the old value is not
	 * part of the dictionary, nothing is changed.
	 * <p>
	 * In case multiple values need to be replaced, use {@link #replaceInDictionary(Column, Map)} instead.
	 *
	 * @param column
	 * 		the categorical column in which to replace dictionary values
	 * @param oldValue
	 * 		the value to replace
	 * @param newValue
	 * 		the replacement value
	 * @return a new column with the same category indices but changed dictionary or the column if the old value is not
	 * present in the dictionary
	 * @throws NullPointerException
	 * 		if one of the parameters is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical or the old or new value not compatible with the type of the column
	 * @throws IllegalReplacementException
	 * 		if a replacement in the dictionary is not possible because the dictionary already contains the new value
	 */
	public static Column replaceSingleInDictionary(Column column, String oldValue, String newValue) {
		Objects.requireNonNull(column, MESSAGE_NULL_COLUMN);
		if (oldValue == null || newValue == null) {
			throw new NullPointerException("Replacement values must not be null");
		}
		if (column.type().category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column must be categorical");
		}
		if (!column.type().elementType().isInstance(oldValue)) {
			throw new IllegalArgumentException("Old value of different type than dictionary entries");
		}
		return tryReplace(column, oldValue, newValue);
	}

	/**
	 * Creates a new column with the values in the dictionary replaced as specified by the map. In case this is not
	 * possible because a new value is already part of the dictionary, a {@link IllegalReplacementException} is thrown.
	 * In that case, the column data needs to be changed, either via a buffer ({@link Buffers#nominalBuffer(Column)})
	 * or an apply function ({@link Transformer#applyObjectToNominal(Class, Function, Context)}. If the old value is
	 * not part of the dictionary, the mapping is ignored.
	 * <p>
	 * In case only one value should be replaced use {@link #replaceSingleInDictionary(Column, String, String)} instead.
	 *
	 * @param column
	 * 		the categorical column in which to replace dictionary values
	 * @param oldToNewValue
	 * 		a map from old to new value, the map order does not matter
	 * @return a new column with the same category indices but the dictionary changed according to the map
	 * @throws NullPointerException
	 * 		if one of the parameters is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column is not categorical or the given type is not compatible with the element type of the column
	 * @throws IllegalReplacementException
	 * 		if a replacement in the dictionary is not possible because the dictionary already contains the new value
	 */
	public static Column replaceInDictionary(Column column, Map<String, String> oldToNewValue) {
		Objects.requireNonNull(column, MESSAGE_NULL_COLUMN);
		Objects.requireNonNull(oldToNewValue, "Map must not be null");
		if (!column.type().elementType().equals(String.class)) {
			throw new IllegalArgumentException("Type '" + String.class + "' of the replacement values not compatible with the " +
					"column element type '" + column.type().elementType() + "'");
		}
		if (column.type().category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Column must be categorical");
		}
		if (column instanceof CategoricalColumn) {
			CategoricalColumn categoricalColumn = (CategoricalColumn) column;
			Dictionary dictionary = categoricalColumn.getDictionary();
			Map<String, Integer> toReplace = createReplacementMap(oldToNewValue, dictionary);
			List<String> newDictionaryValues = new ArrayList<>(dictionary.getValueList());
			for (Map.Entry<String, Integer> entry : toReplace.entrySet()) {
				newDictionaryValues.set(entry.getValue(), entry.getKey());
			}
			return replaceDictionaryWithValues(categoricalColumn, dictionary, newDictionaryValues);
		} else {
			throw new AssertionError(MESSAGE_CATEGORICAL_IMPLEMENTATION);
		}
	}

	/**
	 * Exception thrown when a replacement in a dictionary is not possible since otherwise the same value would be at
	 * two indices. In case of this exception, a replacement purely on dictionary level is not possible. The category
	 * index data needs to be changed via a buffer (e.g. {@link Buffers#nominalBuffer(Column)}) or an apply
	 * function (e.g. {@link Transformer#applyObjectToNominal(Class, Function, Context)}.
	 */
	public static final class IllegalReplacementException extends IllegalArgumentException {

		private static final long serialVersionUID = 5521597910473990747L;

		private IllegalReplacementException() {
			super("Replacement value already present in dictionary");
		}

	}

	/**
	 * Creates a new dictionary with the old value replaced by the new value and creates a new column with this new
	 * dictionary and the same data.
	 */
	private static Column tryReplace(Column column, String oldValue, String newValue) {
		if (column instanceof CategoricalColumn) {
			CategoricalColumn categoricalColumn = (CategoricalColumn) column;

			Dictionary dictionary = categoricalColumn.getDictionary();
			List<String> dictionaryValues = dictionary.getValueList();
			int indexOfOld = -1;
			int i = 0;
			for (String value : dictionaryValues) {
				if (oldValue.equals(value)) {
					indexOfOld = i;
				}
				i++;
				if (newValue.equals(value)) {
					throw new IllegalReplacementException();
				}
			}

			if (indexOfOld < 0) {
				return column;
			}
			List<String> newDictionaryValues = new ArrayList<>(dictionaryValues);
			newDictionaryValues.set(indexOfOld, newValue);
			return replaceDictionaryWithValues(categoricalColumn, dictionary, newDictionaryValues);
		} else {
			throw new AssertionError(MESSAGE_CATEGORICAL_IMPLEMENTATION);
		}
	}

	/**
	 * Swaps the dictionary of the column with a new dictionary created from the given values and keeps the positive
	 * index if the dictionary is boolean.
	 */
	private static Column replaceDictionaryWithValues(CategoricalColumn categoricalColumn,
														  Dictionary dictionary, List<String> newDictionaryValues) {
		Dictionary newDictionary;
		if (dictionary.isBoolean()) {
			newDictionary = new BooleanDictionary(newDictionaryValues, dictionary.getPositiveIndex());
		} else {
			newDictionary = new Dictionary(newDictionaryValues);
		}
		return categoricalColumn.swapDictionary(newDictionary);
	}



	/**
	 * Creates a map from target values of the oldToNewValue map to the indices the source values have in the
	 * dictionary. Checks that when applying the replacement map to the dictionary, every value appears at only one
	 * index.
	 */
	private static Map<String, Integer> createReplacementMap(Map<String, String> oldToNewValue, Dictionary dictionary) {
		Map<String, Integer> inverse = dictionary.createInverse();
		// Set used to check that no new value is the same as one of the remaining old values
		Set<String> uniquenessCheck = new HashSet<>(inverse.keySet());
		uniquenessCheck.removeAll(oldToNewValue.keySet());

		Map<String, Integer> toReplace = new HashMap<>();
		for (Map.Entry<String, String> entry : oldToNewValue.entrySet()) {
			String oldValue = entry.getKey();
			String newValue = entry.getValue();
			if (oldValue == null || newValue == null) {
				throw new NullPointerException("Replacement values must not be null");
			}
			Integer index = inverse.get(oldValue);
			if (index != null) {
				if (uniquenessCheck.contains(newValue)) {
					throw new IllegalReplacementException();
				}
				if (toReplace.containsKey(newValue)) {
					throw new IllegalReplacementException();
				} else {
					toReplace.put(newValue, index);
				}
			}
		}
		return toReplace;
	}

	private Columns() {
		// Suppress default constructor to prevent instantiation
		throw new AssertionError();
	}

	/**
	 * Column methods for internal access.
	 */
	public abstract static class InternalColumns {

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract  CategoricalColumn newCategoricalColumn(ColumnType<String> type, int[] data,
																	  List<String> dictionary);

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract  CategoricalColumn newCategoricalColumn(ColumnType<String> type, short[] data,
																	  List<String> dictionary);

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract  CategoricalColumn newCategoricalColumn(ColumnType<String> type,
																	  IntegerFormats.PackedIntegers bytes,
																	  List<String> dictionary);

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract  CategoricalColumn newCategoricalColumn(ColumnType<String> type, int[] data,
																	  List<String> dictionary, int positiveIndex);

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract  CategoricalColumn newCategoricalColumn(ColumnType<String> type, short[] data,
																	  List<String> dictionary, int positiveIndex);

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract  CategoricalColumn newCategoricalColumn(ColumnType<String> type,
																	  IntegerFormats.PackedIntegers bytes,
																	  List<String> dictionary, int positiveIndex);

		/**
		 * Creates a new sparse categorical column from the given data.
		 */
		public abstract  CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
																			int[] nonDefaultValues, List<String> dictionary,
																			int defaultValue, int size);

		/**
		 * Creates a new sparse categorical column from the given data.
		 */
		public abstract  CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
																			short[] nonDefaultValues, List<String> dictionary,
																			short defaultValue, int size);

		/**
		 * Creates a new sparse categorical column from the given data.
		 */
		public abstract  CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
																			byte[] nonDefaultValues, List<String> dictionary,
																			byte defaultValue, int size);

		/**
		 * Creates a new sparse categorical column from the given data.
		 */
		public abstract  CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
																			int[] nonDefaultValues, List<String> dictionary,
																			int defaultValue, int size, int positiveIndex);

		/**
		 * Creates a new sparse categorical column from the given data.
		 */
		public abstract  CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
																			short[] nonDefaultValues, List<String> dictionary,
																			short defaultValue, int size, int positiveIndex);

		/**
		 * Creates a new sparse categorical column from the given data.
		 */
		public abstract  CategoricalColumn newSparseCategoricalColumn(ColumnType<String> type, int[] nonDefaultIndices,
																			byte[] nonDefaultValues, List<String> dictionary,
																			byte defaultValue, int size, int positiveIndex);

		/**
		 * Creates a new time column from the given data.
		 */
		public abstract TimeColumn newTimeColumn(long[] data);

		/**
		 * Creates a new sparse numeric column from the given sparse data.
		 */
		public abstract Column newSparseNumericColumn(Column.TypeId type, double defaultValue, int[] nonDefaultIndices,
													  double[] nonDefaultValues, int size);

		/**
		 * Creates a new categorical column from the given data.
		 */
		public abstract <T> Column newObjectColumn(ColumnType<T> type, Object[] data);

		/**
		 * Creates a new numeric column from the given data.
		 */
		public abstract Column newNumericColumn(Column.TypeId type, double[] src);

		/**
		 * Creates a new sparse numeric column from the given sparse data.
		 */
		public abstract Column newSparseTimeColumn(long defaultValue, int[] nonDefaultIndices, long[] nonDefaultValues,
												   int size);

		/**
		 * Creates a new date-time column from the given data. The second parameter can be {@code null} in case of no
		 * subsecond precision.
		 */
		public abstract DateTimeColumn newDateTimeColumn(long[] seconds, int[] nanos);

		/**
		 * Creates a new sparse date-time column from the given data. The second parameter can be {@code null} in case
		 * of no subsecond precision.
		 */
		public abstract DateTimeColumn newSparseDateTimeColumn(long defaultValue, int[] nonDefaultIndices,
															   long[] nonDefaultSeconds, int[] nanos, int size);

		/**
		 * Creates a new categorical column of the given size that is constantly one value, represented as sparse column.
		 */
		public abstract  CategoricalColumn newSingleValueCategoricalColumn(ColumnType<String> type, String value, int size);

		/**
		 * Returns a copy of the byte data of a categorical column.
		 */
		public abstract byte[] getByteDataCopy(CategoricalColumn column);

		/**
		 * Returns a copy of the short data of a categorical column.
		 */
		public abstract short[] getShortDataCopy(CategoricalColumn column);

		/**
		 * Returns the internal list of dictionary values.
		 */
		public abstract  List<String> getDictionaryList(Dictionary dictionary);

		/**
		 * Maps a column wrt. to a mapping, see {@link Column#map(int[], boolean)}.
		 */
		public abstract Column map(Column column, int[] mapping, boolean preferView);

		InternalColumns() {
		}
	}

}
