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
import java.util.List;
import java.util.Map;

import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Column with data associated to integer categories. Data can be accessed via a {@link CategoricalReader}
 * or a {@link NumericReader} together with access to the mapping by {@link #getDictionary()} or via a
 * {@link ObjectReader}.
 *
 * @author Gisa Meier, Michael Knopf
 */
public abstract class CategoricalColumn extends Column {

	private static final int[] EMPTY_INT_ARRAY = new int[0];
	private static final String MESSAGE_STEP_SIZE = "step size must not be smaller than 1";

	private final ColumnType<String> columnType;

	CategoricalColumn(ColumnType<String> columnType, int size) {
		super(size);
		this.columnType = columnType;
	}

	@Override
	public void fill(double[] array, int rowIndex) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(array, rowIndex);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(array, rowIndex);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(array, rowIndex);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(array, rowIndex);
				break;
			case SIGNED_INT32:
				fillFromInt32(array, rowIndex);
				break;
		}
	}

	abstract void fillFromUInt2(double[] array, int rowIndex);

	abstract void fillFromUInt4(double[] array, int rowIndex);

	abstract void fillFromUInt8(double[] array, int rowIndex);

	abstract void fillFromUInt16(double[] array, int rowIndex);

	abstract void fillFromInt32(double[] array, int rowIndex);

	@Override
	public void fill(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException(MESSAGE_STEP_SIZE);
		}
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case SIGNED_INT32:
				fillFromInt32(array, startIndex, arrayOffset, arrayStepSize);
				break;
		}
	}

	abstract void fillFromUInt2(double[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt4(double[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt8(double[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt16(double[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromInt32(double[] array, int startIndex, int arrayOffset, int arrayStepSize);

	@Override
	public void fill(int[] array, int rowIndex) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(array, rowIndex);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(array, rowIndex);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(array, rowIndex);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(array, rowIndex);
				break;
			case SIGNED_INT32:
				fillFromInt32(array, rowIndex);
				break;
		}
	}

	abstract void fillFromUInt2(int[] array, int rowIndex);

	abstract void fillFromUInt4(int[] array, int rowIndex);

	abstract void fillFromUInt8(int[] array, int rowIndex);

	abstract void fillFromUInt16(int[] array, int rowIndex);

	abstract void fillFromInt32(int[] array, int rowIndex);

	@Override
	public void fill(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException(MESSAGE_STEP_SIZE);
		}
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case SIGNED_INT32:
				fillFromInt32(array, startIndex, arrayOffset, arrayStepSize);
				break;
		}
	}

	abstract void fillFromUInt2(int[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt4(int[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt8(int[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt16(int[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromInt32(int[] array, int startIndex, int arrayOffset, int arrayStepSize);

	@Override
	public void fill(Object[] array, int rowIndex) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(array, rowIndex);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(array, rowIndex);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(array, rowIndex);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(array, rowIndex);
				break;
			case SIGNED_INT32:
				fillFromInt32(array, rowIndex);
				break;
		}
	}

	abstract void fillFromUInt2(Object[] array, int rowIndex);

	abstract void fillFromUInt4(Object[] array, int rowIndex);

	abstract void fillFromUInt8(Object[] array, int rowIndex);

	abstract void fillFromUInt16(Object[] array, int rowIndex);

	abstract void fillFromInt32(Object[] array, int rowIndex);

	@Override
	public void fill(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		if (arrayStepSize < 1) {
			throw new IllegalArgumentException(MESSAGE_STEP_SIZE);
		}
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(array, startIndex, arrayOffset, arrayStepSize);
				break;
			case SIGNED_INT32:
				fillFromInt32(array, startIndex, arrayOffset, arrayStepSize);
				break;
		}
	}

	abstract void fillFromUInt2(Object[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt4(Object[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt8(Object[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromUInt16(Object[] array, int startIndex, int arrayOffset, int arrayStepSize);

	abstract void fillFromInt32(Object[] array, int startIndex, int arrayOffset, int arrayStepSize);


	/**
	 * Creates a column that replaces this column's dictionary with the given one. The categorical indices are updated
	 * to match the replacement dictionary (the dictionary itself remains unchanged). Values not contained in the
	 * replacement dictionary are declared missing.
	 *
	 * @param newDictionary
	 * 		the dictionary for the remapped column
	 * @return a new categorical column with the given dictionary
	 */
	CategoricalColumn remap(Dictionary newDictionary) {
		Dictionary oldDictionary = getDictionary();
		if (newDictionary == oldDictionary) {
			// no need to do anything in case of exactly the same dictionaries
			// we do not want an unnecessary identity remapping in that case
			return this;
		}
		Map<String, Integer> inverseNewDictionary = newDictionary.createInverse();
		int[] remapping = new int[oldDictionary.maximalIndex() + 1];
		for (int j = 1; j <= oldDictionary.maximalIndex(); j++) {
			Integer newIndex = inverseNewDictionary.get(oldDictionary.get(j));
			if (newIndex != null) {
				remapping[j] = newIndex;
			}
			//otherwise keep at 0 == missing
		}
		return remap(newDictionary, remapping);
	}


	/**
	 * Merges the current dictionary of the column with the given dictionary. The new dictionary starts with the given
	 * dictionary and values of this column's dictionary that are not part of the other dictionary are added
	 * afterwards. The category indices are remapped to match the merged dictionary.
	 *
	 * @param otherDictionary the dictionary to merge with
	 * @return a new categorical column with the merged dictionary
	 */
	CategoricalColumn mergeDictionaries(Dictionary otherDictionary) {
		Dictionary oldDictionary = getDictionary();
		if (otherDictionary == getDictionary()) {
			// no need to do anything in case of exactly the same dictionaries
			// we do not want an unnecessary identity remapping in that case
			return this;
		}
		Map<String, Integer> inverseOtherDictionary = otherDictionary.createInverse();
		int[] remapping = new int[oldDictionary.maximalIndex() + 1];
		List<String> mergedDictionaryList = null;
		for (int j = 1; j <= oldDictionary.maximalIndex(); j++) {
			String value = oldDictionary.get(j);
			Integer newIndex = inverseOtherDictionary.get(value);
			if (newIndex != null) {
				remapping[j] = newIndex;
			} else {
				if (mergedDictionaryList == null) {
					mergedDictionaryList = new ArrayList<>(otherDictionary.getValueList());
				}
				mergedDictionaryList.add(value);
				remapping[j] = mergedDictionaryList.size() - 1;
			}
		}
		Dictionary newDictionary;
		if (mergedDictionaryList == null) {
			//every value of the current dictionary is in the other dictionary
			newDictionary = otherDictionary;
		} else {
			boolean keepPositiveIndex = otherDictionary.isBoolean() && oldDictionary.hasPositive()
					&& mergedDictionaryList.size() <= BooleanDictionary.MAXIMAL_RAW_SIZE;
			newDictionary = keepPositiveIndex ? new BooleanDictionary(mergedDictionaryList,
					otherDictionary.getPositiveIndex()) : new Dictionary(mergedDictionaryList);
		}
		return remap(newDictionary, remapping);
	}

	/**
	 * Remaps the column depending on the instance.
	 *
	 * @param newDictionary
	 * 		the dictionary to use
	 * @param remapping
	 * 		the remapping to apply to the values
	 * @return a new remapped column
	 */
	abstract CategoricalColumn remap(Dictionary newDictionary, int[] remapping);

	/**
	 * Returns the format of the underlying category indices.
	 *
	 * @return the index format
	 */
	public abstract Format getFormat();

	/**
	 * Returns the underlying {@code byte} category indices of this column. Returns {@code null} if the column is not of
	 * a format in {@link IntegerFormats#BYTE_BACKED_FORMATS}.
	 *
	 * @return the index array or {@code null}
	 */
	protected abstract PackedIntegers getByteData();

	/**
	 * Returns the underlying unsigned {@code short} category indices of this column. Returns {@code null} if the column
	 * is not of format {@link Format#UNSIGNED_INT16}.
	 *
	 * @return the index array or {@code null}
	 */
	protected abstract short[] getShortData();

	/**
	 * Returns the underlying {@code int} category indices of this column. Returns {@code null} if the column is not of
	 * format {@link Format#SIGNED_INT32}.
	 *
	 * @return the index array or {@code null}
	 */
	protected abstract int[] getIntData();

	@Override
	public String toString() {
		return ColumnPrinter.print(this);
	}

	@Override
	public final ColumnType<String> type() {
		return columnType;
	}

	/**
	 * Changes the dictionary of this categorical column to one with the given positive value.
	 *
	 * @param positiveValue
	 * 		the new positive value
	 * @return a new categorical column with a boolean dictionary
	 */
	CategoricalColumn toBoolean(String positiveValue) {
		BooleanDictionary newDictionary = getDictionary().toBoolean(positiveValue);
		return swapDictionary(newDictionary);
	}

	/**
	 * Creates a new categorical column with the same data and the given dictionary. The new dictionary must be
	 * compatible with the data of the column in the sense that every category index in the data is mapped to a
	 * different non-null object value except for 0 which is mapped to {@code null}. The compatibility is not
	 * checked by this method.
	 *
	 * @param newDictionary
	 * 		the new dictionary
	 * @return a new categorical column
	 */
	protected abstract CategoricalColumn swapDictionary(Dictionary newDictionary);

	@Override
	public Column stripData() {
		return new SimpleCategoricalColumn(columnType, EMPTY_INT_ARRAY, getDictionary());
	}
}
