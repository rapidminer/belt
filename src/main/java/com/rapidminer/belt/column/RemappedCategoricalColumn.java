/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import static com.rapidminer.belt.column.MappedDoubleArrayColumn.MAPPING_THRESHOLD;
import static com.rapidminer.belt.util.IntegerFormats.BYTE_BACKED_FORMATS;
import static com.rapidminer.belt.util.IntegerFormats.readUInt2;
import static com.rapidminer.belt.util.IntegerFormats.readUInt4;

import java.util.Comparator;
import java.util.Objects;

import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Column with data associated to integer categories and a remapping that adjusts the data to the dictionary. Data can
 * be accessed via a {@link CategoricalReader} or a {@link NumericReader} together with access to the dictionary by
 * {@link #getDictionary(Class)}.
 *
 * @author Gisa Meier, Michael Knopf
 */
class RemappedCategoricalColumn<R> extends CategoricalColumn<R> {

	private static final String NULL_DATA = "Data must not be null";
	private static final String NULL_MAPPING = "Categorical dictionary must not be null";
	private static final String NULL_REMAPPING = "Remapping must not be null";

	private final Format format;
	private final PackedIntegers byteData;
	private final short[] shortData;
	private final int[] intData;

	private final int[] remapping;

	private final Dictionary<R> dictionary;

	RemappedCategoricalColumn(ColumnType<R> type, PackedIntegers bytes, Dictionary<R> dictionary, int[] remapping) {
		super(type, bytes.size());
		if (!BYTE_BACKED_FORMATS.contains(bytes.format())) {
			throw new IllegalArgumentException("Given data management not backed by byte array");
		}
		this.format = bytes.format();
		this.byteData = Objects.requireNonNull(bytes, NULL_DATA);
		this.shortData = null;
		this.intData = null;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.remapping = Objects.requireNonNull(remapping, NULL_REMAPPING);
	}

	RemappedCategoricalColumn(ColumnType<R> type, short[] data, Dictionary<R> dictionary, int[] remapping) {
		super(type, data.length);
		this.format = Format.UNSIGNED_INT16;
		this.byteData = null;
		this.shortData = data;
		this.intData = null;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.remapping = Objects.requireNonNull(remapping, NULL_REMAPPING);
	}

	RemappedCategoricalColumn(ColumnType<R> type, int[] data, Dictionary<R> dictionary, int[] remapping) {
		super(type, data.length);
		this.format = Format.SIGNED_INT32;
		this.byteData = null;
		this.shortData = null;
		this.intData = data;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.remapping = Objects.requireNonNull(remapping, NULL_REMAPPING);
	}

	@Override
	void fillFromUInt2(double[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[readUInt2(byteData.data(), j)];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[i] = Double.NaN;
			} else {
				array[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt4(double[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[readUInt4(byteData.data(), j)];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[i] = Double.NaN;
			} else {
				array[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt8(double[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, byteData.size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[Byte.toUnsignedInt(byteData.data()[j])];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[i] = Double.NaN;
			} else {
				array[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt16(double[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, shortData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[Short.toUnsignedInt(shortData[j])];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[i] = Double.NaN;
			} else {
				array[i] = datum;
			}
		}
	}

	@Override
	void fillFromInt32(double[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, intData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[intData[j]];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[i] = Double.NaN;
			} else {
				array[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt2(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, byteData.size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[readUInt2(byteData.data(), rowIndex)];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[arrayIndex] = Double.NaN;
			} else {
				array[arrayIndex] = datum;
			}
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[readUInt4(byteData.data(), rowIndex)];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[arrayIndex] = Double.NaN;
			} else {
				array[arrayIndex] = datum;
			}
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[Byte.toUnsignedInt(byteData.data()[rowIndex])];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[arrayIndex] = Double.NaN;
			} else {
				array[arrayIndex] = datum;
			}
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, shortData.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[Short.toUnsignedInt(shortData[rowIndex])];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[arrayIndex] = Double.NaN;
			} else {
				array[arrayIndex] = datum;
			}
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, intData.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[intData[rowIndex]];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				array[arrayIndex] = Double.NaN;
			} else {
				array[arrayIndex] = datum;
			}
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt2(int[] array, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int length = Math.min(array.length, size() - start);
		for (int i = 0; i < length; i++) {
			array[i] = remapping[readUInt2(byteData.data(), start + i)];
		}
	}

	@Override
	void fillFromUInt4(int[] array, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int length = Math.min(array.length, size() - start);
		for (int i = 0; i < length; i++) {
			array[i] = remapping[readUInt4(byteData.data(), start + i)];
		}
	}

	@Override
	void fillFromUInt8(int[] array, int rowIndex) {
		int start = Math.min(byteData.size(), rowIndex);
		int length = Math.min(array.length, byteData.size() - start);
		for (int i = 0; i < length; i++) {
			array[i] = remapping[Byte.toUnsignedInt(byteData.data()[start + i])];
		}
	}

	@Override
	void fillFromUInt16(int[] array, int rowIndex) {
		int start = Math.min(shortData.length, rowIndex);
		int length = Math.min(array.length, shortData.length - start);
		for (int i = 0; i < length; i++) {
			array[i] = remapping[Short.toUnsignedInt(shortData[start + i])];
		}
	}

	@Override
	void fillFromInt32(int[] array, int rowIndex) {
		int start = Math.min(intData.length, rowIndex);
		int length = Math.min(array.length, intData.length - start);
		for (int i = 0; i < length; i++) {
			array[i] = remapping[intData[start + i]];
		}
	}

	@Override
	void fillFromUInt2(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = remapping[readUInt2(byteData.data(), rowIndex)];
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = remapping[readUInt4(byteData.data(), rowIndex)];
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, byteData.size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = remapping[Byte.toUnsignedInt(byteData.data()[rowIndex])];
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, shortData.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = remapping[Short.toUnsignedInt(shortData[rowIndex])];
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, intData.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			array[arrayIndex] = remapping[intData[rowIndex]];
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt2(Object[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[readUInt2(byteData.data(), j)];
			array[i] = dictionary.get(datum);
		}
	}

	@Override
	void fillFromUInt4(Object[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[readUInt4(byteData.data(), j)];
			array[i] = dictionary.get(datum);
		}
	}

	@Override
	void fillFromUInt8(Object[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, byteData.size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[Byte.toUnsignedInt(byteData.data()[j])];
			array[i] = dictionary.get(datum);
		}
	}

	@Override
	void fillFromUInt16(Object[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, shortData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[Short.toUnsignedInt(shortData[j])];
			array[i] = dictionary.get(datum);
		}
	}

	@Override
	void fillFromInt32(Object[] array, int rowIndex) {
		int max = Math.min(rowIndex + array.length, intData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = remapping[intData[j]];
			array[i] = dictionary.get(datum);
		}
	}

	@Override
	void fillFromUInt2(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[readUInt2(byteData.data(), rowIndex)];
			array[arrayIndex] = dictionary.get(datum);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[readUInt4(byteData.data(), rowIndex)];
			array[arrayIndex] = dictionary.get(datum);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, byteData.size());
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[Byte.toUnsignedInt(byteData.data()[rowIndex])];
			array[arrayIndex] = dictionary.get(datum);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, shortData.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[Short.toUnsignedInt(shortData[rowIndex])];
			array[arrayIndex] = dictionary.get(datum);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		int max = Math.min(startIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, intData.length);
		int rowIndex = startIndex;
		int arrayIndex = arrayOffset;
		while (rowIndex < max) {
			int datum = remapping[intData[rowIndex]];
			array[arrayIndex] = dictionary.get(datum);
			arrayIndex += arrayStepSize;
			rowIndex++;
		}
	}

	@Override
	CategoricalColumn<R> remap(Dictionary<R> newDictionary, int[] remapping) {
		int[] mergedRemapping = Mapping.merge(this.remapping, remapping);
		return deriveWithNewDictionary(newDictionary, mergedRemapping);
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		boolean createView = preferView || mapping.length > size() * MAPPING_THRESHOLD;
		return createView ? deriveMappedColumn(mapping) : deriveRemappedColumn(mapping);
	}

	@Override
	protected CategoricalColumn<R> swapDictionary(Dictionary<R> newDictionary) {
		return deriveWithNewDictionary(newDictionary, remapping);
	}

	private CategoricalColumn<R> deriveWithNewDictionary(Dictionary<R> newDictionary, int[] remapping) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				return new RemappedCategoricalColumn<>(type(), byteData, newDictionary, remapping);
			case UNSIGNED_INT16:
				return new RemappedCategoricalColumn<>(type(), shortData, newDictionary, remapping);
			case SIGNED_INT32:
				return new RemappedCategoricalColumn<>(type(), intData, newDictionary, remapping);
			default:
				throw new IllegalStateException();
		}
	}

	private Column deriveRemappedColumn(int[] mapping) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				PackedIntegers mappedBytes = Mapping.apply(byteData, mapping);
				return new RemappedCategoricalColumn<>(type(), mappedBytes, dictionary, remapping);
			case UNSIGNED_INT16:
				short[] mappedShorts = Mapping.apply(shortData, mapping);
				return new RemappedCategoricalColumn<>(type(), mappedShorts, dictionary, remapping);
			case SIGNED_INT32:
				int[] mappedIntegers = Mapping.apply(intData, mapping);
				return new RemappedCategoricalColumn<>(type(), mappedIntegers, dictionary, remapping);
			default:
				throw new IllegalStateException();
		}
	}

	private Column deriveMappedColumn(int[] mapping) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				return new RemappedMappedCategoricalColumn<>(type(), byteData, dictionary, remapping, mapping);
			case UNSIGNED_INT16:
				return new RemappedMappedCategoricalColumn<>(type(), shortData, dictionary, remapping, mapping);
			case SIGNED_INT32:
				return new RemappedMappedCategoricalColumn<>(type(), intData, dictionary, remapping, mapping);
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public Format getFormat() {
		return format;
	}

	@Override
	protected PackedIntegers getByteData() {
		return byteData;
	}

	@Override
	protected short[] getShortData() {
		return shortData;
	}

	@Override
	protected int[] getIntData() {
		return intData;
	}

	@Override
	protected Dictionary<R> getDictionary() {
		return dictionary;
	}

	@Override
	public int[] sort(Order order) {
		Comparator<R> comparator = type().comparator();
		if (comparator == null) {
			throw new UnsupportedOperationException();
		}
		Comparator<R> comparatorWithNull = Comparator.nullsLast(comparator);
		switch (format) {
			case UNSIGNED_INT2:
				return Sorting.sort(size(),
						(a, b) -> comparatorWithNull.compare(dictionary.get(remapping[readUInt2(byteData.data(), a)]),
								dictionary.get(remapping[readUInt2(byteData.data(), b)])), order);
			case UNSIGNED_INT4:
				return Sorting.sort(size(),
						(a, b) -> comparatorWithNull.compare(dictionary.get(remapping[readUInt4(byteData.data(), a)]),
								dictionary.get(remapping[readUInt4(byteData.data(), b)])), order);
			case UNSIGNED_INT8:
				return Sorting.sort(byteData.size(),
						(a, b) -> comparatorWithNull
								.compare(dictionary.get(remapping[Byte.toUnsignedInt(byteData.data()[a])]),
										dictionary.get(remapping[Byte.toUnsignedInt(byteData.data()[b])])), order);
			case UNSIGNED_INT16:
				return Sorting.sort(shortData.length,
						(a, b) -> comparatorWithNull
								.compare(dictionary.get(remapping[Short.toUnsignedInt(shortData[a])]),
										dictionary.get(remapping[Short.toUnsignedInt(shortData[b])])), order);
			case SIGNED_INT32:
				return Sorting.sort(intData.length,
						(a, b) -> comparatorWithNull.compare(dictionary.get(remapping[intData[a]]),
								dictionary.get(remapping[intData[b]])), order);
			default:
				throw new IllegalStateException();
		}
	}
}
