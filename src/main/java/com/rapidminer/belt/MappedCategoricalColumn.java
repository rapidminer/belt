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

import static com.rapidminer.belt.MappedDoubleArrayColumn.MAPPING_THRESHOLD;
import static com.rapidminer.belt.util.IntegerFormats.BYTE_BACKED_FORMATS;
import static com.rapidminer.belt.util.IntegerFormats.Format;
import static com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import static com.rapidminer.belt.util.IntegerFormats.readUInt2;
import static com.rapidminer.belt.util.IntegerFormats.readUInt4;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;

/**
 * Column with data associated to integer categories. Data can be accessed via a {@link CategoricalReader}, an {@link
 * ObjectReader}, or a {@link NumericReader} together with access to the mapping by {@link #getDictionary(Class)}.
 *
 * @author Gisa Meier, Michael Knopf
 */
class MappedCategoricalColumn<R> extends CategoricalColumn<R> implements CacheMappedColumn {

	private static final String NULL_DATA = "Data must not be null";
	private static final String NULL_CATEGORY_MAPPING = "Categorical mapping must not be null";

	private final int[] mapping;

	private final Format format;
	private final PackedIntegers byteData;
	private final short[] shortData;
	private final int[] intData;

	private final List<R> dictionary;

	MappedCategoricalColumn(ColumnType<R> type, PackedIntegers data, List<R> dictionary, int[] mapping) {
		super(type, mapping.length);
		if (!BYTE_BACKED_FORMATS.contains(data.format())) {
			throw new IllegalArgumentException("Given data management not backed by byte array");
		}
		this.format = data.format();
		this.byteData = Objects.requireNonNull(data, NULL_DATA);
		this.shortData = null;
		this.intData = null;
		this.mapping = mapping;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_CATEGORY_MAPPING);
	}

	MappedCategoricalColumn(ColumnType<R> type, short[] data, List<R> dictionary, int[] mapping) {
		super(type, mapping.length);
		this.format = Format.UNSIGNED_INT16;
		this.byteData = null;
		this.shortData = Objects.requireNonNull(data, NULL_DATA);
		this.intData = null;
		this.mapping = mapping;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_CATEGORY_MAPPING);
	}

	MappedCategoricalColumn(ColumnType<R> type, int[] data, List<R> dictionary, int[] mapping) {
		super(type, mapping.length);
		this.format = Format.SIGNED_INT32;
		this.byteData = null;
		this.shortData = null;
		this.intData = data;
		this.mapping = mapping;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_CATEGORY_MAPPING);
	}

	MappedCategoricalColumn(ColumnType<R> type, PackedIntegers data, List<R> dictionary, int[] mapping, int positiveIndex) {
		super(type, mapping.length, positiveIndex);
		if (!BYTE_BACKED_FORMATS.contains(data.format())) {
			throw new IllegalArgumentException("Given data management not backed by byte array");
		}
		this.format = data.format();
		this.byteData = Objects.requireNonNull(data, NULL_DATA);
		this.shortData = null;
		this.intData = null;
		this.mapping = mapping;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_CATEGORY_MAPPING);
	}

	MappedCategoricalColumn(ColumnType<R> type, short[] data, List<R> dictionary, int[] mapping, int positiveIndex) {
		super(type, mapping.length, positiveIndex);
		this.format = Format.UNSIGNED_INT16;
		this.byteData = null;
		this.shortData = Objects.requireNonNull(data, NULL_DATA);
		this.intData = null;
		this.mapping = mapping;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_CATEGORY_MAPPING);
	}

	MappedCategoricalColumn(ColumnType<R> type, int[] data, List<R> dictionary, int[] mapping, int positiveIndex) {
		super(type, mapping.length, positiveIndex);
		this.format = Format.SIGNED_INT32;
		this.byteData = null;
		this.shortData = null;
		this.intData = data;
		this.mapping = mapping;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_CATEGORY_MAPPING);
	}

	@Override
	void fillFromUInt2(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpNumericUInt2(j);
		}
	}

	@Override
	void fillFromUInt4(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpNumericUInt4(j);
		}
	}

	@Override
	void fillFromUInt8(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpNumericUInt8(j);
		}
	}

	@Override
	void fillFromUInt16(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpNumericUInt16(j);
		}
	}

	@Override
	void fillFromInt32(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpNumericInt32(j);
		}
	}

	@Override
	void fillFromUInt2(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpNumericUInt2(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpNumericUInt4(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpNumericUInt8(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpNumericUInt16(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpNumericInt32(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt2(int[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpIndexUInt2(j);
		}
	}

	@Override
	void fillFromUInt4(int[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpIndexUInt4(j);
		}
	}

	@Override
	void fillFromUInt8(int[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpIndexUInt8(j);
		}
	}

	@Override
	void fillFromUInt16(int[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpIndexUInt16(j);
		}
	}

	@Override
	void fillFromInt32(int[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpIndexInt32(j);
		}
	}

	@Override
	void fillFromUInt2(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpIndexUInt2(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpIndexUInt4(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpIndexUInt8(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpIndexUInt16(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpIndexInt32(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt2(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpUInt2(j);
		}
	}

	@Override
	void fillFromUInt4(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpUInt4(j);
		}
	}

	@Override
	void fillFromUInt8(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpUInt8(j);
		}
	}

	@Override
	void fillFromUInt16(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpUInt16(j);
		}
	}

	@Override
	void fillFromInt32(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, mapping.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			buffer[i] = lookUpInt32(j);
		}
	}

	@Override
	void fillFromUInt2(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpUInt2(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpUInt4(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpUInt8(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpUInt16(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, mapping.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = lookUpInt32(rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	/**
	 * Returns the categorical mapping associated with this column.
	 *
	 * @return the categorical mapping
	 */
	@Override
	protected List<R> getDictionary() {
		return dictionary;
	}

	/**
	 * The Row mapping.
	 *
	 * @return the mapping
	 */
	protected int[] getRowMapping() {
		return mapping;
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
	protected int[] sort(Order order) {
		Comparator<R> comparator = type().comparator();
		if(comparator==null){
			throw new UnsupportedOperationException();
		}
		Comparator<R> comparatorWithNull = Comparator.nullsLast(comparator);
		switch (format) {
			case UNSIGNED_INT2:
				return Sorting.sort(mapping.length,
						(a, b) -> comparatorWithNull.compare(lookUpUInt2(a), lookUpUInt2(b)),
						order);
			case UNSIGNED_INT4:
				return Sorting.sort(mapping.length,
						(a, b) -> comparatorWithNull.compare(lookUpUInt4(a), lookUpUInt4(b)),
						order);
			case UNSIGNED_INT8:
				return Sorting.sort(mapping.length,
						(a, b) -> comparatorWithNull.compare(lookUpUInt8(a), lookUpUInt8(b)),
						order);
			case UNSIGNED_INT16:
				return Sorting.sort(mapping.length,
						(a, b) -> comparatorWithNull.compare(lookUpUInt16(a), lookUpUInt16(b)),
						order);
			case SIGNED_INT32:
				return Sorting.sort(mapping.length,
						(a, b) -> comparatorWithNull.compare(lookUpInt32(a), lookUpInt32(b)),
						order);
			default:
				throw new IllegalStateException("Unknown data format");
		}
	}

	@Override
	public Column map(int[] remapping, boolean preferView, Map<int[], int[]> cache) {
		int[] mergedMapping = cache.computeIfAbsent(getRowMapping(), k -> Mapping.merge(remapping, mapping));
		boolean keepView = preferView || mergedMapping.length > size() * MAPPING_THRESHOLD;
		return keepView ? deriveMappedColumn(mergedMapping) : deriveSimpleColumn(mergedMapping);
	}

	@Override
	Column map(int[] remapping, boolean preferView) {
		int[] mergedMapping = Mapping.merge(remapping, mapping);
		boolean keepView = preferView || mergedMapping.length > size() * MAPPING_THRESHOLD;
		return keepView ? deriveMappedColumn(mergedMapping) : deriveSimpleColumn(mergedMapping);
	}

	private Column deriveMappedColumn(int[] mapping) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				return new MappedCategoricalColumn<>(type(), byteData, dictionary, mapping, positiveIndex());
			case UNSIGNED_INT16:
				return new MappedCategoricalColumn<>(type(), shortData, dictionary, mapping, positiveIndex());
			case SIGNED_INT32:
				return new MappedCategoricalColumn<>(type(), intData, dictionary, mapping, positiveIndex());
			default:
				throw new IllegalStateException();
		}
	}

	private Column deriveSimpleColumn(int[] mapping) {
		switch (format) {
			case UNSIGNED_INT2:
			case UNSIGNED_INT4:
			case UNSIGNED_INT8:
				PackedIntegers mappedBytes = Mapping.apply(byteData, mapping);
				return new SimpleCategoricalColumn<>(type(), mappedBytes, dictionary, positiveIndex());
			case UNSIGNED_INT16:
				short[] mappedShorts = Mapping.apply(shortData, mapping);
				return new SimpleCategoricalColumn<>(type(), mappedShorts, dictionary, positiveIndex());
			case SIGNED_INT32:
				int[] mappedIntegers = Mapping.apply(intData, mapping);
				return new SimpleCategoricalColumn<>(type(), mappedIntegers, dictionary, positiveIndex());
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	long writeToChannel(FileChannel channel, long startPosition) throws IOException {
		throw new UnsupportedOperationException();
	}

	private int lookUpIndexUInt2(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			return readUInt2(byteData.data(), position);
		} else {
			return 0;
		}
	}

	private int lookUpIndexUInt4(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			return readUInt4(byteData.data(), position);
		} else {
			return 0;
		}
	}

	private int lookUpIndexUInt8(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			return Byte.toUnsignedInt(byteData.data()[position]);
		} else {
			return 0;
		}
	}

	private int lookUpIndexUInt16(int i) {
		int position = mapping[i];
		if (position >= 0 && position < shortData.length) {
			return Short.toUnsignedInt(shortData[position]);
		} else {
			return 0;
		}
	}

	private int lookUpIndexInt32(int i) {
		int position = mapping[i];
		if (position >= 0 && position < intData.length) {
			return intData[position];
		} else {
			return 0;
		}
	}

	private double lookUpNumericUInt2(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			int datum = readUInt2(byteData.data(), position);
			return datum == CategoricalReader.MISSING_CATEGORY ? Double.NaN : datum;
		} else {
			return Double.NaN;
		}
	}

	private double lookUpNumericUInt4(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			int datum = readUInt4(byteData.data(), position);
			return datum == CategoricalReader.MISSING_CATEGORY ? Double.NaN : datum;
		} else {
			return Double.NaN;
		}
	}

	private double lookUpNumericUInt8(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			int datum = Byte.toUnsignedInt(byteData.data()[position]);
			return datum == CategoricalReader.MISSING_CATEGORY ? Double.NaN : datum;
		} else {
			return Double.NaN;
		}
	}

	private double lookUpNumericUInt16(int i) {
		int position = mapping[i];
		if (position >= 0 && position < shortData.length) {
			int datum = Short.toUnsignedInt(shortData[position]);
			return datum == CategoricalReader.MISSING_CATEGORY ? Double.NaN : datum;
		} else {
			return Double.NaN;
		}
	}

	private double lookUpNumericInt32(int i) {
		int position = mapping[i];
		if (position >= 0 && position < intData.length) {
			int datum = intData[position];
			return datum == CategoricalReader.MISSING_CATEGORY ? Double.NaN : datum;
		} else {
			return Double.NaN;
		}
	}

	private R lookUpUInt2(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			int datum = readUInt2(byteData.data(), position);
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				return null;
			} else {
				return dictionary.get(datum);
			}
		} else {
			return null;
		}
	}

	private R lookUpUInt4(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			int datum = readUInt4(byteData.data(), position);
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				return null;
			} else {
				return dictionary.get(datum);
			}
		} else {
			return null;
		}
	}

	private R lookUpUInt8(int i) {
		int position = mapping[i];
		if (position >= 0 && position < byteData.size()) {
			int datum = Byte.toUnsignedInt(byteData.data()[position]);
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				return null;
			} else {
				return dictionary.get(datum);
			}
		} else {
			return null;
		}
	}

	private R lookUpUInt16(int i) {
		int position = mapping[i];
		if (position >= 0 && position < shortData.length) {
			int datum = Short.toUnsignedInt(shortData[position]);
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				return null;
			} else {
				return dictionary.get(datum);
			}
		} else {
			return null;
		}
	}

	private R lookUpInt32(int i) {
		int position = mapping[i];
		if (position >= 0 && position < intData.length) {
			int datum = intData[position];
			if (datum == CategoricalReader.MISSING_CATEGORY) {
				return null;
			} else {
				return dictionary.get(datum);
			}
		} else {
			return null;
		}
	}

}
