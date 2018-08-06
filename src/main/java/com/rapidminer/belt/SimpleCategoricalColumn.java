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
import static com.rapidminer.belt.util.IntegerFormats.readUInt2;
import static com.rapidminer.belt.util.IntegerFormats.readUInt4;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Column with data associated to integer categories. Data can be accessed via a {@link CategoricalColumnReader} or a
 * {@link ColumnReader} together with access to the dictionary by {@link #getDictionary(Class)}.
 *
 * @author Gisa Meier, Michael Knopf
 */
class SimpleCategoricalColumn<R> extends CategoricalColumn<R> {

	private static final String NULL_DATA = "Data must not be null";
	private static final String NULL_MAPPING = "Categorical dictionary must not be null";

	private final Format format;
	private final PackedIntegers byteData;
	private final short[] shortData;
	private final int[] intData;

	private final List<R> dictionary;

	SimpleCategoricalColumn(ColumnType<R> type, PackedIntegers bytes, List<R> dictionary) {
		super(type, bytes.size());
		if (!BYTE_BACKED_FORMATS.contains(bytes.format())) {
			throw new IllegalArgumentException("Given data management not backed by byte array");
		}
		this.format = bytes.format();
		this.byteData = Objects.requireNonNull(bytes, NULL_DATA);
		this.shortData = null;
		this.intData = null;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
	}

	SimpleCategoricalColumn(ColumnType<R> type, short[] data, List<R> dictionary) {
		super(type , data.length);
		this.format = Format.UNSIGNED_INT16;
		this.byteData = null;
		this.shortData = data;
		this.intData = null;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
	}

	SimpleCategoricalColumn(ColumnType<R> type, int[] data, List<R> dictionary) {
		super(type, data.length);
		this.format = Format.SIGNED_INT32;
		this.byteData = null;
		this.shortData = null;
		this.intData = data;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
	}

	SimpleCategoricalColumn(ColumnType<R> type, PackedIntegers bytes, List<R> dictionary, int positiveIndex) {
		super(type, bytes.size(), positiveIndex);
		if (!BYTE_BACKED_FORMATS.contains(bytes.format())) {
			throw new IllegalArgumentException("Given data management not backed by byte array");
		}
		this.format = bytes.format();
		this.byteData = Objects.requireNonNull(bytes, NULL_DATA);
		this.shortData = null;
		this.intData = null;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
	}

	SimpleCategoricalColumn(ColumnType<R> type, short[] data, List<R> dictionary, int positiveIndex) {
		super(type , data.length, positiveIndex);
		this.format = Format.UNSIGNED_INT16;
		this.byteData = null;
		this.shortData = data;
		this.intData = null;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
	}

	SimpleCategoricalColumn(ColumnType<R> type, int[] data, List<R> dictionary, int positiveIndex) {
		super(type, data.length, positiveIndex);
		this.format = Format.SIGNED_INT32;
		this.byteData = null;
		this.shortData = null;
		this.intData = data;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
	}

	@Override
	void fillFromUInt2(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = readUInt2(byteData.data(), j);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = Double.NaN;
			} else {
				buffer[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt4(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = readUInt4(byteData.data(), j);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = Double.NaN;
			} else {
				buffer[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt8(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, byteData.size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = Byte.toUnsignedInt(byteData.data()[j]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = Double.NaN;
			} else {
				buffer[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt16(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, shortData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = Short.toUnsignedInt(shortData[j]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = Double.NaN;
			} else {
				buffer[i] = datum;
			}
		}
	}

	@Override
	void fillFromInt32(double[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, intData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = intData[j];
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = Double.NaN;
			} else {
				buffer[i] = datum;
			}
		}
	}

	@Override
	void fillFromUInt2(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = readUInt2(byteData.data(), rowIndex);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = Double.NaN;
			} else {
				buffer[bufferIndex] = datum;
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = readUInt4(byteData.data(), rowIndex);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = Double.NaN;
			} else {
				buffer[bufferIndex] = datum;
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, byteData.size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = Byte.toUnsignedInt(byteData.data()[rowIndex]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = Double.NaN;
			} else {
				buffer[bufferIndex] = datum;
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, shortData.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = Short.toUnsignedInt(shortData[rowIndex]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = Double.NaN;
			} else {
				buffer[bufferIndex] = datum;
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, intData.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = intData[rowIndex];
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = Double.NaN;
			} else {
				buffer[bufferIndex] = datum;
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt2(int[] buffer, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int length = Math.min(buffer.length, size() - start);
		for (int i = 0; i < length; i++) {
			buffer[i] = readUInt2(byteData.data(), start + i);
		}
	}

	@Override
	void fillFromUInt4(int[] buffer, int rowIndex) {
		int start = Math.min(size(), rowIndex);
		int length = Math.min(buffer.length, size() - start);
		for (int i = 0; i < length; i++) {
			buffer[i] = readUInt4(byteData.data(), start + i);
		}
	}

	@Override
	void fillFromUInt8(int[] buffer, int rowIndex) {
		int start = Math.min(byteData.size(), rowIndex);
		int length = Math.min(buffer.length, byteData.size() - start);
		for (int i = 0; i < length; i++) {
			buffer[i] = Byte.toUnsignedInt(byteData.data()[start + i]);
		}
	}

	@Override
	void fillFromUInt16(int[] buffer, int rowIndex) {
		int start = Math.min(shortData.length, rowIndex);
		int length = Math.min(buffer.length, shortData.length - start);
		for (int i = 0; i < length; i++) {
			buffer[i] = Short.toUnsignedInt(shortData[start + i]);
		}
	}

	@Override
	void fillFromInt32(int[] buffer, int rowIndex) {
		int start = Math.min(intData.length, rowIndex);
		int length = Math.min(buffer.length, intData.length - start);
		System.arraycopy(intData, start, buffer, 0, length);
	}

	@Override
	void fillFromUInt2(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = readUInt2(byteData.data(), rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = readUInt4(byteData.data(), rowIndex);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, byteData.size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = Byte.toUnsignedInt(byteData.data()[rowIndex]);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, shortData.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = Short.toUnsignedInt(shortData[rowIndex]);
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, intData.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			buffer[bufferIndex] = intData[rowIndex];
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt2(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = readUInt2(byteData.data(), j);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = null;
			} else {
				buffer[i] = dictionary.get(datum);
			}
		}
	}

	@Override
	void fillFromUInt4(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = readUInt4(byteData.data(), j);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = null;
			} else {
				buffer[i] = dictionary.get(datum);
			}
		}
	}

	@Override
	void fillFromUInt8(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, byteData.size());
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = Byte.toUnsignedInt(byteData.data()[j]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = null;
			} else {
				buffer[i] = dictionary.get(datum);
			}
		}
	}

	@Override
	void fillFromUInt16(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, shortData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = Short.toUnsignedInt(shortData[j]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = null;
			} else {
				buffer[i] = dictionary.get(datum);
			}
		}
	}

	@Override
	void fillFromInt32(Object[] buffer, int rowIndex) {
		int max = Math.min(rowIndex + buffer.length, intData.length);
		for (int i = 0, j = rowIndex; j < max; i++, j++) {
			int datum = intData[j];
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[i] = null;
			} else {
				buffer[i] = dictionary.get(datum);
			}
		}
	}

	@Override
	void fillFromUInt2(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = readUInt2(byteData.data(), rowIndex);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = null;
			} else {
				buffer[bufferIndex] = dictionary.get(datum);
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt4(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = readUInt4(byteData.data(), rowIndex);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = null;
			} else {
				buffer[bufferIndex] = dictionary.get(datum);
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt8(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, byteData.size());
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = Byte.toUnsignedInt(byteData.data()[rowIndex]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = null;
			} else {
				buffer[bufferIndex] = dictionary.get(datum);
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromUInt16(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, shortData.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = Short.toUnsignedInt(shortData[rowIndex]);
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = null;
			} else {
				buffer[bufferIndex] = dictionary.get(datum);
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	void fillFromInt32(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		int remainder = buffer.length % bufferStepSize > bufferOffset ? 1 : 0;
		int max = Math.min(startIndex + buffer.length / bufferStepSize + remainder, intData.length);
		int rowIndex = startIndex;
		int bufferIndex = bufferOffset;
		while (rowIndex < max) {
			int datum = intData[rowIndex];
			if (datum == CategoricalColumnReader.MISSING_CATEGORY) {
				buffer[bufferIndex] = null;
			} else {
				buffer[bufferIndex] = dictionary.get(datum);
			}
			bufferIndex += bufferStepSize;
			rowIndex++;
		}
	}

	@Override
	Column map(int[] mapping, boolean preferView) {
		boolean createView = preferView || mapping.length > size() * MAPPING_THRESHOLD;
		return createView ? deriveMappedColumn(mapping) : deriveSimpleColumn(mapping);
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

	@Override
	long writeToChannel(FileChannel channel, long startPosition) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	protected Format getFormat() {
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
	protected List<R> getDictionary() {
		return dictionary;
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
				return Sorting.sort(size(),
						(a, b) -> comparatorWithNull.compare(dictionary.get(readUInt2(byteData.data(), a)),
								dictionary.get(readUInt2(byteData.data(), b))), order);
			case UNSIGNED_INT4:
				return Sorting.sort(size(),
						(a, b) -> comparatorWithNull.compare(dictionary.get(readUInt4(byteData.data(), a)),
								dictionary.get(readUInt4(byteData.data(), b))), order);
			case UNSIGNED_INT8:
				return Sorting.sort(byteData.size(),
						(a, b) -> comparatorWithNull.compare(dictionary.get(Byte.toUnsignedInt(byteData.data()[a])),
								dictionary.get(Byte.toUnsignedInt(byteData.data()[b]))), order);
			case UNSIGNED_INT16:
				return Sorting.sort(shortData.length,
						(a, b) -> comparatorWithNull.compare(dictionary.get(Short.toUnsignedInt(shortData[a])),
								dictionary.get(Short.toUnsignedInt(shortData[b]))), order);
			case SIGNED_INT32:
				return Sorting.sort(intData.length,
						(a, b) -> comparatorWithNull.compare(dictionary.get(intData[a]),
								dictionary.get(intData[b])), order);
			default:
				throw new IllegalStateException();
		}
	}
}
