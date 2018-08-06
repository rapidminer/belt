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

import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Column with data associated to integer categories. Data can be accessed via a {@link CategoricalColumnReader}
 * or a {@link ColumnReader} together with access to the mapping by {@link #getDictionary(Class)} or via a
 * {@link ObjectColumnReader}.
 *
 * @author Gisa Meier, Michael Knopf
 */
abstract class CategoricalColumn<R> extends Column {

	private static final Set<Capability> SORTABLE = EnumSet.of(Capability.NUMERIC_READABLE,
			Capability.OBJECT_READABLE, Capability.SORTABLE);

	private static final Set<Capability> NOT_SORTABLE = EnumSet.of(Capability.NUMERIC_READABLE,
			Capability.OBJECT_READABLE);

	private static final Set<Capability> SORTABLE_BOOLEAN = EnumSet.of(Capability.NUMERIC_READABLE,
			Capability.OBJECT_READABLE, Capability.SORTABLE, Capability.BOOLEAN);

	private static final Set<Capability> NOT_SORTABLE_BOOLEAN = EnumSet.of(Capability.NUMERIC_READABLE,
			Capability.OBJECT_READABLE, Capability.BOOLEAN);

	static final int NOT_BOOLEAN = -1;

	private final ColumnType<R> columnType;
	private final int positiveIndex;

	CategoricalColumn(ColumnType<R> columnType, int size) {
		super(columnType.comparator() != null ? SORTABLE : NOT_SORTABLE, size);
		this.columnType = columnType;
		this.positiveIndex = NOT_BOOLEAN;
	}

	CategoricalColumn(ColumnType<R> columnType, int size, int positiveIndex) {
		super(computeCapabilities(columnType.comparator(), positiveIndex), size);
		this.columnType = columnType;
		this.positiveIndex = positiveIndex;
	}

	private static Set<Capability> computeCapabilities(Comparator<?> comparator, int positiveIndex) {
		if (comparator == null) {
			if (positiveIndex == NOT_BOOLEAN) {
				return NOT_SORTABLE;
			} else {
				return NOT_SORTABLE_BOOLEAN;
			}
		} else if (positiveIndex == NOT_BOOLEAN) {
			return SORTABLE;
		} else {
			return SORTABLE_BOOLEAN;
		}
	}

	@Override
	void fill(double[] buffer, int rowIndex) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(buffer, rowIndex);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(buffer, rowIndex);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(buffer, rowIndex);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(buffer, rowIndex);
				break;
			case SIGNED_INT32:
				fillFromInt32(buffer, rowIndex);
				break;
		}
	}

	abstract void fillFromUInt2(double[] buffer, int rowIndex);

	abstract void fillFromUInt4(double[] buffer, int rowIndex);

	abstract void fillFromUInt8(double[] buffer, int rowIndex);

	abstract void fillFromUInt16(double[] buffer, int rowIndex);

	abstract void fillFromInt32(double[] buffer, int rowIndex);

	@Override
	void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case SIGNED_INT32:
				fillFromInt32(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
		}
	}

	abstract void fillFromUInt2(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt4(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt8(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt16(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromInt32(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	@Override
	void fill(int[] buffer, int rowIndex) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(buffer, rowIndex);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(buffer, rowIndex);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(buffer, rowIndex);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(buffer, rowIndex);
				break;
			case SIGNED_INT32:
				fillFromInt32(buffer, rowIndex);
				break;
		}
	}

	abstract void fillFromUInt2(int[] buffer, int rowIndex);

	abstract void fillFromUInt4(int[] buffer, int rowIndex);

	abstract void fillFromUInt8(int[] buffer, int rowIndex);

	abstract void fillFromUInt16(int[] buffer, int rowIndex);

	abstract void fillFromInt32(int[] buffer, int rowIndex);

	@Override
	void fill(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case SIGNED_INT32:
				fillFromInt32(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
		}
	}

	abstract void fillFromUInt2(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt4(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt8(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt16(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromInt32(int[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	@Override
	void fill(Object[] buffer, int rowIndex) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(buffer, rowIndex);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(buffer, rowIndex);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(buffer, rowIndex);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(buffer, rowIndex);
				break;
			case SIGNED_INT32:
				fillFromInt32(buffer, rowIndex);
				break;
		}
	}

	abstract void fillFromUInt2(Object[] buffer, int rowIndex);

	abstract void fillFromUInt4(Object[] buffer, int rowIndex);

	abstract void fillFromUInt8(Object[] buffer, int rowIndex);

	abstract void fillFromUInt16(Object[] buffer, int rowIndex);

	abstract void fillFromInt32(Object[] buffer, int rowIndex);

	@Override
	void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize) {
		switch (getFormat()) {
			case UNSIGNED_INT2:
				fillFromUInt2(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT4:
				fillFromUInt4(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT8:
				fillFromUInt8(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case UNSIGNED_INT16:
				fillFromUInt16(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
			case SIGNED_INT32:
				fillFromInt32(buffer, startIndex, bufferOffset, bufferStepSize);
				break;
		}
	}

	abstract void fillFromUInt2(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt4(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt8(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromUInt16(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	abstract void fillFromInt32(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	@Override
	public <T> List<T> getDictionary(Class<T> elementType) {
		Class<?> columnElementType = type().elementType();
		if (elementType.isAssignableFrom(columnElementType)) {
			// the cast is safe because of the check above
			@SuppressWarnings("unchecked")
			List<T> result = Collections.unmodifiableList((List<? extends T>) getDictionary());
			return result;
		} else {
			throw new IllegalArgumentException("Element type is not super type of " + columnElementType);
		}
	}

	/**
	 * Returns the format of the underlying category indices.
	 *
	 * @return the index format
	 */
	protected abstract Format getFormat();

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
		return PrettyPrinter.print(this);
	}

	/**
	 * Returns the dictionary associated with this column.
	 *
	 * @return the dictionary
	 */
	protected abstract List<R> getDictionary();

	@Override
	public final ColumnType<R> type() {
		return columnType;
	}

	/** Returns the positive index or -1 if there is none.
	 *
	 * @return the positive category index
	 */
	protected int positiveIndex() {
		return positiveIndex;
	}

	@Override
	public boolean toBoolean(int categoryIndex) {
		if (positiveIndex == NOT_BOOLEAN) {
			throw new UnsupportedOperationException();
		}
		return categoryIndex == positiveIndex;
	}

	@Override
	public boolean toBoolean(double value) {
		if (positiveIndex == NOT_BOOLEAN) {
			throw new UnsupportedOperationException();
		}
		return value == positiveIndex;
	}

	@Override
	public boolean toBoolean(Object value) {
		if (positiveIndex == NOT_BOOLEAN) {
			throw new UnsupportedOperationException();
		}
		return getDictionary().get(positiveIndex).equals(value);
	}
}
