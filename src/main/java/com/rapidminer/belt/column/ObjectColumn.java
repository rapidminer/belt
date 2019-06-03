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

package com.rapidminer.belt.column;


import com.rapidminer.belt.reader.ObjectReader;


/**
 * Column with object data that is not categorical. Data can be accessed via a {@link ObjectReader}.
 *
 * @author Gisa Meier
 */
abstract class ObjectColumn<R> extends Column {

	private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

	private final ColumnType<R> columnType;

	ObjectColumn(ColumnType<R> type, int size) {
		super(size);
		this.columnType = type;
	}

	@Override
	public abstract void fill(Object[] array, int rowIndex);

	@Override
	public abstract void fill(Object[] array, int startIndex, int arrayOffset, int arrayStepSize);

	@Override
	public String toString() {
		return ColumnPrinter.print(this);
	}

	@Override
	public ColumnType<R> type() {
		return columnType;
	}

	@Override
	public Column stripData() {
		return new SimpleObjectColumn<>(columnType, EMPTY_OBJECT_ARRAY);
	}
}
