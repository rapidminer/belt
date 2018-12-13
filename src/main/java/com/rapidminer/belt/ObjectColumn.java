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


import java.util.EnumSet;
import java.util.Set;


/**
 * Column with object data that is not categorical. Data can be accessed via a {@link ObjectReader}.
 *
 * @author Gisa Meier
 */
abstract class ObjectColumn<R> extends Column {

	private static final Set<Capability> SORTABLE = EnumSet.of(Capability.OBJECT_READABLE, Capability.SORTABLE);
	private static final Set<Capability> NOT_SORTABLE = EnumSet.of(Capability.OBJECT_READABLE);

	private final ColumnType<R> columnType;

	ObjectColumn(ColumnType<R> type, int size) {
		super(type.comparator() == null ? NOT_SORTABLE : SORTABLE, size);
		this.columnType = type;
	}

	@Override
	abstract void fill(Object[] buffer, int rowIndex);

	@Override
	abstract void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize);

	/**
	 * Returns the underlying category index data of this column.
	 *
	 * @return the data
	 */
	protected abstract Object[] getData();

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}

	@Override
	public ColumnType<R> type() {
		return columnType;
	}

}
