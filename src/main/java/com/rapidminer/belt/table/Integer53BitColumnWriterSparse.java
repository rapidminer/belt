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

package com.rapidminer.belt.table;

import com.rapidminer.belt.column.Column;


/**
 * A sparse {@link NumericColumnWriter} that grows its size to fit the given data. It stores double values without
 * fractional digits in a memory efficient sparse format.
 *
 * @author Kevin Majchrzak
 */
final class Integer53BitColumnWriterSparse extends RealColumnWriterSparse {

	/**
	 * Creates a sparse column writer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#INTEGER_53_BIT}.
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 */
	Integer53BitColumnWriterSparse(double defaultValue) {
		super(Double.isFinite(defaultValue) ? Math.round(defaultValue) : defaultValue);
	}

	/**
	 * Sets the next logical index to the given value. Finite non-integer values will be rounded.
	 */
	@Override
	protected void setNext(double value) {
		super.setNext(Double.isFinite(value) ? Math.round(value) : value);
	}

	/**
	 * Returns the buffer's {@link Column.TypeId}.
	 *
	 * @return {@link Column.TypeId#INTEGER_53_BIT}.
	 */
	@Override
	protected Column.TypeId type() {
		return Column.TypeId.INTEGER_53_BIT;
	}
}
