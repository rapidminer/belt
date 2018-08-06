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

import com.rapidminer.belt.util.Order;


/**
 * Immutable column of double precision values. Data can be accessed via a {@link ColumnReader} wrapping the column.
 *
 * @author Gisa Meier
 */
abstract class NumericColumn extends Column {

	private static final Set<Capability> CAPABILITIES = EnumSet.of(Capability.NUMERIC_READABLE, Capability.SORTABLE);

	NumericColumn(int size) {
		super(CAPABILITIES, size);
	}

	@Override
	abstract void fill(double[] buffer, int rowIndex);

	@Override
	abstract void fill(double[] buffer, int rowIndex, int bufferOffset, int bufferStepSize);

	@Override
	abstract int[] sort(Order order);

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}
}
