/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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
 * Writer for non-numeric columns. Used by {@link ComplexRowWriter} and {@link MixedRowWriter}.
 *
 * @author Gisa Meier
 */
interface ComplexWriter {

	/**
	 * Fills in the values from the given buffer starting with the given row index and using the given buffer offset and
	 * step size until the given height is reached.
	 *
	 * @param buffer
	 * 		the source buffer
	 * @param startIndex
	 * 		the row index to start from
	 * @param bufferOffset
	 * 		the offset in the buffer
	 * @param bufferStepSize
	 * 		the step size in between values
	 * @param height
	 * 		the maximal row
	 */
	void fill(Object[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height);

	/**
	 * @return a new column containing the data written by previous calls to {@link #fill}
	 */
	Column toColumn();

}
