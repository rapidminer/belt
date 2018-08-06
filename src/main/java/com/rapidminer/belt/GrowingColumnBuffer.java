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

/**
 * {@link ColumnBuffer} that can grow.
 *
 * @author Gisa Meier
 */
public abstract class GrowingColumnBuffer extends ColumnBuffer{

	/**
	 * Resizes the buffer. The underlying data structure might be bigger than the given length to prevent copies on
	 * every resize.
	 *
	 * @param length
	 * 		the new length of the buffer.
	 */
	public abstract void resize(int length);

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
	abstract void fill(double[] buffer, int startIndex, int bufferOffset, int bufferStepSize, int height);
}
