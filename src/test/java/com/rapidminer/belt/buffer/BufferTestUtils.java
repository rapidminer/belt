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

package com.rapidminer.belt.buffer;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;


/**
 * @author Gisa Meier
 */
public class BufferTestUtils {

	public static Int32NominalBuffer getInt32Buffer(Column column, ColumnType<String> type) {
		return new Int32NominalBuffer(column, type);
	}

	public static <T> ObjectBuffer<T> getObjectBuffer(Column column, ColumnType<T> type) {
		return new ObjectBuffer<>(type, column);
	}
}
