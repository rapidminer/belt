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

package com.rapidminer.belt.util;

/**
 * Marks a column as having a special role such as being and identifier ({@link #ID}.
 *
 * @author Michael Knopf
 */
public enum ColumnRole implements ColumnMetaData {

	BATCH,
	CLUSTER,
	ID,
	LABEL,
	METADATA,
	OUTLIER,
	PREDICTION,
	SCORE,
	WEIGHT;

	private static final String TYPE_ID = "com.rapidminer.belt.meta.column.role";

	@Override
	public String type() {
		return TYPE_ID;
	}

	@Override
	public Uniqueness uniqueness() {
		return Uniqueness.COLUMN;
	}

}
