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

import java.util.Map;


/**
 * Interface for an extra map method for mapped columns that supports caching. This interface is not part of the Belt
 * API and might be removed without prior notice.
 *
 * @author Gisa Meier
 */
public interface CacheMappedColumn {

	/**
	 * Specialized implementation of {@link Column#map(int[], boolean)} that checks the given cache for previously
	 * computed merges of the current and the given mapping.
	 *
	 * @param mapping
	 * 		the index mapping
	 * @param preferView
	 * 		whether a view is preferred over a deep copy
	 * @param cache
	 * 		map from old mapping to merged mapping when merged with the given mapping
	 * @return a mapped column
	 */
	Column map(int[] mapping, boolean preferView, Map<int[], int[]> cache);

}
