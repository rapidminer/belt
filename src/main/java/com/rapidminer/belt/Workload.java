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
 * Indicates the expected workload per data point. Used to determine reasonable batch-sizes, e.g., when processing data
 * in parallel.
 *
 * <p>By default Belt assumes a small workload per data-point. However, {@link #DEFAULT} and {@link #SMALL} are not
 * guaranteed to show the same behavior.
 *
 * <p>It is recommended to use {@link #DEFAULT} unless measurements indicate the CPU is underutilized (e.g., utilizing
 * fewer CPU cores than specified in the {@link Context}). In that case it is recommended to choose the smallest
 * workload that results in adequate CPU usage. Specifying an overly large workload might result in a significant
 * overhead for scheduling.
 */
public enum Workload {
	/**
	 * Use the default configuration. By default Belt assumes a small workload per data-point. However, it is not
	 * guaranteed to show the same behavior as {@link #SMALL}.
	 */
	DEFAULT,

	/**
	 * Small workload per data point, e.g., a few arithmetic operations.
	 **/
	SMALL,

	/**
	 * Medium workload per data point, e.g., string manipulations.
	 */
	MEDIUM,

	/**
	 * Large workload per data point, e.g., computation of complex metrics between multiple data points.
	 */
	LARGE,

	/**
	 * Huge workload per data point, i.e., a workload so high that there is no benefit in batch-processing the data.
	 **/
	HUGE
}
