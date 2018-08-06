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

import java.time.Instant;


/**
 * Abstract super-class of buffers for date-time instances
 *
 * @author Gisa Meier
 */
abstract class AbstractDateTimeBuffer {

	protected static final long MIN_SECOND = Instant.MIN.getEpochSecond();
	protected static final long MAX_SECOND = Instant.MAX.getEpochSecond();

	/**
	 * Retrieves the value at the given index.
	 *
	 * <p>Set operations are not atomic. Multiple threads working on the same index require additional
	 * synchronization, however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index to look up
	 * @return the value at the index
	 */
	public abstract Instant get(int index);

	/**
	 * Sets the data at the given index to the given value.
	 *
	 * <p>This method is not atomic. Multiple threads working on the same index require additional synchronization,
	 * however, concurrent operations on disjoint intervals do not.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param instant
	 * 		the instant it should be set to, can be {@code null}
	 */
	public abstract void set(int index, Instant instant);

	/**
	 * @return the size of the buffer
	 */
	public abstract int size();


	/**
	 * Creates a new date-time column from the buffer.
	 *
	 * @return a new {@link DateTimeColumn}
	 */
	public abstract DateTimeColumn toColumn();


	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}

}
