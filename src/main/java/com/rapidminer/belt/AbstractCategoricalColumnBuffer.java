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

import java.util.List;


/**
 * Abstract super class of all implementations of {@link CategoricalColumnBuffer}. This class is intentionally not
 * public since the get and set methods should only be called on the implementation classes for performance reasons.
 * Implementations must ensure that {@link #get}, {@link #set}, {@link #setSave} and {@link #differentValues()} are
 * thread-safe.
 *
 * @author Gisa Meier
 */
abstract class AbstractCategoricalColumnBuffer<T> implements CategoricalColumnBuffer<T> {

	/**
	 * Error message when trying to change frozen buffer
	 */
	protected static final String BUFFER_FROZEN_MESSAGE = "Buffer cannot be changed after used to create column";


	/**
	 * Retrieves the value at the given index. This method is thread-safe and can be used alongside invocations of
	 * {@link #set} and {@link #setSave}. However, this method might not return the latest value set on another thread.
	 *
	 * @param index
	 * 		the index to look up
	 * @return the value at the index
	 */
	public abstract T get(int index);

	/**
	 * Sets the data at the given index to the given value.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param value
	 * 		the value to set
	 * @throws IllegalStateException
	 * 		if called after the buffer was used to create a {@link Column}
	 */
	public abstract void set(int index, T value);


	/**
	 * Tries to set the data at the given index to the given value. Returns {@code true} if it worked and {@code false}
	 * if the buffer format cannot hold any more different values.
	 *
	 * @param index
	 * 		the index where the value should be set
	 * @param value
	 * 		the value to set
	 * @return {@code false} if the buffer contained already to many different values to take this value, {@code true}
	 * otherwise
	 * @throws IllegalStateException
	 * 		if called after the buffer was used to create a {@link Column}
	 */
	public abstract boolean setSave(int index, T value);


	/**
	 * {@inheritDoc} This method is thread-safe.
	 */
	@Override
	public abstract int differentValues();

	/**
	 * @return the mapping
	 */
	abstract List<T> getMapping();


	/**
	 * Freezes the current state of the buffer. It becomes read-only. Should be called when a buffer is used to create a
	 * {@link Column}.
	 */
	abstract void freeze();


	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}
}