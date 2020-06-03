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

package com.rapidminer.belt.util;

/**
 * A configuration for the double, integer, short, byte and long array builder classes. It holds the initial chunk size,
 * the growth factor and the maximum chunk size. It also defines sensible default values ({@link
 * #DEFAULT_INITIAL_CHUNK_SIZE}, {@link #DEFAULT_GROWTH_FACTOR} and {@link #DEFAULT_MAX_CHUNK_SIZE}).
 *
 * @author Kevin Majchrzak
 * @see DoubleArrayBuilder
 * @see IntegerArrayBuilder
 * @see LongArrayBuilder
 * @see ShortArrayBuilder
 * @see ByteArrayBuilder
 */
public final class ArrayBuilderConfiguration {

	/**
	 * The default initial chunk size.
	 */
	public static final int DEFAULT_INITIAL_CHUNK_SIZE = 1024;

	/**
	 * The default growth factor.
	 */
	public static final double DEFAULT_GROWTH_FACTOR = 1.01;

	/**
	 * The default maximum chunk size.
	 */
	public static final int DEFAULT_MAX_CHUNK_SIZE = Integer.MAX_VALUE;

	/**
	 * The initial size of the chunk pool.
	 */
	static final int INITIAL_CHUNK_POOL_SIZE = 100;

	private static final ArrayBuilderConfiguration defaultConfig =
			new ArrayBuilderConfiguration(null, null, null);

	/**
	 * The initial chunk size.
	 */
	final int initialChunkSize;

	/**
	 * Whenever the current chunk is full the next chunk will be grown by this factor.
	 */
	final double growthFactor;

	/**
	 * chunks will never grow bigger than {@code max(maxChunkSize, initialChunkSize)}
	 */
	final int maxChunkSize;

	/**
	 * Creates a new configuration for the array builder classes. Any of the parameters can be {@code null}. The
	 * configuration will fall back to sensible default values in this case.
	 *
	 * @param initialChunkSize
	 * 		number of elements internally allocated at the start (can be {@code null})
	 * @param growthFactor
	 * 		growth factor used to increase the size of the internally used chunks (can be {@code null})
	 * @param maxChunkSize
	 * 		chunks will never grow bigger than {@code max(maxChunkSize, initialChunkSize)} (can be {@code null})
	 */
	public ArrayBuilderConfiguration(Integer initialChunkSize, Double growthFactor, Integer maxChunkSize) {
		this.initialChunkSize = initialChunkSize == null ? DEFAULT_INITIAL_CHUNK_SIZE : initialChunkSize;
		this.growthFactor = growthFactor == null ? DEFAULT_GROWTH_FACTOR : growthFactor;
		this.maxChunkSize = Math.max(this.initialChunkSize, maxChunkSize == null ? DEFAULT_MAX_CHUNK_SIZE : maxChunkSize);
	}

	/**
	 * Returns the default configuration.
	 */
	public static ArrayBuilderConfiguration getDefault() {
		return defaultConfig;
	}

}
