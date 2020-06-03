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
 * Memory and time efficient builder for short arrays that can be used if the length of the short array is unknown while
 * building it. Internally it uses a chunked array representation to represent the short array. Adding elements takes
 * constant time. Elements will be added at the end of the array. Elements cannot be modified or randomly accessed.
 * Creating the final short array after filling the elements into the builder takes time linear in the number of
 * elements.
 *
 * @author Kevin Majchrzak
 */
public class ShortArrayBuilder {

	private final ArrayBuilderConfiguration config;

	/**
	 * pool of chunks used for memory efficient representation of the data
	 */
	private short[][] chunkPool;

	/**
	 * the currently active chunk
	 */
	private short[] currentChunk;

	/**
	 * The next free intra-chunk index of the current chunk.
	 */
	private int currentIndexInChunk;

	/**
	 * the index of the currently active chunk in the chunk-pool
	 */
	private int currentChunkIndex;

	/**
	 * The number of data points stored so far.
	 */
	private int size;

	/**
	 * Creates a new builder with the default initial chunk size, default growth factor and default maximum chunk size.
	 * The default configuration will lead to a very low memory overhead and good time efficiency.
	 */
	public ShortArrayBuilder() {
		this(ArrayBuilderConfiguration.getDefault());
	}

	/**
	 * Creates a new builder. The initial chunk size is the number of elements internally allocated at the start. Every
	 * time the internal array is full a new array will be created and added to the internal chunk pool. Therefore,
	 * elements will never be copied to avoid wasting time or memory. Every chunk will be bigger than the last one by
	 * the given growth factor. Chunks never grow bigger than the specified maximum chunk size.
	 *
	 * @param config
	 * 		the array builders configuration, holding the initial chunk size, the growth factor and the maximum chunk size
	 */
	public ShortArrayBuilder(ArrayBuilderConfiguration config) {
		this.config = config;
		currentChunkIndex = 0;
		size = 0;
		currentChunk = new short[config.initialChunkSize];
		chunkPool = new short[ArrayBuilderConfiguration.INITIAL_CHUNK_POOL_SIZE][];
		chunkPool[currentChunkIndex] = currentChunk;
	}

	/**
	 * Adds the given value to the end of the array.
	 *
	 * @param value
	 * 		the value to add
	 */
	public void setNext(short value) {
		if (currentIndexInChunk >= currentChunk.length) {
			moveToNextChunk();
		}
		currentChunk[currentIndexInChunk] = value;
		currentIndexInChunk++;
		size++;
	}

	/**
	 * Returns the stored short data in a newly created short array. The method takes time linear in the number of
	 * elements stored.
	 */
	public short[] getData() {
		short[] result = new short[size];
		int index = 0;
		// add non-default indices from the past chunks to result
		for (int chunkIndex = 0; chunkIndex < currentChunkIndex; chunkIndex++) {
			short[] chunk = chunkPool[chunkIndex];
			for (int intraChunkIndex = 0, len = chunk.length; intraChunkIndex < len; intraChunkIndex++) {
				result[index++] = chunk[intraChunkIndex];
			}
		}
		// add non-default indices from the current chunk to result
		for (int intraChunkIndex = 0; intraChunkIndex < currentIndexInChunk; intraChunkIndex++) {
			result[index++] = currentChunk[intraChunkIndex];
		}
		return result;
	}

	/**
	 * Returns the number of elements stored inside builder.
	 */
	public int size() {
		return size;
	}

	/**
	 * Creates a new chunk and sets it to be the current.
	 */
	private void moveToNextChunk() {
		int oldLength = currentChunk.length;
		int newLength = (int) (oldLength * config.growthFactor + 1);
		if (newLength > config.maxChunkSize) {
			newLength = config.maxChunkSize;
		}
		currentChunkIndex++;
		currentIndexInChunk = 0;
		currentChunk = new short[newLength];
		if (currentChunkIndex >= chunkPool.length) {
			growChunkPool();
		}
		chunkPool[currentChunkIndex] = currentChunk;
	}

	/**
	 * Doubles the size of the current chunk pool.
	 */
	private void growChunkPool() {
		short[][] newChunkPool = new short[chunkPool.length * 2][];
		System.arraycopy(chunkPool, 0, newChunkPool, 0, chunkPool.length);
		chunkPool = newChunkPool;
	}

}
