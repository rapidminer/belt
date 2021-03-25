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

package com.rapidminer.belt.column;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;


/**
 * @author Kevin Majchrzak
 */
public class SparseBitmapTest {

	@Test
	public void testNoDefaultValues() {
		int size = 1023;
		int[] nonDefaultIndices = new int[size];
		Arrays.setAll(nonDefaultIndices, i -> i);
		SparseBitmap bitmap = new SparseBitmap(false, nonDefaultIndices, size);
		// assert that there are "size" non-default indices
		assertEquals(size, bitmap.countNonDefaultIndices(nonDefaultIndices));
		// assert that get always returns the original index (because the non-defaults are actually 0,1,2..,size-1)
		assertArrayEquals(nonDefaultIndices, Arrays.stream(nonDefaultIndices).map(bitmap::get).toArray());
		// assert that there are no default indices
		int[] allZero = new int[size];
		Arrays.fill(allZero, 0);
		assertArrayEquals(allZero, Arrays.stream(nonDefaultIndices).map(i -> bitmap.isDefaultIndex(i) ? SparseBitmap.DEFAULT_INDEX : 0).toArray());
		// assert that the size is correct
		assertEquals(size, bitmap.size());
	}

	@Test
	public void testOnlyDefaultValues() {
		int size = 1025;
		SparseBitmap bitmap = new SparseBitmap(false, new int[0], size);
		int[] allIndices = new int[size];
		Arrays.setAll(allIndices, i -> i);
		// assert that there are no non-default indices
		assertEquals(0, bitmap.countNonDefaultIndices(allIndices));
		// assert that get always returns SparseBitmap.DEFAULT_INDEX
		int[] allDefault = new int[size];
		Arrays.fill(allDefault, SparseBitmap.DEFAULT_INDEX);
		assertArrayEquals(allDefault, Arrays.stream(allIndices).map(bitmap::get).toArray());
		// assert that there are only default indices
		assertArrayEquals(allDefault, Arrays.stream(allIndices).map(i -> bitmap.isDefaultIndex(i) ? SparseBitmap.DEFAULT_INDEX : 0).toArray());
		// assert that the size is correct
		assertEquals(size, bitmap.size());
	}

	@Test
	public void testOnlyOneDefaultValue() {
		SparseBitmap bitmap = new SparseBitmap(false, new int[0], 1);
		assertTrue(bitmap.isDefaultIndex(0));
		assertFalse(bitmap.isDefaultIndex(1));
		assertFalse(bitmap.isDefaultIndex(-1));
		assertEquals(SparseBitmap.DEFAULT_INDEX, bitmap.get(0));
		assertEquals(SparseBitmap.OUT_OF_BOUNDS_INDEX, bitmap.get(-1));
		assertEquals(SparseBitmap.OUT_OF_BOUNDS_INDEX, bitmap.get(1));
	}

	@Test
	public void testOnlyOneNonDefaultValue() {
		SparseBitmap bitmap = new SparseBitmap(false, new int[]{0}, 1);
		assertFalse(bitmap.isDefaultIndex(0));
		assertFalse(bitmap.isDefaultIndex(1));
		assertFalse(bitmap.isDefaultIndex(-1));
		assertEquals(0, bitmap.get(0));
		assertEquals(SparseBitmap.OUT_OF_BOUNDS_INDEX, bitmap.get(-1));
		assertEquals(SparseBitmap.OUT_OF_BOUNDS_INDEX, bitmap.get(1));
	}

	@Test
	public void testEmpty() {
		SparseBitmap bitmap = new SparseBitmap(false, new int[0], 0);
		assertFalse(bitmap.isDefaultIndex(0));
		assertFalse(bitmap.isDefaultIndex(1));
		assertFalse(bitmap.isDefaultIndex(-1));

		SparseBitmap nanBitmap = new SparseBitmap(true, new int[0], 0);
		assertTrue(nanBitmap.isDefaultIndex(0));
		assertTrue(nanBitmap.isDefaultIndex(1));
		assertTrue(nanBitmap.isDefaultIndex(-1));

		int[] someIndices = new int[10];
		Arrays.setAll(someIndices, i -> i - 5);
		assertEquals(10, bitmap.countNonDefaultIndices(someIndices));
		assertEquals(0, nanBitmap.countNonDefaultIndices(someIndices));

		assertEquals(0, bitmap.size());
		assertEquals(0, nanBitmap.size());

		// assert that get always returns SparseBitmap.OUT_OF_BOUNDS_INDEX
		int[] allOutOfBounds = new int[10];
		Arrays.fill(allOutOfBounds, SparseBitmap.OUT_OF_BOUNDS_INDEX);
		assertArrayEquals(Arrays.stream(someIndices).map(bitmap::get).toArray(), allOutOfBounds);

		// assert that get always returns SparseBitmap.DEFAULT_INDEX
		int[] allDefault = new int[10];
		Arrays.fill(allDefault, SparseBitmap.DEFAULT_INDEX);
		assertArrayEquals(Arrays.stream(someIndices).map(nanBitmap::get).toArray(), allDefault);
	}

	@Test
	public void testOneDefaultAndOneNonDefaultValue() {
		SparseBitmap bitmap = new SparseBitmap(false, new int[]{1}, 2);
		assertTrue(bitmap.isDefaultIndex(0));
		assertFalse(bitmap.isDefaultIndex(1));
		assertFalse(bitmap.isDefaultIndex(2));
		assertFalse(bitmap.isDefaultIndex(-1));
		assertEquals(SparseBitmap.DEFAULT_INDEX, bitmap.get(0));
		assertEquals(SparseBitmap.OUT_OF_BOUNDS_INDEX, bitmap.get(-1));
		assertEquals(0, bitmap.get(1));
		assertEquals(SparseBitmap.OUT_OF_BOUNDS_INDEX, bitmap.get(2));
	}

	@Test
	public void testRandom() {
		int maxNumberOfNonDefaults = 1024;
		int[] nonDefaultIndices = new int[maxNumberOfNonDefaults];
		Random rand = new Random();
		int size = 16384;
		Arrays.setAll(nonDefaultIndices, i -> rand.nextInt(maxNumberOfNonDefaults));
		nonDefaultIndices = Arrays.stream(nonDefaultIndices).distinct().sorted().toArray();
		SparseBitmap bitmap = new SparseBitmap(Double.isNaN(Math.random() - 0.5d), nonDefaultIndices, size);

		assertEquals(nonDefaultIndices.length, bitmap.countNonDefaultIndices(nonDefaultIndices));

		int[] readNonDefaultIndices = new int[nonDefaultIndices.length];
		int index = 0;
		for (int i = 0; i < size; i++) {
			if (bitmap.get(i) >= 0) {
				readNonDefaultIndices[index++] = i;
			}
		}
		assertArrayEquals(nonDefaultIndices, readNonDefaultIndices);

		assertEquals(size, bitmap.size());
	}

	@Test
	public void testDefaultIsNaN() {
		int maxNumberOfNonDefaults = 1024;
		int[] nonDefaultIndices = new int[maxNumberOfNonDefaults];
		Random rand = new Random();
		int size = 16384;
		Arrays.setAll(nonDefaultIndices, i -> rand.nextInt(maxNumberOfNonDefaults));
		nonDefaultIndices = Arrays.stream(nonDefaultIndices).distinct().sorted().toArray();
		SparseBitmap bitmap = new SparseBitmap(true, nonDefaultIndices, size);

		assertEquals(nonDefaultIndices.length, bitmap.countNonDefaultIndices(nonDefaultIndices));

		int[] readNonDefaultIndices = new int[nonDefaultIndices.length];
		int index = 0;
		for (int i = 0; i < size; i++) {
			if (bitmap.get(i) >= 0) {
				readNonDefaultIndices[index++] = i;
			}
		}
		assertArrayEquals(nonDefaultIndices, readNonDefaultIndices);

		assertEquals(size, bitmap.size());
	}

	@Test
	public void testDefaultIsNotNaN() {
		int maxNumberOfNonDefaults = 1024;
		int[] nonDefaultIndices = new int[maxNumberOfNonDefaults];
		Random rand = new Random();
		int size = 16384;
		Arrays.setAll(nonDefaultIndices, i -> rand.nextInt(maxNumberOfNonDefaults));
		nonDefaultIndices = Arrays.stream(nonDefaultIndices).distinct().sorted().toArray();
		SparseBitmap bitmap = new SparseBitmap(false, nonDefaultIndices, size);

		assertEquals(nonDefaultIndices.length, bitmap.countNonDefaultIndices(nonDefaultIndices));

		int[] readNonDefaultIndices = new int[nonDefaultIndices.length];
		int index = 0;
		for (int i = 0; i < size; i++) {
			if (bitmap.get(i) >= 0) {
				readNonDefaultIndices[index++] = i;
			}
		}
		assertArrayEquals(nonDefaultIndices, readNonDefaultIndices);

		assertEquals(size, bitmap.size());
	}


}
