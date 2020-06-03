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

package com.rapidminer.belt.buffer;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;


/**
 * Tests for {@link Integer53BitBufferSparse}.
 *
 * @author Kevin Majchrzak
 */
public class Integer53BitBufferSparseTests {

	private static final double SPARSITY = 0.1;
	private static final double EPSILON = 1e-10;
	public static final int MAX_RANDOM_INT = 100_000;

	private static double[] random(int n) {
		double[] data = new double[n];
		double defaultValue = Math.random() * MAX_RANDOM_INT;
		Arrays.setAll(data, i -> Math.random() < SPARSITY ? defaultValue : Math.random() * 100_000);
		return data;
	}

	@Test
	public void testSet() {
		int n = 123;
		double[] testData = random(n);
		Integer53BitBufferSparse buffer = Buffers.sparseInteger53BitBuffer(ColumnTestUtils.getMostFrequentValue(testData, -17.3), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(Arrays.stream(testData).map(Math::round).toArray(), readData, EPSILON);
	}

	@Test
	public void testSetInterleaved() {
		int n = 100;
		double defaultValue = -3.9;
		Integer53BitBufferSparse buffer = Buffers.sparseInteger53BitBuffer(defaultValue, n);
		buffer.setNext(17, defaultValue);
		buffer.setNext(20, 213.2);
		buffer.setNext(21, -3.88);
		buffer.setNext(87, 0);
		buffer.setNext(99, 15);
		double[] testData = new double[n];
		Arrays.fill(testData, defaultValue);
		testData[20] = 213.2;
		testData[21] = -3.88;
		testData[87] = 0;
		testData[99] = 15;
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(Arrays.stream(testData).map(Math::round).toArray(), readData, EPSILON);
	}

	@Test
	public void testNumberOfNonDefaults() {
		int n = 100;
		double[] testData = random(n);
		// default value will not be part of test data
		double defaultValue = MAX_RANDOM_INT + 1.3;
		Integer53BitBufferSparse buffer = Buffers.sparseInteger53BitBuffer(defaultValue, n);
		// add default value one..
		buffer.setNext(defaultValue);
		for (int i = 1; i < n / 2; i++) {
			buffer.setNext(testData[i]);
		}
		// two..
		buffer.setNext(defaultValue);
		for (int i = n / 2 + 1; i < n - 1; i++) {
			buffer.setNext(testData[i]);
		}
		// three times
		buffer.setNext(defaultValue);
		// check if default value has been detected three times
		assertEquals(n - 3, buffer.getNumberOfNonDefaults());
	}

	@Test
	public void testGrow() {
		int n = 1_000_000;
		double[] testData = random(n);
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -17.3), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(Arrays.stream(testData).map(Math::round).toArray(), readData, EPSILON);
	}

	@Test
	public void testGrowFullCoverage() {
		// n is chosen relative to the buffers initial chunk
		// size and growth factor to assure full code coverage.
		// Do not change it if you are not sure what you are doing.
		int n = 1124;
		double[] testData = random(n);
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -17.3), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(Arrays.stream(testData).map(Math::round).toArray(), readData, EPSILON);
	}

	@Test
	public void testBufferLength() {
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(Math.random(), 197);
		assertEquals(197, buffer.size());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBufferNegativeLengthTwo() {
		Buffers.sparseInteger53BitBuffer(Math.random(), -5);
	}

	@Test
	public void testZeroBufferLength() {
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(Math.random(), 0);
		assertEquals(0, buffer.size());
	}

	@Test
	public void testToColumnToBuffer() {
		int n = 42;
		double[] testData = random(n);
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -17.3), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		NumericBuffer buffer2 = new Integer53BitBuffer(buffer.toColumn());

		double[] resultData = new double[n];
		for (int i = 0; i < n; i++) {
			resultData[i] = buffer2.get(i);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(readData, resultData, EPSILON);
	}

	@Test(expected = IllegalStateException.class)
	public void testSetAfterFreeze() {
		int n = 12;
		double[] testData = random(n);
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -17.3), n);
		for (int i = 0; i < n - 1; i++) {
			buffer.setNext(testData[i]);
		}
		buffer.toColumn();
		buffer.setNext(testData[n - 1]);
	}

	@Test
	public void testNotFinite() {
		double[] data = {Double.NaN, Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(ColumnTestUtils.getMostFrequentValue(data, -17.3), data.length);
		for (int i = 0; i < data.length; i++) {
			buffer.setNext(data[i]);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(data, readData, EPSILON);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIllegalIndex() {
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(10, 100);
		buffer.setNext(17, 0.3);
		buffer.setNext(0, -2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void duplicateIndex() {
		Integer53BitBufferSparse buffer = new Integer53BitBufferSparse(10, 100);
		buffer.setNext(17, 0.3);
		buffer.setNext(17, 10);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void outOfLowerBounds() {
		Integer53BitBufferSparse buffer = Buffers.sparseInteger53BitBuffer(random(1)[0], 100);
		buffer.setNext(-1, random(1)[0]);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void outOfUpperBounds() {
		Integer53BitBufferSparse buffer = Buffers.sparseInteger53BitBuffer(random(1)[0], 100);
		buffer.setNext(100, random(1)[0]);
	}

}
