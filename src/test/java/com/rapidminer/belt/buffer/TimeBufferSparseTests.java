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

import java.time.LocalTime;
import java.util.Arrays;
import java.util.SplittableRandom;

import org.junit.Test;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;


/**
 * Tests for {@link TimeBufferSparse}.
 *
 * @author Kevin Majchrzak
 */
public class TimeBufferSparseTests {

	private static final double SPARSITY = 0.1;
	private static final double EPSILON = 1e-10;

	private static long[] random(int n) {
		SplittableRandom random = new SplittableRandom();
		long[] data = new long[n];
		long defaultValue = TimeBuffer.MIN_NANO + random.nextLong(TimeBuffer.MAX_NANO - TimeBuffer.MIN_NANO);
		Arrays.setAll(data, i -> Math.random() < SPARSITY ? defaultValue :
				TimeBuffer.MIN_NANO + random.nextLong(TimeBuffer.MAX_NANO - TimeBuffer.MIN_NANO));
		return data;
	}

	@Test
	public void testSetPrimitive() {
		int n = 123;
		long[] testData = random(n);
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(ColumnTestUtils.getMostFrequentValue(testData,
				TimeBuffer.MIN_NANO), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(Arrays.stream(testData).mapToDouble(x -> x).toArray(), readData, EPSILON);
	}

	@Test
	public void testNumberOfNonDefaultsPrimitive() {
		int n = 100;
		long[] testData = random(n);
		long defaultValue = 0;
		// default value will not be part of test data
		testData = Arrays.stream(testData).map(i -> i == defaultValue ? i + 1 : i).toArray();
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(defaultValue, n);
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
	public void testSetInterleaved() {
		int n = 100;
		long defaultValue = 100;
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(defaultValue, n);
		buffer.setNext(17, defaultValue);
		buffer.setNext(20, 213);
		buffer.setNext(21, 388);
		buffer.setNext(87, 0);
		buffer.setNext(99, 15);
		double[] testData = new double[n];
		Arrays.fill(testData, defaultValue);
		testData[20] = 213;
		testData[21] = 388;
		testData[87] = 0;
		testData[99] = 15;
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(testData, readData, EPSILON);
	}

	@Test
	public void testNumberOfNonDefaultsObject() {
		int n = 100;
		long[] testData = random(n);
		long defaultValue = 0;
		// default value will not be part of test data
		testData = Arrays.stream(testData).map(i -> i == defaultValue ? i + 1 : i).toArray();
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(LocalTime.ofNanoOfDay(defaultValue), n);
		// add default value one..
		buffer.setNext(LocalTime.ofNanoOfDay(defaultValue));
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
	public void testSetObject() {
		int n = 123;
		long[] randomData = random(n);
		LocalTime[] testData = Arrays.stream(randomData).mapToObj(LocalTime::ofNanoOfDay).toArray(LocalTime[]::new);
		LocalTime defaultValue = LocalTime.ofNanoOfDay(ColumnTestUtils.getMostFrequentValue(randomData,
				TimeBuffer.MIN_NANO));
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(defaultValue, n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		Object[] readData = new Object[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(testData, readData);
	}

	@Test
	public void testGrow() {
		int n = 1_000_000;
		long[] testData = random(n);
		TimeBufferSparse buffer = new TimeBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -1), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		double[] readData = new double[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(Arrays.stream(testData).mapToDouble(x -> x).toArray(), readData, EPSILON);
	}

	@Test
	public void testGrowFullCoverage() {
		// n is chosen relative to the buffers initial chunk
		// size and growth factor to assure full code coverage.
		// Do not change it if you are not sure what you are doing.
		int n = 1124;
		LocalTime[] testData = new LocalTime[n];
		Arrays.setAll(testData, LocalTime::ofNanoOfDay);
		LocalTime defaultValue = LocalTime.ofNanoOfDay(1);
		TimeBufferSparse buffer = new TimeBufferSparse(defaultValue, n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		Column column = buffer.toColumn();
		LocalTime[] readData = new LocalTime[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(testData, readData);
	}

	@Test
	public void testBufferLength() {
		TimeBufferSparse buffer = new TimeBufferSparse(random(1)[0], 197);
		assertEquals(197, buffer.size());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBufferNegativeLengthOne() {
		new TimeBufferSparse(random(1)[0], -5);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBufferNegativeLengthTwo() {
		Buffers.sparseTimeBuffer(random(1)[0], -5);
	}

	@Test
	public void testZeroBufferLength() {
		TimeBufferSparse buffer = new TimeBufferSparse(random(1)[0], 0);
		assertEquals(0, buffer.size());
	}

	@Test
	public void testToColumnToBuffer() {
		int n = 42;
		long[] testData = random(n);
		TimeBufferSparse buffer = new TimeBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -1), n);
		for (int i = 0; i < n; i++) {
			buffer.setNext(testData[i]);
		}
		TimeBuffer buffer2 = new TimeBuffer(buffer.toColumn());

		LocalTime[] resultData = new LocalTime[n];
		for (int i = 0; i < n; i++) {
			resultData[i] = buffer2.get(i);
		}
		Column column = buffer.toColumn();
		LocalTime[] readData = new LocalTime[column.size()];
		column.fill(readData, 0);
		assertArrayEquals(readData, resultData);
	}

	@Test(expected = IllegalStateException.class)
	public void testSetAfterFreeze() {
		int n = 12;
		long[] testData = random(n);
		TimeBufferSparse buffer = new TimeBufferSparse(ColumnTestUtils.getMostFrequentValue(testData, -1), n);
		for (int i = 0; i < n - 1; i++) {
			buffer.setNext(testData[i]);
		}
		buffer.toColumn();
		buffer.setNext(testData[n - 1]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testIllegalIndex() {
		TimeBufferSparse buffer = new TimeBufferSparse(10, 100);
		buffer.setNext(17, random(1)[0]);
		buffer.setNext(0, random(1)[0]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void duplicateIndex() {
		TimeBufferSparse buffer = new TimeBufferSparse(10, 100);
		buffer.setNext(17, random(1)[0]);
		buffer.setNext(17, random(1)[0]);
	}

	@Test(expected = IllegalArgumentException.class)
	public void toSmallValueConstructor() {
		Buffers.sparseTimeBuffer(TimeBuffer.MIN_NANO - 1, 100);
	}

	@Test(expected = IllegalArgumentException.class)
	public void toLargeValueConstructor() {
		Buffers.sparseTimeBuffer(TimeBuffer.MAX_NANO + 1, 100);
	}

	@Test(expected = IllegalArgumentException.class)
	public void toSmallValueSet() {
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(random(1)[0], 100);
		buffer.setNext(TimeBuffer.MIN_NANO - 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void toLargeValueSet() {
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(random(1)[0], 100);
		buffer.setNext(10, TimeBuffer.MAX_NANO + 1);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void outOfLowerBounds() {
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(random(1)[0], 100);
		buffer.setNext(-1, random(1)[0]);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void outOfUpperBounds() {
		TimeBufferSparse buffer = Buffers.sparseTimeBuffer(random(1)[0], 100);
		buffer.setNext(100, random(1)[0]);
	}

}
