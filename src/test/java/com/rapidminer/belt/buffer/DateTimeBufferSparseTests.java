/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import java.time.Instant;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;


/**
 * Tests {@link SecondDateTimeBufferSparse} and {@link NanoDateTimeBufferSparse}.
 *
 * @author Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class DateTimeBufferSparseTests {

	private static final double SPARSITY = 0.1;

	private static final String HIGH_PRECISION = "NanosecondPrecision";
	private static final String LOW_PRECISION = "SecondPrecision";

	private static DateTimeBufferSparse buffer(long defaultValue, int length, String format) {
		if (HIGH_PRECISION.equals(format)) {
			return Buffers.sparseDateTimeBuffer(defaultValue, length, true);
		} else {
			return Buffers.sparseDateTimeBuffer(defaultValue, length, false);
		}
	}

	private static DateTimeBufferSparse buffer(Instant defaultValue, int length, String format) {
		if (HIGH_PRECISION.equals(format)) {
			return Buffers.sparseDateTimeBuffer(defaultValue, length, true);
		} else {
			return Buffers.sparseDateTimeBuffer(defaultValue, length, false);
		}
	}

	private static int[] random(int n) {
		int[] data = new int[n];
		Random random = new Random();
		int defaultValue = random.nextInt();
		Arrays.setAll(data, i -> Math.random() < SPARSITY ? defaultValue : random.nextInt());
		return data;
	}


	@RunWith(Parameterized.class)
	public static class ValueStoring {

		@Parameter
		public String format;

		@Parameters(name = "{0}")
		public static Iterable<String> tests() {
			return Arrays.asList(HIGH_PRECISION, LOW_PRECISION);
		}

		private DateTimeBufferSparse buffer(long defaultValue, int length) {
			return DateTimeBufferSparseTests.buffer(defaultValue, length, format);
		}

		private DateTimeBufferSparse buffer(Instant defaultValue, int length) {
			return DateTimeBufferSparseTests.buffer(defaultValue, length, format);
		}

		@Test
		public void testBufferLength() {
			DateTimeBufferSparse buffer = buffer(DateTimeBuffer.MIN_SECOND, 197);
			assertEquals(197, buffer.size());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLengthOne() {
			buffer(DateTimeBuffer.MIN_SECOND, -5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLengthTwo() {
			new SecondDateTimeBufferSparse(DateTimeBuffer.MIN_SECOND, -5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLengthThree() {
			new NanoDateTimeBufferSparse(DateTimeBuffer.MIN_SECOND, -5);
		}

		@Test
		public void testZeroBufferLength() {
			DateTimeBufferSparse buffer = buffer(DateTimeBuffer.MIN_SECOND, 0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			DateTimeBufferSparse buffer = buffer(
					Instant.ofEpochSecond(ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L), n);
			for (int i = 0; i < n - 4; i++) {
				buffer.setNext(i, Instant.ofEpochSecond(1526634325L + testData[i]));
			}
			buffer.setNext(1526634325L + testData[n - 4], 0);
			buffer.setNext(Instant.ofEpochSecond(1526634325L + testData[n - 3]));
			buffer.setNext(n - 2, null);
			buffer.setNext(n - 1, 1526634325L + testData[n - 1]);

			Instant[] expected = Arrays.stream(testData).mapToObj(i -> Instant.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			expected[n - 2] = null;

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testSetInterleaved() {
			int n = 100;
			long defaultValue = 100;
			DateTimeBufferSparse buffer = buffer(defaultValue, n);
			buffer.setNext(17, defaultValue);
			buffer.setNext(20, 213L);
			buffer.setNext(21, 388L);
			buffer.setNext(87, 0L);
			buffer.setNext(99, 15L);
			Object[] testData = new Object[n];
			Arrays.fill(testData, Instant.ofEpochSecond(defaultValue));
			testData[20] = Instant.ofEpochSecond(213);
			testData[21] = Instant.ofEpochSecond(388);
			testData[87] = Instant.ofEpochSecond(0);
			testData[99] = Instant.ofEpochSecond(15);
			Column column = buffer.toColumn();
			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);
			assertArrayEquals(testData, result);
		}

		@Test
		public void testNumberOfNonDefaultsPrimitive() {
			int n = 100;
			int[] testData = random(n);
			long defaultValue = 1526634325L;
			// default value will not be part of test data
			testData = Arrays.stream(testData).map(i -> i == defaultValue ? i + 1 : i).toArray();
			DateTimeBufferSparse buffer = buffer(defaultValue, n);
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
		public void testNumberOfNonDefaultsObject() {
			int n = 100;
			int[] testData = random(n);
			long defaultValue = 1526634325L;
			// default value will not be part of test data
			testData = Arrays.stream(testData).map(i -> i == defaultValue ? i + 1 : i).toArray();
			DateTimeBufferSparse buffer = buffer(Instant.ofEpochSecond(defaultValue), n);
			// add default value one..
			buffer.setNext(Instant.ofEpochSecond(defaultValue));
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

		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			DateTimeBufferSparse buffer = buffer(Instant.EPOCH.getEpochSecond(), n);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, Instant.EPOCH);
			}
			buffer.toColumn();
			buffer.setNext(5, Instant.MAX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testIllegalIndex() {
			DateTimeBufferSparse buffer = buffer(Instant.EPOCH.getEpochSecond(), 100);
			buffer.setNext(17, Instant.MAX);
			buffer.setNext(0, Instant.MAX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void duplicateIndex() {
			DateTimeBufferSparse buffer = buffer(Instant.EPOCH.getEpochSecond(), 100);
			buffer.setNext(17, Instant.MAX);
			buffer.setNext(17, Instant.MAX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void outOfLowerBounds() {
			DateTimeBufferSparse buffer = buffer(Instant.EPOCH.getEpochSecond(), 100);
			buffer.setNext(-1, Instant.MAX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void outOfUpperBounds() {
			DateTimeBufferSparse buffer = buffer(Instant.EPOCH.getEpochSecond(), 100);
			buffer.setNext(100, Instant.MAX);
		}

	}

	public static class NanosecondPrecision {

		@Test
		public void testSetPrimitive() {
			int n = 14;
			int[] testData = random(n);

			Instant[] expected = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					Instant.ofEpochSecond(ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L), n, true);
			for (int i = 0; i < n; i++) {
				buffer.setNext(expected[i].getEpochSecond(), expected[i].getNano());
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testGrow() {
			int n = 1_000_000;
			int[] testData = random(n);

			Instant[] expected = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, true);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, expected[i].getEpochSecond(), expected[i].getNano());
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testSetPrimitiveWithoutNanos() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, true);
			for (int i = 0; i < n; i++) {
				buffer.setNext(instantData[i].getEpochSecond());
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooLow() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4, true);
			buffer.setNext(1, DateTimeBuffer.MIN_SECOND - 1, 0);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooHigh() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4, true);
			buffer.setNext(1, DateTimeBuffer.MAX_SECOND + 1, 0);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveNanosTooHigh() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4, true);
			buffer.setNext(1, 0, 1_000_000_000);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorPrimitiveSecondsTooLow() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND - 1,
					4, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorPrimitiveSecondsTooHigh() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MAX_SECOND + 1,
					4, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveNanosTooLow() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4,
					true);
			buffer.setNext(1, 0, -1);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetPrimitiveFrozen() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4,
					true);
			buffer.toColumn();
			buffer.setNext(1, 0, 1);
		}

		@Test
		public void testSetInstantPrecision() {
			int n = 14;
			int[] testData = random(n);

			Instant[] expected = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, true);
			for (int i = 0; i < n - 1; i++) {
				buffer.setNext(i, expected[i]);
			}
			buffer.setNext(expected[n - 1]);

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testToColumnPrecision() {
			int n = 312;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, true);

			for (int i = 0; i < n; i++) {
				buffer.setNext(i, instantData[i]);
			}

			Column column = buffer.toColumn();
			Object[] actual = new Object[n];
			column.fill(actual, 0);

			assertArrayEquals(instantData, actual);
		}
	}

	public static class SecondPrecision {

		@Test
		public void testSetPrimitive() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, false);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, instantData[i].getEpochSecond());
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test
		public void testGrow() {
			int n = 1_000_000;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, false);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, instantData[i].getEpochSecond());
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test
		public void testSetPrimitiveWithNanos() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, false);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, instantData[i].getEpochSecond(), instantData[i].getNano());
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooLow() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MAX_SECOND, 4, false);
			buffer.setNext(1, DateTimeBuffer.MIN_SECOND - 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooHigh() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MAX_SECOND, 4,
					false);
			buffer.setNext(1, DateTimeBuffer.MAX_SECOND + 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorPrimitiveSecondsTooLow() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND - 1,
					4, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorPrimitiveSecondsTooHigh() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MAX_SECOND + 1,
					4, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveNanosTooLow() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4,
					false);
			buffer.setNext(1, 0, -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveNanosTooHigh() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MIN_SECOND, 4, false);
			buffer.setNext(1, 0, 1_000_000_000);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetPrimitiveFrozen() {
			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(DateTimeBuffer.MAX_SECOND, 4,
					false);
			buffer.toColumn();
			buffer.setNext(1, 1L);
		}

		@Test
		public void testSetInstantPrecision() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, Math.abs(424749700 + i) % (NanosecondDateTimeBuffer.MAX_NANO + 1)))
					.toArray(Instant[]::new);

			DateTimeBufferSparse buffer = Buffers.sparseDateTimeBuffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, false);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, instantData[i]);
			}

			Object[] result = new Object[n];
			buffer.toColumn().fill(result, 0);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

	}


	@RunWith(Parameterized.class)
	public static class Conversion {

		@Parameter
		public String format;


		@Parameters(name = "{0}")
		public static Iterable<String> tests() {
			return Arrays.asList(HIGH_PRECISION, LOW_PRECISION);
		}

		@Test
		public void testToColumn() {
			int n = 312;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			DateTimeBufferSparse buffer = buffer(
					ColumnTestUtils.getMostFrequentValue(testData, 0) + 1526634325L, n, format);

			for (int i = 0; i < n; i++) {
				buffer.setNext(i, instantData[i]);
			}

			Column column = buffer.toColumn();
			Object[] actual = new Object[n];
			column.fill(actual, 0);

			assertArrayEquals(instantData, actual);
		}

	}

}
