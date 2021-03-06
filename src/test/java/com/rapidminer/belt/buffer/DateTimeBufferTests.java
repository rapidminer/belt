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

package com.rapidminer.belt.buffer;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.StringJoiner;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class DateTimeBufferTests {

	private static final String HIGH_PRECISION = "NanosecondPrecision";
	private static final String LOW_PRECISION = "SecondPrecision";


	private static DateTimeBuffer buffer(int length, String format) {
		if (HIGH_PRECISION.equals(format)) {
			return Buffers.dateTimeBuffer(length, true);
		} else {
			return Buffers.dateTimeBuffer(length, false);
		}
	}

	private static DateTimeBuffer buffer(int length, String format, boolean initialize) {
		if (HIGH_PRECISION.equals(format)) {
			return Buffers.dateTimeBuffer(length, true, initialize);
		} else {
			return Buffers.dateTimeBuffer(length, false, initialize);
		}
	}

	private static int[] random(int n) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt());
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

		private DateTimeBuffer buffer(int length) {
			return DateTimeBufferTests.buffer(length, format);
		}

		@Test
		public void testBufferLength() {
			DateTimeBuffer buffer = buffer(197);
			assertEquals(197, buffer.size());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLength() {
			buffer(-5);
		}


		@Test
		public void testZeroBufferLength() {
			DateTimeBuffer buffer = buffer(0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			DateTimeBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, Instant.ofEpochSecond(1526634325L + testData[i]));
			}
			buffer.set(42, null);

			Instant[] expected = Arrays.stream(testData).mapToObj(i -> Instant.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			expected[42] = null;

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(expected, result);
		}


		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			DateTimeBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, Instant.EPOCH);
			}
			buffer.toColumn();
			buffer.set(5, Instant.MAX);
		}

		@Test
		public void testInitialization() {
			int n = 42;
			DateTimeBuffer initialized = buffer(n);
			DateTimeBuffer notInitialized = DateTimeBufferTests.buffer(n, format, false);
			for(int i = 0; i< notInitialized.size(); i++){
				notInitialized.set(i, null);
			}

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, initialized::get);

			Instant[] result = new Instant[n];
			Arrays.setAll(result, notInitialized::get);

			assertArrayEquals(expected, result);
		}

	}

	public static class NanosecondPrecision {

		@Test
		public void testSetPrimitive() {
			int n = 14;
			int[] testData = random(n);

			Instant[] expected = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, true);
			for (int i = 0; i < n; i++) {
				buffer.set(i, expected[i].getEpochSecond(), expected[i].getNano());
			}

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testSetPrimitiveWithoutNanos() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, true);
			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i].getEpochSecond());
			}

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooHigh() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, true);
			buffer.set(1, DateTimeBuffer.MIN_SECOND - 1, 0);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooLow() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, true);
			buffer.set(1, DateTimeBuffer.MAX_SECOND + 1, 0);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveNanosTooHigh() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, true);
			buffer.set(1, 0, 1_000_000_000);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveNanosTooLow() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, true);
			buffer.set(1, 0, -1);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetPrimitiveFrozen() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, true);
			buffer.toColumn();
			buffer.set(1, 0, 1);
		}

		@Test
		public void testSetInstantPrecision() {
			int n = 14;
			int[] testData = random(n);

			Instant[] expected = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, true);
			for (int i = 0; i < n; i++) {
				buffer.set(i, expected[i]);
			}

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testFromHighPrecisionColumn() {
			int n = 112;
			int[] testData = random(n);
			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);
			long[] longData = Arrays.stream(instantData).mapToLong(Instant::getEpochSecond).toArray();
			int[] intData = Arrays.stream(instantData).mapToInt(Instant::getNano).toArray();

			DateTimeColumn column = ColumnAccessor.get().newDateTimeColumn(longData, intData);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(column);

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(instantData, result);
		}

		@Test
		public void testToColumnPrecision() {
			int n = 312;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, true);

			for(int i = 0; i<n; i++){
				buffer.set(i, instantData[i]);
			}

			DateTimeColumn column = buffer.toColumn();
			Object[] actual = new Object[n];
			column.fill(actual, 0);

			assertArrayEquals(instantData, actual);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			Buffers.dateTimeBuffer(null);
		}
	}

	public static class SecondPrecision {

		@Test
		public void testSetPrimitive() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, false);
			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i].getEpochSecond());
			}

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

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
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, false);
			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i].getEpochSecond(), instantData[i].getNano());
			}

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooHigh() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, false);
			buffer.set(1, DateTimeBuffer.MIN_SECOND - 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveSecondsTooLow() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, false);
			buffer.set(1, DateTimeBuffer.MAX_SECOND + 1);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetPrimitiveFrozen() {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(4, false);
			buffer.toColumn();
			buffer.set(1, 1);
		}

		@Test
		public void testSetInstantPrecision() {
			int n = 14;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i, 424749700 + i))
					.toArray(Instant[]::new);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(n, false);
			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i]);
			}

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

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
		public void testFromLowPrecisionMappedColumn() {
			int n = 112;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			long[] longData = Arrays.stream(instantData).mapToLong(Instant::getEpochSecond).toArray();

			int[] dummyMapping = new int[longData.length];
			Arrays.setAll(dummyMapping, i -> i);
			Column column = ColumnTestUtils.getMappedDateTimeColumn(longData, null, dummyMapping);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(column);

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test
		public void testFromHighPrecisionMappedColumn() {
			int n = 112;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			long[] longData = Arrays.stream(instantData).mapToLong(Instant::getEpochSecond).toArray();
			int[] intData = Arrays.stream(instantData).mapToInt(Instant::getNano).toArray();

			int[] dummyMapping = new int[longData.length];
			Arrays.setAll(dummyMapping, i -> i);
			Column column = ColumnTestUtils.getMappedDateTimeColumn(longData, intData, dummyMapping);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(column);

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test
		public void testFromLowPrecisionColumn() {
			int n = 112;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant
							.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			long[] longData = Arrays.stream(instantData).mapToLong(Instant::getEpochSecond).toArray();

			DateTimeColumn column = ColumnAccessor.get().newDateTimeColumn(longData, null);

			DateTimeBuffer buffer = Buffers.dateTimeBuffer(column);

			Instant[] result = new Instant[n];
			Arrays.setAll(result, buffer::get);

			Instant[] expected = new Instant[n];
			Arrays.setAll(expected, i -> Instant.ofEpochSecond(instantData[i].getEpochSecond()));
			assertArrayEquals(expected, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromCategoricalColumn() {
			int[] data = random(177);
			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, new ArrayList<>());
			Buffers.dateTimeBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromNumericColumn() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[101]);
			Buffers.dateTimeBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromFreeColumn() {
			Column column =
					ColumnAccessor.get().newObjectColumn(ColumnTestUtils.categoricalType(
							Object.class, null), new Object[0]
					);
			Buffers.dateTimeBuffer(column);
		}

		@Test
		public void testToColumn(){
			int n = 312;
			int[] testData = random(n);

			Instant[] instantData = Arrays.stream(testData)
					.mapToObj(i -> Instant.ofEpochSecond(1526634325L + i))
					.toArray(Instant[]::new);
			DateTimeBuffer buffer = buffer(n,format);

			for(int i = 0; i<n; i++){
				buffer.set(i, instantData[i]);
			}

			DateTimeColumn column = buffer.toColumn();
			Object[] actual = new Object[n];
			column.fill(actual, 0);

			assertArrayEquals(instantData, actual);
		}

	}

	@RunWith(Parameterized.class)
	public static class ToString {

		@Parameter
		public String format;

		@Parameters(name = "{0}")
		public static Iterable<String> tests() {
			return Arrays.asList(HIGH_PRECISION, LOW_PRECISION);
		}

		private DateTimeBuffer buffer(int length) {
			return DateTimeBufferTests.buffer(length, format);
		}

		@Test
		public void testToStringSmall() {
			String[] data =
					{"2018-05-18T09:05:25Z", "2018-01-18T09:05:25Z", "2018-05-18T06:05:25Z", "2018-05-18T09:05:25Z",
							"2010-05-18T09:05:25Z", "2018-05-18T09:05:24Z", "2017-05-17T09:05:25Z"};
			DateTimeBuffer buffer = buffer(8);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, Instant.parse(data[i]));
			}
			buffer.set(7, null);

			StringBuilder builder = new StringBuilder("Date-Time Buffer (8)\n(");
			for (String datum : data) {
				builder.append(datum);
				builder.append(", ");
			}
			builder.append("?)");

			assertEquals(builder.toString(), buffer.toString());
		}

		@Test
		public void testToStringMaxFit() {
			String[] datablock =
					{"2018-05-18T09:05:25Z", "2018-01-18T09:05:25Z", "2018-05-18T06:05:25Z", "2018-05-18T09:05:25Z",
							"2010-05-18T09:05:25Z", "2018-05-18T09:05:24Z", "2017-05-17T09:05:25Z",
							"2017-03-11T09:05:25Z"};
			DateTimeBuffer buffer = buffer(32);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, Instant.parse(datablock[i % datablock.length]));
			}

			StringJoiner joiner = new StringJoiner(", ", "Date-Time Buffer (32)\n(", ")");
			for (int i = 0; i < 4; i++) {
				for (String datum : datablock) {
					joiner.add(datum);
				}
			}

			assertEquals(joiner.toString(), buffer.toString());
		}

		@Test
		public void testToStringBigger() {
			String[] datablock =
					{"2018-05-18T09:05:25Z", "2018-01-18T09:05:25Z", "2018-05-18T06:05:25Z", "2018-05-18T09:05:25Z",
							"2010-05-18T09:05:25Z", "2018-05-18T09:05:24Z", "2017-05-17T09:05:25Z",
							"2017-03-11T09:05:25Z"};
			int length = 33;
			DateTimeBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, Instant.parse(datablock[i % datablock.length]));
			}
			buffer.set(buffer.size() - 1, Instant.parse(datablock[0]));


			StringJoiner joiner = new StringJoiner(", ", "Date-Time Buffer (33)\n(", ")");
			for (int i = 0; i < 3; i++) {
				for (String datum : datablock) {
					joiner.add(datum);
				}
			}
			for (int i = 0; i < datablock.length - 2; i++) {
				joiner.add(datablock[i]);
			}
			joiner.add("...");
			joiner.add(datablock[0]);

			assertEquals(joiner.toString(), buffer.toString());
		}

		@Test
		public void testToStringBiggerLastMissing() {
			String[] datablock =
					{"2018-05-18T09:05:25Z", "2018-01-18T09:05:25Z", "2018-05-18T06:05:25Z", "2018-05-18T09:05:25Z",
							"2010-05-18T09:05:25Z", "2018-05-18T09:05:24Z", "2017-05-17T09:05:25Z",
							"2017-03-11T09:05:25Z"};
			int length = 33;
			DateTimeBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, Instant.parse(datablock[i % datablock.length]));
			}
			buffer.set(buffer.size() - 1, null);

			StringJoiner joiner = new StringJoiner(", ", "Date-Time Buffer (33)\n(", ")");
			for (int i = 0; i < 3; i++) {
				for (String datum : datablock) {
					joiner.add(datum);
				}
			}
			for (int i = 0; i < datablock.length - 2; i++) {
				joiner.add(datablock[i]);
			}
			joiner.add("...");
			joiner.add("?");

			assertEquals(joiner.toString(), buffer.toString());

		}
	}

}
