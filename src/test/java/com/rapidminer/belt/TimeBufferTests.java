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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.StringJoiner;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class TimeBufferTests {


	private static int[] random(int n) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt());
		return data;
	}


	public static class ValueStoring {

		private TimeColumnBuffer buffer(int length) {
			return new TimeColumnBuffer(length);
		}

		@Test
		public void testBufferLength() {
			TimeColumnBuffer buffer = buffer(197);
			assertEquals(197, buffer.size());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLength() {
			buffer(-5);
		}


		@Test
		public void testZeroBufferLength() {
			TimeColumnBuffer buffer = buffer(0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			TimeColumnBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, LocalTime.ofNanoOfDay(29854001230045L + testData[i]));
			}
			buffer.set(42, null);

			LocalTime[] expected = Arrays.stream(testData).mapToObj(i -> LocalTime.ofNanoOfDay(29854001230045L + i))
					.toArray(LocalTime[]::new);
			expected[42] = null;

			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(expected, result);
		}


		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			TimeColumnBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, LocalTime.NOON);
			}
			buffer.toColumn();
			buffer.set(5, LocalTime.MIDNIGHT);
		}


		@Test
		public void testSetPrimitive() {
			int n = 14;
			int[] testData = random(n);

			LocalTime[] instantData = Arrays.stream(testData)
					.mapToObj(i -> LocalTime.ofNanoOfDay(29854001230045L + i))
					.toArray(LocalTime[]::new);

			TimeColumnBuffer buffer = new TimeColumnBuffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i].toNanoOfDay());
			}

			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(instantData, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveTooHigh() {
			TimeColumnBuffer buffer = new TimeColumnBuffer(4);
			buffer.set(1, LocalTime.MAX.toNanoOfDay() + 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveTooLow() {
			TimeColumnBuffer buffer = new TimeColumnBuffer(4);
			buffer.set(1, LocalTime.MIN.toNanoOfDay() - 1);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetPrimitiveFrozen() {
			TimeColumnBuffer buffer = new TimeColumnBuffer(4);
			buffer.toColumn();
			buffer.set(1, 1);
		}
	}


	public static class Conversion {


		@Test
		public void testFromMappedColumn() {
			int n = 112;
			int[] testData = random(n);

			LocalTime[] instantData = Arrays.stream(testData)
					.mapToObj(i -> LocalTime.ofNanoOfDay(29854001230045L + i))
					.toArray(LocalTime[]::new);
			long[] longData = Arrays.stream(instantData).mapToLong(LocalTime::toNanoOfDay).toArray();

			int[] dummyMapping = new int[longData.length];
			Arrays.setAll(dummyMapping, i -> i);
			MappedTimeColumn column = new MappedTimeColumn(longData, dummyMapping);

			TimeColumnBuffer buffer = new TimeColumnBuffer(column);

			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(instantData, result);
		}

		@Test
		public void testFromNormalColumn() {
			int n = 112;
			int[] testData = random(n);

			LocalTime[] instantData = Arrays.stream(testData)
					.mapToObj(i -> LocalTime.ofNanoOfDay(29854001230045L + i))
					.toArray(LocalTime[]::new);
			long[] longData = Arrays.stream(instantData).mapToLong(LocalTime::toNanoOfDay).toArray();

			TimeColumn column = new TimeColumn(longData);
			TimeColumnBuffer buffer = new TimeColumnBuffer(column);

			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(instantData, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromCategoricalColumn() {
			int[] data = random(177);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			new TimeColumnBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromNumericColumn() {
			Column column = new DoubleArrayColumn(new double[101]);
			new TimeColumnBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromFreeColumn() {
			Column column =
					new SimpleFreeColumn<>(ColumnTypes.categoricalType("com.rapidminer.belt.column.test.objectcolumn",
							Object.class, null), new Object[0]
					);
			new TimeColumnBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromDateColumn() {
			Column column = new DateTimeColumn(new long[10]);
			new TimeColumnBuffer(column);
		}

		@Test
		public void testToColumn() {
			int n = 312;
			int[] testData = random(n);

			LocalTime[] instantData = Arrays.stream(testData)
					.mapToObj(i -> LocalTime.ofNanoOfDay(29854001230045L + i))
					.toArray(LocalTime[]::new);
			TimeColumnBuffer buffer = new TimeColumnBuffer(n);

			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i]);
			}

			TimeColumn column = buffer.toColumn();
			Object[] actual = new Object[n];
			column.fill(actual, 0);

			assertArrayEquals(instantData, actual);
		}

	}

	public static class ToString {

		private TimeColumnBuffer buffer(int length) {
			return new TimeColumnBuffer(length);
		}

		@Test
		public void testToStringSmall() {
			String[] data =
					{"10:15:30.123", "23:23:23.100", "23:23:23.100000101", "11:17",
							"12:13:14.156", "12:34:56.789101", "00:00"};
			TimeColumnBuffer buffer = buffer(8);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, LocalTime.parse(data[i]));
			}
			buffer.set(7, null);

			StringBuilder builder = new StringBuilder("Time Buffer (8)\n(");
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
					{"10:15:30.123", "23:23:23.100", "23:23:23.100000101", "11:17",
							"12:13:14.156", "12:34:56.789101", "00:00", "11:11:11"};
			TimeColumnBuffer buffer = buffer(32);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, LocalTime.parse(datablock[i % datablock.length]));
			}

			StringJoiner joiner = new StringJoiner(", ", "Time Buffer (32)\n(", ")");
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
					{"10:15:30.123", "23:23:23.100", "23:23:23.100000101", "11:17",
							"12:13:14.156", "12:34:56.789101", "00:00", "11:11:11"};
			int length = 33;
			TimeColumnBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, LocalTime.parse(datablock[i % datablock.length]));
			}
			buffer.set(buffer.size() - 1, LocalTime.parse(datablock[0]));


			StringJoiner joiner = new StringJoiner(", ", "Time Buffer (33)\n(", ")");
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
					{"10:15:30.123", "23:23:23.100", "23:23:23.100000101", "11:17",
							"12:13:14.156", "12:34:56.789101", "00:00", "11:11:11"};
			int length = 33;
			TimeColumnBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, LocalTime.parse(datablock[i % datablock.length]));
			}
			buffer.set(buffer.size() - 1, null);

			StringJoiner joiner = new StringJoiner(", ", "Time Buffer (33)\n(", ")");
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
