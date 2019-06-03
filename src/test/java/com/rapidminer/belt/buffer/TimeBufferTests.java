/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.StringJoiner;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.column.TimeColumn;


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


		@Test
		public void testBufferLength() {
			TimeBuffer buffer = Buffers.timeBuffer(197);
			assertEquals(197, buffer.size());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLength() {
			Buffers.timeBuffer(-5);
		}


		@Test
		public void testZeroBufferLength() {
			TimeBuffer buffer = Buffers.timeBuffer(0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			TimeBuffer buffer = Buffers.timeBuffer(n);
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

		@Test
		public void testInitialization() {
			int n = 42;
			TimeBuffer initialized = Buffers.timeBuffer(n);
			TimeBuffer notInitialized = Buffers.timeBuffer(n, false);
			for(int i = 0; i<notInitialized.size(); i++){
				notInitialized.set(i, null);
			}

			LocalTime[] expected = new LocalTime[n];
			Arrays.setAll(expected, initialized::get);
			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, notInitialized::get);

			assertArrayEquals(expected, result);
		}


		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			TimeBuffer buffer = Buffers.timeBuffer(n);
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

			TimeBuffer buffer = Buffers.timeBuffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i].toNanoOfDay());
			}

			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(instantData, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveTooHigh() {
			TimeBuffer buffer = Buffers.timeBuffer(4);
			buffer.set(1, LocalTime.MAX.toNanoOfDay() + 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetPrimitiveTooLow() {
			TimeBuffer buffer = Buffers.timeBuffer(4);
			buffer.set(1, LocalTime.MIN.toNanoOfDay() - 1);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetPrimitiveFrozen() {
			TimeBuffer buffer = Buffers.timeBuffer(4);
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
			Column column = ColumnTestUtils.getMappedTimeColumn(longData, dummyMapping);

			TimeBuffer buffer = Buffers.timeBuffer(column);

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

			TimeColumn column = ColumnAccessor.get().newTimeColumn(longData);
			TimeBuffer buffer = Buffers.timeBuffer(column);

			LocalTime[] result = new LocalTime[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(instantData, result);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromCategoricalColumn() {
			int[] data = random(177);
			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnTypes.NOMINAL, data,
					new ArrayList<>());
			Buffers.timeBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromNumericColumn() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[101]);
			Buffers.timeBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromFreeColumn() {
			Column column =
					ColumnAccessor.get().newObjectColumn(ColumnTypes.categoricalType("com.rapidminer.belt.column.test" +
									".objectcolumn",
							Object.class, null), new Object[0]
					);
			Buffers.timeBuffer(column);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFromDateColumn() {
			Column column = ColumnAccessor.get().newDateTimeColumn(new long[10], null);
			Buffers.timeBuffer(column);
		}

		@Test
		public void testToColumn() {
			int n = 312;
			int[] testData = random(n);

			LocalTime[] instantData = Arrays.stream(testData)
					.mapToObj(i -> LocalTime.ofNanoOfDay(29854001230045L + i))
					.toArray(LocalTime[]::new);
			TimeBuffer buffer = Buffers.timeBuffer(n);

			for (int i = 0; i < n; i++) {
				buffer.set(i, instantData[i]);
			}

			TimeColumn column = buffer.toColumn();
			Object[] actual = new Object[n];
			column.fill(actual, 0);

			assertArrayEquals(instantData, actual);
		}

		@Test(expected = NullPointerException.class)
		public void testFromNullColumn() {
			Buffers.timeBuffer(null);
		}
	}

	public static class ToString {

		private TimeBuffer buffer(int length) {
			return Buffers.timeBuffer(length);
		}

		@Test
		public void testToStringSmall() {
			String[] data =
					{"10:15:30.123", "23:23:23.100", "23:23:23.100000101", "11:17",
							"12:13:14.156", "12:34:56.789101", "00:00"};
			TimeBuffer buffer = buffer(8);
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
			TimeBuffer buffer = buffer(32);
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
			TimeBuffer buffer = buffer(length);
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
			TimeBuffer buffer = buffer(length);
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
