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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class ObjectColumnReaderTests {

	private static final String CATEGORICAL = "categorical";
	private static final String FREE = "free";

	private static String[] numbers(int n) {
		String[] numbers = new String[n];
		Arrays.setAll(numbers, Integer::toString);
		return numbers;
	}

	private static void readAll(ObjectColumnReader<?> reader) {
		while (reader.hasRemaining()) {
			reader.read();
		}
	}

	private static String[] readAllToArray(ObjectColumnReader<String> reader) {
		String[] result = new String[reader.remaining()];
		int i = 0;
		while (reader.hasRemaining()) {
			result[i++] = reader.read();
		}
		return result;
	}

	private static Column column(String columnImplementation, String[] data) {
		switch (columnImplementation) {
			case CATEGORICAL:
				List<String> mapping = new ArrayList<>(Arrays.asList(data));
				mapping.add(0, null);
				int[] categories = new int[data.length];
				Arrays.setAll(categories, i -> i + 1);
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, categories, mapping);
			case FREE:
				return new SimpleFreeColumn<>(ColumnTypes.freeType("com.rapidminer.belt.column.test.stringcolumn",
						String.class, String::compareTo), data);
			default:
				throw new IllegalStateException("Unknown column implementation");
		}
	}

	@RunWith(Parameterized.class)
	public static class Reading {

		@Parameter
		public String column;

		@Parameters(name = "{0}")
		public static Iterable<String> column() {
			return Arrays.asList(CATEGORICAL, FREE);
		}

		private Column column(String[] data) {
			return ObjectColumnReaderTests.column(column, data);
		}

		@Test
		public void testReadingFromSingleCompleteBuffer() {
			String[] input = numbers(ColumnReader.DEFAULT_BUFFER_SIZE);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column(input), String.class);
			String[] output = readAllToArray(reader);

			assertArrayEquals(input, output);
		}

		@Test
		public void testReadingTooSmallBuffer() {
			String[] input = numbers(ColumnReader.DEFAULT_BUFFER_SIZE);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column(input), String.class, 1, input.length);
			String[] output = readAllToArray(reader);

			assertArrayEquals(input, output);
		}

		@Test
		public void testReadingFromSingleIncompleteBuffer() {
			String[] input = numbers((int) (0.67 * ColumnReader.DEFAULT_BUFFER_SIZE));
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column(input), String.class, ColumnReader.DEFAULT_BUFFER_SIZE, input.length);
			String[] output = readAllToArray(reader);

			assertArrayEquals(input, output);
		}

		@Test
		public void testReadingFromMultipleCompleteBuffers() {
			String[] input = numbers(5 * ColumnReader.DEFAULT_BUFFER_SIZE);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column(input), String.class, ColumnReader.DEFAULT_BUFFER_SIZE, input.length);
			String[] output = readAllToArray(reader);

			assertArrayEquals(input, output);
		}

		@Test
		public void testReadingFromMultipleIncompleteBuffers() {
			String[] input = numbers((int) (3.33 * ColumnReader.DEFAULT_BUFFER_SIZE));
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column(input), String.class, ColumnReader.DEFAULT_BUFFER_SIZE, input.length);
			String[] output = readAllToArray(reader);

			assertArrayEquals(input, output);
		}

	}

	public static class Interaction {

		@Test
		public void testColumnInteractionWithSingleBuffer() {
			Column column = spy(column(CATEGORICAL, new String[ColumnReader.DEFAULT_BUFFER_SIZE]));
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, ColumnReader.DEFAULT_BUFFER_SIZE, column.size());

			readAll(reader);

			verify(column).fill(any(Object[].class), eq(0));
			verify(column, times(1)).fill(any(Object[].class), anyInt());
		}

		@Test
		public void testColumnInteractionWithMultipleBuffers() {
			Column column = spy(column(CATEGORICAL, new String[3 * ColumnReader.DEFAULT_BUFFER_SIZE]));
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, ColumnReader.DEFAULT_BUFFER_SIZE, column.size());
			readAll(reader);

			verify(column).fill(any(Object[].class), eq(0));
			verify(column).fill(any(Object[].class), eq(ColumnReader.DEFAULT_BUFFER_SIZE));
			verify(column).fill(any(Object[].class), eq(2 * ColumnReader.DEFAULT_BUFFER_SIZE));
			verify(column, times(3)).fill(any(Object[].class), anyInt());
		}
	}

	@RunWith(Parameterized.class)
	public static class Remaining {

		@Parameter
		public String column;

		@Parameters(name = "{0}")
		public static Iterable<String> column() {
			return Arrays.asList(CATEGORICAL, FREE);
		}

		private Column column(String[] data) {
			return ObjectColumnReaderTests.column(column, data);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			new ObjectColumnReader<>(null, Boolean.class, 0);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeColumn() {
			new ObjectColumnReader<>(column(new String[3]), Boolean.class);
		}

		@Test
		public void testInitialRemainder() {
			int n = 64;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, column.size());

			assertEquals(n, reader.remaining());
		}

		@Test
		public void testRemainder() {
			int n = 64;
			int reads = 16;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, column.size());
			for (int i = 0; i < reads; i++) {
				reader.read();
			}

			assertEquals(n - reads, reader.remaining());
		}

		@Test
		public void testFinalRemainder() {
			int n = 64;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, column.size());
			readAll(reader);

			assertEquals(0, reader.remaining());
		}
	}

	@RunWith(Parameterized.class)
	public static class Position {

		@Parameter
		public String column;

		@Parameters(name = "{0}")
		public static Iterable<String> column() {
			return Arrays.asList(CATEGORICAL, FREE);
		}

		private Column column(String[] data) {
			return ObjectColumnReaderTests.column(column, data);
		}

		@Test
		public void testGet() {
			int n = 64;
			int reads = 16;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, column.size());
			for (int i = 0; i < reads; i++) {
				reader.read();
			}
			assertEquals(reads - 1, reader.position());
		}

		@Test
		public void testGetBufferSmaller() {
			int n = 64;
			int reads = 16;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			for (int i = 0; i < reads; i++) {
				reader.read();
			}
			assertEquals(reads - 1, reader.position());
		}

		@Test
		public void testGetAtStart() {
			int n = 64;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, column.size());
			assertEquals(Row.BEFORE_FIRST, reader.position());
		}

		@Test
		public void testGetAtEnd() {
			int n = 64;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, column.size());
			while (reader.hasRemaining()) {
				reader.read();
			}
			assertEquals(column.size() - 1, reader.position());
		}

		@Test
		public void testGetAtEndBufferSmaller() {
			int n = 64;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			while (reader.hasRemaining()) {
				reader.read();
			}
			assertEquals(column.size() - 1, reader.position());
		}

		@Test
		public void testSet() {
			int n = 64;
			int position = 16;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(Integer.toString(position + 1), reader.read());
			assertEquals(Integer.toString(position + 2), reader.read());
		}

		@Test
		public void testSetAfterReads() {
			int n = 64;
			int position = 16;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(Integer.toString(position + 1), reader.read());
			assertEquals(Integer.toString(position + 2), reader.read());
		}

		@Test
		public void testSetAfterReadsSmallerInSameBuffer() {
			int n = 64;
			int position = 11;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(Integer.toString(position + 1), reader.read());
			assertEquals(Integer.toString(position + 2), reader.read());
		}

		@Test
		public void testSetAfterReadsSmaller() {
			int n = 64;
			int position = 3;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(Integer.toString(position + 1), reader.read());
			assertEquals(Integer.toString(position + 2), reader.read());
		}

		@Test
		public void testSetBefore() {
			int n = 64;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			reader.setPosition(Row.BEFORE_FIRST);
			assertEquals(-1, reader.position());
			assertEquals(Integer.toString(0), reader.read());
			assertEquals(Integer.toString(1), reader.read());
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testSetNegative() {
			int n = 64;
			Column column = column(new String[n]);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			reader.setPosition(-5);
		}


		@Test
		public void testSetEnd() {
			int n = 64;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			reader.setPosition(n - 2);
			assertEquals(Integer.toString(n - 1), reader.read());
			assertEquals(n - 1, reader.position());
		}

		@Test
		public void testSetBeforeAfterReads() {
			int n = 64;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(Row.BEFORE_FIRST);
			assertEquals(Row.BEFORE_FIRST, reader.position());
			assertEquals(Integer.toString(0), reader.read());
			assertEquals(Integer.toString(1), reader.read());
		}

		@Test
		public void testSetEndAfterReads() {
			int n = 64;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(n - 2);
			assertEquals(Integer.toString(n - 1), reader.read());
			assertEquals(n - 1, reader.position());
		}

		@Test
		public void testSetMultipleTimes() {
			int n = 64;
			String[] testArray = numbers(n);
			Column column = column(testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			reader.setPosition(16);
			assertEquals(16, reader.position());
			assertEquals(Integer.toString(17), reader.read());
			reader.setPosition(18);
			assertEquals(18, reader.position());
			assertEquals(Integer.toString(19), reader.read());
			reader.setPosition(11);
			assertEquals(11, reader.position());
			assertEquals(Integer.toString(12), reader.read());
			reader.setPosition(11);
			assertEquals(11, reader.position());
			assertEquals(Integer.toString(12), reader.read());
			reader.setPosition(25);
			assertEquals(25, reader.position());
			assertEquals(Integer.toString(26), reader.read());
			reader.setPosition(23);
			assertEquals(23, reader.position());
			assertEquals(Integer.toString(24), reader.read());
		}
	}

	public static class ToString {

		@Test
		public void testStart() {
			int nRows = 64;
			String[] testArray = new String[nRows];
			Column column = column(FREE, testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			String expected = "Object column reader (" + nRows + ")\n" + "Position: " + Row.BEFORE_FIRST;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testMiddle() {
			int nRows = 64;
			String[] testArray = new String[nRows];
			Column column = column(FREE, testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			reader.read();
			reader.read();
			reader.read();
			reader.read();
			reader.read();
			String expected = "Object column reader (" + nRows + ")\n" + "Position: " + 4;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testEnd() {
			int nRows = 64;
			String[] testArray = new String[nRows];
			Column column = column(FREE, testArray);
			ObjectColumnReader<String> reader = new ObjectColumnReader<>(column, String.class, 10, column.size());
			while (reader.hasRemaining()) {
				reader.read();
			}
			String expected = "Object column reader (" + nRows + ")\n" + "Position: " + (nRows - 1);
			assertEquals(expected, reader.toString());
		}
	}

}
