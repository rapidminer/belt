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

import java.util.Arrays;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class ColumnReaderTests {

	private static final double EPSILON = 1e-10;

	private static double[] randomNumbers(int n) {
		double[] numbers = new double[n];
		Arrays.setAll(numbers, i -> Math.random());
		return numbers;
	}

	private static void readAll(ColumnReader reader) {
		while (reader.hasRemaining()) {
			reader.read();
		}
	}

	private static double[] readAllToArray(ColumnReader reader) {
		double[] result = new double[reader.remaining()];
		int i = 0;
		while (reader.hasRemaining()) {
			result[i++] = reader.read();
		}
		return result;
	}

	public static class Reading {

		@Test
		public void testReadingFromSingleCompleteBuffer() {
			double[] input = randomNumbers(ColumnReader.DEFAULT_BUFFER_SIZE);
			ColumnReader reader = new ColumnReader(new DoubleArrayColumn(input), ColumnReader.DEFAULT_BUFFER_SIZE,
					input.length);
			double[] output = readAllToArray(reader);

			assertArrayEquals(input, output, EPSILON);
		}

		@Test
		public void testReadingFromSingleIncompleteBuffer() {
			double[] input = randomNumbers((int) (0.67 * ColumnReader.DEFAULT_BUFFER_SIZE));
			ColumnReader reader = new ColumnReader(new DoubleArrayColumn(input), ColumnReader.DEFAULT_BUFFER_SIZE,
					input.length);
			double[] output = readAllToArray(reader);

			assertArrayEquals(input, output, EPSILON);
		}

		@Test
		public void testReadingFromMultipleCompleteBuffers() {
			double[] input = randomNumbers(5 * ColumnReader.DEFAULT_BUFFER_SIZE);
			ColumnReader reader = new ColumnReader(new DoubleArrayColumn(input), ColumnReader.DEFAULT_BUFFER_SIZE,
					input.length);
			double[] output = readAllToArray(reader);

			assertArrayEquals(input, output, EPSILON);
		}

		@Test
		public void testReadingFromMultipleIncompleteBuffers() {
			double[] input = randomNumbers((int) (3.33 * ColumnReader.DEFAULT_BUFFER_SIZE));
			ColumnReader reader = new ColumnReader(new DoubleArrayColumn(input), ColumnReader.DEFAULT_BUFFER_SIZE,
					input.length);
			double[] output = readAllToArray(reader);

			assertArrayEquals(input, output, EPSILON);
		}


		@Test
		public void testColumnInteractionWithSingleBuffer() {
			Column column = spy(new DoubleArrayColumn(new double[ColumnReader.DEFAULT_BUFFER_SIZE]));
			ColumnReader reader = new ColumnReader(column, ColumnReader.DEFAULT_BUFFER_SIZE, column.size());
			readAll(reader);

			verify(column).fill(any(double[].class), eq(0));
			verify(column, times(1)).fill(any(double[].class), anyInt());
		}

		@Test
		public void testColumnInteractionWithMultipleBuffers() {
			Column column = spy(new DoubleArrayColumn(new double[3 * ColumnReader.DEFAULT_BUFFER_SIZE]));
			ColumnReader reader = new ColumnReader(column, ColumnReader.DEFAULT_BUFFER_SIZE, column.size());
			readAll(reader);

			verify(column).fill(any(double[].class), eq(0));
			verify(column).fill(any(double[].class), eq(ColumnReader.DEFAULT_BUFFER_SIZE));
			verify(column).fill(any(double[].class), eq(2 * ColumnReader.DEFAULT_BUFFER_SIZE));
			verify(column, times(3)).fill(any(double[].class), anyInt());
		}
	}

	public static class Remaining {

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			new ColumnReader(null, 0);
		}

		@Test
		public void testInitialRemainder() {
			int n = 64;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, column.size());

			assertEquals(n, reader.remaining());
		}

		@Test
		public void testRemainder() {
			int n = 64;
			int reads = 16;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, column.size());
			for (int i = 0; i < reads; i++) {
				reader.read();
			}

			assertEquals(n - reads, reader.remaining());
		}

		@Test
		public void testFinalRemainder() {
			int n = 64;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, column.size());
			readAll(reader);

			assertEquals(0, reader.remaining());
		}
	}

	public static class Position {

		@Test
		public void testGet() {
			int n = 64;
			int reads = 16;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, column.size());
			for (int i = 0; i < reads; i++) {
				reader.read();
			}
			assertEquals(reads - 1, reader.position());
		}

		@Test
		public void testGetBufferSmaller() {
			int n = 64;
			int reads = 16;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			for (int i = 0; i < reads; i++) {
				reader.read();
			}
			assertEquals(reads - 1, reader.position());
		}

		@Test
		public void testGetAtStart() {
			int n = 64;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, column.size());
			assertEquals(Row.BEFORE_FIRST, reader.position());
		}

		@Test
		public void testGetAtEnd() {
			int n = 64;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, column.size());
			while (reader.hasRemaining()) {
				reader.read();
			}
			assertEquals(column.size() - 1, reader.position());
		}

		@Test
		public void testGetAtEndBufferSmaller() {
			int n = 64;
			Column column = new DoubleArrayColumn(new double[n]);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			while (reader.hasRemaining()) {
				reader.read();
			}
			assertEquals(column.size() - 1, reader.position());
		}

		@Test
		public void testSet() {
			int n = 64;
			int position = 16;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(position + 1, reader.read(), EPSILON);
			assertEquals(position + 2, reader.read(), EPSILON);
		}

		@Test
		public void testSetAfterReads() {
			int n = 64;
			int position = 16;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(position + 1, reader.read(), EPSILON);
			assertEquals(position + 2, reader.read(), EPSILON);
		}

		@Test
		public void testSetAfterReadsSmallerInSameBuffer() {
			int n = 64;
			int position = 11;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(position + 1, reader.read(), EPSILON);
			assertEquals(position + 2, reader.read(), EPSILON);
		}

		@Test
		public void testSetAfterReadsSmaller() {
			int n = 64;
			int position = 3;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			assertEquals(position + 1, reader.read(), EPSILON);
			assertEquals(position + 2, reader.read(), EPSILON);
		}

		@Test
		public void testSetBefore() {
			int n = 64;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			reader.setPosition(Row.BEFORE_FIRST);
			assertEquals(-1, reader.position());
			assertEquals(0, reader.read(), EPSILON);
			assertEquals(1, reader.read(), EPSILON);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testSetNegative() {
			int n = 64;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			reader.setPosition(-5);
		}


		@Test
		public void testSetEnd() {
			int n = 64;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			reader.setPosition(n - 2);
			assertEquals(n - 1, reader.read(), EPSILON);
			assertEquals(n - 1, reader.position());
		}

		@Test
		public void testSetBeforeAfterReads() {
			int n = 64;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(Row.BEFORE_FIRST);
			assertEquals(-1, reader.position());
			assertEquals(0, reader.read(), EPSILON);
			assertEquals(1, reader.read(), EPSILON);
		}

		@Test
		public void testSetEndAfterReads() {
			int n = 64;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			for (int i = 0; i < 13; i++) {
				reader.read();
			}
			reader.setPosition(n - 2);
			assertEquals(n - 1, reader.read(), EPSILON);
			assertEquals(n - 1, reader.position());
		}

		@Test
		public void testSetMultipleTimes() {
			int n = 64;
			double[] testArray = new double[n];
			Arrays.setAll(testArray, i -> i);
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			reader.setPosition(16);
			assertEquals(16, reader.position());
			assertEquals(17, reader.read(), EPSILON);
			reader.setPosition(18);
			assertEquals(18, reader.position());
			assertEquals(19, reader.read(), EPSILON);
			reader.setPosition(11);
			assertEquals(11, reader.position());
			assertEquals(12, reader.read(), EPSILON);
			reader.setPosition(11);
			assertEquals(11, reader.position());
			assertEquals(12, reader.read(), EPSILON);
			reader.setPosition(25);
			assertEquals(25, reader.position());
			assertEquals(26, reader.read(), EPSILON);
			reader.setPosition(23);
			assertEquals(23, reader.position());
			assertEquals(24, reader.read(), EPSILON);
		}
	}

	public static class ToString {

		@Test
		public void testStart() {
			int nRows = 64;
			double[] testArray = new double[nRows];
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			String expected = "Column reader (" + nRows + ")\n" + "Position: " + Row.BEFORE_FIRST;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testMiddle() {
			int nRows = 64;
			double[] testArray = new double[nRows];
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			reader.read();
			reader.read();
			reader.read();
			reader.read();
			reader.read();
			String expected = "Column reader (" + nRows + ")\n" + "Position: " + 4;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testEnd() {
			int nRows = 64;
			double[] testArray = new double[nRows];
			Column column = new DoubleArrayColumn(testArray);
			ColumnReader reader = new ColumnReader(column, 10, column.size());
			while (reader.hasRemaining()) {
				reader.read();
			}
			String expected = "Column reader (" + nRows + ")\n" + "Position: " + (nRows - 1);
			assertEquals(expected, reader.toString());
		}
	}

}
