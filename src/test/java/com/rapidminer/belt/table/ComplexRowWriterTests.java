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

package com.rapidminer.belt.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;

import junit.framework.TestCase;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ComplexRowWriterTests {

	private static final ColumnType<String> TEXT = ColumnTypes.objectType("text", String.class, null);

	private static double[] random(int n) {
		double[] numbers = new double[n];
		Arrays.setAll(numbers, i -> Math.random());
		return numbers;
	}

	private static Object[][] readTableToArray(Table table) {
		Object[][] result = new Object[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	private static Object[] readColumnToArray(Table table, int column) {
		Object[] data = new Object[table.height()];
		ObjectReader<Object> reader = Readers.objectReader(table.column(column), Object.class);
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	public static class RowEstimation {

		@Test
		public void testUnknownRows() {
			int numberOfRows = 313;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.TIME, TEXT, ColumnTypes.DATETIME));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] * 5));
				writer.set(1, LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
				writer.set(2, "value" + third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] * 5));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);

			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, new Object[numberOfRows]},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testTooSmallRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.TIME, TEXT, ColumnTypes.DATETIME), numberOfRows /
							2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i] > 0.2 ? "" + first[i] : null);
				writer.set(1, LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
				writer.set(2, "value" + third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> first[i] > 0.2 ? "" + first[i] : null);
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);

			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, new Object[numberOfRows]},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testExactRows() {
			int numberOfRows = 211;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, TEXT, ColumnTypes.TIME), numberOfRows);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(1, Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
				writer.set(2, "value" + third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + first[i]);
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);

			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, new Object[numberOfRows]},
					readTableToArray(table));
		}

		@Test
		public void testTooManyRows() {
			int numberOfRows = 142;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);


			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, TEXT, ColumnTypes.TIME), numberOfRows *
							2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(1, Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
				writer.set(2, "value" + third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + first[i]);
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);

			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, new Object[numberOfRows]},
					readTableToArray(table));
		}
	}

	public static class Freezing {

		@Test(expected = IllegalStateException.class)
		public void testCreateTwice() {
			int numberOfRows = 142;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);


			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, TEXT, ColumnTypes.TIME), numberOfRows *
							2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(1, Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
				writer.set(2, "value" + third[i]);
			}
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceDatetime() {
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("b", "c", "d"),
					Arrays.asList(ColumnTypes.DATETIME, TEXT, ColumnTypes.TIME), 2);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceFree() {
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("c", "d"),
					Arrays.asList(TEXT, ColumnTypes.TIME), 2);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceTime() {
			ComplexRowWriter writer = new ComplexRowWriter(Collections.singletonList("d"),
					Collections.singletonList(ColumnTypes.TIME), 2);
			writer.create();
			writer.create();
		}
	}

	public static class ColumnTypeTest {

		@Test
		public void testUnknownRows() {
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, ColumnTypes.TIME, TEXT));
			Table table = writer.create();

			assertEquals(ColumnTypes.NOMINAL, table.column(0).type());
			assertEquals(ColumnTypes.DATETIME, table.column(1).type());
			assertEquals(ColumnTypes.TIME, table.column(2).type());
			assertEquals(TEXT, table.column(3).type());
		}


		@Test
		public void testKnownRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] third = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, ColumnTypes.TIME, TEXT), numberOfRows /
							2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(2, null);
				writer.set(3, "value" + third[i]);
			}
			Table table = writer.create();

			assertEquals(ColumnTypes.NOMINAL, table.column(0).type());
			assertEquals(ColumnTypes.DATETIME, table.column(1).type());
			assertEquals(ColumnTypes.TIME, table.column(2).type());
			assertEquals(TEXT, table.column(3).type());
		}

	}

	public static class ToString {

		@Test
		public void testNoRows() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.NOMINAL));
			String expected = "Object row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testNoRowsMoved() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.NOMINAL));
			writer.move();
			writer.move();
			writer.move();
			String expected = "Object row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedRows() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.NOMINAL),
							19);
			String expected = "Object row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedMoved() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.NOMINAL),
							19);
			writer.move();
			writer.move();
			writer.move();
			String expected = "Object row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testWidth() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.NOMINAL));
			String expected = "Object row writer (0x" + writer.width() + ")";
			assertEquals(expected, writer.toString());
		}

	}
}
