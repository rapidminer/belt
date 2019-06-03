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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;

import junit.framework.TestCase;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class MixedRowWriterTests {

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
		if (table.column(column).type().category() == Column.Category.NUMERIC) {
			NumericReader reader = Readers.numericReader(table.column(column));
			for (int j = 0; j < table.height(); j++) {
				data[j] = reader.read();
			}
		} else {
			ObjectReader<Object> reader = Readers.objectReader(table.column(column), Object.class);
			for (int j = 0; j < table.height(); j++) {
				data[j] = reader.read();
			}
		}
		return data;
	}

	public static class InputValidation {

		@Test(expected = NullPointerException.class)
		public void testNullLabels() {
			Writers.mixedRowWriter(null, Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.NOMINAL), true);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRows() {
			Writers.mixedRowWriter(null, Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.NOMINAL), 1, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabels() {
			Writers.mixedRowWriter(new ArrayList<>(), Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.REAL), true);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypes() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), null, true);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypesWithRows() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), null, 1,true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRows() {
			Writers.mixedRowWriter(new ArrayList<>(), Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.TIME), 1, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRows() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.REAL, ColumnTypes.NOMINAL),
							-1, true);
		}


		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLength() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), Collections.singletonList(ColumnTypes.NOMINAL), false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLengthWithRows() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), Collections.singletonList(ColumnTypes.NOMINAL), 5, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidLabels() {
			Writers.mixedRowWriter(Arrays.asList("a", "a"), Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.INTEGER), 5, false)
					.create();
		}

	}


	public static class RowEstimation {

		@Test
		public void testUnknownRows() {
			int numberOfRows = 313;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);
			double[] fourth = random(numberOfRows);
			double[] fifth = random(numberOfRows);

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.INTEGER, ColumnTypes.TIME, TEXT, ColumnTypes.REAL,
							ColumnTypes.DATETIME), false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] * 5));
				writer.set(2, LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
				writer.set(3, "value" + third[i]);
				writer.set(1, fourth[i]);
				writer.set(4, fifth[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] * 5));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] fourthExpected = new Object[numberOfRows];
			Arrays.setAll(fourthExpected, i -> (double) Math.round(fourth[i]));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> fifth[i]);

			assertArrayEquals(
					new Object[][]{firstExpected, fourthExpected, secondExpected, thirdExpected, fifthExpected,
							new Object[numberOfRows]},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e", "f"}, table.labelArray());
		}

		@Test
		public void testTooSmallRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);
			double[] fourth = random(numberOfRows);
			double[] fifth = random(numberOfRows);

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.NOMINAL, ColumnTypes.TIME, TEXT, ColumnTypes.DATETIME,
							ColumnTypes.INTEGER), numberOfRows / 2, false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(1, first[i] > 0.2 ? "" + first[i] : null);
				writer.set(2, LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
				writer.set(3, "value" + third[i]);
				writer.set(0, fifth[i]);
				writer.set(5, fourth[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> first[i] > 0.2 ? "" + first[i] : null);
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> LocalTime.ofSecondOfDay(Math.abs(Math.round(second[i] * 80_000))));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] fourthExpected = new Object[numberOfRows];
			Arrays.setAll(fourthExpected, i -> (double) Math.round(fourth[i]));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> fifth[i]);

			assertArrayEquals(new Object[][]{fifthExpected, firstExpected, secondExpected, thirdExpected,
					new Object[numberOfRows], fourthExpected}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e", "f"}, table.labelArray());
		}

		@Test
		public void testExactRows() {
			int numberOfRows = 211;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);
			double[] fourth = random(numberOfRows);
			double[] fifth = random(numberOfRows);

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, ColumnTypes.INTEGER, ColumnTypes.REAL,
							TEXT, ColumnTypes.TIME), numberOfRows, false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(1, Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
				writer.set(2, fourth[i]);
				writer.set(3, fifth[i]);
				writer.set(4, "value" + third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + first[i]);
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] fourthExpected = new Object[numberOfRows];
			Arrays.setAll(fourthExpected, i -> (double) Math.round(fourth[i]));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> fifth[i]);

			assertArrayEquals(
					new Object[][]{firstExpected, secondExpected, fourthExpected, fifthExpected, thirdExpected,
							new Object[numberOfRows]}, readTableToArray(table));
		}

		@Test
		public void testTooManyRows() {
			int numberOfRows = 142;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);
			double[] fourth = random(numberOfRows);
			double[] fifth = random(numberOfRows);


			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, TEXT, ColumnTypes.TIME, ColumnTypes.REAL,
							ColumnTypes.INTEGER), numberOfRows * 2, false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(1, Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
				writer.set(2, "value" + third[i]);
				writer.set(4, fifth[i]);
				writer.set(5, fourth[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + first[i]);
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Instant.ofEpochMilli(Math.round(second[i] * 8_000_000)));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] fourthExpected = new Object[numberOfRows];
			Arrays.setAll(fourthExpected, i -> (double) Math.round(fourth[i]));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> fifth[i]);

			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, new Object[numberOfRows],
					fifthExpected, fourthExpected}, readTableToArray(table));
		}
	}

	public static class Initialization {

		@Test
		public void testNotInitialized() {

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME,
							ColumnTypes.INTEGER), false);
			writer.move();
			writer.move();
			writer.set(0, "Yes");
			writer.set(1, Instant.EPOCH);
			writer.set(2, 1.987);
			writer.move();

			Table table = writer.create();
			assertArrayEquals(new Object[][]{new Object[]{null, "Yes", null}, new Object[]{null, Instant.EPOCH, null},
					new Object[]{0.0, 2.0, 0.0}}, readTableToArray(table));
		}

		@Test
		public void testInitialized() {

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME,
							ColumnTypes.INTEGER), true);
			writer.move();
			writer.move();
			writer.set(0, "Yes");
			writer.set(1, Instant.EPOCH);
			writer.set(2, 1.987);
			writer.move();

			Table table = writer.create();
			assertArrayEquals(new Object[][]{new Object[]{null, "Yes", null}, new Object[]{null, Instant.EPOCH, null},
					new Object[]{Double.NaN, 2.0, Double.NaN}}, readTableToArray(table));
		}

		@Test
		public void testSecondFillInitialized() {

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "c"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.INTEGER), true);
			writer.move();
			writer.move();
			writer.set(0, "Yes");
			writer.set(1, 1.987);
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE; i++) {
				writer.move();
			}
			writer.move();

			Table table = writer.create();

			Object[] firstExpected = new Object[table.height()];
			firstExpected[1] = "Yes";
			Object[] secondExpected = new Object[table.height()];
			Arrays.fill(secondExpected, Double.NaN);
			secondExpected[1] = 2.0;

			assertArrayEquals(new Object[][]{firstExpected, secondExpected}, readTableToArray(table));
		}


		@Test
		public void testSecondFillNotInitialized() {

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "c"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.INTEGER), false);
			writer.move();
			writer.move();
			writer.set(0, "Yes");
			writer.set(1, 1.987);
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE; i++) {
				writer.move();
				writer.set(0, null);
				writer.set(1, 0.0);
			}

			Table table = writer.create();

			Object[] firstExpected = new Object[table.height()];
			firstExpected[1] = "Yes";
			Object[] secondExpected = new Object[table.height()];
			Arrays.fill(secondExpected, 0.0);
			secondExpected[1] = 2.0;

			assertArrayEquals(new Object[][]{firstExpected, secondExpected}, readTableToArray(table));
		}
	}


	public static class Freezing {

		@Test(expected = IllegalStateException.class)
		public void testCreateTwice() {
			int numberOfRows = 142;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);


			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, TEXT, ColumnTypes.TIME), numberOfRows *
							2, false);
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
			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("b", "c", "d"),
					Arrays.asList(ColumnTypes.INTEGER, TEXT, ColumnTypes.TIME), 2, false);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceFree() {
			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("c", "d"),
					Arrays.asList(TEXT, ColumnTypes.TIME), 2, false);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceTime() {
			MixedRowWriter writer = Writers.mixedRowWriter(Collections.singletonList("d"),
					Collections.singletonList(ColumnTypes.REAL), 2, false);
			writer.create();
			writer.create();
		}
	}

	public static class ColumnTypeTest {

		@Test
		public void testUnknownRows() {
			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.INTEGER, ColumnTypes.REAL, ColumnTypes.DATETIME,
							ColumnTypes.TIME, TEXT), false);
			Table table = writer.create();

			assertEquals(ColumnTypes.NOMINAL, table.column(0).type());
			assertEquals(ColumnTypes.INTEGER, table.column(1).type());
			assertEquals(ColumnTypes.REAL, table.column(2).type());
			assertEquals(ColumnTypes.DATETIME, table.column(3).type());
			assertEquals(ColumnTypes.TIME, table.column(4).type());
			assertEquals(TEXT, table.column(5).type());
		}


		@Test
		public void testKnownRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] third = random(numberOfRows);

			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
					Arrays.asList(ColumnTypes.NOMINAL, ColumnTypes.DATETIME, ColumnTypes.REAL, ColumnTypes.TIME,
							ColumnTypes.INTEGER, TEXT), numberOfRows / 2, false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(3, null);
				writer.set(2, third[i]);
				writer.set(5, "value" + third[i]);
			}
			Table table = writer.create();

			assertEquals(ColumnTypes.NOMINAL, table.column(0).type());
			assertEquals(ColumnTypes.DATETIME, table.column(1).type());
			assertEquals(ColumnTypes.REAL, table.column(2).type());
			assertEquals(ColumnTypes.TIME, table.column(3).type());
			assertEquals(ColumnTypes.INTEGER, table.column(4).type());
			assertEquals(TEXT, table.column(5).type());
		}

	}

	public static class ToString {

		@Test
		public void testNoRows() {
			MixedRowWriter writer =
					new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes
							.NOMINAL), true);
			String expected = "General row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testNoRowsMoved() {
			MixedRowWriter writer =
					new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.INTEGER, ColumnTypes
							.NOMINAL), true);
			writer.move();
			writer.move();
			writer.move();
			String expected = "General row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedRows() {
			MixedRowWriter writer =
					new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.REAL),
							19, true);
			String expected = "General row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedMoved() {
			MixedRowWriter writer =
					new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.TIME, ColumnTypes.NOMINAL),
							19, true);
			writer.move();
			writer.move();
			writer.move();
			String expected = "General row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testWidth() {
			MixedRowWriter writer =
					new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.INTEGER, ColumnTypes
							.NOMINAL), true);
			String expected = "General row writer (0x" + writer.width() + ")";
			assertEquals(expected, writer.toString());
		}

	}
}
