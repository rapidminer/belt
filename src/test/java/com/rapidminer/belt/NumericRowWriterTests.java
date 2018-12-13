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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import junit.framework.TestCase;

/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class NumericRowWriterTests {

	private static double[] random(int n) {
		double[] numbers = new double[n];
		Arrays.setAll(numbers, i -> Math.random());
		return numbers;
	}

	private static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		NumericReader reader = Readers.numericReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	public static class InputValidation {

		@Test(expected = NullPointerException.class)
		public void testNullLabels() {
			Writers.realRowWriter(null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsWithTypes() {
			Writers.numericRowWriter(null, Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.REAL));
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRows() {
			Writers.realRowWriter(null, 1);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRowsWithTypes() {
			Writers.numericRowWriter(null, Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.REAL), 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabels() {
			Writers.realRowWriter(new ArrayList<>());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsWithTypes() {
			Writers.numericRowWriter(new ArrayList<>(), Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.REAL));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRows() {
			Writers.realRowWriter(new ArrayList<>(), 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRowsWithTypes() {
			Writers.numericRowWriter(new ArrayList<>(), Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.REAL), 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRows() {
			Writers.realRowWriter(Arrays.asList("a", "b"), -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRowsWithTypes() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), Arrays.asList(ColumnTypes.INTEGER, ColumnTypes.REAL), -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLength() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), Collections.singletonList(ColumnTypes.INTEGER));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLengthWithRows() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), Collections.singletonList(ColumnTypes.INTEGER), 5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidLabels() {
			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "a"), 5);
			writer.move();
			writer.set(0, 1);
			writer.create();
		}

	}


	public static class RowEstimation {

		@Test
		public void testUnknownRows() {
			int numberOfRows = 313;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
		}

		@Test
		public void testTooSmallRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), numberOfRows / 2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
		}

		@Test
		public void testExactRows() {
			int numberOfRows = 211;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), numberOfRows);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
		}

		@Test
		public void testTooManyRows() {
			int numberOfRows = 142;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.INTEGER, ColumnTypes.REAL), numberOfRows * 2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			double[] roundedSecond = new double[second.length];
			Arrays.setAll(roundedSecond, i->Math.round(second[i]));
			assertArrayEquals(new double[][]{first, roundedSecond, third}, readTableToArray(table));
		}
	}

	public static class ColumnTypesTest {

		@Test
		public void testUnknownRows() {
			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.INTEGER));
			Table table = writer.create();

			assertEquals(ColumnTypes.REAL, table.column(0).type());
			assertEquals(ColumnTypes.INTEGER, table.column(1).type());
		}


		@Test
		public void testKnownRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.INTEGER, ColumnTypes.REAL), numberOfRows / 2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();

			assertEquals(ColumnTypes.REAL, table.column(0).type());
			assertEquals(ColumnTypes.INTEGER, table.column(1).type());
			assertEquals(ColumnTypes.REAL, table.column("c").type());
		}

		@Test
		public void testUnknownRowsReal() {
			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.REAL));
			Table table = writer.create();

			NumericRowWriter writer2 = Writers.realRowWriter(Arrays.asList("a", "b"));
			Table table2 = writer2.create();

			assertEquals(table.column(0).type(), table2.column(0).type());
			assertEquals(table.column(1).type(), table2.column(1).type());
		}


		@Test
		public void testKnownRowsReal() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.REAL, ColumnTypes.REAL), numberOfRows);
			NumericRowWriter writer2 = Writers.realRowWriter(Arrays.asList("a", "b", "c"), numberOfRows);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer2.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
				writer2.set(0, first[i]);
				writer2.set(1, second[i]);
				writer2.set(2, third[i]);
			}
			Table table = writer.create();
			Table table2 = writer2.create();

			assertEquals(table.column(0).type(), table2.column(0).type());
			assertEquals(table.column(1).type(), table2.column("b").type());
			assertEquals(table.column("c").type(), table2.column(2).type());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonNumericType() {
			Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.freeType("broken", Void.class, null),
							ColumnTypes.REAL));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonNumericTypeWithRows() {
			Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(ColumnTypes.REAL, ColumnTypes.freeType("broken", Void.class, null),
							ColumnTypes.REAL), 3);
		}
	}

	public static class ToString {

		@Test
		public void testNoRows() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"));
			String expected = "Row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testNoRowsMoved() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"));
			writer.move();
			writer.move();
			writer.move();
			String expected = "Row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedRows() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), 19);
			String expected = "Row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedMoved() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), 19);
			writer.move();
			writer.move();
			writer.move();
			String expected = "Row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testWidth() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"));
			String expected = "Row writer (0x" + writer.width() + ")";
			assertEquals(expected, writer.toString());
		}

	}
}
