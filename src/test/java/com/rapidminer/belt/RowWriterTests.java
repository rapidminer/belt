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

import com.rapidminer.belt.Column.TypeId;

import junit.framework.TestCase;

/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class RowWriterTests {

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
		ColumnReader reader = new ColumnReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	public static class InputValidation {

		@Test(expected = NullPointerException.class)
		public void testNullLabels() {
			Table.writeTable(null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsWithTypes() {
			Table.writeTable(null, Arrays.asList(TypeId.INTEGER, Column.TypeId.REAL));
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRows() {
			Table.writeTable(null, 1);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRowsWithTypes() {
			Table.writeTable(null, Arrays.asList(TypeId.INTEGER, TypeId.REAL), 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabels() {
			Table.writeTable(new ArrayList<>());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsWithTypes() {
			Table.writeTable(new ArrayList<>(), Arrays.asList(Column.TypeId.INTEGER, Column.TypeId.REAL));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRows() {
			Table.writeTable(new ArrayList<>(), 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRowsWithTypes() {
			Table.writeTable(new ArrayList<>(), Arrays.asList(TypeId.INTEGER, TypeId.REAL), 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRows() {
			Table.writeTable(Arrays.asList("a", "b"), -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRowsWithTypes() {
			Table.writeTable(Arrays.asList("a", "b"), Arrays.asList(TypeId.INTEGER, Column.TypeId.REAL), -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLength() {
			Table.writeTable(Arrays.asList("a", "b"), Collections.singletonList(Column.TypeId.INTEGER));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLengthWithRows() {
			Table.writeTable(Arrays.asList("a", "b"), Collections.singletonList(TypeId.INTEGER), 5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidLabels() {
			RowWriter writer = Table.writeTable(Arrays.asList("a", "a"), 5);
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

			RowWriter writer = Table.writeTable(Arrays.asList("a", "b", "c"));
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

			RowWriter writer = Table.writeTable(Arrays.asList("a", "b", "c"), numberOfRows / 2);
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

			RowWriter writer = Table.writeTable(Arrays.asList("a", "b", "c"), numberOfRows);
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

			RowWriter writer = Table.writeTable(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER, TypeId.REAL), numberOfRows * 2);
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
	}

	public static class ColumnTypes {

		@Test
		public void testUnknownRows() {
			RowWriter writer = Table.writeTable(Arrays.asList("a", "b"),
					Arrays.asList(Column.TypeId.REAL, TypeId.INTEGER));
			Table table = writer.create();

			assertEquals(TypeId.REAL, table.column(0).type().id());
			assertEquals(Column.TypeId.INTEGER, table.column(1).type().id());
		}


		@Test
		public void testKnownRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			RowWriter writer = Table.writeTable(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER, Column.TypeId.REAL), numberOfRows / 2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();

			assertEquals(TypeId.REAL, table.column(0).type().id());
			assertEquals(TypeId.INTEGER, table.column(1).type().id());
			assertEquals(TypeId.REAL, table.column("c").type().id());
		}

		@Test
		public void testUnknownRowsReal() {
			RowWriter writer = Table.writeTable(Arrays.asList("a", "b"),
					Arrays.asList(Column.TypeId.REAL, Column.TypeId.REAL));
			Table table = writer.create();

			RowWriter writer2 = Table.writeTable(Arrays.asList("a", "b"));
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

			RowWriter writer = Table.writeTable(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.REAL, Column.TypeId.REAL, Column.TypeId.REAL), numberOfRows);
			RowWriter writer2 = Table.writeTable(Arrays.asList("a", "b", "c"), numberOfRows);
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
	}

	public static class ToString {

		@Test
		public void testNoRows() {
			RowWriter writer = new RowWriter(Arrays.asList("a", "b"));
			String expected = "Row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testNoRowsMoved() {
			RowWriter writer = new RowWriter(Arrays.asList("a", "b"));
			writer.move();
			writer.move();
			writer.move();
			String expected = "Row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedRows() {
			RowWriter writer = new RowWriter(Arrays.asList("a", "b"), 19);
			String expected = "Row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedMoved() {
			RowWriter writer = new RowWriter(Arrays.asList("a", "b"), 19);
			writer.move();
			writer.move();
			writer.move();
			String expected = "Row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testWidth() {
			RowWriter writer = new RowWriter(Arrays.asList("a", "b"));
			String expected = "Row writer (0x" + writer.width() + ")";
			assertEquals(expected, writer.toString());
		}

	}
}
