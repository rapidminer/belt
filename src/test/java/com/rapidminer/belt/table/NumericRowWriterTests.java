/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.SplittableRandom;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.ArrayBuilderConfiguration;

import junit.framework.TestCase;


/**
 * @author Gisa Meier, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class NumericRowWriterTests {

	private static double[] random(int n) {
		double[] numbers = new double[n];
		Arrays.setAll(numbers, i -> Math.random());
		return numbers;
	}

	private static double[] sparseRandom(int n, boolean defaultIsNan) {
		double sparsity = 0.75;
		SplittableRandom random = new SplittableRandom(5374033188480834L);
		double[] numbers = new double[n];
		double defaultValue = defaultIsNan ? Double.NaN : random.nextDouble() * 1_000_000;
		Arrays.setAll(numbers, i -> random.nextDouble() < sparsity ?
				defaultValue : random.nextDouble() * 1_000_000);
		return numbers;
	}

	private static double[] sparseRandom(int n, double defaultValue) {
		double sparsity = 0.75;
		SplittableRandom random = new SplittableRandom(5374033188480834L);
		double[] numbers = new double[n];
		Arrays.setAll(numbers, i -> random.nextDouble() < sparsity ?
				defaultValue : random.nextDouble() * 1_000_000);
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
			Writers.realRowWriter(null, false);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsWithTypes() {
			Writers.numericRowWriter(null, Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL), false);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypes() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), null, false);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypesWithRows() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), null, 2, false);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRows() {
			Writers.realRowWriter(null, 1, false);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRowsWithTypes() {
			Writers.numericRowWriter(null, Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL), 1, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabels() {
			Writers.realRowWriter(new ArrayList<>(), false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsWithTypes() {
			Writers.numericRowWriter(new ArrayList<>(), Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL), false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRows() {
			Writers.realRowWriter(new ArrayList<>(), 1, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRowsWithTypes() {
			Writers.numericRowWriter(new ArrayList<>(), Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL), 1,
					false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRows() {
			Writers.realRowWriter(Arrays.asList("a", "b"), -1, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRowsWithTypes() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL), -1
					, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLength() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), Collections.singletonList(TypeId.INTEGER_53_BIT), false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLengthWithRows() {
			Writers.numericRowWriter(Arrays.asList("a", "b"), Collections.singletonList(TypeId.INTEGER_53_BIT), 5, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidLabels() {
			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "a"), 5, false);
			writer.move();
			writer.set(0, 1);
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testIllegalStateSparse() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, true);
			double[] third = sparseRandom(numberOfRows, false);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			assertTrue(writer.getColumns()[0] instanceof RealColumnWriter);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriter);
			assertTrue(writer.getColumns()[2] instanceof RealColumnWriter);
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof RealColumnWriterSparse);

			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
			// try to modify buffer even though it is frozen
			writer.move();
			writer.set(1, second[0]);
			writer.create();
		}

	}


	public static class RowEstimation {

		private static final double EPSILON = 1e-10;

		@Test
		public void testUnknownRows() {
			int numberOfRows = 313;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), false);
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
		public void testUnknownColumnsSparse() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10 +
					ArrayBuilderConfiguration.DEFAULT_INITIAL_CHUNK_SIZE * 10;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, true);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), false);
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE + 10; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof RealColumnWriter);
			for (int i = NumericReader.SMALL_BUFFER_SIZE + 10; i < numberOfRows; i++) {
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
		public void testKnownColumnsSparse() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10 +
					ArrayBuilderConfiguration.DEFAULT_INITIAL_CHUNK_SIZE * 10;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, false);
			double[] third = sparseRandom(numberOfRows, true);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL, TypeId.INTEGER_53_BIT), false);
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE + 10; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof Integer53BitColumnWriterSparse);
			for (int i = NumericReader.SMALL_BUFFER_SIZE + 10; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new double[][]{Arrays.stream(first).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray(),
							second, Arrays.stream(third).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray()},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
		}

		@Test
		public void testAutoSparsity() {
			// this many rows will force the NumericRowWriter to check for sparsity
			int numberOfRows = NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW + 1;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, false);
			double[] third = sparseRandom(numberOfRows, true);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL, TypeId.INTEGER_53_BIT), true);
			for (int i = 0; i < numberOfRows - 1; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			// this is the max number of rows before checking for sparsity
			assertTrue(writer.getColumns()[0] instanceof Integer53BitColumnWriter);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriter);
			assertTrue(writer.getColumns()[2] instanceof Integer53BitColumnWriter);
			writer.move();
			writer.set(0, first[numberOfRows - 1]);
			writer.set(1, second[numberOfRows - 1]);
			writer.set(2, third[numberOfRows - 1]);
			// now the check for sparsity should have taken place
			assertTrue(writer.getColumns()[0] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof Integer53BitColumnWriterSparse);
			Table table = writer.create();
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertEquals(ColumnType.INTEGER_53_BIT, table.getColumns()[0].type());
			assertEquals(ColumnType.REAL, table.getColumns()[1].type());
			assertEquals(ColumnType.INTEGER_53_BIT, table.getColumns()[2].type());
			TestCase.assertEquals(numberOfRows, table.height());
			assertArrayEquals(new double[][]{Arrays.stream(first).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray(),
							second, Arrays.stream(third).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray()},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
		}

		@Test
		public void testFirstSparseThenDense() {
			int firstNumberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;
			double[] first = new double[firstNumberOfRows * 11];
			double[] second = new double[firstNumberOfRows * 11];
			double[] third = new double[firstNumberOfRows * 11];
			System.arraycopy(sparseRandom(firstNumberOfRows, false), 0,
					first, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, false), 0,
					second, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, true), 0,
					third, 0, firstNumberOfRows);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL, TypeId.INTEGER_53_BIT), false);
			for (int i = 0; i < firstNumberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof Integer53BitColumnWriterSparse);
			// now we add 10 times more dense data so that the overall column should be dense
			System.arraycopy(random(firstNumberOfRows * 10), 0,
					first, firstNumberOfRows, firstNumberOfRows * 10);
			System.arraycopy(random(firstNumberOfRows * 10), 0,
					second, firstNumberOfRows, firstNumberOfRows * 10);
			System.arraycopy(random(firstNumberOfRows * 10), 0,
					third, firstNumberOfRows, firstNumberOfRows * 10);
			for (int i = firstNumberOfRows; i < firstNumberOfRows * 11; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(firstNumberOfRows * 11, table.height());
			assertArrayEquals(new double[][]{Arrays.stream(first).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray(),
							second, Arrays.stream(third).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray()},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
			// check that the columns are dense and of the correct type
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertEquals(ColumnType.INTEGER_53_BIT, table.getColumns()[0].type());
			assertEquals(ColumnType.REAL, table.getColumns()[1].type());
			assertEquals(ColumnType.INTEGER_53_BIT, table.getColumns()[2].type());
		}

		@Test
		public void testFirstSparseThenSparseWithOtherDefault() {
			int firstNumberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;
			double[] first = new double[firstNumberOfRows * 11];
			double[] second = new double[firstNumberOfRows * 11];
			double[] third = new double[firstNumberOfRows * 11];
			System.arraycopy(sparseRandom(firstNumberOfRows, -199), 0,
					first, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, 17.3), 0,
					second, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, Double.NaN), 0,
					third, 0, firstNumberOfRows);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.REAL, TypeId.INTEGER_53_BIT), false);
			for (int i = 0; i < firstNumberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof RealColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof Integer53BitColumnWriterSparse);
			assertEquals(-199, ((Integer53BitColumnWriterSparse) writer.getColumns()[0]).getDefaultValue(), EPSILON);
			assertEquals(17.3, ((RealColumnWriterSparse) writer.getColumns()[1]).getDefaultValue(), EPSILON);
			assertEquals(Double.NaN, ((Integer53BitColumnWriterSparse) writer.getColumns()[2]).getDefaultValue(), EPSILON);
			// now we add 10 times more sparse data with different default
			// values so that the default value of the column should change
			System.arraycopy(sparseRandom(firstNumberOfRows * 10, 19), 0,
					first, firstNumberOfRows, firstNumberOfRows * 10);
			System.arraycopy(sparseRandom(firstNumberOfRows * 10, Double.NaN), 0,
					second, firstNumberOfRows, firstNumberOfRows * 10);
			System.arraycopy(sparseRandom(firstNumberOfRows * 10, Double.POSITIVE_INFINITY), 0,
					third, firstNumberOfRows, firstNumberOfRows * 10);
			for (int i = firstNumberOfRows; i < firstNumberOfRows * 11; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(firstNumberOfRows * 11, table.height());
			assertArrayEquals(new double[][]{Arrays.stream(first).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray(),
							second, Arrays.stream(third).map(x -> Double.isFinite(x) ? Math.round(x) : x).toArray()},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
			// check that the columns are sparse and, of the correct type and with the correct default value
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertEquals(19, ColumnTestUtils.getDefaultValue(table.getColumns()[0]), EPSILON);
			assertEquals(Double.NaN, ColumnTestUtils.getDefaultValue(table.getColumns()[1]), EPSILON);
			assertEquals(Double.POSITIVE_INFINITY, ColumnTestUtils.getDefaultValue(table.getColumns()[2]), EPSILON);
			assertEquals(ColumnType.INTEGER_53_BIT, table.getColumns()[0].type());
			assertEquals(ColumnType.REAL, table.getColumns()[1].type());
			assertEquals(ColumnType.INTEGER_53_BIT, table.getColumns()[2].type());
		}

		@Test
		public void testTooSmallRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), numberOfRows / 2, false);
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

			NumericRowWriter writer = Writers.realRowWriter(Arrays.asList("a", "b", "c"), numberOfRows, false);
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
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT, TypeId.REAL), numberOfRows * 2, false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			double[] roundedSecond = new double[second.length];
			Arrays.setAll(roundedSecond, i -> Math.round(second[i]));
			assertArrayEquals(new double[][]{first, roundedSecond, third}, readTableToArray(table));
		}
	}

	public static class Initialization {

		@Test
		public void testNotInitialized() {

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT), false);
			writer.move();
			writer.move();
			writer.set(0, 1.53);
			writer.set(1, 1.987);
			writer.move();

			Table table = writer.create();
			assertArrayEquals(new double[][]{new double[]{0.0, 1.53, 0.0},
					new double[]{0.0, 2.0, 0.0}}, readTableToArray(table));
		}

		@Test
		public void testInitialized() {

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT), true);
			writer.move();
			writer.move();
			writer.set(0, 1.53);
			writer.set(1, 1.987);
			writer.move();

			Table table = writer.create();
			assertArrayEquals(new double[][]{new double[]{Double.NaN, 1.53, Double.NaN},
					new double[]{Double.NaN, 2.0, Double.NaN}}, readTableToArray(table));
		}

		@Test
		public void testSecondFillInitialized() {

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT), true);
			writer.move();
			writer.move();
			writer.set(0, 1.53);
			writer.set(1, 1.987);
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE; i++) {
				writer.move();
			}
			writer.move();

			Table table = writer.create();

			double[] firstExpected = new double[table.height()];
			Arrays.fill(firstExpected, Double.NaN);
			firstExpected[1] = 1.53;
			double[] secondExpected = new double[table.height()];
			Arrays.fill(secondExpected, Double.NaN);
			secondExpected[1] = 2.0;

			assertArrayEquals(new double[][]{firstExpected, secondExpected}, readTableToArray(table));
		}

		@Test
		public void testSecondFillNotInitialized() {

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT), false);
			writer.move();
			writer.move();
			writer.set(0, 1.53);
			writer.set(1, 1.987);
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE; i++) {
				writer.move();
				writer.set(0, 0.0);
				writer.set(1, 0.0);
			}

			Table table = writer.create();

			double[] firstExpected = new double[table.height()];
			firstExpected[1] = 1.53;
			double[] secondExpected = new double[table.height()];
			secondExpected[1] = 2.0;

			assertArrayEquals(new double[][]{firstExpected, secondExpected}, readTableToArray(table));
		}

	}


	public static class ColumnTypeTest {

		@Test
		public void testUnknownRows() {
			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT), false);
			Table table = writer.create();

			assertEquals(ColumnType.REAL, table.column(0).type());
			assertEquals(ColumnType.INTEGER_53_BIT, table.column(1).type());
		}


		@Test
		public void testKnownRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);

			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.REAL, TypeId.INTEGER_53_BIT, TypeId.REAL), numberOfRows / 2, false);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, first[i]);
				writer.set(1, second[i]);
				writer.set(2, third[i]);
			}
			Table table = writer.create();

			assertEquals(ColumnType.REAL, table.column(0).type());
			assertEquals(ColumnType.INTEGER_53_BIT, table.column(1).type());
			assertEquals(ColumnType.REAL, table.column("c").type());
		}

		@Test
		public void testUnknownRowsReal() {
			NumericRowWriter writer = Writers.numericRowWriter(Arrays.asList("a", "b"),
					Arrays.asList(TypeId.REAL, TypeId.REAL), false);
			Table table = writer.create();

			NumericRowWriter writer2 = Writers.realRowWriter(Arrays.asList("a", "b"), false);
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
					Arrays.asList(TypeId.REAL, TypeId.REAL, TypeId.REAL), numberOfRows, false);
			NumericRowWriter writer2 = Writers.realRowWriter(Arrays.asList("a", "b", "c"), numberOfRows, false);
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
					Arrays.asList(TypeId.REAL, TypeId.TIME, TypeId.REAL), false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonNumericTypeWithRows() {
			Writers.numericRowWriter(Arrays.asList("a", "b", "c"),
					Arrays.asList(TypeId.REAL, TypeId.TIME,
							TypeId.REAL), 3, false);
		}
	}

	public static class ToString {

		@Test
		public void testNoRows() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), true);
			String expected = "Row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testNoRowsMoved() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), true);
			writer.move();
			writer.move();
			writer.move();
			String expected = "Row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedRows() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), 19, true);
			String expected = "Row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedMoved() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), 19, true);
			writer.move();
			writer.move();
			writer.move();
			String expected = "Row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testWidth() {
			NumericRowWriter writer = new NumericRowWriter(Arrays.asList("a", "b"), true);
			String expected = "Row writer (0x" + writer.width() + ")";
			assertEquals(expected, writer.toString());
		}

	}
}
