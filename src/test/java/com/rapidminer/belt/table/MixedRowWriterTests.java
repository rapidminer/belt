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

package com.rapidminer.belt.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.SplittableRandom;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.ArrayBuilderConfiguration;

import junit.framework.TestCase;


/**
 * @author Gisa Meier, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class MixedRowWriterTests {

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
			Writers.mixedRowWriter(null, Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.NOMINAL), true);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabelsPosRows() {
			Writers.mixedRowWriter(null, Arrays.asList(TypeId.NOMINAL, TypeId.NOMINAL), 1, false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabels() {
			Writers.mixedRowWriter(new ArrayList<>(), Arrays.asList(TypeId.NOMINAL, TypeId.REAL), true);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypes() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), null, true);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypesWithRows() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), null, 1, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyLabelsPosRows() {
			Writers.mixedRowWriter(new ArrayList<>(), Arrays.asList(TypeId.NOMINAL, TypeId.TIME), 1, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeRows() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.REAL, TypeId.NOMINAL),
					-1, true);
		}


		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLength() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), Collections.singletonList(TypeId.NOMINAL), false);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchedTypesLengthWithRows() {
			Writers.mixedRowWriter(Arrays.asList("a", "b"), Collections.singletonList(TypeId.NOMINAL), 5, true);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidLabels() {
			Writers.mixedRowWriter(Arrays.asList("a", "a"), Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT), 5, false)
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
					Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT, TypeId.TIME, TypeId.TEXT, TypeId.REAL,
							TypeId.DATE_TIME), false);
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
		public void testSparse() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10 +
					ArrayBuilderConfiguration.DEFAULT_INITIAL_CHUNK_SIZE * 10;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, true);
			double[] third = sparseRandom(numberOfRows, false);
			double[] forth = sparseRandom(numberOfRows, false);
			double[] fifth = sparseRandom(numberOfRows, false);

			MixedRowWriter writer = new MixedRowWriter(Arrays.asList("a", "b", "c", "d", "e"),
					Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT, TypeId.TIME, TypeId.DATE_TIME, TypeId.REAL));
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE + 10; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, second[i]);
				writer.set(2, LocalTime.ofNanoOfDay((long) third[i]));
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
				writer.set(4, fifth[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriter);
			assertTrue(writer.getNumericColumns()[1] instanceof Integer53BitColumnWriter);
			assertTrue(writer.getObjectColumns()[2] instanceof TimeColumnWriter);
			assertTrue(writer.getObjectColumns()[3] instanceof NanosecondsDateTimeWriter);
			assertTrue(writer.getNumericColumns()[4] instanceof RealColumnWriter);
			writer.checkForSparsity();
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getNumericColumns()[1] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getObjectColumns()[2] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getObjectColumns()[3] instanceof NanoDateTimeWriterSparse);
			assertTrue(writer.getNumericColumns()[4] instanceof RealColumnWriterSparse);
			for (int i = NumericReader.SMALL_BUFFER_SIZE + 10; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, second[i]);
				writer.set(2, LocalTime.ofNanoOfDay((long) third[i]));
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
				writer.set(4, fifth[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[4]));

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Double.isFinite(second[i]) ? Math.round(second[i]) : second[i]);
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> LocalTime.ofNanoOfDay((long) (third[i])));
			Object[] forthExpected = new Object[numberOfRows];
			Arrays.setAll(forthExpected, i -> Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> first[i]);


			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected, fifthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e"}, table.labelArray());
		}

		@Test
		public void testSparseDefaultIsNan() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10 +
					ArrayBuilderConfiguration.DEFAULT_INITIAL_CHUNK_SIZE * 10;
			double[] first = sparseRandom(numberOfRows, true);
			double[] second = sparseRandom(numberOfRows, true);
			double[] third = sparseRandom(numberOfRows, true);
			double[] forth = sparseRandom(numberOfRows, true);
			double[] fifth = sparseRandom(numberOfRows, true);

			MixedRowWriter writer = new MixedRowWriter(Arrays.asList("a", "b", "c", "d", "e"),
					Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT, TypeId.TIME, TypeId.DATE_TIME, TypeId.REAL));
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE + 10; i++) {
				writer.move();
				writer.set(0, Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
				writer.set(1, second[i]);
				writer.set(2, Double.isNaN(third[i]) ? null : LocalTime.ofNanoOfDay((long) third[i]));
				writer.set(3, Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond((long) forth[i], 0));
				writer.set(4, fifth[i]);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriter);
			assertTrue(writer.getNumericColumns()[1] instanceof Integer53BitColumnWriter);
			assertTrue(writer.getObjectColumns()[2] instanceof TimeColumnWriter);
			assertTrue(writer.getObjectColumns()[3] instanceof NanosecondsDateTimeWriter);
			assertTrue(writer.getNumericColumns()[4] instanceof RealColumnWriter);
			writer.checkForSparsity();
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getNumericColumns()[1] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getObjectColumns()[2] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getObjectColumns()[3] instanceof NanoDateTimeWriterSparse);
			assertTrue(writer.getNumericColumns()[4] instanceof RealColumnWriterSparse);
			for (int i = NumericReader.SMALL_BUFFER_SIZE + 10; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
				writer.set(1, second[i]);
				writer.set(2, Double.isNaN(third[i]) ? null : LocalTime.ofNanoOfDay((long) third[i]));
				writer.set(3, Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond((long) forth[i], 0));
				writer.set(4, fifth[i]);
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[4]));

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Double.isFinite(second[i]) ? Math.round(second[i]) : second[i]);
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> Double.isNaN(first[i]) ? null : LocalTime.ofNanoOfDay((long) (third[i])));
			Object[] forthExpected = new Object[numberOfRows];
			Arrays.setAll(forthExpected, i -> Double.isNaN(first[i]) ? null : Instant.ofEpochSecond((long) forth[i], 0));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> first[i]);


			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected, fifthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e"}, table.labelArray());
		}

		@Test
		public void testAutoSparsity() {
			// this many rows will force the ComplexRowWriter to check for sparsity
			int numberOfRows = NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW + 1;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, false);
			double[] third = sparseRandom(numberOfRows, false);
			double[] forth = sparseRandom(numberOfRows, false);
			double[] fifth = sparseRandom(numberOfRows, false);

			MixedRowWriter writer = new MixedRowWriter(Arrays.asList("a", "b", "c", "d", "e"),
					Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT, TypeId.TIME, TypeId.DATE_TIME, TypeId.REAL));
			for (int i = 0; i < numberOfRows - 1; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, second[i]);
				writer.set(2, LocalTime.ofNanoOfDay((long) third[i]));
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
				writer.set(4, fifth[i]);
			}
			// this is the max number of rows before checking for sparsity
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriter);
			assertTrue(writer.getNumericColumns()[1] instanceof Integer53BitColumnWriter);
			assertTrue(writer.getObjectColumns()[2] instanceof TimeColumnWriter);
			assertTrue(writer.getObjectColumns()[3] instanceof NanosecondsDateTimeWriter);
			assertTrue(writer.getNumericColumns()[4] instanceof RealColumnWriter);
			// add another row
			writer.move();
			writer.set(0, "" + Math.round(first[numberOfRows - 1] % 1000));
			writer.set(1, second[numberOfRows - 1]);
			writer.set(2, LocalTime.ofNanoOfDay((long) third[numberOfRows - 1]));
			writer.set(3, Instant.ofEpochSecond((long) forth[numberOfRows - 1], (int) third[numberOfRows - 1]));
			writer.set(4, fifth[numberOfRows - 1]);
			// now the check for sparsity should have taken place
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getNumericColumns()[1] instanceof Integer53BitColumnWriterSparse);
			assertTrue(writer.getObjectColumns()[2] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getObjectColumns()[3] instanceof NanoDateTimeWriterSparse);
			assertTrue(writer.getNumericColumns()[4] instanceof RealColumnWriterSparse);
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[4]));

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Double.isFinite(second[i]) ? Math.round(second[i]) : second[i]);
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> LocalTime.ofNanoOfDay((long) (third[i])));
			Object[] forthExpected = new Object[numberOfRows];
			Arrays.setAll(forthExpected, i -> Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			Object[] fifthExpected = new Object[numberOfRows];
			Arrays.setAll(fifthExpected, i -> first[i]);


			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected, fifthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e"}, table.labelArray());
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
					Arrays.asList(TypeId.REAL, TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME,
							TypeId.INTEGER_53_BIT), numberOfRows / 2, false);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.INTEGER_53_BIT, TypeId.REAL,
							TypeId.TEXT, TypeId.TIME), numberOfRows, false);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TEXT, TypeId.TIME, TypeId.REAL,
							TypeId.INTEGER_53_BIT), numberOfRows * 2, false);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME,
							TypeId.INTEGER_53_BIT), false);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME,
							TypeId.INTEGER_53_BIT), true);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT), true);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT), false);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TEXT, TypeId.TIME), numberOfRows *
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
					Arrays.asList(TypeId.INTEGER_53_BIT, TypeId.TEXT, TypeId.TIME), 2, false);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceFree() {
			MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("c", "d"),
					Arrays.asList(TypeId.TEXT, TypeId.TIME), 2, false);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceTime() {
			MixedRowWriter writer = Writers.mixedRowWriter(Collections.singletonList("d"),
					Collections.singletonList(TypeId.REAL), 2, false);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testIllegalStateSparseCategorical() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;

			MixedRowWriter writer = new MixedRowWriter(Arrays.asList("a"), Arrays.asList(TypeId.NOMINAL));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "defaultValue");
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getObjectColumns()[0] instanceof Int32CategoricalWriterSparse);
			writer.create();
			writer.move();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testIllegalStateSparseDateTime() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;

			Instant defaultValue = Instant.ofEpochSecond(398523);
			MixedRowWriter writer = new MixedRowWriter(Arrays.asList("a"), Arrays.asList(TypeId.DATE_TIME));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, defaultValue);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getObjectColumns()[0] instanceof NanoDateTimeWriterSparse);
			writer.create();
			writer.move();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testIllegalStateSparseTime() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;

			LocalTime defaultValue = LocalTime.ofNanoOfDay(398523);
			MixedRowWriter writer = new MixedRowWriter(Arrays.asList("a"), Arrays.asList(TypeId.TIME));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, defaultValue);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getObjectColumns()[0] instanceof TimeColumnWriterSparse);
			writer.create();
			writer.move();
			writer.create();
		}

		public static class ColumnTypeTest {

			@Test
			public void testUnknownRows() {
				MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
						Arrays.asList(TypeId.NOMINAL, TypeId.INTEGER_53_BIT, TypeId.REAL, TypeId.DATE_TIME,
								TypeId.TIME, TypeId.TEXT), false);
				Table table = writer.create();

				assertEquals(ColumnType.NOMINAL, table.column(0).type());
				assertEquals(ColumnType.INTEGER_53_BIT, table.column(1).type());
				assertEquals(ColumnType.REAL, table.column(2).type());
				assertEquals(ColumnType.DATETIME, table.column(3).type());
				assertEquals(ColumnType.TIME, table.column(4).type());
				assertEquals(ColumnType.TEXT, table.column(5).type());
			}


			@Test
			public void testKnownRows() {
				int numberOfRows = 192;
				double[] first = random(numberOfRows);
				double[] third = random(numberOfRows);

				MixedRowWriter writer = Writers.mixedRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f"),
						Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.REAL, TypeId.TIME,
								TypeId.INTEGER_53_BIT, TypeId.TEXT), numberOfRows / 2, false);
				for (int i = 0; i < numberOfRows; i++) {
					writer.move();
					writer.set(0, "" + first[i]);
					writer.set(3, null);
					writer.set(2, third[i]);
					writer.set(5, "value" + third[i]);
				}
				Table table = writer.create();

				assertEquals(ColumnType.NOMINAL, table.column(0).type());
				assertEquals(ColumnType.DATETIME, table.column(1).type());
				assertEquals(ColumnType.REAL, table.column(2).type());
				assertEquals(ColumnType.TIME, table.column(3).type());
				assertEquals(ColumnType.INTEGER_53_BIT, table.column(4).type());
				assertEquals(ColumnType.TEXT, table.column(5).type());
			}

		}

		public static class ToString {

			@Test
			public void testNoRows() {
				MixedRowWriter writer =
						new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId
								.NOMINAL), true);
				String expected = "General row writer (0x2)";
				assertEquals(expected, writer.toString());
			}

			@Test
			public void testNoRowsMoved() {
				MixedRowWriter writer =
						new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.INTEGER_53_BIT, TypeId
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
						new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.REAL),
								19, true);
				String expected = "General row writer (0x2)";
				assertEquals(expected, writer.toString());
			}

			@Test
			public void testExpectedMoved() {
				MixedRowWriter writer =
						new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.NOMINAL),
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
						new MixedRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.INTEGER_53_BIT, TypeId
								.NOMINAL), true);
				String expected = "General row writer (0x" + writer.width() + ")";
				assertEquals(expected, writer.toString());
			}

		}
	}
}
