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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SplittableRandom;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.ArrayBuilderConfiguration;

import junit.framework.TestCase;


/**
 * @author Gisa Meier, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class ComplexRowWriterTests {

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
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME));
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
		public void testSparse() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10 +
					ArrayBuilderConfiguration.DEFAULT_INITIAL_CHUNK_SIZE * 10;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, false);
			double[] third = sparseRandom(numberOfRows, false);
			double[] forth = sparseRandom(numberOfRows, false);
			double[] sixth = random(numberOfRows);
			double[] seventh = random(numberOfRows);
			double[] eight = random(numberOfRows);
			double[] ninth = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i"),
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME, TypeId.DATE_TIME,
							TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME));
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE + 10; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, "value" + third[i]);
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
				writer.set(5, "" + Math.round(sixth[i] * 1000));
				writer.set(6, LocalTime.ofNanoOfDay((long) (seventh[i] * 1_000_000)));
				writer.set(7, "value" + eight[i]);
				writer.set(8, Instant.ofEpochSecond((long) (ninth[i] * 1_000_000), (int) (eight[i] * 1_000_000)));
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[3] instanceof NanosecondsDateTimeWriter);
			assertTrue(writer.getColumns()[4] instanceof NanosecondsDateTimeWriter);
			assertTrue(writer.getColumns()[5] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[6] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[7] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[8] instanceof NanosecondsDateTimeWriter);
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter); // there is no sparse object writer
			assertTrue(writer.getColumns()[3] instanceof NanoDateTimeWriterSparse);
			assertTrue(writer.getColumns()[4] instanceof NanoDateTimeWriterSparse);
			assertTrue(writer.getColumns()[5] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[6] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[7] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[8] instanceof NanosecondsDateTimeWriter);
			for (int i = NumericReader.SMALL_BUFFER_SIZE + 10; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, "value" + third[i]);
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (long) third[i]));
				writer.set(5, "" + Math.round(sixth[i] * 1000));
				writer.set(6, LocalTime.ofNanoOfDay((long) (seventh[i] * 1_000_000)));
				writer.set(7, "value" + eight[i]);
				writer.set(8, Instant.ofEpochSecond((long) (ninth[i] * 1_000_000), (int) (eight[i] * 1_000_000)));
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[4]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[5]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[6]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[7]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[8]));

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> LocalTime.ofNanoOfDay((long) second[i]));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] forthExpected = new Object[numberOfRows];
			Arrays.setAll(forthExpected, i -> Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			Object[] sixthExpected = new Object[numberOfRows];
			Arrays.setAll(sixthExpected, i -> "" + Math.round(sixth[i] * 1000));
			Object[] seventhExpected = new Object[numberOfRows];
			Arrays.setAll(seventhExpected, i -> LocalTime.ofNanoOfDay((long) (seventh[i] * 1_000_000)));
			Object[] eightExpected = new Object[numberOfRows];
			Arrays.setAll(eightExpected, i -> "value" + eight[i]);
			Object[] ninthExpected = new Object[numberOfRows];
			Arrays.setAll(ninthExpected, i -> Instant.ofEpochSecond((long) (ninth[i] * 1_000_000), (int) (eight[i] * 1_000_000)));

			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected,
							new Object[numberOfRows], sixthExpected, seventhExpected, eightExpected, ninthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, table.labelArray());
		}

		@Test
		public void testSparseDefaultIsNan() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10 +
					ArrayBuilderConfiguration.DEFAULT_INITIAL_CHUNK_SIZE * 10;
			double[] first = sparseRandom(numberOfRows, false);
			first[numberOfRows / 2] = Double.NaN;
			double[] second = sparseRandom(numberOfRows, true);
			double[] third = sparseRandom(numberOfRows, true);
			double[] forth = sparseRandom(numberOfRows, true);
			double[] sixth = random(numberOfRows);
			double[] seventh = random(numberOfRows);
			double[] eight = random(numberOfRows);
			double[] ninth = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d", "e", "f", "g", "h", "i"),
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME, TypeId.NOMINAL,
							TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME));
			for (int i = 0; i < NumericReader.SMALL_BUFFER_SIZE + 10; i++) {
				writer.move();
				writer.set(0, Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
				writer.set(1, Double.isNaN(second[i]) ? null : LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, Double.isNaN(third[i]) ? null : "value" + third[i]);
				writer.set(3, Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond((long) forth[i], (int) third[i]));
				writer.set(5, Double.isNaN(sixth[i]) ? null : "" + Math.round(sixth[i] * 1000));
				writer.set(6, Double.isNaN(seventh[i]) ? null : LocalTime.ofNanoOfDay((long) (seventh[i] * 1_000_000)));
				writer.set(7, Double.isNaN(eight[i]) ? null : "value" + eight[i]);
				writer.set(8, Double.isNaN(ninth[i]) ? null : Instant.ofEpochSecond((long) (ninth[i] * 1_000_000),
						(int) (eight[i] * 1_000_000)));
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[3] instanceof NanosecondsDateTimeWriter);
			assertTrue(writer.getColumns()[4] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[5] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[6] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[7] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[8] instanceof NanosecondsDateTimeWriter);
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter); // there is no sparse object writer
			assertTrue(writer.getColumns()[3] instanceof NanoDateTimeWriterSparse);
			assertTrue(writer.getColumns()[4] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getColumns()[5] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[6] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[7] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[8] instanceof NanosecondsDateTimeWriter);
			for (int i = NumericReader.SMALL_BUFFER_SIZE + 10; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
				writer.set(1, Double.isNaN(second[i]) ? null : LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, Double.isNaN(third[i]) ? null : "value" + third[i]);
				writer.set(3, Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond((long) forth[i], (long) third[i]));
				writer.set(5, Double.isNaN(sixth[i]) ? null : "" + Math.round(sixth[i] * 1000));
				writer.set(6, Double.isNaN(seventh[i]) ? null : LocalTime.ofNanoOfDay((long) (seventh[i] * 1_000_000)));
				writer.set(7, Double.isNaN(eight[i]) ? null : "value" + eight[i]);
				writer.set(8, Double.isNaN(ninth[i]) ? null : Instant.ofEpochSecond((long) (ninth[i] * 1_000_000),
						(int) (eight[i] * 1_000_000)));
			}
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[4]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[5]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[6]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[7]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[8]));

			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> Double.isNaN(second[i]) ? null : LocalTime.ofNanoOfDay((long) second[i]));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> Double.isNaN(third[i]) ? null : "value" + third[i]);
			Object[] forthExpected = new Object[numberOfRows];
			Arrays.setAll(forthExpected, i -> Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond((long) forth[i],
					(int) third[i]));
			Object[] sixthExpected = new Object[numberOfRows];
			Arrays.setAll(sixthExpected, i -> Double.isNaN(sixth[i]) ? null : "" + Math.round(sixth[i] * 1000));
			Object[] seventhExpected = new Object[numberOfRows];
			Arrays.setAll(seventhExpected, i -> Double.isNaN(seventh[i]) ? null : LocalTime.ofNanoOfDay(
					(long) (seventh[i] * 1_000_000)));
			Object[] eightExpected = new Object[numberOfRows];
			Arrays.setAll(eightExpected, i -> Double.isNaN(eight[i]) ? null : "value" + eight[i]);
			Object[] ninthExpected = new Object[numberOfRows];
			Arrays.setAll(ninthExpected, i -> Double.isNaN(ninth[i]) ? null : Instant.ofEpochSecond(
					(long) (ninth[i] * 1_000_000), (int) (eight[i] * 1_000_000)));

			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected,
							new Object[numberOfRows], sixthExpected, seventhExpected, eightExpected, ninthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, table.labelArray());
		}

		@Test
		public void testAutoSparsity() {
			// this many rows will force the ComplexRowWriter to check for sparsity
			int numberOfRows = NumericRowWriter.MAX_CHECK_FOR_SPARSITY_ROW + 1;
			double[] first = sparseRandom(numberOfRows, false);
			double[] second = sparseRandom(numberOfRows, false);
			double[] third = sparseRandom(numberOfRows, false);
			double[] forth = sparseRandom(numberOfRows, false);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME));
			for (int i = 0; i < numberOfRows - 1; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, "value" + third[i]);
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			}
			// this is the max number of rows before checking for sparsity
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriter);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriter);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[3] instanceof NanosecondsDateTimeWriter);
			// add another row
			writer.move();
			writer.set(0, "" + Math.round(first[numberOfRows - 1] % 1000));
			writer.set(1, LocalTime.ofNanoOfDay((long) second[numberOfRows - 1]));
			writer.set(2, "value" + third[numberOfRows - 1]);
			writer.set(3, Instant.ofEpochSecond((long) forth[numberOfRows - 1], (int) third[numberOfRows - 1]));
			// now the check for sparsity should have taken place
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter); // there is no sparse object writer
			assertTrue(writer.getColumns()[3] instanceof NanoDateTimeWriterSparse);
			Table table = writer.create();
			TestCase.assertEquals(numberOfRows, table.height());
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));
			Object[] firstExpected = new Object[numberOfRows];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[numberOfRows];
			Arrays.setAll(secondExpected, i -> LocalTime.ofNanoOfDay((long) second[i]));
			Object[] thirdExpected = new Object[numberOfRows];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] forthExpected = new Object[numberOfRows];
			Arrays.setAll(forthExpected, i -> Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			assertEquals(numberOfRows, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testFirstSparseThenDense() {
			int firstNumberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;
			double[] first = new double[firstNumberOfRows * 11];
			double[] second = new double[firstNumberOfRows * 11];
			double[] third = new double[firstNumberOfRows * 11];
			double[] forth = new double[firstNumberOfRows * 11];
			System.arraycopy(sparseRandom(firstNumberOfRows, false), 0, first, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, false), 0, second, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, false), 0, third, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, false), 0, forth, 0, firstNumberOfRows);
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME));
			for (int i = 0; i < firstNumberOfRows; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, "value" + third[i]);
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[3] instanceof NanoDateTimeWriterSparse);
			// now we add 10 times more dense data so that the overall column should be dense
			for (int i = firstNumberOfRows; i < firstNumberOfRows * 11; i++) {
				first[i] = Math.random() * 1_000_000;
				second[i] = Math.random() * 1_000_000;
				third[i] = Math.random() * 1_000_000;
				forth[i] = Math.random() * 1_000_000;
			}
			for (int i = firstNumberOfRows; i < firstNumberOfRows * 11; i++) {
				writer.move();
				writer.set(0, "" + Math.round(first[i] % 1000));
				writer.set(1, LocalTime.ofNanoOfDay((long) second[i]));
				writer.set(2, "value" + third[i]);
				writer.set(3, Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			}
			Table table = writer.create();
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[3]));
			Object[] firstExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(firstExpected, i -> "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(secondExpected, i -> LocalTime.ofNanoOfDay((long) second[i]));
			Object[] thirdExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(thirdExpected, i -> "value" + third[i]);
			Object[] forthExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(forthExpected, i -> Instant.ofEpochSecond((long) forth[i], (int) third[i]));
			assertEquals(firstNumberOfRows * 11, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected},
					readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void firstSparseThenSparseWithOtherDefault() {
			int firstNumberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;
			double[] first = new double[firstNumberOfRows * 11];
			double[] second = new double[firstNumberOfRows * 11];
			double[] third = new double[firstNumberOfRows * 11];
			double[] forth = new double[firstNumberOfRows * 11];
			System.arraycopy(sparseRandom(firstNumberOfRows, 17), 0, first, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, Double.NaN), 0, second, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, -100), 0, third, 0, firstNumberOfRows);
			System.arraycopy(sparseRandom(firstNumberOfRows, 42), 0, forth, 0, firstNumberOfRows);
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME));
			for (int i = 0; i < firstNumberOfRows; i++) {
				writer.move();
				writer.set(0, Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
				writer.set(1, Double.isNaN(second[i]) ? null : LocalTime.ofNanoOfDay(Math.round(second[i])));
				writer.set(2, Double.isNaN(third[i]) ? null : "value" + third[i]);
				writer.set(3, Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond(Math.round(forth[i]), 0));
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriterSparse);
			assertTrue(writer.getColumns()[1] instanceof TimeColumnWriterSparse);
			assertTrue(writer.getColumns()[2] instanceof ObjectWriter);
			assertTrue(writer.getColumns()[3] instanceof NanoDateTimeWriterSparse);
			assertEquals("" + 17, ((Int32CategoricalWriterSparse) writer.getColumns()[0]).getDefaultValue());
			assertEquals(TimeColumn.MISSING_VALUE, ((TimeColumnWriterSparse) writer.getColumns()[1]).getDefaultValue());
			assertEquals(42, ((NanoDateTimeWriterSparse) writer.getColumns()[3]).getDefaultValue());
			// now we add 10 times more sparse data with different default
			// values so that the default value of the column should change
			for (int i = firstNumberOfRows; i < firstNumberOfRows * 11; i++) {
				first[i] = 10;
				second[i] = 10;
				third[i] = 10;
				forth[i] = Double.NaN;
			}
			for (int i = firstNumberOfRows; i < firstNumberOfRows * 11; i++) {
				writer.move();
				writer.set(0, Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
				writer.set(1, Double.isNaN(second[i]) ? null : LocalTime.ofNanoOfDay(Math.round(second[i])));
				writer.set(2, Double.isNaN(third[i]) ? null : "value" + third[i]);
				writer.set(3, Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond(Math.round(forth[i]),
						0));
			}
			Table table = writer.create();

			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[0]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[1]));
			assertFalse(ColumnTestUtils.isSparse(table.getColumns()[2]));
			assertTrue(ColumnTestUtils.isSparse(table.getColumns()[3]));

			@SuppressWarnings("unchecked")
			List<String> mapping = ((Int32CategoricalWriterSparse) writer.getColumns()[0]).getValueLookup();
			int defaultCategoricalIndex = (int) ColumnTestUtils.getDefaultValue(table.getColumns()[0]);
			assertEquals("" + 10, mapping.get(defaultCategoricalIndex));
			assertEquals(10, Math.round(ColumnTestUtils.getDefaultValue(table.getColumns()[1])));
			assertEquals(DateTimeColumn.MISSING_VALUE, Math.round(ColumnTestUtils.getDefaultValue(table.getColumns()[3])));

			Object[] firstExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(firstExpected, i -> Double.isNaN(first[i]) ? null : "" + Math.round(first[i] % 1000));
			Object[] secondExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(secondExpected, i -> Double.isNaN(second[i]) ? null : LocalTime.ofNanoOfDay(Math.round(second[i])));
			Object[] thirdExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(thirdExpected, i -> Double.isNaN(third[i]) ? null : "value" + third[i]);
			Object[] forthExpected = new Object[firstNumberOfRows * 11];
			Arrays.setAll(forthExpected, i -> Double.isNaN(forth[i]) ? null : Instant.ofEpochSecond(Math.round(forth[i]),
					0));

			assertEquals(firstNumberOfRows * 11, table.height());
			assertArrayEquals(new Object[][]{firstExpected, secondExpected, thirdExpected, forthExpected},
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
					Arrays.asList(TypeId.NOMINAL, TypeId.TIME, TypeId.TEXT, TypeId.DATE_TIME), numberOfRows /
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TEXT, TypeId.TIME), numberOfRows);
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
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TEXT, TypeId.TIME), numberOfRows *
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
		public void testIllegalStateSparseCategorical() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a"), Arrays.asList(TypeId.NOMINAL));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "defaultValue");
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof Int32CategoricalWriterSparse);
			writer.create();
			writer.move();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testIllegalStateSparseDateTime() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;

			Instant defaultValue = Instant.ofEpochSecond(398523);
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a"), Arrays.asList(TypeId.DATE_TIME));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, defaultValue);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof NanoDateTimeWriterSparse);
			writer.create();
			writer.move();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testIllegalStateSparseTime() {
			int numberOfRows = NumericReader.SMALL_BUFFER_SIZE + 10;

			LocalTime defaultValue = LocalTime.ofNanoOfDay(398523);
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a"), Arrays.asList(TypeId.TIME));
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, defaultValue);
			}
			// we added more than NumericReader.SMALL_BUFFER_SIZE values. Therefore, the buffer has been written to the
			// columns and we can now check for sparsity to force sparse columns
			writer.checkForSparsity();
			assertTrue(writer.getColumns()[0] instanceof TimeColumnWriterSparse);
			writer.create();
			writer.move();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwice() {
			int numberOfRows = 142;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			double[] third = random(numberOfRows);


			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TEXT, TypeId.TIME), numberOfRows *
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
					Arrays.asList(TypeId.DATE_TIME, TypeId.TEXT, TypeId.TIME), 2);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceFree() {
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("c", "d"),
					Arrays.asList(TypeId.TEXT, TypeId.TIME), 2);
			writer.create();
			writer.create();
		}

		@Test(expected = IllegalStateException.class)
		public void testCreateTwiceTime() {
			ComplexRowWriter writer = new ComplexRowWriter(Collections.singletonList("d"),
					Collections.singletonList(TypeId.TIME), 2);
			writer.create();
			writer.create();
		}
	}

	public static class ColumnTypeTest {

		@Test
		public void testUnknownRows() {
			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TIME, TypeId.TEXT));
			Table table = writer.create();

			assertEquals(ColumnType.NOMINAL, table.column(0).type());
			assertEquals(ColumnType.DATETIME, table.column(1).type());
			assertEquals(ColumnType.TIME, table.column(2).type());
			assertEquals(ColumnType.TEXT, table.column(3).type());
		}


		@Test
		public void testKnownRows() {
			int numberOfRows = 192;
			double[] first = random(numberOfRows);
			double[] third = random(numberOfRows);

			ComplexRowWriter writer = new ComplexRowWriter(Arrays.asList("a", "b", "c", "d"),
					Arrays.asList(TypeId.NOMINAL, TypeId.DATE_TIME, TypeId.TIME, TypeId.TEXT), numberOfRows /
					2);
			for (int i = 0; i < numberOfRows; i++) {
				writer.move();
				writer.set(0, "" + first[i]);
				writer.set(2, null);
				writer.set(3, "value" + third[i]);
			}
			Table table = writer.create();

			assertEquals(ColumnType.NOMINAL, table.column(0).type());
			assertEquals(ColumnType.DATETIME, table.column(1).type());
			assertEquals(ColumnType.TIME, table.column(2).type());
			assertEquals(ColumnType.TEXT, table.column(3).type());
		}

	}

	public static class CategoricalWriter {

		@Test
		public void testConstructor() {
			int numberOfRows = 2354;
			String defaultValue = "defaultValue";
			Int32CategoricalWriterSparse writer =
					new Int32CategoricalWriterSparse(ColumnType.NOMINAL, defaultValue);
			assertEquals(2, writer.getValueLookup().size());
			assertNull(writer.getValueLookup().get(0));
			assertEquals(defaultValue, writer.getValueLookup().get(1));
			assertEquals(defaultValue, writer.getDefaultValue());
			String[] expected = Arrays.stream(sparseRandom(numberOfRows, false)).mapToObj(x -> "" + x)
					.toArray(String[]::new);
			writer.fill(expected, 0, 0, 1, expected.length);
			Column column = writer.toColumn();
			Object[] readData = new Object[column.size()];
			column.fill(readData, 0);
			assertArrayEquals(expected, readData);
		}

		@Test
		public void testConstructorDefaultIsNull() {
			int numberOfRows = 2354;
			String defaultValue = null;
			Int32CategoricalWriterSparse writer =
					new Int32CategoricalWriterSparse(ColumnType.NOMINAL, defaultValue);
			assertEquals(1, writer.getValueLookup().size());
			assertNull(writer.getValueLookup().get(0));
			assertNull(writer.getDefaultValue());
			String[] expected = Arrays.stream(sparseRandom(numberOfRows, true))
					.mapToObj(x -> Double.isNaN(x) ? null : "" + x).toArray(String[]::new);
			writer.fill(expected, 0, 0, 1, expected.length);
			Column column = writer.toColumn();
			Object[] readData = new Object[column.size()];
			column.fill(readData, 0);
			assertArrayEquals(expected, readData);
		}
	}

	public static class ToString {

		@Test
		public void testNoRows() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.NOMINAL));
			String expected = "Object row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testNoRowsMoved() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.NOMINAL));
			writer.move();
			writer.move();
			writer.move();
			String expected = "Object row writer (3x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedRows() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.NOMINAL),
							19);
			String expected = "Object row writer (0x2)";
			assertEquals(expected, writer.toString());
		}

		@Test
		public void testExpectedMoved() {
			ComplexRowWriter writer =
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.NOMINAL),
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
					new ComplexRowWriter(Arrays.asList("a", "b"), Arrays.asList(TypeId.TIME, TypeId.NOMINAL));
			String expected = "Object row writer (0x" + writer.width() + ")";
			assertEquals(expected, writer.toString());
		}

	}
}
