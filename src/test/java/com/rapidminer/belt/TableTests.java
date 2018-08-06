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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.Column.TypeId;
import com.rapidminer.belt.util.ColumnAnnotation;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.TaskAbortedException;

/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class TableTests {

	private static final double EPSILON = 1e-10;

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	/**
	 * Simple context that becomes inactive the moment the first job is submitted.
	 */
	private static class OneShotContext implements Context {

		private AtomicBoolean active = new AtomicBoolean(true);

		@Override
		public boolean isActive() {
			return active.get();
		}

		@Override
		public int getParallelism() {
			return CTX.getParallelism();
		}

		@Override
		public <T> Future<T> submit(Callable<T> job) {
			if (active.get()) {
				active.set(false);
				return CTX.submit(job);
			} else {
				throw new RejectedExecutionException("You had your one shot");
			}
		}
	}

	private static Column[] random(int width, int height) {
		Column[] columns = new Column[width];
		Arrays.setAll(columns, i -> {
			double[] data = new double[height];
			Arrays.setAll(data, j -> Math.random());
			return new DoubleArrayColumn(data);
		});
		return columns;
	}

	private static String[] randomLabels(int n) {
		String[] labels = new String[n];
		Arrays.setAll(labels, i -> "col" + i);
		return labels;
	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	private static double[] randomWithSame(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.floor(10 * Math.random()));
		return data;
	}

	private static double[] readAllToArray(ColumnReader reader) {
		double[] result = new double[reader.remaining()];
		int i = 0;
		while (reader.hasRemaining()) {
			result[i++] = reader.read();
		}
		return result;
	}

	private static double[][] readAllColumnsToArrays(RowReader reader) {
		double[][] columns = new double[reader.width()][];
		Arrays.setAll(columns, i -> new double[reader.remaining()]);
		int i = 0;
		while (reader.hasRemaining()) {
			reader.move();
			for (int j = 0; j < reader.width(); j++) {
				columns[j][i] = reader.get(j);
			}
			i++;
		}
		return columns;
	}

	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		ColumnReader reader = new ColumnReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	private static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	public static class InputValidation {

		@Test(expected = NullPointerException.class)
		public void testConstructorWithNullColumnSet() {
			new Table(null, new String[0]);
		}

		@Test(expected = NullPointerException.class)
		public void testConstructorWithNullColumn() {
			Column[] columns = new Column[]{
					new DoubleArrayColumn(new double[128]),
					null,
					new DoubleArrayColumn(new double[128])
			};
			new Table(columns, new String[]{"a", "b", "c"});
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorWithVaryingColumnSizes() {
			Column[] columns = new Column[]{
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[256])
			};
			new Table(columns, new String[]{"a", "b", "c"});
		}

		@Test(expected = NullPointerException.class)
		public void testConstructorWithNullLabelSet() {
			new Table(new Column[0], null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorWithMismatchingLabelSet() {
			Column[] columns = new Column[]{
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128])
			};
			new Table(columns, new String[]{"a", "b", "c", "d"});
		}


		@Test(expected = NullPointerException.class)
		public void testConstructorWithNullLabel() {
			Column[] columns = new Column[]{
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128])
			};
			new Table(columns, new String[]{"a", null, "c"});
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorWithEmptyLabel() {
			Column[] columns = new Column[]{
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128])
			};
			new Table(columns, new String[]{"a", "", "c"});
		}

		@Test(expected = IllegalArgumentException.class)
		public void testConstructorWithDuplicateLabel() {
			Column[] columns = new Column[]{
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128]),
					new DoubleArrayColumn(new double[128])
			};
			new Table(columns, new String[]{"a", "a", "c"});
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testColumnWithNegativeIndex() {
			Column[] columns = random(3, 128);
			Table table = new Table(columns, new String[]{"a", "b", "c"});
			table.column(-1);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testColumnWithInvalidIndex() {
			Column[] columns = random(3, 128);
			Table table = new Table(columns, new String[]{"a", "b", "c"});
			table.column(123);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testColumnWithInvalidLabel() {
			Column[] columns = random(3, 128);
			Table table = new Table(columns, new String[]{"a", "b", "c"});
			table.column("invalid");
		}
	}

	public static class Dimensions {

		@Test
		public void testRowReaderWithEmptyColumnSet() {
			Column[] columns = random(3, 128);
			Table table = new Table(columns, new String[]{"a", "b", "c"});
			RowReader reader = new RowReader(new Column[]{});
			assertFalse(reader.hasRemaining());
		}

		@Test
		public void testZeroDimensions() {
			Table table = new Table(new Column[0], new String[0]);
			assertEquals(0, table.width());
			assertEquals(0, table.height());
		}

		@Test
		public void testSingleColumnDimensions() {
			int width = 1;
			int height = 1536;
			Table table = new Table(random(width, height), randomLabels(width));
			assertEquals(width, table.width());
			assertEquals(height, table.height());
		}

		@Test
		public void testSingleRowDimensions() {
			int width = 256;
			int height = 1;
			Table table = new Table(random(width, height), randomLabels(width));
			assertEquals(width, table.width());
			assertEquals(height, table.height());
		}

		@Test
		public void testDimensions() {
			int width = 16;
			int height = 2048;
			Table table = new Table(random(width, height), randomLabels(width));
			assertEquals(width, table.width());
			assertEquals(height, table.height());
		}
	}

	public static class ColumnOrder {

		@Test
		public void testColumnReader() {
			int width = 3;
			int height = 768;
			Column[] columns = random(width, height);
			Table table = new Table(columns, randomLabels(width));
			for (int i = 0; i < width; i++) {
				ColumnReader direct = new ColumnReader(columns[i], table.height());
				ColumnReader tableReader = new ColumnReader(table.column(i));
				double[] directValues = readAllToArray(direct);
				double[] tableValues = readAllToArray(tableReader);
				assertArrayEquals(directValues, tableValues, EPSILON);
			}
		}

		@Test
		public void testDefaultWithRowReader() {
			int width = 5;
			int height = 256;
			Column[] columns = random(width, height);
			Table table = new Table(columns, randomLabels(width));
			RowReader direct = new RowReader(columns);
			RowReader tableReader = new RowReader(table);
			double[][] directValues = readAllColumnsToArrays(direct);
			double[][] tableValues = readAllColumnsToArrays(tableReader);
			assertEquals(5, directValues.length);
			assertEquals(5, tableValues.length);
			for (int i = 0; i < directValues.length; i++) {
				assertArrayEquals(directValues[i], tableValues[i], EPSILON);
			}
		}

		@Test
		public void testWithRowReader() {
			int width = 3;
			int height = 512;
			Column[] columns = random(width, height);
			Table table = new Table(columns, randomLabels(width));
			RowReader direct = new RowReader(new Column[]{columns[0], columns[2], columns[1], columns[0], columns[1]});
			RowReader tableReader = new RowReader(new Column[]{table.column(0), table.column(2),
					table.column(1), table.column(0), table.column(1)});
			double[][] directValues = readAllColumnsToArrays(direct);
			double[][] tableValues = readAllColumnsToArrays(tableReader);
			assertEquals(5, directValues.length);
			assertEquals(5, tableValues.length);
			for (int i = 0; i < directValues.length; i++) {
				assertArrayEquals(directValues[i], tableValues[i], EPSILON);
			}
		}
	}

	public static class Mapping {

		@Test
		public void testSameMappingForAllColumns() {
			int height = 512;
			double[] data = new double[height];
			Arrays.setAll(data, i -> i);
			int[] data2 = new int[height];
			Arrays.setAll(data2, i -> i);
			Column[] columns = {new DoubleArrayColumn(data),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data2, new ArrayList<>()),
					new DoubleArrayColumn(data)};
			Table table = new Table(columns, randomLabels(3));

			int[] mapping = {4, 123, 6, 11, 456, 99, 6};
			Table mappedTable = table.map(mapping, true);

			RowReader reader = new RowReader(mappedTable);
			double[][] tableValues = readAllColumnsToArrays(reader);
			double[] expected = {4, 123, 6, 11, 456, 99, 6};
			for (double[] tableValue : tableValues) {
				assertArrayEquals(expected, tableValue, EPSILON);
			}
		}

		@Test
		public void testMappingTwice() {
			int height = 512;
			double[] data = new double[height];
			Arrays.setAll(data, i -> i);
			int[] data2 = new int[height];
			Arrays.setAll(data2, i -> i);
			Column[] columns = {new DoubleArrayColumn(data),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data2, new ArrayList<>()),
					new DoubleArrayColumn(data)};
			Table table = new Table(columns, randomLabels(3));

			int[] mapping = {4, 123, 6, 11, 456, 99, 6, 0, 4, 21, 123};
			Table mappedTable = table.map(mapping, true);

			int[] mapping2 = {1, 0, 6, 10, 8, 5};
			Table mappedTable2 = mappedTable.map(mapping2, true);

			RowReader reader = new RowReader(mappedTable2);
			double[][] tableValues = readAllColumnsToArrays(reader);
			double[] expected = {123, 4, 6, 123, 4, 99};
			for (double[] tableValue : tableValues) {
				assertArrayEquals(expected, tableValue, EPSILON);
			}
		}
	}



	public static class ColumnReordering {

		@Test(expected = IllegalArgumentException.class)
		public void testMultipleSelection() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			table.columns(new int[]{0, 1, 2, 0, 1, 2});
		}

		@Test
		public void testSingleColumn() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table reordered = table.columns(new int[]{2});

			assertArrayEquals(new double[][] { first, second, third }, readTableToArray(table));
			assertArrayEquals(new double[][] { third }, readTableToArray(reordered));
		}

		@Test(expected = NullPointerException.class)
		public void testNullOrdering() {
			Table table = Table.newTable(2)
					.addReal("a", i -> 0)
					.addReal("b", i -> i)
					.addReal("c", i -> 2 * i)
					.addReal("d", i -> 3 * i)
					.addReal("e", i -> 4 * i).build(CTX);
			table.columns((int[]) null);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			Table table = Table.newTable(2)
					.addReal("a", i -> 0)
					.addReal("b", i -> i)
					.addReal("c", i -> 2 * i)
					.addReal("d", i -> 3 * i)
					.addReal("e", i -> 4 * i).build(CTX);
			table.columns(new int[]{3, 4, 1, 5, 1, 2, 3, 0});
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			Table table = Table.newTable(2)
					.addReal("a", i -> 0)
					.addReal("b", i -> i)
					.addReal("c", i -> 2 * i)
					.addReal("d", i -> 3 * i)
					.addReal("e", i -> 4 * i).build(CTX);
			table.columns(new int[]{3, -1, 1, 5, 1, 2, 3, 0});
		}
	}

	public static class RowSelection {

		@Test
		public void testMultipleRows() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table rowSubset = table.rows(new int[]{2, 32, 17, 2}, CTX);

			assertArrayEquals(new double[][] { first, second, third }, readTableToArray(table));
			assertArrayEquals(new double[][] {
				new double[] { first[2], first[32], first[17], first[2] },
				new double[] { second[2], second[32], second[17], second[2] },
				new double[] { third[2], third[32], third[17], third[2] }
			}, readTableToArray(rowSubset));
		}

		@Test
		public void testOneRow() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table rowSubset = table.rows(new int[]{14}, CTX);

			assertArrayEquals(new double[][] { first, second, third }, readTableToArray(table));
			assertArrayEquals(new double[][] {
				new double[] { first[14] },
				new double[] { second[14] },
				new double[] { third[14] }
			}, readTableToArray(rowSubset));
		}

		@Test
		public void testInvalidRow() {
			int numberOfRows = 20;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.build(CTX);
			int[] selectedRows = {4, 19, 22, 13};
			Table selected = table.rows(selectedRows, CTX);
			double[] firstExpected = new double[]{first[4], first[19], Double.NaN, first[13]};
			double[] secondExpected = new double[]{second[4], second[19], Double.NaN, second[13]};
			assertArrayEquals(new double[][]{first, second}, readTableToArray(table));
			assertArrayEquals(new double[][]{firstExpected, secondExpected}, readTableToArray(selected));
		}

		@Test
		public void testOneInvalidRow() {
			int numberOfRows = 20;
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> i)
					.addReal("b", i -> 2 * i)
					.addReal("c", i -> 3 * i)
					.build(CTX);
			Table selected = table.rows(new int[]{20}).run(CTX);
			assertArrayEquals(new double[][]{new double[]{Double.NaN}, new double[]{Double.NaN},
					new double[]{Double.NaN}}, readTableToArray(selected));
		}

		@Test
		public void testNegativeRow() {
			int numberOfRows = 20;
			double[] first = random(numberOfRows);
			double[] second = random(numberOfRows);
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.build(CTX);
			Table selected = table.rows(new int[]{1, 3, -1, 2}).run(CTX);
			assertArrayEquals(new double[][]{first, second}, readTableToArray(table));
			double[] firstExpected = new double[]{first[1], first[3], Double.NaN, first[2]};
			double[] secondExpected = new double[]{second[1], second[3], Double.NaN, second[2]};
			assertArrayEquals(new double[][]{firstExpected, secondExpected}, readTableToArray(selected));
		}
	}

	public static class RangeSelection {

		@Test
		public void testRange() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table rowSubset = table.rows(11, 15, CTX);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new double[][]{
					new double[]{first[11], first[12], first[13], first[14]},
					new double[]{second[11], second[12], second[13], second[14]},
					new double[]{third[11], third[12], third[13], third[14]}
			}, readTableToArray(rowSubset));
		}

		@Test
		public void testEndRange() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table rowSubset = table.rows(NUMBER_OF_ROWS - 4, NUMBER_OF_ROWS, CTX);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new double[][]{
					new double[]{first[NUMBER_OF_ROWS - 4], first[NUMBER_OF_ROWS - 3], first[NUMBER_OF_ROWS - 2], first[NUMBER_OF_ROWS - 1]},
					new double[]{second[NUMBER_OF_ROWS - 4], second[NUMBER_OF_ROWS - 3], second[NUMBER_OF_ROWS - 2], second[NUMBER_OF_ROWS - 1]},
					new double[]{third[NUMBER_OF_ROWS - 4], third[NUMBER_OF_ROWS - 3], third[NUMBER_OF_ROWS - 2], third[NUMBER_OF_ROWS - 1]}
			}, readTableToArray(rowSubset));
		}

		@Test
		public void testOneRow() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table rowSubset = table.rows(14, 15, CTX);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new double[][]{
					new double[]{first[14]},
					new double[]{second[14]},
					new double[]{third[14]}
			}, readTableToArray(rowSubset));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testInvalidStart() {
			int numberOfRows = 20;
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> i)
					.addReal("b", i -> 2 * i)
					.addReal("c", i -> 3 * i)
					.build(CTX);
			table.rows(22, 35);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeStart() {
			int numberOfRows = 20;
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> i)
					.addReal("b", i -> 2 * i)
					.addReal("c", i -> 3 * i)
					.build(CTX);
			table.rows(-2, 5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEndTooSmall() {
			int numberOfRows = 20;
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> i)
					.addReal("b", i -> 2 * i)
					.addReal("c", i -> 3 * i)
					.build(CTX);
			table.rows(11, 9);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEndTooBig() {
			int numberOfRows = 20;
			Table table = Table.newTable(numberOfRows)
					.addReal("a", i -> i)
					.addReal("b", i -> 2 * i)
					.addReal("c", i -> 3 * i)
					.build(CTX);
			table.rows(1, numberOfRows + 1);
		}
	}

	public static class Interaction {

		@Test
		public void testSelectSortOne() {
			double[] first = randomWithSame(NUMBER_OF_ROWS);
			double[] second = randomWithSame(NUMBER_OF_ROWS);
			double[] third = randomWithSame(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			int[] rows = new int[NUMBER_OF_ROWS * 2];
			Random random = new Random();
			Arrays.setAll(rows, i -> random.nextInt(NUMBER_OF_ROWS));

			Table rowSuperset = table.rows(rows, CTX);

			double[] firstExpected = new double[rows.length];
			Arrays.setAll(firstExpected, i -> first[rows[i]]);
			double[] secondExpected = new double[rows.length];
			Arrays.setAll(secondExpected, i -> second[rows[i]]);
			double[] thirdExpected = new double[rows.length];
			Arrays.setAll(thirdExpected, i -> third[rows[i]]);

			Table sorted = rowSuperset.sort(new int[]{1}, Order.ASCENDING, CTX);
			int[] reorder = new DoubleArrayColumn(secondExpected).sort(Order.ASCENDING);

			double[] jdkSorting = Arrays.copyOf(secondExpected, secondExpected.length);
			Arrays.sort(jdkSorting);

			double[] firstReordered = new double[rows.length];
			Arrays.setAll(firstReordered, i -> firstExpected[reorder[i]]);
			double[] thirdReordered = new double[rows.length];
			Arrays.setAll(thirdReordered, i -> thirdExpected[reorder[i]]);

			assertArrayEquals(new double[][] { first, second, third }, readTableToArray(table));
			assertArrayEquals(new double[][] { firstExpected, secondExpected, thirdExpected },
					readTableToArray(rowSuperset));
			assertArrayEquals(new double[][] { firstReordered, jdkSorting, thirdReordered }, readTableToArray(sorted));
		}

		@Test
		public void testSelectAddSort() {
			double[] data = { 6, 5, 4, 3, 2, 1, 0 };
			int[] mapping = { 3, 5, 4, 5 };

			Table unmapped = Table.newTable(data.length)
					.addReal("a", i -> data[i])
					.build(CTX);
			Table mapped = unmapped.rows(mapping, CTX);

			double[] data2 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 };
			int[] mapping2 = { 17, 11, 13, 13 };

			Table unmapped2 = Table.newTable(data2.length).addReal("a", i -> data2[i]).build(CTX);
			Table mapped2 = unmapped2.rows(mapping2, CTX);

			Table union = Table.from(mapped2).add("b", mapped.column(0)).build(CTX);
			Table sorted = union.sort(new int[]{0, 1}, Order.DESCENDING, CTX);

			double[] first = { 17, 13, 13, 11 };
			double[] second = { 3, 2, 1, 1 };

			assertArrayEquals(new double[][] { first, second }, readTableToArray(sorted));

		}

		@Test
		public void testSelectAddReplace() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i]).build(CTX);

			int[] rows = new int[NUMBER_OF_ROWS / 3];
			Random random = new Random();
			Arrays.setAll(rows, i -> random.nextInt(NUMBER_OF_ROWS));

			Table rowSubset = table.rows(rows, CTX);

			double[] firstExpected = new double[rows.length];
			Arrays.setAll(firstExpected, i -> first[rows[i]]);
			double[] secondExpected = new double[rows.length];
			Arrays.setAll(secondExpected, i -> second[rows[i]]);
			double[] thirdExpected = new double[rows.length];
			Arrays.setAll(thirdExpected, i -> third[rows[i]]);

			double[] fourth = random(rows.length);
			double[] newSecond = random(rows.length);
			double[] fifth = random(rows.length);
			ColumnBuffer buffer = new FixedRealBuffer(rows.length);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, fifth[i]);
			}

			Table modified = Table.from(rowSubset)
					.addReal("d", i -> fourth[i])
					.replaceReal("b", i -> newSecond[i])
					.add("e", buffer.toColumn())
					.build(CTX);

			assertArrayEquals(new double[][] { first, second, third }, readTableToArray(table));
			assertArrayEquals(new double[][] { firstExpected, secondExpected, thirdExpected },
					readTableToArray(rowSubset));
			assertArrayEquals(new double[][] { firstExpected, newSecond, thirdExpected, fourth, fifth },
					readTableToArray(modified));

		}

	}

	public static class ToString {

		@Test
		public void testEmptyTable() {
			Column[] columns = new Column[0];
			Table table = new Table(columns, new String[0]);
			assertEquals("Table (0x0)\n\n", table.toString());
		}


		@Test
		public void testSmallTable() {
			double[] data = {5, 7, 3, 111, 4, 47, 89};
			double[] data2 = { 50, 70.1, 300.56, 1000.1111, 40, 40.7, 80.99 };
			double[] data3 = { 5.111, 7.111, 3.111, 1.1111, 4.1111, 4.7111, 8.9111 };
			Column[] columns = {new DoubleArrayColumn(Column.TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3) };
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (3x7)\n" +
					"col0    | col1     | col2 \n" +
					"Integer | Real     | Real \n" +
					"      5 |   50.000 | 5.111\n" +
					"      7 |   70.100 | 7.111\n" +
					"      3 |  300.560 | 3.111\n" +
					"    111 | 1000.111 | 1.111\n" +
					"      4 |   40.000 | 4.111\n" +
					"     47 |   40.700 | 4.711\n" +
					"     89 |   80.990 | 8.911";
			assertEquals(expected, table.toString());
		}

		private class Custom {

			private final int n;

			Custom(int n) {
				this.n = n;
			}

			@Override
			public String toString() {
				StringBuilder builder = new StringBuilder();
				for (int i = 0; i < n + 1; i++) {
					builder.append(i);
				}
				return builder.toString();
			}
		}

		@Test
		public void testWithDifferentTypes() {
			double[] data = {5, Double.NaN, 3, 111, 4, 47, 89};
			double[] data2 = {50, 70.1, 300.56, 1000.1111, Double.NaN, 40.7, 80.99};
			UInt4CategoricalBuffer<String> buffer = new UInt4CategoricalBuffer<>(data.length);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + (i % 3));
			}
			buffer.set(4, null);
			Object[] objects = new Object[data.length];
			Arrays.setAll(objects, i -> "free" + i);
			HighPrecisionDateTimeBuffer highBuffer = new HighPrecisionDateTimeBuffer(data.length);
			for (int i = 0; i < data.length; i++) {
				highBuffer.set(i, i * 101010101, i * 9990099);
			}
			highBuffer.set(2, null);

			LowPrecisionDateTimeBuffer lowBuffer = new LowPrecisionDateTimeBuffer(data.length);
			for (int i = 0; i < data.length; i++) {
				lowBuffer.set(i, i * 909090909);
			}
			lowBuffer.set(5, null);
			TimeColumnBuffer timeBuffer = new TimeColumnBuffer(data.length);
			for (int i = 0; i < data.length; i++) {
				timeBuffer.set(i, 8639999099099L * i);
			}
			timeBuffer.set(3, null);
			Object[] veryCustom = new Object[data.length];
			Arrays.setAll(veryCustom, Custom::new);
			Column[] columns = {new DoubleArrayColumn(Column.TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					buffer.toColumn(ColumnTypes.NOMINAL),
					new SimpleFreeColumn<>(ColumnTypes.freeType("com.rapidminer.test", String.class, null), objects),
					highBuffer.toColumn(), lowBuffer.toColumn(), timeBuffer.toColumn(),
					new SimpleFreeColumn<>(ColumnTypes.freeType("com.rapidminer.custom", Custom.class, null),
							veryCustom)};
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (8x7)\n" +
					"col0    | col1     | col2    | col3   | col4                           | col5                 |" +
					" " +
					"col6               | col7   \n" +
					"Integer | Real     | Nominal | Custom | Date-Time                      | Date-Time            |" +
					" " +
					"Time               | Custom \n" +
					"      5 |   50.000 |  value0 |  free0 |           1970-01-01T00:00:00Z | 1970-01-01T00:00:00Z | " +
					" " +
					"            00:00 |       0\n" +
					"      ? |   70.100 |  value1 |  free1 | 1973-03-15T02:21:41.009990099Z | 1998-10-22T21:15:09Z |" +
					" " +
					"02:23:59.999099099 |      01\n" +
					"      3 |  300.560 |  value2 |  free2 |                              ? | 2027-08-13T18:30:18Z |" +
					" " +
					"04:47:59.998198198 |     012\n" +
					"    111 | 1000.111 |  value0 |  free3 | 1979-08-09T07:05:03.029970297Z | 1920-04-28T09:17:11Z | " +
					" " +
					"                ? |    0123\n" +
					"      4 |        ? |       ? |  free4 | 1982-10-21T09:26:44.039960396Z | 1949-02-17T06:32:20Z |" +
					" " +
					"09:35:59.996396396 |   01234\n" +
					"     47 |   40.700 |  value2 |  free5 | 1986-01-02T11:48:25.049950495Z |                    ? |" +
					" " +
					"11:59:59.995495495 |  012345\n" +
					"     89 |   80.990 |  value0 |  free6 | 1989-03-16T14:10:06.059940594Z | 2006-09-30T01:02:38Z |" +
					" " +
					"14:23:59.994594594 | 0123456";
			assertEquals(expected, table.toString());
		}

		@Test
		public void testMoreColumns() {
			double[] data = {5, 7, 36, 111, 4, 47, 89};
			double[] data2 = { 50, 70.1, 300.56, 1000.1111, 40, 40.7, 80.99 };
			double[] data3 = { 5.111, 7.111, 3.111, 1.1111, 4.1111, 4.7111, 8.9111 };
			Column[] columns = { new DoubleArrayColumn(data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3), new DoubleArrayColumn(TypeId.INTEGER, data),
					new DoubleArrayColumn(data2), new DoubleArrayColumn(data3), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data2), new DoubleArrayColumn(data3)};
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (9x7)\n" +
					"col0    | col1     | col2  | col3    | col4     | col5  | ... | col8 \n" +
					"Real    | Real     | Real  | Integer | Real     | Real  | ... | Real \n" +
					"  5.000 |   50.000 | 5.111 |       5 |   50.000 | 5.111 | ... | 5.111\n" +
					"  7.000 |   70.100 | 7.111 |       7 |   70.100 | 7.111 | ... | 7.111\n" +
					" 36.000 |  300.560 | 3.111 |      36 |  300.560 | 3.111 | ... | 3.111\n" +
					"111.000 | 1000.111 | 1.111 |     111 | 1000.111 | 1.111 | ... | 1.111\n" +
					"  4.000 |   40.000 | 4.111 |       4 |   40.000 | 4.111 | ... | 4.111\n" +
					" 47.000 |   40.700 | 4.711 |      47 |   40.700 | 4.711 | ... | 4.711\n" +
					" 89.000 |   80.990 | 8.911 |      89 |   80.990 | 8.911 | ... | 8.911";
			assertEquals(expected, table.toString());
		}

		@Test
		public void testColumnsMaxFit() {
			double[] data = { 5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99 };
			double[] data2 = {50, 70.1, 300.56, 1000.1111, 40, 40.7, 80.99099};
			double[] data3 = {5, 70, 3, 100, 4, 4, 800};
			Column[] columns = { new DoubleArrayColumn(data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(Column.TypeId.INTEGER, data3), new DoubleArrayColumn(data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3), new DoubleArrayColumn(data), new DoubleArrayColumn(data2) };
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (8x7)\n" +
					"col0  | col1     | col2    | col3  | col4     | col5    | col6  | col7    \n" +
					"Real  | Real     | Integer | Real  | Real     | Real    | Real  | Real    \n" +
					"5.000 |   50.000 |       5 | 5.000 |   50.000 |   5.000 | 5.000 |   50.000\n" +
					"7.100 |   70.100 |      70 | 7.100 |   70.100 |  70.000 | 7.100 |   70.100\n" +
					"3.560 |  300.560 |       3 | 3.560 |  300.560 |   3.000 | 3.560 |  300.560\n" +
					"1.111 | 1000.111 |     100 | 1.111 | 1000.111 | 100.000 | 1.111 | 1000.111\n" +
					"4.000 |   40.000 |       4 | 4.000 |   40.000 |   4.000 | 4.000 |   40.000\n" +
					"4.700 |   40.700 |       4 | 4.700 |   40.700 |   4.000 | 4.700 |   40.700\n" +
					"8.990 |   80.991 |     800 | 8.990 |   80.991 | 800.000 | 8.990 |   80.991";
			assertEquals(expected, table.toString());
		}

		@Test
		public void testMoreRows() {
			double[] data = {5, 7, 3, 1, 40, 4, 8, 1, 2, 3, 1_100, 10_000, 0};
			double[] data2 = { 50, 70.1, 300.56, 1000.1111, 40, 40.7, 80.99, 30_000.1, 2, 3, 4, 5, 1 };
			double[] data3 = { 5.111, 7.111, 3.111, 1.1111, 4.1111, 4.7111, 8.9111, 0.1111, 1.1111, 2.1111, 3_1111,
					14_000, 50.1111 };
			Column[] columns = {new DoubleArrayColumn(Column.TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3) };
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (3x13)\n" +
					"col0    | col1      | col2  \n" +
					"Integer | Real      | Real  \n" +
					"      5 |    50.000 |  5.111\n" +
					"      7 |    70.100 |  7.111\n" +
					"      3 |   300.560 |  3.111\n" +
					"      1 |  1000.111 |  1.111\n" +
					"     40 |    40.000 |  4.111\n" +
					"      4 |    40.700 |  4.711\n" +
					"      8 |    80.990 |  8.911\n" +
					"      1 | 30000.100 |  0.111\n" +
					"      2 |     2.000 |  1.111\n" +
					"      3 |     3.000 |  2.111\n" +
					"    ... |       ... |    ...\n" +
					"      0 |     1.000 | 50.111";
			assertEquals(expected, table.toString());
		}

		@Test
		public void testRowsMaxFit() {
			double[] data = {5, 7, 3, 1, 4, 48, 89, 1, 2, 3, 11, 0};
			double[] data2 = { 50, 70.1, 300.56, 10.1111, 40, 40.7, 80.99, 30_000.1, 2, 3, 4000, 5 };
			double[] data3 = { 5.111, 7.111, 3.111, 1.1111, 4.1111, 4.7111, 8.9111, 10.10, 11, 12, 130, 14 };
			Column[] columns = {new DoubleArrayColumn(Column.TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3) };
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (3x12)\n" +
					"col0    | col1      | col2   \n" +
					"Integer | Real      | Real   \n" +
					"      5 |    50.000 |   5.111\n" +
					"      7 |    70.100 |   7.111\n" +
					"      3 |   300.560 |   3.111\n" +
					"      1 |    10.111 |   1.111\n" +
					"      4 |    40.000 |   4.111\n" +
					"     48 |    40.700 |   4.711\n" +
					"     89 |    80.990 |   8.911\n" +
					"      1 | 30000.100 |  10.100\n" +
					"      2 |     2.000 |  11.000\n" +
					"      3 |     3.000 |  12.000\n" +
					"     11 |  4000.000 | 130.000\n" +
					"      0 |     5.000 |  14.000";
			assertEquals(expected, table.toString());
		}

		@Test
		public void testMoreColumnsAndRows() {
			double[] data = {5, 7, 3, 1, 4, 47, 899, 1, 2, 3, 1_100, 10_000, 0};
			double[] data2 = { 50, 70.1, 300.56, 1000.1111, 40, 40.7, 80.99, 3.1, 2, 30_000.1, 4, 5, 1 };
			double[] data3 = { 5.111, 7.111, 3.111, 1.1111, 4.1111, 4.7111, 8.9111, 0.1111, 1.1111, 2.1111, 3_1111,
					14_000, 50.1111 };
			Column[] columns = { new DoubleArrayColumn(data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3), new DoubleArrayColumn(TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3), new DoubleArrayColumn(Column.TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3), new DoubleArrayColumn(TypeId.INTEGER, data), new DoubleArrayColumn(data2),
					new DoubleArrayColumn(data3) };
			Table table = new Table(columns, randomLabels(columns.length));

			String expected = "Table (12x13)\n" +
					"col0    | col1      | col2   | col3    | col4      | col5   | ... | col11 \n" +
					"Real    | Real      | Real   | Integer | Real      | Real   | ... | Real  \n" +
					"  5.000 |    50.000 |  5.111 |       5 |    50.000 |  5.111 | ... |  5.111\n" +
					"  7.000 |    70.100 |  7.111 |       7 |    70.100 |  7.111 | ... |  7.111\n" +
					"  3.000 |   300.560 |  3.111 |       3 |   300.560 |  3.111 | ... |  3.111\n" +
					"  1.000 |  1000.111 |  1.111 |       1 |  1000.111 |  1.111 | ... |  1.111\n" +
					"  4.000 |    40.000 |  4.111 |       4 |    40.000 |  4.111 | ... |  4.111\n" +
					" 47.000 |    40.700 |  4.711 |      47 |    40.700 |  4.711 | ... |  4.711\n" +
					"899.000 |    80.990 |  8.911 |     899 |    80.990 |  8.911 | ... |  8.911\n" +
					"  1.000 |     3.100 |  0.111 |       1 |     3.100 |  0.111 | ... |  0.111\n" +
					"  2.000 |     2.000 |  1.111 |       2 |     2.000 |  1.111 | ... |  1.111\n" +
					"  3.000 | 30000.100 |  2.111 |       3 | 30000.100 |  2.111 | ... |  2.111\n" +
					"    ... |       ... |    ... |     ... |       ... |    ... | ... |    ...\n" +
					"  0.000 |     1.000 | 50.111 |       0 |     1.000 | 50.111 | ... | 50.111";
				assertEquals(expected, table.toString());
		}

	}

	public static class TestCancellation {

		@Test(expected = TaskAbortedException.class)
		public void testOneSortCancellation() {
			Table table = Table.newTable(100)
					.addReal("a", i -> Math.random())
					.addReal("b", i -> Math.random())
					.addReal("c", i -> Math.random())
					.build(CTX);

			Context ctx = new OneShotContext();
			table.sort(1, Order.ASCENDING, ctx);
		}

		@Test(expected = TaskAbortedException.class)
		public void testConstantSortCancellation() {
			Table table = Table.newTable(100)
					.addReal("a", i -> Math.random())
					.addReal("b", i -> Math.random())
					.addReal("c", i -> Math.random())
					.build(CTX);

			Context ctx = new OneShotContext();
			table.sort(new int[]{0, 1, 2}, Order.ASCENDING, ctx);
		}

		@Test(expected = TaskAbortedException.class)
		public void testVaryingSortCancellation() {
			Table table = Table.newTable(100)
					.addReal("a", i -> Math.random())
					.addReal("b", i -> Math.random())
					.addReal("c", i -> Math.random())
					.build(CTX);

			Context ctx = new OneShotContext();
			table.sort(new int[]{1, 2}, Arrays.asList(Order.ASCENDING, Order.DESCENDING), ctx);
		}

	}


	public static class ColumnLabels {

		private static String[] labels = new String[]{"zero", "one", "two"};
		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Table.newTable(100)
					.addReal(labels[0], i -> 0)
					.addReal(labels[1], i -> 1)
					.addReal(labels[2], i -> 2)
					.build(CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testLookUpOfOutOfBoundsIndex() {
			table.label(100);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testLookUpOfNegativeIndex() {
			table.label(-3);
		}

		@Test
		public void testIndexLookup() {
			String[] lookup = new String[]{
					table.label(0),
					table.label(1),
					table.label(2)
			};
			assertArrayEquals(labels, lookup);
		}

		@Test
		public void testContains() {
			boolean[] lookup = new boolean[]{
					table.contains("two"),
					table.contains(null),
					table.contains("one"),
					table.contains("two"),
					table.contains("not in table"),
			};
			assertArrayEquals(new boolean[]{true, false, true, true, false}, lookup);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testLookupOfInvalidLabel() {
			table.index("not in table");
		}

		@Test
		public void testLabelLookup() {
			int[] lookup = new int[]{
					table.index("zero"),
					table.index("one"),
					table.index("two"),
					table.index("zero"),
					table.index("one"),
			};
			assertArrayEquals(new int[]{0, 1, 2, 0, 1}, lookup);
		}

		@Test
		public void testLabelList() {
			String[] lookup = table.labels().toArray(new String[table.width()]);
			assertArrayEquals(labels, lookup);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testLabelListImmutability() {
			List<String> lookup = table.labels();
			lookup.add("my label");
		}

		@Test
		public void testLabelArray() {
			assertArrayEquals(new String[]{"zero", "one", "two"}, table.labelArray());
		}


		@Test
		public void testRowReader() {
			double[] indexAccess = new double[200];
			RowReader reader = new RowReader(table.column(1), table.column(2));
			int i = 0;
			while (reader.hasRemaining()) {
				reader.move();
				indexAccess[i++] = reader.get(0);
				indexAccess[i++] = reader.get(1);
			}

			double[] labelAccess = new double[200];
			reader = new RowReader(table.column("one"), table.column("two"));
			i = 0;
			while (reader.hasRemaining()) {
				reader.move();
				labelAccess[i++] = reader.get(0);
				labelAccess[i++] = reader.get(1);
			}

			assertArrayEquals(indexAccess, labelAccess, EPSILON);
		}

		@Test
		public void testColumn() {
			double[] indexAccess = new double[100];
			Column column = table.column(0);
			ColumnReader reader = new ColumnReader(column, column.size());
			for (int i = 0; i < column.size(); i++) {
				indexAccess[i] = reader.read();
			}

			double[] labelAccess = new double[100];
			column = table.column("zero");
			reader = new ColumnReader(column, column.size());
			for (int i = 0; i < column.size(); i++) {
				labelAccess[i] = reader.read();
			}

			assertArrayEquals(indexAccess, labelAccess, EPSILON);
		}

		@Test
		public void testReordering() {
			Table reorderedByIndex = table.columns(new int[]{1, 0, 2});
			Table reorderedByLabel = table.columns(Arrays.asList("one", "zero", "two"));
			assertArrayEquals(readTableToArray(reorderedByIndex), readTableToArray(reorderedByLabel));
		}

		@Test
		public void testSimpleSorting() {
			Table table = Table.newTable(100)
					.addReal("zero", i -> 0)
					.addReal("positive", i -> i)
					.addReal("negative", i -> -i)
					.build(CTX);

			Table reorderedByIndex = table.sort(2, Order.ASCENDING, CTX);
			Table reorderedByLabel = table.sort("negative", Order.ASCENDING, CTX);

			assertArrayEquals(readTableToArray(reorderedByIndex), readTableToArray(reorderedByLabel));
		}

		@Test
		public void testSortingSingleOrder() {
			Table table = Table.newTable(100)
					.addReal("zero", i -> 0)
					.addReal("positive", i -> i)
					.addReal("negative", i -> -i)
					.build(CTX);

			Table reorderedByIndex = table.sort(new int[]{0, 2}, Order.ASCENDING, CTX);
			Table reorderedByLabel = table.sort(Arrays.asList("zero", "negative"), Order.ASCENDING, CTX);

			assertArrayEquals(readTableToArray(reorderedByIndex), readTableToArray(reorderedByLabel));
		}

		@Test
		public void testSortingMultipleOrders() {
			Table table = Table.newTable(100)
					.addReal("zero", i -> 0)
					.addReal("positive", i -> i)
					.addReal("negative", i -> -i)
					.build(CTX);

			Table reorderedByIndex = table.sort(new int[]{0, 2}, Arrays.asList(Order.DESCENDING, Order.ASCENDING), CTX);
			Table reorderedByLabel = table.sort(Arrays.asList("zero", "negative"), Arrays.asList(Order.DESCENDING,
					Order.ASCENDING), CTX);

			assertArrayEquals(readTableToArray(reorderedByIndex), readTableToArray(reorderedByLabel));
		}

		@Test
		public void testPrimitiveFilter() {
			Table filteredByIndex = table.filterNumeric(1, i -> i > 50, Workload.DEFAULT, CTX);
			Table filteredByLabel = table.filterNumeric("one", i -> i > 50, Workload.DEFAULT, CTX);
			assertArrayEquals(readTableToArray(filteredByIndex), readTableToArray(filteredByLabel));
		}

		@Test
		public void testPairFilter() {
			Table filteredByIndex = table.filterNumeric(new int[]{2, 1}, row -> (row.get(0) + row.get(1)) > 50,
					Workload.DEFAULT, CTX);
			Table filteredByLabel = table.filterNumeric("two", "one", (two, one) -> two + one > 50, Workload.DEFAULT,
					CTX);
			assertArrayEquals(readTableToArray(filteredByIndex), readTableToArray(filteredByLabel));
		}

		@Test
		public void testRowFilter() {
			Table filteredByIndex = table.filterNumeric(new int[]{2, 1}, row -> (row.get(0) + row.get(1)) > 50,
					Workload.DEFAULT, CTX);
			Table filteredByLabel = table.filterNumeric(Arrays.asList("two", "one"),
					row -> (row.get(0) + row.get(1)) > 50, Workload.DEFAULT, CTX);
			assertArrayEquals(readTableToArray(filteredByIndex), readTableToArray(filteredByLabel));
		}

	}

	public static class Transformation {

		private static String[] labels = new String[]{"zero", "one", "two"};
		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Table.newTable(100)
					.addReal(labels[0], i -> 0)
					.addReal(labels[1], i -> 1)
					.addReal(labels[2], i -> 2)
					.build(CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testTransformOutOfBoundsIndex() {
			table.transform(100);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testTransformNegativeIndex() {
			table.transform(-3);
		}

		@Test(expected = NullPointerException.class)
		public void testTransformNullLabel() {
			table.transform((String) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformWrongLabel() {
			table.transform("three");
		}

		@Test(expected = NullPointerException.class)
		public void testTransformNullIndexArray() {
			table.transform((int[]) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformEmptyIndexArray() {
			table.transform(new int[0]);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testTransformEmptyIndexArrayInvalid() {
			table.transform(new int[]{0, 2, 3});
		}

		@Test(expected = NullPointerException.class)
		public void testTransformNullLabelList() {
			table.transform((List<String>) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformEmptyLabelList() {
			table.transform(new ArrayList<>());
		}

		@Test(expected = NullPointerException.class)
		public void testTransformNullEntryLabelList() {
			table.transform(Arrays.asList("one", null, "two"));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformWrongEntryLabelList() {
			table.transform(Arrays.asList("one", "five", "two"));
		}

		@Test(expected = NullPointerException.class)
		public void testTransformNullLabelArray() {
			table.transform((String[]) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformEmptyLabelArray() {
			table.transform(new String[0]);
		}

		@Test(expected = NullPointerException.class)
		public void testTransformNullEntryLabelArray() {
			table.transform("one", null, "two");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformWrongEntryLabelArray() {
			table.transform("one", "five", "two");
		}

		@Test(expected = NullPointerException.class)
		public void testTransformerNull() {
			new Transformer(null);
		}

		@Test(expected = NullPointerException.class)
		public void testTransformerMultiNull() {
			new TransformerMulti((List<Column>) null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformerMultiEmptyList() {
			new TransformerMulti(new ArrayList<>());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTransformerMultiEmpty() {
			new TransformerMulti(new Column[0]);
		}
	}

	public static class MetaData {

		static class DummyData implements ColumnMetaData {

			@Override
			public String type() {
				return "moo";
			}
		}

		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Table.newTable(100)
					.addReal("zero", i -> 0)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 3)
					.addReal("four", i -> 4)
					.addMetaData("zero", ColumnRole.ID)
					.addMetaData("one", ColumnRole.METADATA)
					.addMetaData("one", new ColumnAnnotation("First annotation"))
					.addMetaData("one", new ColumnAnnotation("Second annotation"))
					.addMetaData("three", ColumnRole.METADATA)
					.addMetaData("four", ColumnRole.LABEL)
					.build(CTX);
		}

		// getMetaData(String label)

		@Test(expected = NullPointerException.class)
		public void testSimpleGetWithNullLabel() {
			table.getMetaData(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSimpleGetWithMissingLabel() {
			table.getMetaData("Does not exist");
		}

		@Test
		public void testSimpleGetForEmptyMetaData() {
			assertEquals(Collections.emptyList(), table.getMetaData("two"));
		}

		@Test
		public void testSimpleGetForSingleMetaData() {
			assertEquals(Arrays.asList(ColumnRole.ID), table.getMetaData("zero"));
		}

		@Test
		public void testSimpleGetForMultipleMetaData() {
			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.METADATA,
					new ColumnAnnotation("First annotation"),
					new ColumnAnnotation("Second annotation")
			);
			// The API does not guarantee the order of meta data
			assertEquals(new HashSet<>(expected), new HashSet<>(table.getMetaData("one")));
		}

		// getMetaData(String, Class)

		@Test(expected = NullPointerException.class)
		public void testTypedGetWithNullLabel() {
			table.getMetaData(null, ColumnRole.class);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTypedGetWithMissingLabel() {
			table.getMetaData("Does not exist", ColumnRole.class);
		}

		@Test(expected = NullPointerException.class)
		public void testTypedGetWithNullType() {
			table.getMetaData("one", null);
		}

		@Test
		public void testTypedGetForEmptyMetaData() {
			assertEquals(Collections.emptyList(), table.getMetaData("two", ColumnAnnotation.class));
		}

		@Test
		public void testTypedGetForSingleMatchngMetaData() {
			assertEquals(Arrays.asList(ColumnRole.ID), table.getMetaData("zero", ColumnRole.class));
		}

		@Test
		public void testTypedGetForMultipleMatchingMetaData() {
			List<ColumnAnnotation> expected = Arrays.asList(new ColumnAnnotation("First annotation"),
					new ColumnAnnotation("Second annotation")
			);
			List<ColumnAnnotation> result = table.getMetaData("one", ColumnAnnotation.class);
			// The API does not guarantee the order of meta data
			assertEquals(new HashSet<>(expected), new HashSet<>(result));
		}

		// getFirstMetaData(String, Class)

		@Test(expected = NullPointerException.class)
		public void testSingleGetWithNullLabel() {
			table.getFirstMetaData(null, ColumnRole.class);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSingleGetWithMissingLabel() {
			table.getFirstMetaData("Does not exist", ColumnRole.class);
		}

		@Test(expected = NullPointerException.class)
		public void testSingleGetWithNullType() {
			table.getFirstMetaData("one", null);
		}

		@Test
		public void testSingleGetForEmptyMetaData() {
			assertNull(table.getFirstMetaData("two", ColumnAnnotation.class));
		}

		@Test
		public void testSingleGetForMismatchingMetaData() {
			assertNull(table.getFirstMetaData("zero", ColumnAnnotation.class));
		}

		@Test
		public void testSingleGetForSingleMatchingMetaData() {
			ColumnRole role = table.getFirstMetaData("zero", ColumnRole.class);
			assertEquals(ColumnRole.ID, role);
		}

		@Test
		public void testSingleGetForMultipleMatchingMetaData() {
			List<ColumnAnnotation> expected = Arrays.asList(new ColumnAnnotation("First annotation"),
					new ColumnAnnotation("Second annotation")
			);
			ColumnAnnotation annotation = table.getFirstMetaData("one", ColumnAnnotation.class);
			// The API does not guarantee the order of meta data
			assertTrue(expected.contains(annotation));
		}

		// withMetaData(Class)

		@Test(expected = NullPointerException.class)
		public void testLookupOfNullType() {
			table.withMetaData((Class<ColumnMetaData>) null);
		}

		@Test
		public void tesTypeLookupWithoutMatch() {
			List<String> matches = table.withMetaData(DummyData.class);
			assertTrue(matches.isEmpty());
		}

		@Test
		public void testTypeLookupWithSingleMatch() {
			List<String> matches = table.withMetaData(ColumnAnnotation.class);
			List<String> expected = Collections.singletonList("one");
			assertEquals(expected, matches);
		}

		@Test
		public void testTypeLookupWithMultipleMatches() {
			List<String> matches = table.withMetaData(ColumnRole.class);
			List<String> expected = Arrays.asList("zero", "one", "three", "four");
			assertEquals(expected, matches);
		}

		// withMetaData(ColumnMetaData)

		@Test(expected = NullPointerException.class)
		public void testLookupOfNullInstance() {
			table.withMetaData((ColumnMetaData) null);
		}

		@Test
		public void testInstanceLookupWithoutMatch() {
			List<String> matches = table.withMetaData(ColumnRole.CLUSTER);
			assertTrue(matches.isEmpty());
		}

		@Test
		public void testInstanceLookupWithSingleMatch() {
			List<String> matches = table.withMetaData(new ColumnAnnotation("First annotation"));
			List<String> expected = Collections.singletonList("one");
			assertEquals(expected, matches);
		}

		@Test
		public void testInstanceLookupWithMultipleMatches() {
			List<String> matches = table.withMetaData(ColumnRole.METADATA);
			List<String> expected = Arrays.asList("one", "three");
			assertEquals(expected, matches);
		}

		// withoutMetaData(Class)

		@Test(expected = NullPointerException.class)
		public void testInverseLookupOfNullType() {
			table.withMetaData((Class<ColumnMetaData>) null);
		}

		@Test
		public void testInverseTypeLookupWithoutMatch() {
			List<String> matches = table.withoutMetaData(DummyData.class);
			assertEquals(table.labels(), matches);
		}

		@Test
		public void testInverseTypeLookupWithSingleMatch() {
			List<String> matches = table.withoutMetaData(ColumnAnnotation.class);
			List<String> expected = Arrays.asList("zero", "two", "three", "four");
			assertEquals(expected, matches);
		}

		@Test
		public void testInverseTypeLookupWithMultipleMatches() {
			List<String> matches = table.withoutMetaData(ColumnRole.class);
			List<String> expected = Collections.singletonList("two");
			assertEquals(expected, matches);
		}

		// withoutMetaData(ColumnMetaData)

		@Test(expected = NullPointerException.class)
		public void testInverseLookupOfNullInstance() {
			table.withMetaData((ColumnMetaData) null);
		}

		@Test
		public void testInverseInstanceLookupWithoutMatch() {
			List<String> matches = table.withoutMetaData(ColumnRole.CLUSTER);
			assertEquals(table.labels(), matches);
		}

		@Test
		public void testInverseInstanceLookupWithSingleMatch() {
			List<String> matches = table.withoutMetaData(new ColumnAnnotation("First annotation"));
			List<String> expected = Arrays.asList("zero", "two", "three", "four");
			assertEquals(expected, matches);
		}

		@Test
		public void testInverseInstanceLookupWithMultipleMatches() {
			List<String> matches = table.withoutMetaData(ColumnRole.METADATA);
			List<String> expected = Arrays.asList("zero", "two", "four");
			assertEquals(expected, matches);
		}

		@Test
		public void testColumnSelection() {
			Table derived = table.columns(Arrays.asList("zero", "four"));
			assertEquals(Collections.singletonList(ColumnRole.ID), derived.getMetaData("zero"));
			assertEquals(Collections.singletonList(ColumnRole.LABEL), derived.getMetaData("four"));
		}

	}


}