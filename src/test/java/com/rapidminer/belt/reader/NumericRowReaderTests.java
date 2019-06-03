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

package com.rapidminer.belt.reader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.table.TableTestUtils;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class NumericRowReaderTests {

	private static final double EPSILON = 1e-10;

	private static double[] randomNumbers(int n) {
		double[] numbers = new double[n];
		Arrays.setAll(numbers, i -> Math.random());
		return numbers;
	}

	private static void readAllColumns(NumericRowReader reader) {
		while (reader.hasRemaining()) {
			reader.move();
			for (int j = 0; j < reader.width(); j++) {
				reader.get(j);
			}
		}
	}

	private static double[][] readAllColumnsToArrays(NumericRowReader reader) {
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


	public static class FactoryMethods {

		@Test(expected = NullPointerException.class)
		public void testNullTable() {
			Readers.numericRowReader((Table) null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnList() {
			Readers.numericRowReader((List<Column>) null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnListElement() {
			Readers.numericRowReader(Collections.singletonList(null));
		}

		@Test(expected = NullPointerException.class)
		public void testNullFirstColumn() {
			Column column = ColumnTestUtils.getNumericColumn(Column.TypeId.REAL, new double[0]);
			Readers.numericRowReader(null, column, column);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSecondColumn() {
			Column column = ColumnTestUtils.getNumericColumn(Column.TypeId.REAL, new double[0]);
			Readers.numericRowReader(column, null, column);
		}

		@Test(expected = NullPointerException.class)
		public void testNullVarArg() {
			Column column = ColumnTestUtils.getNumericColumn(Column.TypeId.REAL, new double[0]);
			Readers.numericRowReader(column, column, column, null, column);
		}

		@Test(expected = NullPointerException.class)
		public void testNullVarArgs() {
			Column column = ColumnTestUtils.getNumericColumn(Column.TypeId.REAL, new double[0]);
			Readers.numericRowReader(column, column, (Column[]) null);
		}

	}

	public static class Reading {

		@Test
		public void testReadingFromSingleCompleteBuffer() {
			int nRows = NumericReader.SMALL_BUFFER_SIZE;
			int nColumns = 5;

			double[][] inputs = new double[nColumns][];
			Arrays.setAll(inputs, i -> randomNumbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, inputs[i]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			for (int i = 0; i < nColumns; i++) {
				assertArrayEquals(inputs[i], outputs[i], EPSILON);
			}
		}

		@Test
		public void testReadingFromSingleIncompleteBuffer() {
			int nRows = (int) (0.33 * NumericReader.SMALL_BUFFER_SIZE);
			int nColumns = 5;

			double[][] inputs = new double[nColumns][];
			Arrays.setAll(inputs, i -> randomNumbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, inputs[i]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			for (int i = 0; i < nColumns; i++) {
				assertArrayEquals(inputs[i], outputs[i], EPSILON);
			}
		}

		@Test
		public void testReadingFromMultipleCompleteBuffers() {
			int nRows = 7 * NumericReader.SMALL_BUFFER_SIZE;
			int nColumns = 3;

			double[][] inputs = new double[nColumns][];
			Arrays.setAll(inputs, i -> randomNumbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, inputs[i]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			for (int i = 0; i < nColumns; i++) {
				assertArrayEquals(inputs[i], outputs[i], EPSILON);
			}
		}

		@Test
		public void testReadingFromMultipleIncompleteBuffers() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);
			int nColumns = 3;

			double[][] inputs = new double[nColumns][];
			Arrays.setAll(inputs, i -> randomNumbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, inputs[i]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			for (int i = 0; i < nColumns; i++) {
				assertArrayEquals(inputs[i], outputs[i], EPSILON);
			}
		}

		@Test
		public void testReadingFromSingleColumn() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			double[] input = randomNumbers(nRows);
			Column[] columns = new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, input)};

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(1, outputs.length);
			assertArrayEquals(input, outputs[0], EPSILON);
		}

		@Test
		public void testReadingFromTwoColumns() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			double[] input0 = randomNumbers(nRows);
			double[] input1 = randomNumbers(nRows);
			double[] input2 = randomNumbers(nRows);
			String[] labels = new String[]{"a", "b", "c"};
			Column[] columns = new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, input0),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input1),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input2)};

			Table table = TableTestUtils.newTable(columns, labels);

			NumericRowReader reader = Readers.numericRowReader(table.column("c"), table.column("a"));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(2, outputs.length);
			assertArrayEquals(input2, outputs[0], EPSILON);
			assertArrayEquals(input0, outputs[1], EPSILON);
		}

		@Test
		public void testReadingFromThreeColumns() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			double[] input0 = randomNumbers(nRows);
			double[] input1 = randomNumbers(nRows);
			double[] input2 = randomNumbers(nRows);
			String[] labels = new String[]{"a", "b", "c"};
			Column[] columns = new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, input0),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input1),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input2)};

			Table table = TableTestUtils.newTable(columns, labels);

			NumericRowReader reader = Readers.numericRowReader(table.column("c"),
					table.column("b"),
					table.column("a"));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(3, outputs.length);
			assertArrayEquals(input2, outputs[0], EPSILON);
			assertArrayEquals(input1, outputs[1], EPSILON);
			assertArrayEquals(input0, outputs[2], EPSILON);
		}

		@Test
		public void testReadingFromColumnArray() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			double[] input0 = randomNumbers(nRows);
			double[] input1 = randomNumbers(nRows);
			double[] input2 = randomNumbers(nRows);
			String[] labels = new String[]{"a", "b", "c"};
			Column[] columns = new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, input0),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input1),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input2)};

			Table table = TableTestUtils.newTable(columns, labels);

			NumericRowReader reader = Readers.numericRowReader(Arrays.asList(table.column("c"),
					table.column("b"),
					table.column("a")));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(3, outputs.length);
			assertArrayEquals(input2, outputs[0], EPSILON);
			assertArrayEquals(input1, outputs[1], EPSILON);
			assertArrayEquals(input0, outputs[2], EPSILON);
		}

		@Test
		public void testReadingFromColumnList() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			double[] input0 = randomNumbers(nRows);
			double[] input1 = randomNumbers(nRows);
			double[] input2 = randomNumbers(nRows);
			String[] labels = new String[]{"a", "b", "c"};
			Column[] columns = new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, input0),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input1),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, input2)};

			Table table = TableTestUtils.newTable(columns, labels);

			NumericRowReader reader = Readers.numericRowReader(Arrays.asList(table.column("c"),
					table.column("b"),
					table.column("a")));
			double[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(3, outputs.length);
			assertArrayEquals(input2, outputs[0], EPSILON);
			assertArrayEquals(input1, outputs[1], EPSILON);
			assertArrayEquals(input0, outputs[2], EPSILON);
		}

		@Test
		public void testColumnInteractionWithSingleBuffer() {
			int nRows = NumericReader.SMALL_BUFFER_SIZE;
			int nColumns = 5;

			double[][] inputs = new double[nColumns][];
			Arrays.setAll(inputs, i -> new double[nRows]);

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> spy(ColumnTestUtils.getNumericColumn(TypeId.REAL, inputs[i])));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			readAllColumns(reader);

			for (Column column : columns) {
				verify(column).fill(any(double[].class), eq(0), anyInt(), anyInt());
				verify(column, times(1)).fill(any(double[].class), anyInt(), anyInt(), anyInt());
			}
		}

		@Test
		public void testColumnInteractionWithMultipleBuffer() {
			int nRows = (int) (2.5 * NumericReader.SMALL_BUFFER_SIZE);
			int nColumns = 3;

			double[][] inputs = new double[nColumns][];
			Arrays.setAll(inputs, i -> new double[nRows]);

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> spy(ColumnTestUtils.getNumericColumn(TypeId.REAL, inputs[i])));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			readAllColumns(reader);

			for (Column column : columns) {
				verify(column).fill(any(double[].class), eq(0), anyInt(), anyInt());
				verify(column).fill(any(double[].class), eq(NumericReader.SMALL_BUFFER_SIZE), anyInt(), anyInt());
				verify(column).fill(any(double[].class), eq(2 * NumericReader.SMALL_BUFFER_SIZE), anyInt(), anyInt());
				verify(column, times(3)).fill(any(double[].class), anyInt(), anyInt(), anyInt());
			}
		}
	}

	public static class Remaining {

		@Test(expected = NullPointerException.class)
		public void testNullSource() {
			new NumericRowReader(null, NumericReader.SMALL_BUFFER_SIZE);
		}

		@Test
		public void testInitialRemainder() {
			int nRows = 64;
			int nColumns = 3;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			assertEquals(nRows, reader.remaining());
		}

		@Test
		public void testRemainder() {
			int nRows = 64;
			int nColumns = 4;
			int nReads = 16;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			for (int i = 0; i < nReads; i++) {
				reader.move();
			}

			assertEquals(nRows - nReads, reader.remaining());
		}

		@Test
		public void testFinalRemainder() {
			int nRows = 64;
			int nColumns = 11;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			while (reader.hasRemaining()) {
				reader.move();
			}

			assertEquals(0, reader.remaining());
		}
	}

	public static class Position {

		@Test
		public void testGet() {
			int nRows = 64;
			int nColumns = 4;
			int nReads = 16;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			for (int i = 0; i < nReads; i++) {
				reader.move();
			}

			assertEquals(nReads - 1, reader.position());
		}

		@Test
		public void testGetBufferSmaller() {
			int nRows = 64;
			int nColumns = 4;
			int nReads = 16;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			for (int i = 0; i < nReads; i++) {
				reader.move();
			}

			assertEquals(nReads - 1, reader.position());
		}

		@Test
		public void testGetAtStart() {
			int nRows = 64;
			int nColumns = 3;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));

			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));

			assertEquals(Readers.BEFORE_FIRST_ROW, reader.position());
		}

		@Test
		public void testGetAtEnd() {
			int nRows = 64;
			int nColumns = 3;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns));
			while (reader.hasRemaining()) {
				reader.move();
			}
			assertEquals(nRows - 1, reader.position());
		}

		@Test
		public void testGetAtEndBufferSmaller() {
			int nRows = 64;
			int nColumns = 3;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, new double[nRows]));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			while (reader.hasRemaining()) {
				reader.move();
			}
			assertEquals(nRows - 1, reader.position());
		}

		@Test
		public void testSet() {
			int nRows = 64;
			int nColumns = 3;
			int position = 16;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 1, reader.get(i), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 2, reader.get(0), EPSILON);
			}
		}

		@Test
		public void testSetAfterMoves() {
			int nRows = 64;
			int nColumns = 3;
			int position = 16;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			for (int i = 0; i < 13; i++) {
				reader.move();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 1, reader.get(i), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 2, reader.get(0), EPSILON);
			}
		}

		@Test
		public void testSetAfterReadsSmallerInSameBuffer() {
			int nRows = 64;
			int nColumns = 3;
			int position = 11;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			for (int i = 0; i < 13; i++) {
				reader.move();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 1, reader.get(i), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 2, reader.get(0), EPSILON);
			}
		}

		@Test
		public void testSetAfterReadsSmaller() {
			int nRows = 64;
			int nColumns = 3;
			int position = 4;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			for (int i = 0; i < 13; i++) {
				reader.move();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 1, reader.get(i), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(position + 2, reader.get(0), EPSILON);
			}
		}

		@Test
		public void testSetBefore() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.setPosition(Readers.BEFORE_FIRST_ROW);
			assertEquals(-1, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(0, reader.get(0), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(1, reader.get(0), EPSILON);
			}
		}

		@Test
		public void testSetZero() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.setPosition(0);
			assertEquals(0, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(1, reader.get(0), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(2, reader.get(0), EPSILON);
			}
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testSetNegative() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.setPosition(0);
			reader.move();
			reader.setPosition(-5);
			reader.move();
		}

		@Test
		public void testSetBeforeAfterMove() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.move();
			reader.move();
			reader.setPosition(-1);
			assertEquals(-1, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(0, reader.get(0), EPSILON);
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(1, reader.get(0), EPSILON);
			}
		}

		@Test
		public void testSetEnd() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.setPosition(nRows - 2);
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(nRows - 1, reader.get(0), EPSILON);
			}
			assertEquals(nRows - 1, reader.position());
		}

		@Test
		public void testSetEndAfterMove() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.move();
			reader.move();
			reader.setPosition(nRows - 2);
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(nRows - 1, reader.get(0), EPSILON);
			}
			assertEquals(nRows - 1, reader.position());
		}

		@Test
		public void testSetMultipleTimes() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Arrays.setAll(testArray, i -> i);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.setPosition(16);
			assertEquals(16, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(17, reader.get(0), EPSILON);
			}
			reader.setPosition(18);
			assertEquals(18, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(19, reader.get(0), EPSILON);
			}
			reader.setPosition(11);
			assertEquals(11, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(12, reader.get(0), EPSILON);
			}
			reader.setPosition(11);
			assertEquals(11, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(12, reader.get(0), EPSILON);
			}
			reader.setPosition(25);
			assertEquals(25, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(26, reader.get(0), EPSILON);
			}
			reader.setPosition(23);
			assertEquals(23, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(24, reader.get(0), EPSILON);
			}
		}
	}

	public static class ToString {

		@Test
		public void testStart() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			String expected = "Numeric Row reader (" + nRows + "x" + nColumns + ")\n"
					+ "Row position: " + Readers.BEFORE_FIRST_ROW;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testMiddle() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			reader.move();
			reader.move();
			reader.move();
			reader.move();
			reader.move();
			String expected = "Numeric Row reader (" + nRows + "x" + nColumns + ")\n" + "Row position: " + 4;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testEnd() {
			int nRows = 64;
			int nColumns = 3;
			double[] testArray = new double[nRows];
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getNumericColumn(TypeId.REAL, testArray));
			NumericRowReader reader = new NumericRowReader(Arrays.asList(columns), 10 * nColumns);
			while (reader.hasRemaining()) {
				reader.move();
			}
			String expected = "Numeric Row reader (" + nRows + "x" + nColumns + ")\n" + "Row position: " + (nRows - 1);
			assertEquals(expected, reader.toString());
		}
	}
}
