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

package com.rapidminer.belt.reader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.table.TableTestUtils;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class ObjectRowReaderTests {

	private static String[] numbers(int n) {
		String[] numbers = new String[n];
		Arrays.setAll(numbers, Integer::toString);
		return numbers;
	}

	private static void readAllColumns(ObjectRowReader<String> reader) {
		while (reader.hasRemaining()) {
			reader.move();
			for (int j = 0; j < reader.width(); j++) {
				reader.get(j);
			}
		}
	}

	private static String[][] readAllColumnsToArrays(ObjectRowReader<String> reader) {
		String[][] columns = new String[reader.width()][reader.remaining()];
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

	/**
	 * Depend on index so that column arrays alternate between categorical and free.
	 */
	private static Column column(int index, String[] data) {
		if (index % 2 == 0) {
			List<String> mapping = new ArrayList<>(Arrays.asList(data));
			mapping.add(0, null);
			int[] categories = new int[data.length];
			Arrays.setAll(categories, i -> i + 1);
			return ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, categories, mapping);
		} else {
			return ColumnTestUtils.getObjectColumn(ColumnType.TEXT, data);
		}
	}

	public static class Reading {

		@Test
		public void testReadingFromSingleCompleteBuffer() {
			int nRows = NumericReader.SMALL_BUFFER_SIZE;
			int nColumns = 5;

			String[][] inputs = new String[nColumns][];
			Arrays.setAll(inputs, i -> numbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, inputs[i]));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			assertArrayEquals(inputs, outputs);
		}

		@Test
		public void testReadingFromSingleIncompleteBuffer() {
			int nRows = (int) (0.33 * NumericReader.SMALL_BUFFER_SIZE);
			int nColumns = 5;

			String[][] inputs = new String[nColumns][];
			Arrays.setAll(inputs, i -> numbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, inputs[i]));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			assertArrayEquals(inputs, outputs);
		}

		@Test
		public void testReadingFromMultipleCompleteBuffers() {
			int nRows = 7 * NumericReader.SMALL_BUFFER_SIZE;
			int nColumns = 3;

			String[][] inputs = new String[nColumns][];
			Arrays.setAll(inputs, i -> numbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, inputs[i]));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			assertArrayEquals(inputs, outputs);
		}

		@Test
		public void testReadingFromMultipleIncompleteBuffers() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);
			int nColumns = 3;

			String[][] inputs = new String[nColumns][];
			Arrays.setAll(inputs, i -> numbers(nRows));

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, inputs[i]));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(nColumns, outputs.length);
			assertArrayEquals(inputs, outputs);
		}

		@Test
		public void testReadingFromSingleColumn() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			String[] input = numbers(nRows);
			Column[] columns = new Column[]{column(0, input)};

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(1, outputs.length);
			assertArrayEquals(input, outputs[0]);
		}

		@Test
		public void testReadingFromTwoColumns() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			String[] input0 = numbers(nRows);
			String[] input1 = numbers(nRows);
			String[] input2 = numbers(nRows);
			Column[] columns = new Column[]{column(0, input0), column(1, input1),
					column(2, input2)};

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns[2], columns[0]),
					String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(2, outputs.length);
			assertArrayEquals(input2, outputs[0]);
			assertArrayEquals(input0, outputs[1]);
		}

		@Test
		public void testReadingWholeTable() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			String[] input0 = numbers(nRows);
			String[] input1 = numbers(nRows);
			String[] input2 = numbers(nRows);
			String[] labels = new String[]{"a", "b", "c"};
			Column[] columns = new Column[]{column(0, input0), column(1, input1),
					column(2, input2)};

			Table table = TableTestUtils.newTable(columns, labels);

			ObjectRowReader<String> reader = new ObjectRowReader<>(table.columnList(), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(3, outputs.length);
			assertArrayEquals(input0, outputs[0]);
			assertArrayEquals(input1, outputs[1]);
			assertArrayEquals(input2, outputs[2]);
		}

		@Test
		public void testReadingFromThreeColumns() {
			int nRows = (int) (6.67 * NumericReader.SMALL_BUFFER_SIZE);

			String[] input0 = numbers(nRows);
			String[] input1 = numbers(nRows);
			String[] input2 = numbers(nRows);
			String[] labels = new String[]{"a", "b", "c"};
			Column[] columns = new Column[]{column(0, input0), column(1, input1),
					column(2, input2)};

			Table table = TableTestUtils.newTable(columns, labels);

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(table.column("c"), table.column("b"),
					table.column("a")), String.class);
			String[][] outputs = readAllColumnsToArrays(reader);

			assertEquals(3, outputs.length);
			assertArrayEquals(input2, outputs[0]);
			assertArrayEquals(input1, outputs[1]);
			assertArrayEquals(input0, outputs[2]);
		}

		@Test
		public void testColumnInteractionWithSingleBuffer() {
			int nRows = NumericReader.SMALL_BUFFER_SIZE;
			int nColumns = 5;

			String[][] inputs = new String[nColumns][];
			Arrays.setAll(inputs, i -> new String[nRows]);

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> spy(column(0, inputs[i])));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			readAllColumns(reader);

			for (Column column : columns) {
				verify(column).fill(any(Object[].class), eq(0), anyInt(), anyInt());
				verify(column, times(1)).fill(any(Object[].class), anyInt(), anyInt(), anyInt());
			}
		}

		@Test
		public void testColumnInteractionWithMultipleBuffer() {
			int nRows = (int) (2.5 * NumericReader.SMALL_BUFFER_SIZE);
			int nColumns = 3;

			String[][] inputs = new String[nColumns][];
			Arrays.setAll(inputs, i -> new String[nRows]);

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> spy(column(0, inputs[i])));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			readAllColumns(reader);

			for (Column column : columns) {
				verify(column).fill(any(Object[].class), eq(0), anyInt(), anyInt());
				verify(column).fill(any(Object[].class), eq(NumericReader.SMALL_BUFFER_SIZE), anyInt(), anyInt());
				verify(column).fill(any(Object[].class), eq(2 * NumericReader.SMALL_BUFFER_SIZE), anyInt(), anyInt());
				verify(column, times(3)).fill(any(Object[].class), anyInt(), anyInt(), anyInt());
			}
		}
	}

	public static class Remaining {

		@Test(expected = NullPointerException.class)
		public void testNullSource() {
			new ObjectRowReader<>(null, Boolean.class, 10);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongClass() {
			new ObjectRowReader<>(Arrays.asList(column(0, new String[5])), Boolean.class);
		}

		@Test
		public void testRightSuper() {
			int[] indices = new int[10];
			Arrays.fill(indices, 1);
			ColumnType<String> type = ColumnTestUtils.categoricalType(
					String.class, null);
			CategoricalColumn stringCol = ColumnTestUtils.getSimpleCategoricalColumn(type, indices,
					Arrays.asList(null, "null"));
			Column doubleCol = ColumnTestUtils.getDoubleObjectTestColumn();
			ObjectRowReader<Object> reader = new ObjectRowReader<>(Arrays.asList(stringCol, doubleCol), Object.class);
			reader.move();
			assertEquals("null", reader.get(0));
			assertEquals(1.0, reader.get(1));
		}

		@Test
		public void testInitialRemainder() {
			int nRows = 64;
			int nColumns = 3;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
			assertEquals(nRows, reader.remaining());
		}

		@Test
		public void testRemainder() {
			int nRows = 64;
			int nColumns = 4;
			int nReads = 16;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
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
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
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
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
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
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
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
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));

			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);

			assertEquals(Readers.BEFORE_FIRST_ROW, reader.position());
		}

		@Test
		public void testGetAtEnd() {
			int nRows = 64;
			int nColumns = 3;

			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class);
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
			Arrays.setAll(columns, i -> ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, new int[nRows],
					new ArrayList<>()));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
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
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 1), reader.get(i));
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 2), reader.get(0));
			}
		}

		@Test
		public void testSetAfterMoves() {
			int nRows = 64;
			int nColumns = 3;
			int position = 16;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			for (int i = 0; i < 13; i++) {
				reader.move();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 1), reader.get(i));
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 2), reader.get(0));
			}
		}

		@Test
		public void testSetAfterReadsSmallerInSameBuffer() {
			int nRows = 64;
			int nColumns = 3;
			int position = 11;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10);
			for (int i = 0; i < 13; i++) {
				reader.move();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 1), reader.get(i));
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 2), reader.get(0));
			}
		}

		@Test
		public void testSetAfterReadsSmaller() {
			int nRows = 64;
			int nColumns = 3;
			int position = 4;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			for (int i = 0; i < 13; i++) {
				reader.move();
			}
			reader.setPosition(position);
			assertEquals(position, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 1), reader.get(i));
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(position + 2), reader.get(0));
			}
		}

		@Test
		public void testSetBefore() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.setPosition(-1);
			assertEquals(Readers.BEFORE_FIRST_ROW, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("0", reader.get(0));
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("1", reader.get(0));
			}
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testSetNegative() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.setPosition(0);
			reader.move();
			reader.setPosition(-5);
			reader.move();
		}

		@Test
		public void testSetBeforeAfterMove() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.move();
			reader.move();
			reader.setPosition(Readers.BEFORE_FIRST_ROW);
			assertEquals(-1, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("0", reader.get(0));
			}
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("1", reader.get(0));
			}
		}

		@Test
		public void testSetEnd() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.setPosition(nRows - 2);
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(nRows - 1), reader.get(0));
			}
			assertEquals(nRows - 1, reader.position());
		}

		@Test
		public void testSetEndAfterMove() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.move();
			reader.move();
			reader.setPosition(nRows - 2);
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals(Integer.toString(nRows - 1), reader.get(0));
			}
			assertEquals(nRows - 1, reader.position());
		}

		@Test
		public void testSetMultipleTimes() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = numbers(nRows);
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.setPosition(16);
			assertEquals(16, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("17", reader.get(0));
			}
			reader.setPosition(18);
			assertEquals(18, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("19", reader.get(0));
			}
			reader.setPosition(11);
			assertEquals(11, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("12", reader.get(0));
			}
			reader.setPosition(11);
			assertEquals(11, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("12", reader.get(0));
			}
			reader.setPosition(25);
			assertEquals(25, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("26", reader.get(0));
			}
			reader.setPosition(23);
			assertEquals(23, reader.position());
			reader.move();
			for (int i = 0; i < reader.width(); i++) {
				assertEquals("24", reader.get(0));
			}
		}
	}

	public static class ToString {

		@Test
		public void testStart() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = new String[nRows];
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			String expected = "Object row reader (" + nRows + "x" + nColumns + ")\n"
					+ "Row position: " + Readers.BEFORE_FIRST_ROW;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testMiddle() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = new String[nRows];
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			reader.move();
			reader.move();
			reader.move();
			reader.move();
			reader.move();
			String expected = "Object row reader (" + nRows + "x" + nColumns + ")\n" + "Row position: " + 4;
			assertEquals(expected, reader.toString());
		}

		@Test
		public void testEnd() {
			int nRows = 64;
			int nColumns = 3;
			String[] testArray = new String[nRows];
			Column[] columns = new Column[nColumns];
			Arrays.setAll(columns, i -> column(i, testArray));
			ObjectRowReader<String> reader = new ObjectRowReader<>(Arrays.asList(columns), String.class, 10 * nColumns);
			while (reader.hasRemaining()) {
				reader.move();
			}
			String expected = "Object row reader (" + nRows + "x" + nColumns + ")\n" + "Row position: " + (nRows - 1);
			assertEquals(expected, reader.toString());
		}
	}
}
