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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.DoubleFunction;
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.DateTimeBuffer;
import com.rapidminer.belt.buffer.Int32NominalBuffer;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.buffer.TimeBuffer;
import com.rapidminer.belt.buffer.UInt2NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionAbortedException;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.table.TableTests.MetaData.NonUnique;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.util.ColumnAnnotation;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnReference;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.belt.util.IntegerFormats;

/**
 * @author Gisa Schaefer
 */
@RunWith(Enclosed.class)
public class TableBuilderTests {

	private static final int NUMBER_OF_ROWS = 25;

	private static final String INIT_NEW_TABLE = "new_table";

	private static final String INIT_EXISTING_TABLE = "from_existing_table";

	private static final Context CTX = Belt.defaultContext();

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	private static double[] randomStretched(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> 50 * Math.random());
		return data;
	}

	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		NumericReader reader = Readers.numericReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	private static Object[] readColumnToObjectArray(Table table, int column) {
		Object[] data = new Object[table.height()];
		ObjectReader<Object> reader = Readers.objectReader(table.column(column), Object.class);
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

	private static Object[][] readTableToObjectArray(Table table) {
		Object[][] result = new Object[table.width()][];
		Arrays.setAll(result, i -> readColumnToObjectArray(table, i));
		return result;
	}

	@RunWith(Parameterized.class)
	public static class WithOneStartColumn {

		private static final double[] FIRST_COLUMN = random(NUMBER_OF_ROWS);

		@Parameter
		public String builderInitialization;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(INIT_NEW_TABLE, INIT_EXISTING_TABLE);
		}

		private TableBuilder builder() {
			switch (builderInitialization) {
				case INIT_NEW_TABLE:
					return Builders.newTableBuilder(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i]);
				case INIT_EXISTING_TABLE:
					Table source = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addReal("a", i -> FIRST_COLUMN[i])
					.build(CTX);
					return Builders.newTableBuilder(source);
				default:
					throw new IllegalStateException("Unknown table initialization");
			}
		}

		@Test
		public void testFillerBufferFiller() {
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);
			double[] fourth = random(NUMBER_OF_ROWS);

			NumericBuffer buffer = Buffers.realBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				buffer.set(i, third[i]);
			}

			Table table = builder().addReal("b", i -> second[i])
					.add("c", buffer.toColumn())
					.addReal("d", i -> fourth[i])
					.build(CTX);

			assertArrayEquals(new double[][] {FIRST_COLUMN, second, third, fourth}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testBufferFillerBuffer() {
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);
			double[] fourth = random(NUMBER_OF_ROWS);

			NumericBuffer secondAsBuffer = Buffers.realBuffer(NUMBER_OF_ROWS);
			NumericBuffer fourthAsBuffer = Buffers.realBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				secondAsBuffer.set(i, second[i]);
				fourthAsBuffer.set(i, fourth[i]);
			}

			Table table = builder().add("b", secondAsBuffer.toColumn())
					.addReal("c", i -> third[i])
					.add("d", fourthAsBuffer.toColumn())
					.build(CTX);

			assertArrayEquals(new double[][] {FIRST_COLUMN, second, third, fourth}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testBufferFillerTable() {
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);
			double[] fourth = random(NUMBER_OF_ROWS);

			NumericBuffer secondAsBuffer = Buffers.realBuffer(NUMBER_OF_ROWS);
			NumericBuffer fourthAsBuffer = Buffers.realBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				secondAsBuffer.set(i, second[i]);
				fourthAsBuffer.set(i, fourth[i]);
			}
			Table fourthAsTable = Builders.newTableBuilder(NUMBER_OF_ROWS).add("a", fourthAsBuffer.toColumn()).build(CTX);

			Table table = builder().add("b", secondAsBuffer.toColumn())
					.addReal("c", i -> third[i])
					.add("d", fourthAsTable.column(0))
					.build(CTX);

			assertArrayEquals(new double[][] { FIRST_COLUMN, second, third, fourth }, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testFillerTableBuffer() {
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);
			double[] fourth = random(NUMBER_OF_ROWS);

			NumericBuffer buffer = Buffers.realBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				buffer.set(i, fourth[i]);
			}

			Table thirdAsTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addReal("a", i -> 0)
					.addReal("b", i -> third[i])
					.addReal("c", i -> 2)
					.build(CTX);

			Table table = builder().addReal("b", i -> second[i])
					.add("c", thirdAsTable.column(1))
					.add("d", buffer.toColumn()).build(CTX);

			assertArrayEquals(new double[][] { FIRST_COLUMN, second, third, fourth }, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c", "d"}, table.labelArray());
		}

		@Test
		public void testRemoveColumnToEmpty() {
			Table table = builder().remove("a").build(CTX);
			assertEquals(0, table.width());
		}

		@Test
		public void testReplaceColumn() {
			double[] replacement = random(NUMBER_OF_ROWS);
			Table table = builder().replaceReal("a", i -> replacement[i]).build(CTX);
			assertArrayEquals(new double[][]{replacement}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testReplaceIntColumn() {
			double[] replacement = random(NUMBER_OF_ROWS);
			Arrays.setAll(replacement, i -> 50 * replacement[i]);
			Table table = builder().replaceInt53Bit("a", i -> replacement[i]).build(CTX);
			Arrays.setAll(replacement, i -> Math.round(replacement[i]));
			assertArrayEquals(new double[][]{replacement}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testReplaceColumnFrom() {
			double[] replacement = random(NUMBER_OF_ROWS);
			Table replacementTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addReal("a", i -> replacement[i]).build(CTX);
			Table table = builder().replace("a", replacementTable.column(0)).build(CTX);
			assertArrayEquals(new double[][] { replacement }, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testReplaceMultipleColumnsOutOfOrder() {
			double[] first = random(NUMBER_OF_ROWS);
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);

			TableBuilder builder = builder().addReal("b", i -> i).addReal("c", i -> i);

			Table table = builder.replaceReal("c", i -> third[i])
					.replaceReal("a", i -> first[i])
					.replaceReal("b", i -> second[i])
					.build(CTX);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "b", "c"}, table.labelArray());
		}

		@Test
		public void testAddRemoveReplacedAddedColumn() {
			double[] firstAddition = random(NUMBER_OF_ROWS);
			double[] replacement = random(NUMBER_OF_ROWS);
			double[] secondAddition = random(NUMBER_OF_ROWS);

			Table table = builder().addReal("b", i -> firstAddition[i])
					.replaceReal("a", i -> replacement[i])
					.remove("a")
					.remove("b")
					.addReal("b", i -> secondAddition[i])
					.build(CTX);

			assertArrayEquals(new double[][]{secondAddition}, readTableToArray(table));
			assertArrayEquals(new String[]{"b"}, table.labelArray());
		}

		@Test
		public void testAddFromRenameColumn() {
			double[] firstAddition = random(NUMBER_OF_ROWS);
			Table firstTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addReal("a", i -> firstAddition[i]).build(CTX);

			Table table = builder().add("b", firstTable.column( "a"))
					.rename("b", "c")
					.build(CTX);

			assertArrayEquals(new double[][]{FIRST_COLUMN, firstAddition}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "c"}, table.labelArray());
		}

		@Test
		public void testRenameReplaceFrom() {
			double[] firstAddition = random(NUMBER_OF_ROWS);
			Table firstTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addReal("a", i -> firstAddition[i]).build(CTX);

			Table table = builder().rename("a", "1")
					.replace("1", firstTable.column("a"))
					.build(CTX);

			assertArrayEquals(new double[][]{firstAddition}, readTableToArray(table));
			assertArrayEquals(new String[]{"1"}, table.labelArray());
		}

		@Test
		public void testRenameWithSame() {
			Table table = builder().rename("a", "a")
					.build(CTX);

			assertArrayEquals(new double[][]{FIRST_COLUMN}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testRemoveReplacedAddedColumn() {
			Table table = builder().addReal("b", i -> 2 * i)
					.replaceReal("b", i -> 3 * i)
					.remove("b")
					.remove("a")
					.build(CTX);
			assertEquals(0, table.width());
		}


		@Test(expected = IllegalArgumentException.class)
		public void testRemoveWrongLabel() {
			builder().remove("x");
		}


		@Test(expected = NullPointerException.class)
		public void testRemoveNullLabel() {
			builder().remove(null);
		}

		@Test(expected = NullPointerException.class)
		public void testAddNullColumn() {
			builder().add("b", (Column) null);
		}

		@Test(expected = NullPointerException.class)
		public void testAddNullColumnFiller() {
			builder().addReal("b", (IntToDoubleFunction) null);
		}

		@Test(expected = NullPointerException.class)
		public void testAddNullColumnFillerInt() {
			builder().addInt53Bit("b", (IntToDoubleFunction) null);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddWrongSizeColumn() {
			builder().add("b", Buffers.realBuffer(5).toColumn());
		}


		@Test(expected = NullPointerException.class)
		public void testReplaceNullColumn() {
			builder().replace("a", (Column) null);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullColumnFiller() {
			builder().replaceReal("a", (IntToDoubleFunction) null);
		}

		@Test(expected = IllegalStateException.class)
		public void testReplaceWrongSizeColumn() {
			builder().replace("a", Buffers.realBuffer(5).toColumn());
		}

		@Test(expected = IllegalStateException.class)
		public void testReplaceWrongSizeTable() {
			Table table = Builders.newTableBuilder(5).addReal("b", i -> i).build(CTX);
			builder().replace("a", table.column("b"));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceWrongLabelColumn() {
			builder().replace("x", Buffers.realBuffer(NUMBER_OF_ROWS).toColumn());
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullLabelColumn() {
			builder().replace(null, Buffers.realBuffer(NUMBER_OF_ROWS).toColumn());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceWrongLabelColumnFiller() {
			builder().replaceReal("x", i -> 2 * i);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullLabelColumnFiller() {
			builder().replaceReal(null, i -> 2 * i);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceWrongLabelColumnFillerInt() {
			builder().replaceInt53Bit("x", i -> 2 * i);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullLabelColumnFillerInt() {
			builder().replaceInt53Bit(null, i -> 2 * i);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullColumnFillerInt() {
			builder().replaceInt53Bit("a", (IntToDoubleFunction) null);
		}

	}

	@RunWith(Parameterized.class)
	public static class WithFourStartColumns {

		private static final double[] FIRST_COLUMN = random(NUMBER_OF_ROWS);
		private static final double[] SECOND_COLUMN = random(NUMBER_OF_ROWS);
		private static final double[] THIRD_COLUMN = random(NUMBER_OF_ROWS);
		private static final double[] FOURTH_COLUMN = random(NUMBER_OF_ROWS);

		@Parameter
		public String builderInitialization;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(INIT_NEW_TABLE, INIT_EXISTING_TABLE);
		}

		private TableBuilder builder() {
			switch (builderInitialization) {
				case INIT_NEW_TABLE:
					return Builders.newTableBuilder(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.addReal("b", i -> SECOND_COLUMN[i])
							.addReal("c", i -> THIRD_COLUMN[i])
							.addReal("d", i -> FOURTH_COLUMN[i]);
				case INIT_EXISTING_TABLE:
					Table source = Builders.newTableBuilder(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.addReal("b", i -> SECOND_COLUMN[i])
							.addReal("c", i -> THIRD_COLUMN[i])
							.addReal("d", i -> FOURTH_COLUMN[i])
							.build(CTX);
					return Builders.newTableBuilder(source);
				default:
					throw new IllegalStateException("Unknown table initialization");
			}
		}

		@Test
		public void testRemoveColumnsOutOfOrder() {
			Table table = builder().remove("b")
					.remove("a")
					.remove("c")
					.build(CTX);
			assertArrayEquals(new double[][]{FOURTH_COLUMN}, readTableToArray(table));
			assertArrayEquals(new String[]{"d"}, table.labelArray());
		}

		@Test
		public void testRemoveAddRemoveColumns() {
			double[] firstAddition = random(NUMBER_OF_ROWS);
			double[] secondAddition = random(NUMBER_OF_ROWS);

			Table table = builder().remove("a")
					.addReal("e", i -> firstAddition[i])
					.addReal("f", i -> secondAddition[i])
					.remove("f")
					.build(CTX);

			assertArrayEquals(new double[][]{SECOND_COLUMN, THIRD_COLUMN, FOURTH_COLUMN, firstAddition},
					readTableToArray(table));
			assertArrayEquals(new String[]{"b", "c", "d", "e"}, table.labelArray());
		}

	}

	public static class WithoutStartColumns {

		@Test(expected = IllegalStateException.class)
		public void testAddColumnDifferentLength() {
			Builders.newTableBuilder(10)
					.add("a", Buffers.realBuffer(10).toColumn())
					.add("b", Buffers.realBuffer(42, false).toColumn());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeNumberOfRows() {
			Builders.newTableBuilder(-11);
		}

		@Test
		public void testZeroColumnsAndRows() {
			Table table = Builders.newTableBuilder(0).build(CTX);
			assertEquals(0, table.width());
			assertEquals(0, table.height());
		}

		@Test
		public void testZeroRowsOneColumn() {
			Table table = Builders.newTableBuilder(0)
					.add("a", Buffers.realBuffer(0, true).toColumn())
					.build(CTX);
			assertEquals(1, table.width());
			assertEquals(0, table.height());
		}

		@Test
		public void testEmtpy() {
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS).build(CTX);
			assertEquals(0, table.width());
			assertEquals(NUMBER_OF_ROWS, table.height());
		}

		@Test
		public void testEmtpyAgain() {
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS).build(CTX);
			Table table2 = Builders.newTableBuilder(table).build(CTX);
			assertEquals(0, table2.width());
			assertEquals(NUMBER_OF_ROWS, table2.height());
		}

		@Test
		public void testFromEmpty() {
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS).build(CTX);
			Table table2 = Builders.newTableBuilder(table).addInt53Bit("new", i->i).build(CTX);
			assertEquals(1, table2.width());
			assertEquals(NUMBER_OF_ROWS, table2.height());
		}

		@Test
		public void testOneColumnFiller() {
			double[] single = random(NUMBER_OF_ROWS);

			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addReal("a", i -> single[i])
					.build(CTX);

			assertArrayEquals(new double[][]{single}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testOneColumnBuffer() {
			double[] single = random(NUMBER_OF_ROWS);

			NumericBuffer buffer = Buffers.realBuffer(NUMBER_OF_ROWS, false);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				buffer.set(i, single[i]);
			}

			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("a", buffer.toColumn())
					.build(CTX);

			assertArrayEquals(new double[][]{single}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test(expected = IllegalStateException.class)
		public void testOneColumnBufferChangedAfterwards() {
			NumericBuffer buffer =Buffers.realBuffer(10);
			for (int i = 0; i < 10; i++) {
				buffer.set(i, i);
			}
			Builders.newTableBuilder(buffer.size()).add("a", buffer.toColumn());
			buffer.set(5, 7);
		}

		@Test(expected = IllegalStateException.class)
		public void testReplaceColumnBufferChangedAfterwards() {
			NumericBuffer buffer = Buffers.realBuffer(10, false);
			for (int i = 0; i < 10; i++) {
				buffer.set(i, i);
			}
			Builders.newTableBuilder(10)
					.addReal("a", i -> i)
					.replace("a", buffer.toColumn());
			buffer.set(5, 7);
		}

		@Test(expected = ExecutionAbortedException.class)
		public void testBuildCancellation() {
			Context oneShortCtx = new Context() {
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
				public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
					if (active.get()) {
						active.set(false);
						return CTX.call(callables);
					} else {
						throw new RejectedExecutionException("You had your one shot");
					}

				}
			};

			Builders.newTableBuilder(100)
					.addReal("a", i -> i)
					.addReal("b", i -> i)
					.addReal("c", i -> i)
					.build(oneShortCtx);
		}

	}

	public static class NumericColumnType {

		@Test
		public void testAddInteger() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addInt53Bit("one", i -> single[i]).build(CTX);
			double[] expected = new double[single.length];
			Arrays.setAll(expected, i -> Math.round(single[i]));
			assertArrayEquals(new double[][]{expected}, readTableToArray(table));
			assertEquals(Column.TypeId.INTEGER_53_BIT, table.column(0).type().id());
		}

		@Test
		public void testAddIntBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addInt53Bit("one", i -> single[i]).build(CTX);
			NumericBuffer buffer = Buffers.integer53BitBuffer(NUMBER_OF_ROWS, false);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, single[i]);
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToArray(table), readTableToArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddReal() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addInt53Bit("one", i -> single[i]).build(CTX);
			assertEquals(Column.TypeId.INTEGER_53_BIT, table.column(0).type().id());
		}

		@Test
		public void testAddRealBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addReal("one", i -> single[i]).build(CTX);
			NumericBuffer buffer = Buffers.realBuffer(NUMBER_OF_ROWS, false);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, single[i]);
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToArray(table), readTableToArray(bufferTable));
			assertEquals(table.column(0).type().id(), bufferTable.column(0).type().id());
		}

	}

	public static class ToString {

		@Test
		public void testEmpty() {
			TableBuilder empty = Builders.newTableBuilder(0);
			assertEquals("Table builder (0x0)\n", empty.toString());
		}

		@Test
		public void testEmptyWithRows() {
			TableBuilder empty = Builders.newTableBuilder(5);
			assertEquals("Table builder (0x5)\n", empty.toString());
		}

		@Test
		public void testAddedColumnsWithRows() {
			TableBuilder builder = Builders.newTableBuilder(5).addReal("a", i -> i).addReal("b", i -> 0);
			assertEquals("Table builder (2x5)\na | b", builder.toString());
		}

		@Test
		public void testMoreColumns() {
			TableBuilder builder = Builders.newTableBuilder(3);
			for (int i = 0; i < 9; i++) {
				builder.addReal("c" + i, j -> j);
			}
			assertEquals("Table builder (9x3)\nc0 | c1 | c2 | c3 | c4 | c5 | ... | c8", builder.toString());
		}

		@Test
		public void testMaxColumns() {
			TableBuilder builder = Builders.newTableBuilder(0);
			for (int i = 0; i < 8; i++) {
				builder.addReal("att" + i, j -> j);
			}
			assertEquals("Table builder (8x0)\natt0 | att1 | att2 | att3 | att4 | att5 | att6 | att7",
					builder.toString());
		}

	}

	public static class ObjectsExceptions {

		private final TableBuilder builder = Builders.newTableBuilder(123).addReal("a", i -> i);

		@Test(expected = NullPointerException.class)
		public void testNominalNullLabel() {
			builder.addNominal(null, i -> "" + i);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNominalUsedLabel() {
			builder.addNominal("a", i -> "" + i);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNominalInvalidLabel() {
			builder.addNominal("", i -> "" + i);
		}

		@Test(expected = NullPointerException.class)
		public void testNominalNullGenerator() {
			builder.addNominal("b", null);
		}

		@Test(expected = NullPointerException.class)
		public void testNominalMaxNullLabel() {
			builder.addNominal(null, i -> "" + i, 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNominalMaxUsedLabel() {
			builder.addNominal("a", i -> "" + i, 11);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNominalMaxInvalidLabel() {
			builder.addNominal("", i -> "" + i, 123);
		}

		@Test(expected = NullPointerException.class)
		public void testNominalMaxNullGenerator() {
			builder.addNominal("b", null, 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNominalMaxWrongMax() {
			builder.addNominal("b", i -> "" + i, 12).build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testFreeNullLabel() {
			builder.addTextset(null, i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFreeUsedLabel() {
			builder.addTextset("a", i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFreeInvalidLabel() {
			builder.addTextset("", i -> null);
		}

		@Test(expected = NullPointerException.class)
		public void testFreeNullGenerator() {
			builder.addTextset("b", null);
		}

		@Test(expected = NullPointerException.class)
		public void testBooleanNullLabel() {
			builder.addBoolean(null, i -> "" + i, "1");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanUsedLabel() {
			builder.addBoolean("a", i -> "" + i, "1");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanInvalidLabel() {
			builder.addBoolean("", i -> "" + i, "1");
		}

		@Test(expected = NullPointerException.class)
		public void testBooleanNullGenerator() {
			builder.addBoolean("b", null, "1");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanTooManyValues() {
			builder.addBoolean("b", i -> "" + i, "1").build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanWrongPositiveValue() {
			builder.addBoolean("b", i -> "" + (i % 2), "5").build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testTimeNullLabel() {
			builder.addTime(null, i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTimeUsedLabel() {
			builder.addTime("a", i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTimeInvalidLabel() {
			builder.addTime("", i -> null);
		}

		@Test(expected = NullPointerException.class)
		public void testTimeNullGenerator() {
			builder.addTime("b", null);
		}

		@Test(expected = NullPointerException.class)
		public void testDateTimeNullLabel() {
			builder.addDateTime(null, i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testDateTimeUsedLabel() {
			builder.addDateTime("a", i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testDateTimeInvalidLabel() {
			builder.addDateTime("", i -> null);
		}

		@Test(expected = NullPointerException.class)
		public void testDateTimeNullGenerator() {
			builder.addDateTime("b", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceNominalUnusedLabel() {
			builder.replaceNominal("b", i -> "" + i);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNominalNullGenerator() {
			builder.replaceNominal("a", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceNominalMaxUnusedLabel() {
			builder.replaceNominal("b", i -> "" + i, 1);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNominalMaxNullGenerator() {
			builder.replaceNominal("a", null, 1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceNominalMaxWrongMax() {
			builder.replaceNominal("a", i -> "" + i, 1).build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceTextsetUnusedLabel() {
			builder.replaceTextset("b", i -> null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceTextUnusedLabel() {
			builder.replaceText("b", i -> null);
		}
		@Test(expected = NullPointerException.class)
		public void testReplaceTextNullGenerator() {
			builder.replaceText("a", null);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceTextsetNullGenerator() {
			builder.replaceTextset("a", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanUnusedLabel() {
			builder.replaceBoolean("b", i -> "" + i, "1");
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceBooleanNullGenerator() {
			builder.replaceBoolean("a", null, "1");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanTooManyValues() {
			builder.replaceBoolean("a", i -> "" + i, "1").build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanWrongPositiveValue() {
			builder.replaceBoolean("a", i -> "" + (i % 2), "5").build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceTimeUnusedLabel() {
			builder.replaceTime("b", i -> null);
		}


		@Test(expected = NullPointerException.class)
		public void testReplaceTimeNullGenerator() {
			builder.replaceTime("a", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceDateTimeUnusedLabel() {
			builder.replaceDateTime("b", i -> null);
		}


		@Test(expected = NullPointerException.class)
		public void testReplaceDateTimeNullGenerator() {
			builder.replaceDateTime("a", null);
		}

	}

	public static class ObjectColumnType {

		private static final int NUMBER_OF_ROWS = 123;

		@Test
		public void testAddNominal() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> "" + Math.round(single[i]));
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
			assertEquals(ColumnType.NOMINAL, table.column(0).type());
		}

		@Test
		public void testAddNominalVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			Int32NominalBuffer buffer = BufferAccessor.get().newInt32Buffer(ColumnType.NOMINAL,
					NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, "" + Math.round(single[i]));
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToArray(bufferTable), readTableToArray(table));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddText() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			DoubleFunction<String> preprocessor = d -> "val" + d;
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addText("one", i -> preprocessor.apply(single[i])).build(CTX);
			assertEquals(Column.TypeId.TEXT, table.column(0).type().id());
			assertEquals(Column.Category.OBJECT, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> preprocessor.apply(single[i]));
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddTextVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			DoubleFunction<String> preprocessor = d -> "val" + d;
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addText("one", i -> preprocessor.apply(single[i])).build(CTX);
			ObjectBuffer<String> buffer = Buffers.textBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, preprocessor.apply(single[i]));
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddTextset() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			DoubleFunction<StringSet> preprocessor = d -> new StringSet(Collections.singleton("val" + d));
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addTextset("one", i -> preprocessor.apply(single[i])).build(CTX);
			assertEquals(Column.TypeId.TEXT_SET, table.column(0).type().id());
			assertEquals(Column.Category.OBJECT, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> preprocessor.apply(single[i]));
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddTextsetVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			DoubleFunction<StringSet> preprocessor = d -> new StringSet(Collections.singleton("val" + d));
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addTextset("one", i -> preprocessor.apply(single[i])).build(CTX);
			ObjectBuffer<StringSet> buffer = Buffers.textsetBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, preprocessor.apply(single[i]));
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddBoolean() {
			double[] single = random(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addBoolean("one", i -> single[i] > 0.5 ? "true" : "false", "true").build(CTX);
			assertEquals(Column.TypeId.NOMINAL, table.column(0).type().id());
			assertEquals(Column.Category.CATEGORICAL, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> single[i] > 0.5 ? "true" : "false");
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddBooleanVsBuffer() {
			double[] single = random(NUMBER_OF_ROWS);
			IntFunction<String> lambda = i -> single[i] > 0.5 ? "true" : "false";
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addBoolean("one", lambda, "true").build(CTX);
			UInt2NominalBuffer buffer =
					BufferAccessor.get().newUInt2Buffer(ColumnType.NOMINAL, NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, lambda.apply(i));
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}


		@Test
		public void testAddNominalVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addInt53Bit("one", i -> i)
					.replaceNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			assertArrayEquals(readTableToArray(replaceTable), readTableToArray(table));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddCategoricalVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addNominal("one", i -> Math.rint(single[i])+"").build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addReal("one", i -> i)
					.replaceNominal("one", i -> Math.rint(single[i])+"").build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}


		@Test
		public void testAddTextsetVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			DoubleFunction<StringSet> preprocessor = d -> new StringSet(Collections.singleton("val" + d));
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addTextset("one", i -> preprocessor.apply(single[i])).build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addReal("one", i -> i)
					.replaceTextset("one", i -> preprocessor.apply(single[i])).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddTextVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			DoubleFunction<String> preprocessor = d -> "val" + d;
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addText("one", i -> preprocessor.apply(single[i])).build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addReal("one", i -> i)
					.replaceText("one", i -> preprocessor.apply(single[i])).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddBooleanVsReplace() {
			double[] single = random(NUMBER_OF_ROWS);
			IntFunction<String> lambda = i -> single[i] > 0.5 ? "true" : "false";
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addBoolean("one", lambda, "true").build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addInt53Bit("one", i->i)
					.replaceBoolean("one", lambda, "true").build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddTime() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addTime("one", i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L))).build(CTX);
			assertEquals(Column.TypeId.TIME, table.column(0).type().id());
			assertEquals(Column.Category.OBJECT, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L)));
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddTimeVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<LocalTime> lambda = i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L));
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS).addTime("one", lambda).build(CTX);
			TimeBuffer buffer = Buffers.timeBuffer(NUMBER_OF_ROWS, false);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, lambda.apply(i));
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddTimeVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<LocalTime> lambda = i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L));
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addTime("one", lambda).build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addInt53Bit("one", i -> i)
					.replaceTime("one", lambda).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddDateTime() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<Instant> lambda =
					i -> Instant.ofEpochSecond(Math.round(single[i] * 631137797288063L), 123456789);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addDateTime("one", lambda).build(CTX);
			assertEquals(Column.TypeId.DATE_TIME, table.column(0).type().id());
			assertEquals(Column.Category.OBJECT, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, lambda);
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddDateTimeVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<Instant> lambda =
					i -> Instant.ofEpochSecond(Math.round(single[i] * 631137797288063L), 123456789);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS).addDateTime("one", lambda).build(CTX);
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(NUMBER_OF_ROWS, true, false);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, lambda.apply(i));
			}
			Table bufferTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddDateTimeVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<Instant> lambda =
					i -> Instant.ofEpochSecond(Math.round(single[i] * 631137797288063L), 123456789);
			Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
					.addDateTime("one", lambda).build(CTX);
			Table replaceTable = Builders.newTableBuilder(NUMBER_OF_ROWS).addInt53Bit("one", i -> i)
					.replaceDateTime("one", lambda).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}
	}


	@RunWith(Parameterized.class)
	public static class CategoricalColumnFormats {

		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> columnImplementations() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		private double[] randomStretched(int n) {
			double[] data = new double[n];
			Arrays.setAll(data, i -> format.maxValue() * Math.random());
			return data;
		}

		@Test
		public void testAddCategoricalMax() {
			int numberOfRows = Math.min(format.maxValue(), IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 1);
			double[] single = randomStretched(numberOfRows);
			Table table = Builders.newTableBuilder(numberOfRows)
					.addNominal("one", i -> single[i]+"", format.maxValue()).build(CTX);
			assertEquals(Column.TypeId.NOMINAL, table.column(0).type().id());
			assertEquals(Column.Category.CATEGORICAL, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> single[i]+"");
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddCategoricalMaxVsReplace() {
			int numberOfRows = Math.min(format.maxValue(), IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 1);
			double[] single = randomStretched(numberOfRows);
			Table table = Builders.newTableBuilder(numberOfRows)
					.addNominal("one", i -> Math.rint(single[i])+"", format.maxValue()).build(CTX);
			Table replaceTable = Builders.newTableBuilder(numberOfRows).addReal("one", i -> i)
					.replaceNominal("one", i -> Math.rint(single[i])+"", format.maxValue()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}
	}

	public static class MetaData {

		static class TableUnique implements ColumnMetaData {

			@Override
			public String type() {
				return "com.rapidminer.belt.met.column.tableunique";
			}

			@Override
			public Uniqueness uniqueness() {
				return Uniqueness.TABLE;
			}

		}

		static class TableUniqueSubclass extends TableUnique {

		}

		@Test(expected = NullPointerException.class)
		public void testAddWithNullLabel() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.addMetaData(null, ColumnRole.METADATA)
					.build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testAddWithMissingLabel() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.addMetaData("does not exist", ColumnRole.METADATA)
					.build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testAddWithNullMetaData() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", (ColumnMetaData) null)
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddColumnUniqueDataTwice() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", ColumnRole.ID)
					.addMetaData("label", ColumnRole.LABEL)
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddTableUniqueDataTwice() {
			Builders.newTableBuilder(100)
					.addReal("one", i -> i)
					.addReal("two", i -> i)
					.addMetaData("one", new TableUnique())
					.addMetaData("two", new TableUnique())
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddSubclassOfTableUniqueData() {
			// Test whether uniqueness constraint checks are independent of class hierarchy
			Builders.newTableBuilder(100)
					.addReal("one", i -> i)
					.addReal("two", i -> i)
					.addMetaData("one", new TableUnique())
					.addMetaData("two", new TableUniqueSubclass())
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddParentOfTableUniqueData() {
			// Test whether uniqueness constraint checks are independent of class hierarchy
			Builders.newTableBuilder(100)
					.addReal("one", i -> i)
					.addReal("two", i -> i)
					.addMetaData("one", new TableUniqueSubclass())
					.addMetaData("two", new TableUnique())
					.build(CTX);
		}

		@Test
		public void testAddTwoInstances() {
			Table table = Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", new NonUnique("Test #1"))
					.addMetaData("label", new NonUnique("Test #2"))
					.build(CTX);
			List<ColumnMetaData> list = table.getMetaData("label");
			List<ColumnMetaData> expected = Arrays.asList(new NonUnique("Test #1"),
					new NonUnique("Test #2"));
			assertEquals(new HashSet<>(expected), new HashSet<>(list));
		}


		@Test
		public void testAddDuplicate() {
			Table table = Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", new NonUnique("Test"))
					.addMetaData("label", new NonUnique("Test"))
					.build(CTX);
			List<ColumnMetaData> list = table.getMetaData("label");
			assertEquals(Collections.singletonList(new NonUnique("Test")), list);
		}

		@Test
		public void testAddingVariousData() {
			Table table = Builders.newTableBuilder(100)
					.addInt53Bit("id", i -> i)
					.addInt53Bit("age", i -> 25)
					.addReal("weight", i -> 73.5)
					.addReal("mystery", i -> 0f)
					.addMetaData("id", ColumnRole.ID)
					.addMetaData("age", new ColumnAnnotation("years"))
					.addMetaData("mystery", ColumnRole.LABEL)
					.addMetaData("mystery", new NonUnique("unit?"))
					.addMetaData("mystery", new NonUnique("name?"))
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), table.getMetaData("id"));
			assertEquals(Collections.singletonList(new ColumnAnnotation("years")), table.getMetaData("age"));
			assertEquals(Collections.emptyList(), table.getMetaData("weight"));
			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.LABEL, new NonUnique("unit?"),
					new NonUnique("name?"));
			assertEquals(new HashSet<>(expected), new HashSet<>(table.getMetaData("mystery")));
		}

		@Test(expected = NullPointerException.class)
		public void testAddListWithNullLabel() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData(null, Collections.emptyList())
					.build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testAddListWithMissingLabel() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("does not exist", Collections.emptyList())
					.build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testAddListWithNullMetaDataList() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", (List<ColumnMetaData>) null)
					.build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testAddListWithNullItem() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", Arrays.asList(ColumnRole.ID, null, new ColumnAnnotation("Annotation")))
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddListWithUniquenessViolation() {
			Builders.newTableBuilder(100)
					.addReal("label", i -> i)
					.addMetaData("label", Arrays.asList(ColumnRole.ID, new ColumnAnnotation("Annotation"),
							ColumnRole.LABEL))
					.build(CTX);
		}

		@Test
		public void testAddingVariousDataViaList() {
			Table table = Builders.newTableBuilder(100)
					.addInt53Bit("id", i -> i)
					.addReal("mystery", i -> 0f)
					.addMetaData("mystery", Arrays.asList(ColumnRole.LABEL, new ColumnAnnotation("unit?"),
							new ColumnReference("name?")))
					.build(CTX);

			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.LABEL, new ColumnAnnotation("unit?"),
					new ColumnReference("name?"));
			assertEquals(new HashSet<>(expected), new HashSet<>(table.getMetaData("mystery")));
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveTypeWithNullLabel() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.removeMetaData(null, ColumnRole.class);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testRemoveTypeWithMissingLabel() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.removeMetaData("does not exist", ColumnRole.class);
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveTypeWithNullType() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.removeMetaData("dummy", (Class<ColumnMetaData>) null);
		}

		@Test
		public void testRemoveTypeWithoutRemainder() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new NonUnique("Annotation #1"))
					.addMetaData("two", new NonUnique("Annotation #2"))
					.addReal("three", i -> 3)
					.removeMetaData("two", NonUnique.class)
					.build(CTX);

			assertEquals(Collections.emptyList(), table.getMetaData("two"));
		}

		@Test
		public void testRemoveTypeWithRemainder() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new NonUnique("Annotation #1"))
					.addMetaData("two", new NonUnique("Annotation #1"))
					.addReal("three", i -> 3)
					.removeMetaData("two", NonUnique.class)
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), table.getMetaData("two"));
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveInstanceWithNullLabel() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.removeMetaData(null, ColumnRole.ID);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testRemoveInstanceWithMissingLabel() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.removeMetaData("does not exist", ColumnRole.ID);
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveInstanceWithNullType() {
			Builders.newTableBuilder(100)
					.addReal("dummy", i -> i)
					.removeMetaData("dummy", (ColumnMetaData) null);
		}

		@Test
		public void testRemoveInstanceWithoutRemainder() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.BATCH)
					.addReal("three", i -> 3)
					.removeMetaData("two", ColumnRole.BATCH)
					.build(CTX);

			assertEquals(Collections.emptyList(), table.getMetaData("two"));
		}

		@Test
		public void testRemoveInstanceWithRemainder() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new ColumnAnnotation("Annotation"))
					.addReal("three", i -> 3)
					.removeMetaData("two", new ColumnAnnotation("Annotation"))
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), table.getMetaData("two"));
		}

		@Test(expected = NullPointerException.class)
		public void testClearNullLabel() {
			Table table = Builders.newTableBuilder(100)
					.addReal("dummy", i -> 1)
					.clearMetaData(null)
					.build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testClearMissingLabel() {
			Table table = Builders.newTableBuilder(100)
					.addReal("dummy", i -> 1)
					.clearMetaData("does nto exist")
					.build(CTX);
		}

		@Test
		public void testRemoveEntireColumn() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new ColumnAnnotation("Annotation"))
					.addReal("three", i -> 3)
					.addMetaData("three", ColumnRole.BATCH)
					.remove("two")
					.addReal("two", i -> 2)
					.build(CTX);

			assertEquals(Collections.emptyList(), table.getMetaData("two"));
			assertEquals(Collections.singletonList(ColumnRole.BATCH), table.getMetaData("three"));
		}

		@Test
		public void testClear() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new ColumnAnnotation("Annotation"))
					.addReal("three", i -> 3)
					.addMetaData("three", ColumnRole.BATCH)
					.clearMetaData("two")
					.build(CTX);

			assertEquals(Collections.emptyList(), table.getMetaData("two"));
			assertEquals(Collections.singletonList(ColumnRole.BATCH), table.getMetaData("three"));
		}

		@Test
		public void testRenameColumn() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addReal("three", i -> 3)
					.addMetaData("three", ColumnRole.BATCH)
					.rename("two", "newtwo")
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), table.getMetaData("newtwo"));
			assertEquals(Collections.singletonList(ColumnRole.BATCH), table.getMetaData("three"));
		}

		@Test
		public void testMetaDataInheritance() {
			Table a = Builders.newTableBuilder(100)
					.addInt53Bit("id", i -> i)
					.addInt53Bit("age", i -> 25)
					.addReal("weight", i -> 73.5)
					.addReal("mystery", i -> 0f)
					.addMetaData("id", ColumnRole.ID)
					.addMetaData("age", new ColumnAnnotation("years"))
					.addMetaData("mystery", ColumnRole.LABEL)
					.addMetaData("mystery", new NonUnique("unit?"))
					.addMetaData("mystery", new NonUnique("name?"))
					.build(CTX);

			Table b = Builders.newTableBuilder(a)
					.addMetaData("id", new ColumnAnnotation("id"))
					.removeMetaData("mystery", NonUnique.class)
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), a.getMetaData("id"));
			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.LABEL, new NonUnique("unit?"),
					new NonUnique("name?"));
			assertEquals(new HashSet<>(expected), new HashSet<>(a.getMetaData("mystery")));

			expected = Arrays.asList(ColumnRole.ID, new ColumnAnnotation("id"));
			assertEquals(new HashSet<>(expected), new HashSet<>(b.getMetaData("id")));
			assertEquals(Collections.singletonList(ColumnRole.LABEL), b.getMetaData("mystery"));
		}

		@Test
		public void testAddInvalidReference() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnReference("does not exist"))
					.build(CTX);
			assertEquals(Collections.singletonList("two"), table.select().withMetaData(ColumnReference.class).labels());
		}

		@Test
		public void testAddLaterValidReference() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnReference("three"))
					.addReal("three", i -> 3)
					.build(CTX);
			assertEquals(Collections.singletonList("two"),
					table.select().withMetaData(ColumnReference.class).labels());
		}

		@Test
		public void testRemoveReferencedColumn() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.remove("one")
					.build(CTX);
			assertEquals(Collections.singletonList("two"), table.select().withMetaData(ColumnReference.class).labels());
		}

		@Test
		public void testRenameReferencedColumn() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.rename("one", "renamed")
					.build(CTX);
			ColumnReference reference = table.getFirstMetaData("two", ColumnReference.class);
			assertEquals("renamed", reference.getColumn());
		}

		@Test
		public void testRenameTwice() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.rename("one", "renamed")
					.rename("renamed", "renamed2")
					.build(CTX);
			ColumnReference reference = table.getFirstMetaData("two", ColumnReference.class);
			assertEquals("renamed2", reference.getColumn());
		}

		@Test
		public void testRenameTwo() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.rename("one", "renamed")
					.rename("two", "renamed2")
					.build(CTX);
			ColumnReference reference = table.getFirstMetaData("renamed2", ColumnReference.class);
			assertEquals("renamed", reference.getColumn());
		}

		@Test
		public void testRenameAndRemoveAndRename() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.rename("one", "renamed")
					.remove("three")
					.rename("two", "renamed2")
					.build(CTX);
			Map<String, List<ColumnMetaData>> expected = new HashMap<>();
			expected.put("renamed2", Collections.singletonList(new ColumnReference("renamed")));
			assertEquals(expected, table.getMetaData());
		}

		@Test
		public void testRenameAndRemoveRefAndRename() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.addMetaData("three", new ColumnReference("one"))
					.rename("one", "renamed")
					.removeMetaData("three", ColumnReference.class)
					.rename("two", "renamed2")
					.build(CTX);
			Map<String, List<ColumnMetaData>> expected = new HashMap<>();
			expected.put("renamed2", Collections.singletonList(new ColumnReference("renamed")));
			assertEquals(expected, table.getMetaData());
		}

		@Test
		public void testRenameAndRemoveRefByValAndRename() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.addMetaData("three", new ColumnReference("one"))
					.rename("one", "renamed")
					.removeMetaData("three", new ColumnReference("renamed"))
					.rename("two", "renamed2")
					.build(CTX);
			Map<String, List<ColumnMetaData>> expected = new HashMap<>();
			expected.put("renamed2", Collections.singletonList(new ColumnReference("renamed")));
			assertEquals(expected, table.getMetaData());
		}

		@Test
		public void testRenameAndAddRefAndRename() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 2)
					.addMetaData("two", new ColumnReference("one"))
					.rename("one", "renamed")
					.addMetaData("three", new ColumnReference("two"))
					.rename("two", "renamed2")
					.build(CTX);
			Map<String, List<ColumnMetaData>> expected = new HashMap<>();
			expected.put("renamed2", Collections.singletonList(new ColumnReference("renamed")));
			expected.put("three", Collections.singletonList(new ColumnReference("renamed2")));
			assertEquals(expected, table.getMetaData());
		}

		@Test
		public void testRenameAndAddRefAndRenameMoreMd() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 2)
					.addMetaData("two", Arrays.asList(new ColumnAnnotation("bla"), new ColumnReference("one")))
					.rename("one", "renamed")
					.addMetaData("three", Arrays.asList(ColumnRole.METADATA, new ColumnReference("two"), new ColumnAnnotation("xx")))
					.rename("two", "renamed2")
					.build(CTX);
			Map<String, List<ColumnMetaData>> expected = new HashMap<>();
			expected.put("renamed2", Arrays.asList(new ColumnAnnotation("bla"), new ColumnReference("renamed")));
			expected.put("three", Arrays.asList(ColumnRole.METADATA, new ColumnReference("renamed2"), new ColumnAnnotation("xx")));
			assertEquals(expected, table.getMetaData());
		}

		@Test
		public void testRenameAndAddRefAndRemoveRefAndRenameMoreMd() {
			Table table = Builders.newTableBuilder(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 2)
					.addMetaData("two", Arrays.asList(new ColumnAnnotation("bla"), new ColumnReference("one")))
					.rename("one", "renamed")
					.addMetaData("three", Arrays.asList(ColumnRole.METADATA, new ColumnReference("two"), new ColumnAnnotation("xx")))
					.removeMetaData("three", ColumnReference.class)
					.rename("two", "renamed2")
					.build(CTX);
			Map<String, List<ColumnMetaData>> expected = new HashMap<>();
			expected.put("renamed2", Arrays.asList(new ColumnAnnotation("bla"), new ColumnReference("renamed")));
			expected.put("three", Arrays.asList(ColumnRole.METADATA, new ColumnAnnotation("xx")));
			assertEquals(expected, table.getMetaData());
		}

	}

}
