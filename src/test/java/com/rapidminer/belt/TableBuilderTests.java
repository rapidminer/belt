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
import static org.junit.Assert.assertArrayEquals;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.util.ColumnAnnotation;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnRole;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.TaskAbortedException;

/**
 * @author Gisa Schaefer
 */
@RunWith(Enclosed.class)
public class TableBuilderTests {

	private static final int NUMBER_OF_ROWS = 25;

	private static final String INIT_NEW_TABLE = "new_table";

	private static final String INIT_EXISTING_TABLE = "from_existing_table";

	private static final String INIT_SINGLE_TABLE_TASK = "from_single_table_task";

	private static final String INIT_CHAINED_TABLE_TASKS = "from_chained_table_task";

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
		ColumnReader reader = new ColumnReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	private static Object[] readColumnToObjectArray(Table table, int column) {
		Object[] data = new Object[table.height()];
		ObjectColumnReader<Object> reader = new ObjectColumnReader<>(table.column(column), Object.class);
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

	/**
	 * Wrapper context that becomes inactive if the given atomic boolean returns {@code false}.
	 */
	private static class BooleanContext implements Context {

		private final AtomicBoolean flag;

		private BooleanContext(AtomicBoolean flag) {
			this.flag = flag;
		}

		@Override
		public boolean isActive() {
			return flag.get();
		}

		@Override
		public int getParallelism() {
			return CTX.getParallelism();
		}

		@Override
		public <T> Future<T> submit(Callable<T> job) {
			if (flag.get()) {
				return CTX.submit(job);
			} else {
				throw new RejectedExecutionException("Context is inactive");
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class WithOneStartColumn {

		private static final double[] FIRST_COLUMN = random(NUMBER_OF_ROWS);

		@Parameter
		public String builderInitialization;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(INIT_NEW_TABLE, INIT_EXISTING_TABLE, INIT_SINGLE_TABLE_TASK);
		}

		private TableBuilder builder() {
			switch (builderInitialization) {
				case INIT_NEW_TABLE:
					return Table.newTable(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i]);
				case INIT_EXISTING_TABLE:
					Table source = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> FIRST_COLUMN[i])
					.build(CTX);
					return Table.from(source);
				case INIT_SINGLE_TABLE_TASK:
					TableTask task = Table.newTable(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.build();
					return Table.from(task);
				default:
					throw new IllegalStateException("Unknown table initialization");
			}
		}

		@Test
		public void testFillerBufferFiller() {
			double[] second = random(NUMBER_OF_ROWS);
			double[] third = random(NUMBER_OF_ROWS);
			double[] fourth = random(NUMBER_OF_ROWS);

			ColumnBuffer buffer = new FixedRealBuffer(NUMBER_OF_ROWS);
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

			ColumnBuffer secondAsBuffer = new FixedRealBuffer(NUMBER_OF_ROWS);
			ColumnBuffer fourthAsBuffer = new FixedRealBuffer(NUMBER_OF_ROWS);
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

			ColumnBuffer secondAsBuffer = new FixedRealBuffer(NUMBER_OF_ROWS);
			ColumnBuffer fourthAsBuffer = new FixedRealBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				secondAsBuffer.set(i, second[i]);
				fourthAsBuffer.set(i, fourth[i]);
			}
			Table fourthAsTable = Table.newTable(NUMBER_OF_ROWS).add("a", fourthAsBuffer.toColumn()).build(CTX);

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

			ColumnBuffer buffer = new FixedRealBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				buffer.set(i, fourth[i]);
			}

			Table thirdAsTable = Table.newTable(NUMBER_OF_ROWS)
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
			Table table = builder().replaceInt("a", i -> replacement[i]).build(CTX);
			Arrays.setAll(replacement, i -> Math.round(replacement[i]));
			assertArrayEquals(new double[][]{replacement}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testReplaceColumnFrom() {
			double[] replacement = random(NUMBER_OF_ROWS);
			Table replacementTable = Table.newTable(NUMBER_OF_ROWS).addReal("a", i -> replacement[i]).build(CTX);
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
			Table firstTable = Table.newTable(NUMBER_OF_ROWS).addReal("a", i -> firstAddition[i]).build(CTX);

			Table table = builder().add("b", firstTable.column( "a"))
					.rename("b", "c")
					.build(CTX);

			assertArrayEquals(new double[][]{FIRST_COLUMN, firstAddition}, readTableToArray(table));
			assertArrayEquals(new String[]{"a", "c"}, table.labelArray());
		}

		@Test
		public void testRenameReplaceFrom() {
			double[] firstAddition = random(NUMBER_OF_ROWS);
			Table firstTable = Table.newTable(NUMBER_OF_ROWS).addReal("a", i -> firstAddition[i]).build(CTX);

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
			builder().addInt("b", (IntToDoubleFunction) null);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddWrongSizeColumn() {
			builder().add("b", new FixedRealBuffer(5).toColumn());
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
			builder().replace("a", new FixedRealBuffer(5).toColumn());
		}

		@Test(expected = IllegalStateException.class)
		public void testReplaceWrongSizeTable() {
			Table table = Table.newTable(5).addReal("b", i -> i).build(CTX);
			builder().replace("a", table.column("b"));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceWrongLabelColumn() {
			builder().replace("x", new FixedRealBuffer(NUMBER_OF_ROWS).toColumn());
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullLabelColumn() {
			builder().replace(null, new FixedRealBuffer(NUMBER_OF_ROWS).toColumn());
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
			builder().replaceInt("x", i -> 2 * i);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullLabelColumnFillerInt() {
			builder().replaceInt(null, i -> 2 * i);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceNullColumnFillerInt() {
			builder().replaceInt("a", (IntToDoubleFunction) null);
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
			return Arrays.asList(INIT_NEW_TABLE, INIT_EXISTING_TABLE, INIT_SINGLE_TABLE_TASK, INIT_CHAINED_TABLE_TASKS);
		}

		private TableBuilder builder() {
			switch (builderInitialization) {
				case INIT_NEW_TABLE:
					return Table.newTable(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.addReal("b", i -> SECOND_COLUMN[i])
							.addReal("c", i -> THIRD_COLUMN[i])
							.addReal("d", i -> FOURTH_COLUMN[i]);
				case INIT_EXISTING_TABLE:
					Table source = Table.newTable(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.addReal("b", i -> SECOND_COLUMN[i])
							.addReal("c", i -> THIRD_COLUMN[i])
							.addReal("d", i -> FOURTH_COLUMN[i])
							.build(CTX);
					return Table.from(source);
				case INIT_SINGLE_TABLE_TASK:
					TableTask task = Table.newTable(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.addReal("b", i -> SECOND_COLUMN[i])
							.addReal("c", i -> THIRD_COLUMN[i])
							.addReal("d", i -> FOURTH_COLUMN[i])
							.build();
					return new TableBuilder(task);
				case INIT_CHAINED_TABLE_TASKS:
					TableTask chained = Table.newTable(NUMBER_OF_ROWS)
							.addReal("a", i -> FIRST_COLUMN[i])
							.addReal("b", i -> SECOND_COLUMN[i])
							.build();
					chained = new TableBuilder(chained)
							.addReal("c", i -> THIRD_COLUMN[i])
							.addReal("d", i -> FOURTH_COLUMN[i])
							.build();
					return new TableBuilder(chained);
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
			Table.newTable(10)
					.add("a", new FixedRealBuffer(10).toColumn())
					.add("b", new FixedRealBuffer(42).toColumn());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeNumberOfRows() {
			Table.newTable(-11);
		}

		@Test
		public void testZeroColumnsAndRows() {
			Table table = Table.newTable(0).build(CTX);
			assertEquals(0, table.width());
			assertEquals(0, table.height());
		}

		@Test
		public void testZeroRowsOneColumn() {
			Table table = Table.newTable(0)
					.add("a", new FixedRealBuffer(0).toColumn())
					.build(CTX);
			assertEquals(1, table.width());
			assertEquals(0, table.height());
		}

		@Test
		public void testNoColumnsIsEmpty() {
			Table table = Table.newTable(NUMBER_OF_ROWS).build(CTX);
			assertEquals(0, table.width());
			assertEquals(0, table.height());
		}


		@Test
		public void testOneColumnFiller() {
			double[] single = random(NUMBER_OF_ROWS);

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("a", i -> single[i])
					.build(CTX);

			assertArrayEquals(new double[][]{single}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testOneColumnBuffer() {
			double[] single = random(NUMBER_OF_ROWS);

			ColumnBuffer buffer = new FixedRealBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < NUMBER_OF_ROWS; i++) {
				buffer.set(i, single[i]);
			}

			Table table = Table.newTable(NUMBER_OF_ROWS)
					.add("a", buffer.toColumn())
					.build(CTX);

			assertArrayEquals(new double[][]{single}, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}

		@Test
		public void testOneFromTable() { //TODO: not needed
			double[] single = random(NUMBER_OF_ROWS);

			Table fromTable = Table.newTable(NUMBER_OF_ROWS).addReal("a", i -> single[i]).build(CTX);

			Table table = Table.newTable(NUMBER_OF_ROWS).add("a", fromTable.column( 0)).build(CTX);

			assertArrayEquals(new double[][] { single }, readTableToArray(table));
			assertArrayEquals(new String[]{"a"}, table.labelArray());
		}


		@Test(expected = IllegalStateException.class)
		public void testOneColumnBufferChangedAfterwards() {
			ColumnBuffer buffer = new FixedRealBuffer(10);
			for (int i = 0; i < 10; i++) {
				buffer.set(i, i);
			}
			Table.newTable(buffer.size()).add("a", buffer.toColumn());
			buffer.set(5, 7);
		}

		@Test(expected = IllegalStateException.class)
		public void testReplaceColumnBufferChangedAfterwards() {
			ColumnBuffer buffer = new FixedRealBuffer(10);
			for (int i = 0; i < 10; i++) {
				buffer.set(i, i);
			}
			Table.newTable(10)
					.addReal("a", i -> i)
					.replace("a", buffer.toColumn());
			buffer.set(5, 7);
		}

		@Test(expected = TaskAbortedException.class)
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
				public <T> Future<T> submit(Callable<T> job) {
					if (active.get()) {
						active.set(false);
						return CTX.submit(job);
					} else {
						throw new RejectedExecutionException("You had your one shot");
					}
				}
			};

			Table.newTable(100)
					.addReal("a", i -> i)
					.addReal("b", i -> i)
					.addReal("c", i -> i)
					.build(oneShortCtx);
		}

		@Test(expected = TaskAbortedException.class)
		public void testAddColumnCancellation() {
			AtomicBoolean flag = new AtomicBoolean(true);
			Context flagContext = new BooleanContext(flag);

			Table table = Table.newTable(10).addReal("a", i -> i).build(CTX);
			TableTask wrappedTask = new TableTask(new String[]{"a"}, 10, (ctx, sentinel) -> {
				flag.set(false);
				return table;
			});

			new TableBuilder(wrappedTask).addReal("b", i -> i).build(flagContext);
		}


	}

	public static class NumericColumnTypes {

		@Test
		public void testAddInteger() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addInt("one", i -> single[i]).build(CTX);
			double[] expected = new double[single.length];
			Arrays.setAll(expected, i -> Math.round(single[i]));
			assertArrayEquals(new double[][]{expected}, readTableToArray(table));
			assertEquals(Column.TypeId.INTEGER, table.column(0).type().id());
		}

		@Test
		public void testAddIntBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addInt("one", i -> single[i]).build(CTX);
			ColumnBuffer buffer = new FixedIntegerBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, single[i]);
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToArray(table), readTableToArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddReal() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addInt("one", i -> single[i]).build(CTX);
			assertEquals(Column.TypeId.INTEGER, table.column(0).type().id());
		}

		@Test
		public void testAddRealBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addReal("one", i -> single[i]).build(CTX);
			ColumnBuffer buffer = new FixedRealBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, single[i]);
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToArray(table), readTableToArray(bufferTable));
			assertEquals(table.column(0).type().id(), bufferTable.column(0).type().id());
		}

	}

	public static class ToString {

		@Test
		public void testEmpty() {
			TableBuilder empty = Table.newTable(0);
			assertEquals("Table builder (0x0)\n", empty.toString());
		}

		@Test
		public void testEmptyWithRows() {
			TableBuilder empty = Table.newTable(5);
			assertEquals("Table builder (0x5)\n", empty.toString());
		}

		@Test
		public void testAddedColumnsWithRows() {
			TableBuilder builder = Table.newTable(5).addReal("a", i -> i).addReal("b", i -> 0);
			assertEquals("Table builder (2x5)\na | b", builder.toString());
		}

		@Test
		public void testMoreColumns() {
			TableBuilder builder = Table.newTable(3);
			for (int i = 0; i < 9; i++) {
				builder.addReal("c" + i, j -> j);
			}
			assertEquals("Table builder (9x3)\nc0 | c1 | c2 | c3 | c4 | c5 | ... | c8", builder.toString());
		}

		@Test
		public void testMaxColumns() {
			TableBuilder builder = Table.newTable(0);
			for (int i = 0; i < 8; i++) {
				builder.addReal("att" + i, j -> j);
			}
			assertEquals("Table builder (8x0)\natt0 | att1 | att2 | att3 | att4 | att5 | att6 | att7",
					builder.toString());
		}

	}

	public static class ObjectsExceptions {

		private static final ColumnType<String> FREE = ColumnTypes.freeType("com.rapidminer.test", String.class, null);

		private final TableBuilder builder = Table.newTable(123).addReal("a", i -> i);

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
		public void testCategoricalNullLabel() {
			builder.addCategorical(null, i -> "" + i, ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalUsedLabel() {
			builder.addCategorical("a", i -> "" + i, ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalInvalidLabel() {
			builder.addCategorical("", i -> "" + i, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testCategoricalNullGenerator() {
			builder.addCategorical("b", null, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testCategoricalNullType() {
			builder.addCategorical("b", i -> "" + i, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalWrongType() {
			builder.addCategorical("b", i -> "" + i, FREE);
		}

		@Test(expected = NullPointerException.class)
		public void testCategoricalMaxNullLabel() {
			builder.addCategorical(null, i -> "" + i, 1, ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalMaxUsedLabel() {
			builder.addCategorical("a", i -> "" + i, 1, ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalMaxInvalidLabel() {
			builder.addCategorical("", i -> "" + i, 1, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testCategoricalMaxNullGenerator() {
			builder.addCategorical("b", null, 1, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testCategoricalMaxNullType() {
			builder.addCategorical("b", i -> "" + i, 1, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalMaxWrongType() {
			builder.addCategorical("b", i -> "" + i, 1, FREE);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testCategoricalMaxWrongMax() {
			builder.addCategorical("b", i -> "" + i, 1, ColumnTypes.NOMINAL).build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testFreeNullLabel() {
			builder.addFree(null, i -> "" + i, FREE);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFreeUsedLabel() {
			builder.addFree("a", i -> "" + i, FREE);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFreeInvalidLabel() {
			builder.addFree("", i -> "" + i, FREE);
		}

		@Test(expected = NullPointerException.class)
		public void testFreeNullGenerator() {
			builder.addFree("b", null, FREE);
		}

		@Test(expected = NullPointerException.class)
		public void testFreeNullType() {
			builder.addFree("b", i -> "" + i, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testFreeWrongType() {
			builder.addFree("b", i -> "" + i, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testBooleanNullLabel() {
			builder.addBoolean(null, i -> "" + i, "1", ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanUsedLabel() {
			builder.addBoolean("a", i -> "" + i, "1", ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanInvalidLabel() {
			builder.addBoolean("", i -> "" + i, "1", ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testBooleanNullGenerator() {
			builder.addBoolean("b", null, "1", ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testBooleanNullType() {
			builder.addBoolean("b", i -> "" + i, "1", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanWrongType() {
			builder.addBoolean("b", i -> "" + i, "1",
					ColumnTypes.freeType("test", String.class, String::compareTo));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanTooManyValues() {
			builder.addBoolean("b", i -> "" + i, "1", ColumnTypes.NOMINAL).build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBooleanWrongPositiveValue() {
			builder.addBoolean("b", i -> "" + (i % 2), "5", ColumnTypes.NOMINAL).build(CTX);
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
		public void testReplaceCategoricalUnusedLabel() {
			builder.replaceCategorical("b", i -> "" + i, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceCategoricalNullGenerator() {
			builder.replaceCategorical("a", null, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceCategoricalNullType() {
			builder.replaceCategorical("a", i -> "" + i, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceCategoricalWrongType() {
			builder.replaceCategorical("a", i -> "" + i, FREE);
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
		public void testReplaceCategoricalMaxUnusedLabel() {
			builder.replaceCategorical("b", i -> "" + i, 1, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceCategoricalMaxNullGenerator() {
			builder.replaceCategorical("a", null, 1, ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceCategoricalMaxNullType() {
			builder.replaceCategorical("a", i -> "" + i, 1, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceCategoricalMaxWrongType() {
			builder.replaceCategorical("a", i -> "" + i, 1, FREE);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceCategoricalMaxWrongMax() {
			builder.replaceCategorical("a", i -> "" + i, 1, ColumnTypes.NOMINAL).build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceFreeUnusedLabel() {
			builder.replaceFree("b", i -> "" + i, FREE);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceFreeNullGenerator() {
			builder.replaceFree("a", null, FREE);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceFreeNullType() {
			builder.replaceFree("a", i -> "" + i, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceFreeWrongType() {
			builder.replaceFree("a", i -> "" + i, ColumnTypes.NOMINAL);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanUnusedLabel() {
			builder.replaceBoolean("b", i -> "" + i, "1", ColumnTypes.NOMINAL);
		}


		@Test(expected = NullPointerException.class)
		public void testReplaceBooleanNullGenerator() {
			builder.replaceBoolean("a", null, "1", ColumnTypes.NOMINAL);
		}

		@Test(expected = NullPointerException.class)
		public void testReplaceBooleanNullType() {
			builder.replaceBoolean("a", i -> "" + i, "1", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanWrongType() {
			builder.replaceBoolean("a", i -> "" + i, "1",
					ColumnTypes.freeType("test", String.class, String::compareTo));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanTooManyValues() {
			builder.replaceBoolean("a", i -> "" + i, "1", ColumnTypes.NOMINAL).build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReplaceBooleanWrongPositiveValue() {
			builder.replaceBoolean("a", i -> "" + (i % 2), "5", ColumnTypes.NOMINAL).build(CTX);
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

	public static class ObjectColumnTypes {

		private static final int NUMBER_OF_ROWS = 123;

		@Test
		public void testAddNominal() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> "" + Math.round(single[i]));
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
			assertEquals(ColumnTypes.NOMINAL, table.column(0).type());
		}

		@Test
		public void testAddNominalVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			Int32CategoricalBuffer<String> buffer = new Int32CategoricalBuffer<>(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, "" + Math.round(single[i]));
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn(ColumnTypes.NOMINAL)).build(CTX);
			assertArrayEquals(readTableToArray(bufferTable), readTableToArray(table));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddCategorical() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addCategorical("one", i -> single[i],
							ColumnTypes.categoricalType("test", Double.class, null)).build(CTX);
			assertEquals(Column.TypeId.CUSTOM, table.column(0).type().id());
			assertEquals(Column.Category.CATEGORICAL, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> single[i]);
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddCategoricalVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			ColumnType<Double> testType = ColumnTypes.categoricalType("test", Double.class, null);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addCategorical("one", i -> Math.rint(single[i]), testType).build(CTX);
			Int32CategoricalBuffer<Double> buffer = new Int32CategoricalBuffer<>(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, Math.rint(single[i]));
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn(testType)).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}


		@Test
		public void testAddFree() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addFree("one", i -> single[i],
							ColumnTypes.freeType("test", Double.class, null)).build(CTX);
			assertEquals(Column.TypeId.CUSTOM, table.column(0).type().id());
			assertEquals(Column.Category.FREE, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> single[i]);
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddFreeVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			ColumnType<Double> testType = ColumnTypes.freeType("test", Double.class, null);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addFree("one", i -> single[i], testType).build(CTX);
			FreeColumnBuffer<Double> buffer = new FreeColumnBuffer<>(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, single[i]);
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn(testType)).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddBoolean() {
			double[] single = random(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addBoolean("one", i -> single[i] > 0.5 ? Boolean.TRUE : Boolean.FALSE, Boolean.TRUE,
							ColumnTypes.categoricalType("test", Boolean.class, null)).build(CTX);
			assertEquals(Column.TypeId.CUSTOM, table.column(0).type().id());
			assertEquals(Column.Category.CATEGORICAL, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> single[i] > 0.5 ? Boolean.TRUE : Boolean.FALSE);
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddBooleanVsBuffer() {
			double[] single = random(NUMBER_OF_ROWS);
			ColumnType<Boolean> testType = ColumnTypes.categoricalType("test", Boolean.class, null);
			IntFunction<Boolean> lambda = i -> single[i] > 0.5 ? Boolean.TRUE : Boolean.FALSE;
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addBoolean("one", lambda, Boolean.TRUE, testType).build(CTX);
			UInt2CategoricalBuffer<Boolean> buffer = new UInt2CategoricalBuffer<>(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, lambda.apply(i));
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn(testType)).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}


		@Test
		public void testAddNominalVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			Table replaceTable = Table.newTable(NUMBER_OF_ROWS).addInt("one", i -> i)
					.replaceNominal("one", i -> "" + Math.round(single[i])).build(CTX);
			assertArrayEquals(readTableToArray(replaceTable), readTableToArray(table));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddCategoricalVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			ColumnType<Double> testType = ColumnTypes.categoricalType("test", Double.class, null);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addCategorical("one", i -> Math.rint(single[i]), testType).build(CTX);
			Table replaceTable = Table.newTable(NUMBER_OF_ROWS).addReal("one", i -> i)
					.replaceCategorical("one", i -> Math.rint(single[i]), testType).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}


		@Test
		public void testAddFreeVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			ColumnType<Double> testType = ColumnTypes.freeType("test", Double.class, null);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addFree("one", i -> single[i], testType).build(CTX);
			Table replaceTable = Table.newTable(NUMBER_OF_ROWS).addReal("one", i -> i)
					.replaceFree("one", i -> single[i], testType).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddBooleanVsReplace() {
			double[] single = random(NUMBER_OF_ROWS);
			ColumnType<Boolean> testType = ColumnTypes.categoricalType("test", Boolean.class, null);
			IntFunction<Boolean> lambda = i -> single[i] > 0.5 ? Boolean.TRUE : Boolean.FALSE;
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addBoolean("one", lambda, Boolean.TRUE, testType).build(CTX);
			Table replaceTable = Table.newTable(NUMBER_OF_ROWS).addInt("one", i->i)
					.replaceBoolean("one", lambda, Boolean.TRUE, testType).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddTime() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addTime("one", i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L))).build(CTX);
			assertEquals(Column.TypeId.TIME, table.column(0).type().id());
			assertEquals(Column.Category.FREE, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L)));
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddTimeVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<LocalTime> lambda = i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L));
			Table table = Table.newTable(NUMBER_OF_ROWS).addTime("one", lambda).build(CTX);
			TimeColumnBuffer buffer = new TimeColumnBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, lambda.apply(i));
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddTimeVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<LocalTime> lambda = i -> LocalTime.ofNanoOfDay(Math.round(single[i] * 1727999999999L));
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addTime("one", lambda).build(CTX);
			Table replaceTable = Table.newTable(NUMBER_OF_ROWS).addInt("one", i -> i)
					.replaceTime("one", lambda).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(replaceTable));
			assertEquals(table.column(0).type(), replaceTable.column(0).type());
		}

		@Test
		public void testAddDateTime() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<Instant> lambda =
					i -> Instant.ofEpochSecond(Math.round(single[i] * 631137797288063L), 123456789);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addDateTime("one", lambda).build(CTX);
			assertEquals(Column.TypeId.DATE_TIME, table.column(0).type().id());
			assertEquals(Column.Category.FREE, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, lambda);
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddDateTimeVsBuffer() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<Instant> lambda =
					i -> Instant.ofEpochSecond(Math.round(single[i] * 631137797288063L), 123456789);
			Table table = Table.newTable(NUMBER_OF_ROWS).addDateTime("one", lambda).build(CTX);
			HighPrecisionDateTimeBuffer buffer = new HighPrecisionDateTimeBuffer(NUMBER_OF_ROWS);
			for (int i = 0; i < single.length; i++) {
				buffer.set(i, lambda.apply(i));
			}
			Table bufferTable = Table.newTable(NUMBER_OF_ROWS)
					.add("one", buffer.toColumn()).build(CTX);
			assertArrayEquals(readTableToObjectArray(table), readTableToObjectArray(bufferTable));
			assertEquals(table.column(0).type(), bufferTable.column(0).type());
		}

		@Test
		public void testAddDateTimeVsReplace() {
			double[] single = randomStretched(NUMBER_OF_ROWS);
			IntFunction<Instant> lambda =
					i -> Instant.ofEpochSecond(Math.round(single[i] * 631137797288063L), 123456789);
			Table table = Table.newTable(NUMBER_OF_ROWS)
					.addDateTime("one", lambda).build(CTX);
			Table replaceTable = Table.newTable(NUMBER_OF_ROWS).addInt("one", i -> i)
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
			Table table = Table.newTable(numberOfRows)
					.addCategorical("one", i -> single[i], format.maxValue(),
							ColumnTypes.categoricalType("test", Double.class, null)).build(CTX);
			assertEquals(Column.TypeId.CUSTOM, table.column(0).type().id());
			assertEquals(Column.Category.CATEGORICAL, table.column(0).type().category());
			Object[] expected = new Object[single.length];
			Arrays.setAll(expected, i -> single[i]);
			assertArrayEquals(new Object[][]{expected}, readTableToObjectArray(table));
		}

		@Test
		public void testAddCategoricalMaxVsReplace() {
			int numberOfRows = Math.min(format.maxValue(), IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 1);
			double[] single = randomStretched(numberOfRows);
			ColumnType<Double> testType = ColumnTypes.categoricalType("test", Double.class, null);
			Table table = Table.newTable(numberOfRows)
					.addCategorical("one", i -> Math.rint(single[i]), format.maxValue(), testType).build(CTX);
			Table replaceTable = Table.newTable(numberOfRows).addReal("one", i -> i)
					.replaceCategorical("one", i -> Math.rint(single[i]), format.maxValue(), testType).build(CTX);
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

		@Test(expected = NullPointerException.class)
		public void testAddWithNullLabel() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.addMetaData(null, ColumnRole.METADATA)
					.build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testAddWithMissingLabel() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.addMetaData("does not exist", ColumnRole.METADATA)
					.build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testAddWithNullMetaData() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", (ColumnMetaData) null)
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddColumnUniqueDataTwice() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", ColumnRole.ID)
					.addMetaData("label", ColumnRole.LABEL)
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddTableUniqueDataTwice() {
			Table.newTable(100)
					.addReal("one", i -> i)
					.addReal("two", i -> i)
					.addMetaData("one", new TableUnique())
					.addMetaData("two", new TableUnique())
					.build(CTX);
		}

		@Test
		public void testAddTwoInstances() {
			Table table = Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", new ColumnAnnotation("Test #1"))
					.addMetaData("label", new ColumnAnnotation("Test #2"))
					.build(CTX);
			List<ColumnMetaData> list = table.getMetaData("label");
			List<ColumnMetaData> expected = Arrays.asList(new ColumnAnnotation("Test #1"),
					new ColumnAnnotation("Test #2"));
			assertEquals(new HashSet<>(expected), new HashSet<>(list));
		}

		@Test
		public void testAddDuplicate() {
			Table table = Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", new ColumnAnnotation("Test"))
					.addMetaData("label", new ColumnAnnotation("Test"))
					.build(CTX);
			List<ColumnMetaData> list = table.getMetaData("label");
			assertEquals(Collections.singletonList(new ColumnAnnotation("Test")), list);
		}

		@Test
		public void testAddingVariousData() {
			Table table = Table.newTable(100)
					.addInt("id", i -> i)
					.addInt("age", i -> 25)
					.addReal("weight", i -> 73.5)
					.addReal("mystery", i -> 0f)
					.addMetaData("id", ColumnRole.ID)
					.addMetaData("age", new ColumnAnnotation("years"))
					.addMetaData("mystery", ColumnRole.LABEL)
					.addMetaData("mystery", new ColumnAnnotation("unit?"))
					.addMetaData("mystery", new ColumnAnnotation("name?"))
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), table.getMetaData("id"));
			assertEquals(Collections.singletonList(new ColumnAnnotation("years")), table.getMetaData("age"));
			assertEquals(Collections.emptyList(), table.getMetaData("weight"));
			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.LABEL, new ColumnAnnotation("unit?"),
					new ColumnAnnotation("name?"));
			assertEquals(new HashSet<>(expected), new HashSet<>(table.getMetaData("mystery")));
		}

		@Test(expected = NullPointerException.class)
		public void testAddListWithNullLabel() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData(null, Collections.emptyList())
					.build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testAddListWithMissingLabel() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("does not exist", Collections.emptyList())
					.build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testAddListWithNullMetaDataList() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", (List<ColumnMetaData>) null)
					.build(CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testAddListWithNullItem() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", Arrays.asList(ColumnRole.ID, null, new ColumnAnnotation("Annotation")))
					.build(CTX);
		}

		@Test(expected = IllegalStateException.class)
		public void testAddListWithUniquenessViolation() {
			Table.newTable(100)
					.addReal("label", i -> i)
					.addMetaData("label", Arrays.asList(ColumnRole.ID, new ColumnAnnotation("Annotation"),
							ColumnRole.LABEL))
					.build(CTX);
		}

		@Test
		public void testAddingVariousDataViaList() {
			Table table = Table.newTable(100)
					.addInt("id", i -> i)
					.addReal("mystery", i -> 0f)
					.addMetaData("mystery", Arrays.asList(ColumnRole.LABEL, new ColumnAnnotation("unit?"),
							new ColumnAnnotation("name?")))
					.build(CTX);

			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.LABEL, new ColumnAnnotation("unit?"),
					new ColumnAnnotation("name?"));
			assertEquals(new HashSet<>(expected), new HashSet<>(table.getMetaData("mystery")));
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveTypeWithNullLabel() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.removeMetaData(null, ColumnRole.class);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testRemoveTypeWithMissingLabel() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.removeMetaData("does not exist", ColumnRole.class);
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveTypeWithNullType() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.removeMetaData("dummy", (Class<ColumnMetaData>) null);
		}

		@Test
		public void testRemoveTypeWithoutRemainder() {
			Table table = Table.newTable(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", new ColumnAnnotation("Annotation #1"))
					.addMetaData("two", new ColumnAnnotation("Annotation #2"))
					.addReal("three", i -> 3)
					.removeMetaData("two", ColumnAnnotation.class)
					.build(CTX);

			assertEquals(Collections.emptyList(), table.getMetaData("two"));
		}

		@Test
		public void testRemoveTypeWithRemainder() {
			Table table = Table.newTable(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new ColumnAnnotation("Annotation #1"))
					.addMetaData("two", new ColumnAnnotation("Annotation #2"))
					.addReal("three", i -> 3)
					.removeMetaData("two", ColumnAnnotation.class)
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), table.getMetaData("two"));
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveInstanceWithNullLabel() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.removeMetaData(null, ColumnRole.ID);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testRemoveInstanceWithMissingLabel() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.removeMetaData("does not exist", ColumnRole.ID);
		}

		@Test(expected = NullPointerException.class)
		public void testRemoveInstanceWithNullType() {
			Table.newTable(100)
					.addReal("dummy", i -> i)
					.removeMetaData("dummy", (ColumnMetaData) null);
		}

		@Test
		public void testRemoveInstanceWithoutRemainder() {
			Table table = Table.newTable(100)
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
			Table table = Table.newTable(100)
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
			Table table = Table.newTable(100)
					.addReal("dummy", i -> 1)
					.clearMetaData(null)
					.build(CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testClearMissingLabel() {
			Table table = Table.newTable(100)
					.addReal("dummy", i -> 1)
					.clearMetaData("does nto exist")
					.build(CTX);
		}

		@Test
		public void testRemoveEntireColumn() {
			Table table = Table.newTable(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new ColumnAnnotation("Annotation #1"))
					.addMetaData("two", new ColumnAnnotation("Annotation #2"))
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
			Table table = Table.newTable(100)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addMetaData("two", ColumnRole.ID)
					.addMetaData("two", new ColumnAnnotation("Annotation #1"))
					.addMetaData("two", new ColumnAnnotation("Annotation #2"))
					.addReal("three", i -> 3)
					.addMetaData("three", ColumnRole.BATCH)
					.clearMetaData("two")
					.build(CTX);

			assertEquals(Collections.emptyList(), table.getMetaData("two"));
			assertEquals(Collections.singletonList(ColumnRole.BATCH), table.getMetaData("three"));
		}

		@Test
		public void testRenameColumn() {
			Table table = Table.newTable(100)
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
			Table a = Table.newTable(100)
					.addInt("id", i -> i)
					.addInt("age", i -> 25)
					.addReal("weight", i -> 73.5)
					.addReal("mystery", i -> 0f)
					.addMetaData("id", ColumnRole.ID)
					.addMetaData("age", new ColumnAnnotation("years"))
					.addMetaData("mystery", ColumnRole.LABEL)
					.addMetaData("mystery", new ColumnAnnotation("unit?"))
					.addMetaData("mystery", new ColumnAnnotation("name?"))
					.build(CTX);

			Table b = Table.from(a)
					.addMetaData("id", new ColumnAnnotation("id"))
					.removeMetaData("mystery", ColumnAnnotation.class)
					.build(CTX);

			assertEquals(Collections.singletonList(ColumnRole.ID), a.getMetaData("id"));
			List<ColumnMetaData> expected = Arrays.asList(ColumnRole.LABEL, new ColumnAnnotation("unit?"),
					new ColumnAnnotation("name?"));
			assertEquals(new HashSet<>(expected), new HashSet<>(a.getMetaData("mystery")));

			expected = Arrays.asList(ColumnRole.ID, new ColumnAnnotation("id"));
			assertEquals(new HashSet<>(expected), new HashSet<>(b.getMetaData("id")));
			assertEquals(Collections.singletonList(ColumnRole.LABEL), b.getMetaData("mystery"));
		}

	}

}
