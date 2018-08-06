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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.TaskAbortedException;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class TableTaskTests {

	private static Context CTX = Belt.defaultContext();

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

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

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

	@RunWith(Parameterized.class)
	public static class ApiComparison {

		@Parameter
		public ApiTest test;

		@Parameters(name = "{0}")
		public static Iterable<ApiTest> tests() {
			List<ApiTest> tests = new ArrayList<>(5);

			Table input = Table.newTable(100)
					.addReal("a", i -> Math.random())
					.addReal("b", i -> Math.random())
					.addReal("c", i -> Math.random())
					.build(CTX);

			tests.add(new ApiTest("sorting_constant",
					input.sort(Arrays.asList("a", "c"), Order.ASCENDING),
					input.sort(Arrays.asList("a", "c"), Order.ASCENDING, CTX),
					3,
					100));

			tests.add(new ApiTest("sorting_varying",
					input.sort(Arrays.asList("a", "c"), Arrays.asList(Order.DESCENDING, Order.ASCENDING)),
					input.sort(Arrays.asList("a", "c"), Arrays.asList(Order.DESCENDING, Order.ASCENDING), CTX),
					3,
					100));

			tests.add(new ApiTest("select_rows",
					input.rows(new int[]{0, 1, 2, 10, 11, 12}),
					input.rows(new int[]{0, 1, 2, 10, 11, 12}, CTX),
					3,
					6));

			tests.add(new ApiTest("select_range",
					input.rows(10, 77),
					input.rows(10, 77, CTX),
					3,
					67));

			tests.add(new ApiTest("add_column",
					Table.from(input).addReal("d", i -> i).build(),
					Table.from(input).addReal("d", i -> i).build(CTX),
					4,
					100));

			return tests;
		}

		@Test
		public void testTableTaskSize() {
			assertEquals(test.width, test.task.width());
			assertEquals(test.height, test.task.height());
		}

		@Test
		public void testLabels() {
			assertEquals(test.task.labels(), test.table.labels());
		}

		@Test
		public void testTableTaskResult() {
			Table table = test.task.run(CTX);
			assertEquals(test.width, table.width());
			assertEquals(test.height, table.height());
			assertArrayEquals(readTableToArray(test.table), readTableToArray(table));
			assertArrayEquals(test.table.labelArray(), table.labelArray());
		}

		@Test
		public void testContextInteraction() {
			Context ctx = spy(CTX);
			test.task.run(ctx);
			verify(ctx, atLeast(1)).isActive();
			verify(ctx, times(1)).submit(any());
		}

		public static class ApiTest {
			final String name;
			final TableTask task;
			final Table table;
			final int width;
			final int height;

			private ApiTest(String name, TableTask task, Table table, int width, int height) {
				this.name = name;
				this.task = task;
				this.table = table;
				this.width = width;
				this.height = height;
			}

			@Override
			public String toString() {
				return name;
			}
		}

	}


	@RunWith(Parameterized.class)
	public static class ChainingTests {

		@Parameter
		public ApiComparison.ApiTest test;

		@Parameters(name = "{0}")
		public static Iterable<ApiComparison.ApiTest> tests() {
			List<ApiComparison.ApiTest> tests = new ArrayList<>(4);

			double[] firstColumn = random(100);
			double[] secondColumn = random(100);
			double[] thirdColumn = random(100);

			TableTask input = Table.newTable(100)
					.addReal("a", i -> firstColumn[i])
					.addReal("b", i -> secondColumn[i])
					.addReal("c", i -> thirdColumn[i])
					.build();

			tests.add(new ApiComparison.ApiTest("sorting_one",
					input.sort("a", Order.ASCENDING),
					input.run(CTX).sort("a", Order.ASCENDING, CTX),
					3,
					100));

			tests.add(new ApiComparison.ApiTest("sorting_one_index",
					input.sort(0, Order.ASCENDING),
					input.run(CTX).sort(0, Order.ASCENDING, CTX),
					3,
					100));

			tests.add(new ApiComparison.ApiTest("sorting_constant",
					input.sort(Arrays.asList("a", "c"), Order.ASCENDING),
					input.run(CTX).sort(Arrays.asList("a", "c"), Order.ASCENDING, CTX),
					3,
					100));

			tests.add(new ApiComparison.ApiTest("sorting_constant_indices",
					input.sort(new int[]{2, 0}, Order.ASCENDING),
					input.run(CTX).sort(new int[]{2, 0}, Order.ASCENDING, CTX),
					3,
					100));

			tests.add(new ApiComparison.ApiTest("sorting_varying",
					input.sort(Arrays.asList("a", "c"), Arrays.asList(Order.DESCENDING, Order.ASCENDING)),
					input.run(CTX).sort(Arrays.asList("a", "c"), Arrays.asList(Order.DESCENDING, Order.ASCENDING), CTX),
					3,
					100));

			tests.add(new ApiComparison.ApiTest("sorting_varying_indices",
					input.sort(new int[]{2, 0}, Arrays.asList(Order.DESCENDING, Order.ASCENDING)),
					input.run(CTX).sort(new int[]{2, 0}, Arrays.asList(Order.DESCENDING, Order.ASCENDING), CTX),
					3,
					100));

			tests.add(new ApiComparison.ApiTest("select_rows",
					input.rows(new int[]{0, 1, 2, 10, 11, 12}),
					input.run(CTX).rows(new int[]{0, 1, 2, 10, 11, 12}, CTX),
					3,
					6));

			tests.add(new ApiComparison.ApiTest("select_range",
					input.rows(10, 77),
					input.run(CTX).rows(10, 77, CTX),
					3,
					67));

			tests.add(new ApiComparison.ApiTest("add_column",
					Table.from(input).addReal("d", i -> i).build(),
					Table.from(input.run(CTX)).addReal("d", i -> i).build(CTX),
					4,
					100));

			tests.add(new ApiComparison.ApiTest("select_columns",
					input.columns(Arrays.asList("c", "a")),
					input.run(CTX).columns(Arrays.asList("c", "a")),
					2,
					100));

			tests.add(new ApiComparison.ApiTest("select_columns_indices",
					input.columns(new int[]{2, 0}),
					input.run(CTX).columns(new int[]{2, 0}),
					2,
					100));
			return tests;
		}

		@Test
		public void testTableTaskSize() {
			assertEquals(test.width, test.task.width());
			assertEquals(test.height, test.task.height());
		}

		@Test
		public void testLabels() {
			assertEquals(test.task.labels(), test.table.labels());
		}

		@Test
		public void testTableTaskResult() {
			Table table = test.task.run(CTX);
			assertEquals(test.width, table.width());
			assertEquals(test.height, table.height());
			assertArrayEquals(readTableToArray(test.table), readTableToArray(table));
			assertArrayEquals(test.table.labelArray(), table.labelArray());
		}

		@Test
		public void testContextInteraction() {
			Context ctx = spy(CTX);
			test.task.run(ctx);
			verify(ctx, atLeast(1)).isActive();
			verify(ctx, times(1)).submit(any());
		}
	}


	@RunWith(Parameterized.class)
	public static class StoppingTests {

		@Parameter
		public TaskContainer test;

		@Parameters(name = "{0}")
		public static Iterable<TaskContainer> tests() {
			List<TaskContainer> tests = new ArrayList<>(4);

			Table table = Table.newTable(100)
					.addReal("a", i -> Math.random())
					.addReal("b", i -> Math.random())
					.addReal("c", i -> Math.random())
					.build(CTX);

			TableTask input = new TableTask(table.labelArray(), 100, (ctx, sentinel) -> table);

			tests.add(new TaskContainer("sorting_one", input.sort("a", Order.ASCENDING)));

			tests.add(new TaskContainer("sorting_one_index", input.sort(0, Order.ASCENDING)));

			tests.add(new TaskContainer("sorting_constant", input.sort(Arrays.asList("a", "c"), Order.ASCENDING)));

			tests.add(new TaskContainer("sorting_constant_indices",
					input.sort(new int[]{2, 0}, Order.ASCENDING)));

			tests.add(new TaskContainer("sorting_varying",
					input.sort(Arrays.asList("a", "c"), Arrays.asList(Order.DESCENDING, Order.ASCENDING))));

			tests.add(new TaskContainer("sorting_varying_indices",
					input.sort(new int[]{2, 0}, Arrays.asList(Order.DESCENDING, Order.ASCENDING))));

			tests.add(new TaskContainer("select_rows",
					input.rows(new int[]{0, 1, 2, 10, 11, 12})));

			tests.add(new TaskContainer("select_range",
					input.rows(10, 77)));

			tests.add(new TaskContainer("select_columns",
					input.columns(Arrays.asList("c", "a"))));

			tests.add(new TaskContainer("select_columns_indices",
					input.columns(new int[]{2, 0})));
			return tests;
		}

		@Test(expected = TaskAbortedException.class)
		public void testContextStopping() {
			Context ctx = new OneShotContext();
			test.task.run(ctx);
		}

		private static class TaskContainer {
			final String name;
			final TableTask task;


			private TaskContainer(String name, TableTask task) {
				this.name = name;
				this.task = task;
			}

			@Override
			public String toString() {
				return name;
			}
		}
	}

	public static class InputValidation {

		private TableTask task() {
			Table table = Table.newTable(100)
					.addReal("a", i -> Math.random())
					.addReal("b", i -> Math.random())
					.addReal("c", i -> Math.random())
					.build(CTX);

			return new TableTask(table.labelArray(), 100, (ctx, sentinel) -> table);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testColumnsNegativeIndex() {
			task().columns(new int[]{1, -1});
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testColumnsWrongIndex() {
			task().columns(new int[]{1, 5});
		}

		@Test(expected = IllegalArgumentException.class)
		public void testRowsNegativeLength() {
			task().rows(10, 2);
		}

	}


	public static class ToString {

		@Test
		public void testSimple() {
			int numberOfRows = 100;
			TableTask task = Table.newTable(numberOfRows)
					.addReal("first", i -> Math.random())
					.addReal("second", i -> Math.random())
					.addReal("third", i -> Math.random())
					.build();
			String expected = "Table task (" + 3 + "x" + numberOfRows + ")\nfirst | second | third";
			assertEquals(expected, task.toString());
		}

		@Test
		public void testEmpty() {
			int numberOfRows = 100;
			TableTask task = Table.newTable(numberOfRows)
					.build();
			String expected = "Table task (" + 0 + "x" + numberOfRows + ")\n";
			assertEquals(expected, task.toString());
		}

		@Test
		public void testMoreColumns() {
			int numberOfRows = 99;
			TableBuilder builder = Table.newTable(numberOfRows);
			for (int i = 0; i < 9; i++) {
				builder.addReal("attribute" + (i + 1), j -> j);
			}
			TableTask task = builder.build();
			String expected = "Table task (" + 9 + "x" + numberOfRows + ")\n" +
					"attribute1 | attribute2 | attribute3 | attribute4 | attribute5 | attribute6 | ... | attribute9";
			assertEquals(expected, task.toString());
		}

		@Test
		public void testColumnsMaxFit() {
			int numberOfRows = 99;
			TableBuilder builder = Table.newTable(numberOfRows);
			for (int i = 0; i < 8; i++) {
				builder.addReal("attribute" + (i + 1), j -> j);
			}
			TableTask task = builder.build();
			String expected = "Table task (" + 8 + "x" + numberOfRows + ")\n" +
					"attribute1 | attribute2 | attribute3 | attribute4 | attribute5 | attribute6 | attribute7 | attribute8";
			assertEquals(expected, task.toString());
		}

	}
}
