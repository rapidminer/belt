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

package com.rapidminer.belt.table;

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;
import static java.util.Arrays.setAll;
import static java.util.Arrays.sort;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.util.Order;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class TableSortingTests {

	private static final String API_SINGLE_ORDER = "single_order_api";
	private static final String API_ORDER_LIST = "multiple_orders_api";

	private static final Context CTX = Belt.defaultContext();

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.floor(50 * Math.random()));
		return data;
	}

	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		Column col = table.column(column);
		if (col.type().id() == Column.TypeId.DATE_TIME) {
			ObjectReader<Instant> reader = Readers.objectReader(col, Instant.class);
			for (int j = 0; j < table.height(); j++) {
				data[j] = reader.read().getEpochSecond();
			}
			return data;
		}
		NumericReader reader = Readers.numericReader(col);
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	private static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	@RunWith(Parameterized.class)
	public static class SingleOrder {

		@Parameterized.Parameter
		public String api;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(API_SINGLE_ORDER, API_ORDER_LIST);
		}

		private Table delegateSort(Table table, Order order, int... columns) {
			switch (api) {
				case API_SINGLE_ORDER:
					return table.sort(columns, order, CTX);
				case API_ORDER_LIST:
					Order[] orders = new Order[columns.length];
					fill(orders, order);
					return table.sort(columns, asList(orders), CTX);
				default:
					throw new IllegalStateException("Unknown sorting API");
			}
		}

		@Test
		public void testSortingOfZeroColumnTable() {
			double[] single = random(250);

			Table table = Builders.newTableBuilder(single.length)
					.addReal("a", i -> single[i]).build(CTX);

			Table sorted = delegateSort(table, Order.DESCENDING);

			assertArrayEquals(new double[][]{single}, readTableToArray(table));
			assertArrayEquals(new double[][]{single}, readTableToArray(sorted));
		}

		@Test
		public void testSortingOfSingleColumnTable() {
			double[] single = random(100);
			double[] expected = copyOf(single, single.length);
			sort(expected);

			Table table = Builders.newTableBuilder(single.length)
					.addReal("a", i -> single[i])
					.build(CTX);

			Table sorted = delegateSort(table, Order.ASCENDING, 0);

			assertArrayEquals(new double[][]{single}, readTableToArray(table));
			assertArrayEquals(new double[][]{expected}, readTableToArray(sorted));
		}

		@Test
		public void testSortingBySingleColumn() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {0, 1, 2, 3, 0, 1, 2, 3};

			double[] expectedFirst = {3, 7, 2, 6, 1, 5, 0, 4};
			double[] expectedSecond = {3, 3, 2, 2, 1, 1, 0, 0};

			Table table = Builders.newTableBuilder(8)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> first[i])
					.build(CTX);

			Table sorted = delegateSort(table, Order.DESCENDING, 1);

			assertArrayEquals(new double[][]{first, second, first}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedFirst}, readTableToArray(sorted));
		}

		@Test
		public void testSortingByMultipleColumns() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {0, 1, 0, 1, 0, 1, 0, 1};
			double[] third = {4, 2, 3, 1, 2, 3, 4, 4};

			double[] expectedFirst = {4, 2, 0, 6, 3, 1, 5, 7};
			double[] expectedSecond = {0, 0, 0, 0, 1, 1, 1, 1};
			double[] expectedThird = {2, 3, 4, 4, 1, 2, 3, 4};

			Table table = Builders.newTableBuilder(8)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table sorted = delegateSort(table, Order.ASCENDING, 1, 2);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(sorted));
		}

		@Test
		public void testSortingIdempotence() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {0, 1, 2, 3, 0, 1, 2, 3};

			double[] expectedFirst = {3, 7, 2, 6, 1, 5, 0, 4};
			double[] expectedSecond = {3, 3, 2, 2, 1, 1, 0, 0};

			Table table = Builders.newTableBuilder(8)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> first[i])
					.build(CTX);

			Table sorted = delegateSort(table, Order.DESCENDING, 1, 1, 1);

			assertArrayEquals(new double[][]{first, second, first}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedFirst}, readTableToArray(sorted));
		}

	}

	public static class MultipleOrders {

		@Test
		public void testSortingByMultipleColumns() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {0, 1, 0, 1, 0, 1, 0, 1};
			double[] third = {4, 2, 3, 1, 2, 3, 4, 4};

			double[] expectedFirst = {0, 6, 7, 2, 5, 4, 1, 3};
			double[] expectedSecond = {0, 0, 1, 0, 1, 0, 1, 1};
			double[] expectedThird = {4, 4, 4, 3, 3, 2, 2, 1};

			Table table = Builders.newTableBuilder(8)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> third[i])
					.build(CTX);

			Table sorted = table.sort(Arrays.asList("c", "b"), asList(Order.DESCENDING, Order.ASCENDING), CTX);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(sorted));
		}

		@Test
		public void testSortingRepeatedlyBySingleColumn() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {0, 1, 2, 3, 0, 1, 2, 3};

			double[] expectedFirst = {3, 7, 2, 6, 1, 5, 0, 4};
			double[] expectedSecond = {3, 3, 2, 2, 1, 1, 0, 0};

			Table table = Builders.newTableBuilder(8)
					.addReal("a", i -> first[i])
					.addReal("b", i -> second[i])
					.addReal("c", i -> first[i])
					.build(CTX);

			Table sorted = table.sort(Arrays.asList("b", "b", "b"),
					asList(Order.DESCENDING, Order.ASCENDING, Order.DESCENDING),
					CTX);

			assertArrayEquals(new double[][]{first, second, first}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedFirst}, readTableToArray(sorted));
		}

	}


	public static class DifferentTypes {

		@Test
		public void testSortingByMultipleColumns() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {1, 2, 1, 2, 1, 2, 1, 2};
			double[] third = {4, 2, 3, 1, 2, 3, 4, 4};

			double[] expectedFirst = {0, 6, 7, 2, 5, 4, 1, 3};
			double[] expectedSecond = {1, 1, 2, 1, 2, 1, 2, 2};
			double[] expectedThird = {4, 4, 4, 3, 3, 2, 2, 1};

			Table table = Builders.newTableBuilder(8)
					.addReal("a", i -> first[i])
					.addNominal("b", i -> "val" + second[i])
					.addDateTime("c", i -> Instant.ofEpochSecond((long) third[i]))
					.build(CTX);

			Table sorted = table.sort(Arrays.asList("c", "b"), asList(Order.DESCENDING, Order.ASCENDING), CTX);

			assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(sorted));
		}

		@Test
		public void testSortingRepeatedlyBySingleColumn() {
			double[] first = {0, 1, 2, 3, 4, 5, 6, 7};
			double[] second = {0, 1, 2, 3, 0, 1, 2, 3};

			double[] expectedFirst = {3, 7, 2, 6, 1, 5, 0, 4};
			double[] expectedSecond = {3, 3, 2, 2, 1, 1, 0, 0};

			Table table = Builders.newTableBuilder(8)
					.addDateTime("a", i -> Instant.ofEpochSecond((long) first[i]))
					.addDateTime("b", i -> Instant.ofEpochSecond((long) second[i]))
					.addDateTime("c", i -> Instant.ofEpochSecond((long) first[i]))
					.build(CTX);

			Table sorted = table.sort(Arrays.asList("b", "b", "b"),
					asList(Order.DESCENDING, Order.ASCENDING, Order.DESCENDING),
					CTX);

			assertArrayEquals(new double[][]{first, second, first}, readTableToArray(table));
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedFirst}, readTableToArray(sorted));
		}

		@Test
		public void testStandardTypesAreSortable() {
			for (ColumnType type : new ColumnType[]{ColumnTypes.REAL, ColumnTypes.INTEGER, ColumnTypes.NOMINAL,
					ColumnTypes.TIME, ColumnTypes.DATETIME}) {
				assertTrue(type.hasCapability(Column.Capability.SORTABLE));
			}
		}

	}

	public static class SingleOrderApiInputValidation {

		@Test(expected = NullPointerException.class)
		public void testNullIndices() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort((int[]) null, Order.ASCENDING, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOrder() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort("a", (Order) null, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testOutOfBoundsColumn() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort(5, Order.ASCENDING, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeColumn() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort(-1, Order.ASCENDING,CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabel() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort((String) null, Order.DESCENDING, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidLabel() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort("x", Order.ASCENDING, CTX);
		}

	}

	public static class MultipleOrdersApiInputValidation {

		@Test(expected = NullPointerException.class)
		public void testNullOrderList() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort(new int[]{0}, (List<Order>) null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullIndices() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort((int[]) null, singletonList(Order.ASCENDING), CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testOutOfBoundsColumn() {
			Table table = Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort(new int[]{5}, singletonList(Order.ASCENDING), CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeColumn() {
			Table table =  Builders.newTableBuilder(10).addReal("a", i -> i).build(CTX);
			table.sort(new int[]{-1}, singletonList(Order.ASCENDING), CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchingOrderList() {
			Table table = Builders.newTableBuilder(10)
					.addReal("a", i -> i)
					.addReal("b", i -> i)
					.addReal("c", i -> i)
					.build(CTX);
			table.sort(new int[]{0, 1, 2}, singletonList(Order.ASCENDING), CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOrders() {
			Table table = Builders.newTableBuilder(10)
					.addReal("a", i -> i)
					.addReal("b", i -> i)
					.addReal("c", i -> i)
					.build(CTX);
			table.sort(new int[]{0, 1, 2}, asList(Order.ASCENDING, null, null), CTX);
		}

	}

}
