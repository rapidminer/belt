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

package com.rapidminer.belt.transform;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.buffer.Int32NominalBuffer;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.Workload;


/**
 * Tests {@link CategoricalColumnReducer},  {@link CategoricalColumnReducerInt}  and {@link CategoricalColumnsReducer}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class CategoricalColumnReducerTests {

	private static final double EPSILON = 1e-10;

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	private static final Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
			.add("a", getNominal())
			.add("b", getNominal())
			.build(CTX);

	private static Column getNominal() {
		String[] data = new String[NUMBER_OF_ROWS];
		Arrays.setAll(data, i -> "value" + (i % 10));
		return getColumn(data);
	}

	private static CategoricalColumn getColumn(String[] data) {
		Int32NominalBuffer buffer = BufferAccessor.get().newInt32Buffer(ColumnType.NOMINAL, data.length);
		for (int i = 0; i < data.length; i++) {
			buffer.set(i, data[i]);
		}
		return buffer.toColumn();
	}

	private static String[] random(int n) {
		String[] data = new String[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(10) + "");
		return data;
	}

	public static class InputValidationOneColumn {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a").reduceCategorical(ArrayList::new, ArrayList::add, ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a").reduceCategorical((Supplier<ArrayList<Integer>>) null, ArrayList::add,
					ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a").reduceCategorical(ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a").reduceCategorical(ArrayList::new, ArrayList::add, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a").reduceCategorical((Supplier<ArrayList<Integer>>) (() -> null), ArrayList::add,
					ArrayList::addAll, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedColumn() {
			Table table2 = Builders.newTableBuilder(table).add("x", ColumnTestUtils.getObjectColumn(
					ColumnType.TEXT, new Object[table.height()])).build(CTX);
			table2.transform(2).reduceCategorical(ArrayList::new, ArrayList::add, ArrayList::addAll, CTX);
		}

	}

	public static class InputValidationOneColumnInt {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a").reduceCategorical(0, Integer::sum, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a").reduceCategorical(0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextCombiner() {
			table.transform("a").reduceCategorical(0, Integer::sum, Integer::sum, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducerCombiner() {
			table.transform("a").reduceCategorical(0, null, Integer::sum, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a").reduceCategorical(0, Integer::sum, null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedColumn() {
			Table table2 = Builders.newTableBuilder(table).add("x", ColumnTestUtils.getObjectColumn(
					ColumnType.TEXT,new Object[table.height()])).build(CTX);
			table2.transform(2).reduceCategorical(0, Integer::sum, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedColumnCombiner() {
			Table table2 = Builders.newTableBuilder(table).add("x", ColumnTestUtils.getObjectColumn(
					ColumnType.TEXT,new Object[table.height()])).build(CTX);
			table2.transform(2).reduceCategorical(0, Integer::sum, Integer::sum, CTX);
		}

	}

	public static class InputValidationMoreColumns {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").reduceCategorical(ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)),
					ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a", "b").reduceCategorical((Supplier<ArrayList<Integer>>) null,
					(t, r) -> t.add(r.get(0) + r.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a", "b").reduceCategorical(ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a", "b").reduceCategorical(ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)), null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a", "b").reduceCategorical((Supplier<ArrayList<Integer>>) (() -> null), (t, r) -> t.add(r
					.get(0) + r.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedColumn() {
			Table table2 = Builders.newTableBuilder(table).add("x", ColumnTestUtils.getObjectColumn(
					ColumnType.TEXT, new Object[table.height()])).build(CTX);
			table2.transform(new int[]{0, 2}).reduceCategorical(ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)),
					ArrayList::addAll, CTX);
		}

	}

	public static class Parts {


		@Test
		public void testOneColumn() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);
			int mappingSize = column.getDictionary().size()+1;
			CategoricalColumnReducer<int[]> calculator = new CategoricalColumnReducer<>(column, () -> new int[mappingSize],
					(t, d) -> t[d] += 1,
					(t1, t2) -> {
						for (int i = 0; i < t1.length; i++) {
							t1[i] += t2[i];
						}
					});
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected =
					Arrays.stream(ColumnTestUtils.getIntData(column)).skip(start).limit(end - start).collect(() -> new int[mappingSize],
					(t, d) -> t[d] += 1,
					(t1, t2) -> {
						for (int i = 0; i < t1.length; i++) {
							t1[i] += t2[i];
						}
					});

			assertArrayEquals(expected, result);
		}

		@Test
		public void testOneColumnInt() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);
			CategoricalColumnReducerInt calculator = new CategoricalColumnReducerInt(column, 0,
					(d, e) -> d + e + d * e);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int result = calculator.getResult();

			int expected = Arrays.stream(ColumnTestUtils.getIntData(column)).skip(start).limit(end - start).reduce(0,
					(d, e) -> d + e + d * e);

			assertEquals(expected, result);
		}

		@Test
		public void testOneColumnCombiner() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);
			CategoricalColumnReducerInt calculator = new CategoricalColumnReducerInt(column, 0,
					(count, d) -> d > 2 ? count + 1 : count, (count1, count2) -> count1 + count2);
			calculator.init(2);
			int start = 10;
			int mid = 20;
			int end = 30;
			calculator.doPart(start, mid, 0);
			calculator.doPart(mid, end, 1);
			double result = calculator.getResult();

			double expected = Arrays.stream(ColumnTestUtils.getIntData(column)).skip(start).limit(end - start)
					.reduce(0, (count, d) -> d > 2 ? count + 1 : count);
			assertEquals(expected, result, EPSILON);
		}


		@Test
		public void testTwoColumns() {
			int size = 75;
			String[] data = random(size);
			String[] data2 = random(size);
			CategoricalColumn column = getColumn(data);
			CategoricalColumn column2 = getColumn(data2);
			NumericColumnsReducer<int[]> calculator = new NumericColumnsReducer<>(Arrays.asList(column,
					column2), () -> new int[2],
					(t, row) -> {
						t[0] += row.get(1) + row.get(0) * 2;
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] both = new int[data.length];
			Arrays.setAll(both, i -> ColumnTestUtils.getIntData(column)[i] * 2 + ColumnTestUtils.getIntData(column2)[i]);
			int[] expected = Arrays.stream(both).skip(start).limit(end - start).collect(() -> new int[2],
					(t, d) -> {
						t[0] += d;
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});

			assertArrayEquals(expected, result);
		}

		@Test
		public void testThreeColumns() {
			int size = 75;
			String[] data = random(size);
			String[] data2 = random(size);
			String[] data3 = random(size);
			CategoricalColumn column = getColumn(data);
			CategoricalColumn column2 = getColumn(data2);
			CategoricalColumn column3 = getColumn(data3);
			NumericColumnsReducer<int[]> calculator = new NumericColumnsReducer<>(Arrays.asList(column,
					column2, column3), () -> new int[2],
					(t, row) -> {
						t[0] += row.get(1) * row.get(0) * row.get(2);
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] both = new int[data.length];
			Arrays.setAll(both,
					i -> ColumnTestUtils.getIntData(column)[i] * ColumnTestUtils.getIntData(column2)[i] * ColumnTestUtils.getIntData(column3)[i]);
			int[] expected = Arrays.stream(both).skip(start).limit(end - start).collect(() -> new int[2],
					(t, d) -> {
						t[0] += d;
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});

			assertArrayEquals(expected, result);
		}


	}


	public static class Whole {

		private static class MutableDouble {
			double value = 0;
		}

		@Test
		public void testOneColumn() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);

			Transformer transformer = new Transformer(column);
			MutableDouble result = transformer.workload(Workload.LARGE)
					.reduceCategorical(MutableDouble::new, (r, v) -> r.value += v, (l, r) -> l.value += r.value, CTX);

			int expected = Arrays.stream(ColumnTestUtils.getIntData(column)).reduce(0, (d, e) -> d + e);

			assertEquals(expected, result.value, EPSILON);
		}

		@Test
		public void testOneColumnInt() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);

			Transformer transformer = new Transformer(column);
			int result = transformer.workload(Workload.LARGE)
				.reduceCategorical(0, (x, y) -> x + y, CTX);

			int expected = Arrays.stream(ColumnTestUtils.getIntData(column)).reduce(0, (d, e) -> d + e);

			assertEquals(expected, result);
		}

		@Test
		public void testOneColumnCombiner() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);

			Transformer transformer = new Transformer(column);
			int result = transformer.reduceCategorical(0, (count, d) -> d > 2 ? count + 1 : count,
					(count1, count2) -> count1 + count2, CTX);

			int expected = Arrays.stream(ColumnTestUtils.getIntData(column)).reduce(0, (count, d) -> d > 2 ? count + 1 : count);

			assertEquals(expected, result);
		}

		@Test
		public void testThreeColumns() {
			int size = 75;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);
			String[] data2 = random(size);
			CategoricalColumn column2 = getColumn(data2);
			String[] data3 = random(size);
			CategoricalColumn column3 = getColumn(data3);

			RowTransformer transformer = new RowTransformer(Arrays.asList(column, column2, column3));
			MutableDouble result = transformer.reduceCategorical(MutableDouble::new,
					(d, row) -> d.value += (row.get(0) + row.get(1) + row.get(2)),
					(l, r) -> l.value += r.value, CTX);

			double[] sum = new double[data.length];
			Arrays.setAll(sum,
					i -> ColumnTestUtils.getIntData(column)[i] + ColumnTestUtils.getIntData(column2)[i] + ColumnTestUtils.getIntData(column3)[i]);
			double expected = Arrays.stream(sum).reduce(0, (a, b) -> a + b);
			assertEquals(expected, result.value, EPSILON);
		}

		@Test
		public void testOneColumnZeroHeight() {
			int size = 0;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);

			Transformer transformer = new Transformer(column);
			MutableDouble result = transformer.workload(Workload.SMALL)
					.reduceCategorical(() -> {
						MutableDouble d = new MutableDouble();
						d.value = 1;
						return d;
					}, (r, v) -> r.value *= v, (l, r) -> l.value *= r.value, CTX);

			assertEquals(1, result.value, EPSILON);
		}

		@Test
		public void testOneColumnIntZeroHeight() {
			int size = 0;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);

			Transformer transformer = new Transformer(column);
			int result = transformer.workload(Workload.LARGE)
					.reduceCategorical(1, (x, y) -> x*y, CTX);

			assertEquals(1, result);
		}

		@Test
		public void testThreeColumnsZeroHeight() {
			int size = 0;
			String[] data = random(size);
			CategoricalColumn column = getColumn(data);

			RowTransformer transformer = new RowTransformer(Arrays.asList(column, column, column));
			MutableDouble result = transformer.reduceCategorical(() -> {
						MutableDouble d = new MutableDouble();
						d.value = 1;
						return d;
					},
					(d, row) -> d.value *= (row.get(0) + row.get(1) + row.get(2)),
					(l, r) -> l.value *= r.value, CTX);

			assertEquals(1, result.value, EPSILON);
		}

	}
}
