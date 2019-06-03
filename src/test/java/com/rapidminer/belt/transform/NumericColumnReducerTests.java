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

package com.rapidminer.belt.transform;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.function.IntToDoubleFunction;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.execution.Context;


/**
 * Tests {@link NumericColumnReducer}, {@link NumericColumnReducerDouble} and {@link NumericColumnsReducer}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class NumericColumnReducerTests {

	private static final double EPSILON = 1e-10;

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	/**
	 * Use anonymous classes instead of lambdas here: lambdas in parallel before outer class is initialized cause
	 * deadlocks!
	 */
	@SuppressWarnings("Convert2Lambda")
	private static final Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
			.addReal("a", new IntToDoubleFunction() {
						@Override
						public double applyAsDouble(int value) {
							return value;
						}
					}
			)
			.addReal("b", new IntToDoubleFunction() {
						@Override
						public double applyAsDouble(int value) {
							return 2 * value;
						}
					}
			)
			.build(CTX);


	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	public static class InputValidationOneColumn {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a").reduceNumeric(ArrayList::new, ArrayList::add, ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a").reduceNumeric((Supplier<ArrayList<Double>>) null, ArrayList::add, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a").reduceNumeric(ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a").reduceNumeric(ArrayList::new, ArrayList::add, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a").reduceNumeric((Supplier<ArrayList<Double>>) (() -> null), ArrayList::add,
					ArrayList::addAll, CTX);
		}

	}

	public static class InputValidationOneColumnDouble {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a").reduceNumeric(0, Double::sum, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a").reduceNumeric(0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextCombiner() {
			table.transform("a").reduceNumeric(0, Double::sum, Double::sum, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducerCombiner() {
			table.transform("a").reduceNumeric(0, null, Double::sum, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a").reduceNumeric(0, Double::sum, null, CTX);
		}

	}

	public static class InputValidationMoreColumns {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").reduceNumeric(ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)),
					ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a", "b").reduceNumeric((Supplier<ArrayList<Double>>) null, (t, r) -> t.add(r.get(0) + r
					.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a", "b").reduceNumeric(ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a", "b").reduceNumeric(ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)), null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a", "b").reduceNumeric((Supplier<ArrayList<Double>>) (() -> null), (t, r) -> t.add(r.get
					(0) + r.get(1)), ArrayList::addAll, CTX);
		}

	}

	public static class Parts {


		@Test
		public void testOneColumn() {
			int size = 75;
			double[] data = random(size);
			NumericColumnReducer<double[]> calculator = new NumericColumnReducer<>(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), () -> new double[2],
					(t, d) -> {
						t[0] += d;
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
			double[] result = calculator.getResult();

			double[] expected = Arrays.stream(data).skip(start).limit(end - start).collect(() -> new double[2],
					(t, d) -> {
						t[0] += d;
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});

			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testOneColumnDouble() {
			int size = 75;
			double[] data = random(size);
			NumericColumnReducerDouble calculator = new NumericColumnReducerDouble(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), 0,
					(d, e) -> d + e + d * e);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			double result = calculator.getResult();

			double expected = Arrays.stream(data).skip(start).limit(end - start).reduce(0, (d, e) -> d + e + d * e);

			assertEquals(expected, result, EPSILON);
		}

		@Test
		public void testOneColumnCombiner() {
			int size = 75;
			double[] data = random(size);
			NumericColumnReducerDouble calculator = new NumericColumnReducerDouble(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), 0,
					(count, d) -> d > 0.5 ? count + 1 : count, (count1, count2) -> count1 + count2);
			calculator.init(2);
			int start = 10;
			int mid = 20;
			int end = 30;
			calculator.doPart(start, mid, 0);
			calculator.doPart(mid, end, 1);
			double result = calculator.getResult();

			double expected = Arrays.stream(data).skip(start).limit(end - start)
					.reduce(0, (count, d) -> d > 0.5 ? count + 1 : count);
			assertEquals(expected, result, EPSILON);
		}


		@Test
		public void testTwoColumns() {
			int size = 75;
			double[] data = random(size);
			double[] data2 = random(size);
			NumericColumnsReducer<double[]> calculator = new NumericColumnsReducer<>(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL, data),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, data2)), () -> new double[2],
					(t, row) -> {
						t[0] += row.get(1)+row.get(0)*2;
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
			double[] result = calculator.getResult();

			double[] both = new double[data.length];
			Arrays.setAll(both, i -> data[i] *2+ data2[i]);
			double[] expected = Arrays.stream(both).skip(start).limit(end - start).collect(() -> new double[2],
					(t, d) -> {
						t[0] += d;
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});

			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testThreeColumns() {
			int size = 75;
			double[] data = random(size);
			double[] data2 = random(size);
			double[] data3 = random(size);
			NumericColumnsReducer<double[]> calculator = new NumericColumnsReducer<>(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL, data),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, data2),ColumnTestUtils.getNumericColumn(TypeId.REAL, data3)), () -> new double[2],
					(t, row) -> {
						t[0] += row.get(1)*row.get(0)*row.get(2);
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
			double[] result = calculator.getResult();

			double[] both = new double[data.length];
			Arrays.setAll(both, i -> data[i] * data2[i]*data3[i]);
			double[] expected = Arrays.stream(both).skip(start).limit(end - start).collect(() -> new double[2],
					(t, d) -> {
						t[0] += d;
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});

			assertArrayEquals(expected, result, EPSILON);
		}


	}

	public static class Whole {

		private static class MutableDouble {
			double value = 0;
		}

		@Test
		public void testOneColumn() {
			int size = 75;
			double[] data = random(size);

			Transformer transformer = new Transformer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data));
			MutableDouble result = transformer.reduceNumeric(MutableDouble::new,
					(r, v) -> r.value += v, (l, r) -> l.value += r.value, CTX);

			double expected = Arrays.stream(data).reduce(0, (x, y) -> x + y);

			assertEquals(expected, result.value, EPSILON);
		}

		@Test
		public void testOneColumnDouble() {
			int size = 75;
			double[] data = random(size);

			Transformer transformer = new Transformer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data));
			double result = transformer.reduceNumeric(0, (x, y) -> x + y, CTX);

			double expected = Arrays.stream(data).reduce(0, (x, y) -> x + y);

			assertEquals(expected, result, EPSILON);
		}

		@Test
		public void testOneColumnCombiner() {
			int size = 75;
			double[] data = random(size);

			Transformer transformer = new Transformer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data));
			double result = transformer.reduceNumeric(0, (count, d) -> d > 0.5 ? count + 1 : count,
					(count1, count2) -> count1 + count2, CTX);

			double expected = Arrays.stream(data).reduce(0, (count, d) -> d > 0.5 ? count + 1 : count);

			assertEquals(expected, result, EPSILON);
		}


		@Test
		public void testThreeColumns() {
			int size = 75;
			double[] data = random(size);
			double[] data2 = random(size);
			double[] data3 = random(size);

			RowTransformer transformer = new RowTransformer(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL, data),
					ColumnTestUtils.getNumericColumn(TypeId.REAL,data2), ColumnTestUtils.getNumericColumn(TypeId.REAL, data3)));
			MutableDouble result = transformer.reduceNumeric(MutableDouble::new,
					(d, row) -> d.value += (row.get(0) + row.get(1) + row.get(2)),
					(l, r) -> l.value += r.value, CTX);

			double[] sum = new double[data.length];
			Arrays.setAll(sum, i -> data[i] + data2[i] + data3[i]);
			double expected = Arrays.stream(sum).reduce(0, (a, b) -> a + b);
			assertEquals(expected, result.value, EPSILON);
		}

		@Test
		public void testOneColumnZeroHeight() {
			double[] data = new double[0];

			Transformer transformer = new Transformer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data));
			MutableDouble result = transformer.reduceNumeric(() -> {
						MutableDouble d = new MutableDouble();
						d.value = 1;
						return d;
					},
					(r, v) -> r.value *= v, (l, r) -> l.value *= r.value, CTX);

			assertEquals(1, result.value, EPSILON);
		}

		@Test
		public void testOneColumnDoubleZeroHeight() {
			double[] data = new double[0];
			Transformer transformer = new Transformer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data));
			double result = transformer.reduceNumeric(1, (x, y) -> x * y, CTX);

			assertEquals(1, result, EPSILON);
		}

		@Test
		public void testThreeColumnsZeroHeight() {
			double[] data = new double[0];

			RowTransformer transformer = new RowTransformer(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL, data),
					ColumnTestUtils.getNumericColumn(TypeId.REAL,data), ColumnTestUtils.getNumericColumn(TypeId.REAL, data)));
			MutableDouble result = transformer.reduceNumeric(() -> {
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
