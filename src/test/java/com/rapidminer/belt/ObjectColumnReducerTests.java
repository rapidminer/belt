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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * Tests {@link ObjectColumnReducer} and {@link ObjectColumnsReducer}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ObjectColumnReducerTests {

	private static final double EPSILON = 1e-10;

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	private static final ColumnType<String> FREE_STRING_TYPE = ColumnTypes.freeType("test", String.class, null);

	private static final Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
			.add("a", getNominal())
			.add("b", getFree())
			.build(CTX);

	private static Column getNominal() {
		String[] data = new String[NUMBER_OF_ROWS];
		Arrays.setAll(data, i -> "value" + (i % 10));
		List<String> mapping = new ArrayList<>(Arrays.asList(data));
		mapping.add(0, null);
		int[] categories = new int[data.length];
		Arrays.setAll(categories, i -> i + 1);
		return new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, categories, mapping);
	}

	private static Column getFree() {
		String[] data = new String[NUMBER_OF_ROWS];
		Arrays.setAll(data, i -> "value" + (i % 10));
		return new SimpleObjectColumn<>(FREE_STRING_TYPE, data);
	}

	private static String[] random(int n) {
		String[] data = new String[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(1000) + "");
		return data;
	}

	public static class InputValidationOneColumn {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform("a").reduceObjects(null, ArrayList::new, ArrayList::add, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a").reduceObjects(String.class, ArrayList::new, ArrayList::add, ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a").reduceObjects(String.class, (Supplier<ArrayList<String>>) null, ArrayList::add,
					ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a").reduceObjects(String.class, ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a").reduceObjects(String.class, ArrayList::new, ArrayList::add, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a").reduceObjects(String.class, (Supplier<ArrayList<String>>) (() -> null),
					ArrayList::add, ArrayList::addAll, CTX);
		}


		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedColumn() {
			Table table2 = Builders.newTableBuilder(table.height())
					.add("a", new SimpleObjectColumn<>(FREE_STRING_TYPE, new Object[table.height()]))
					.add("b", new DoubleArrayColumn(new double[table.height()])).build(CTX);
			table2.transform(1).reduceObjects(Void.class, ArrayList::new, (t, r) -> {
			}, ArrayList::addAll, CTX);
		}

	}

	public static class InputValidationMoreColumns {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform("a", "b").reduceObjects((Class<String>) null, ArrayList::new,
					(t, r) -> t.add(r.get(0) + r.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").reduceObjects(String.class, ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)),
					ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a", "b").reduceObjects(String.class, (Supplier<ArrayList<String>>) null,
					(t, r) -> t.add(r.get(0) + r.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a", "b").reduceObjects(String.class, ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a", "b").reduceObjects(String.class, ArrayList::new, (t, r) -> t.add(r.get(0) + r.get(1)),
					null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a", "b").reduceObjects(String.class, (Supplier<ArrayList<String>>) (() -> null),
					(t, r) -> t.add(r.get(0) + r.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongClass() {
			table.transform(new int[]{0, 1}).reduceObjects(Integer.class, ArrayList::new,
					(t, r) -> t.add(r.get(0) + r.get(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedColumn() {
			Table table2 = Builders.newTableBuilder(table.height())
					.add("a", new SimpleObjectColumn<>(FREE_STRING_TYPE, new Object[table.height()]))
					.add("b", new DoubleArrayColumn(new double[table.height()])).build(CTX);
			table2.transform(new int[]{0, 1}).reduceObjects(Object.class, ArrayList::new, (t, r) -> {},
					ArrayList::addAll, CTX);
		}
	}

	public static class Parts {


		@Test
		public void testOneColumn() {
			int size = 75;
			String[] data = random(size);
			Column column = new SimpleObjectColumn<>(FREE_STRING_TYPE, data);
			ObjectColumnReducer<double[], String> calculator =
					new ObjectColumnReducer<>(column, String.class, () -> new double[2],
							(t, d) -> {
								t[0] += d.length();
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
						t[0] += d.length();
						t[1] += 1;
					},
					(t1, t2) -> {
						t1[0] += t2[0];
						t1[1] += t2[1];
					});

			assertArrayEquals(expected, result, EPSILON);
		}


		@Test
		public void testTwoColumns() {
			int size = 75;
			String[] data = random(size);
			String[] data2 = random(size);
			Column column = new SimpleObjectColumn<>(FREE_STRING_TYPE, data);
			Column column2 = new SimpleObjectColumn<>(FREE_STRING_TYPE, data2);
			ObjectColumnsReducer<double[], String> calculator =
					new ObjectColumnsReducer<>(new Column[]{column, column2},
							String.class, () -> new double[2],
							(t, row) -> {
								t[0] += row.get(1).length() + row.get(0).length() * 2;
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
			Arrays.setAll(both, i -> data[i].length() * 2 + data2[i].length());
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
			String[] data = random(size);
			String[] data2 = random(size);
			String[] data3 = random(size);
			Column column = new SimpleObjectColumn<>(FREE_STRING_TYPE, data);
			Column column2 = new SimpleObjectColumn<>(FREE_STRING_TYPE, data2);
			Column column3 = new SimpleObjectColumn<>(FREE_STRING_TYPE, data3);
			ObjectColumnsReducer<double[], String> calculator =
					new ObjectColumnsReducer<>(new Column[]{column, column2, column3},
							String.class, () -> new double[2],
							(t, row) -> {
								t[0] += row.get(1).length() * row.get(0).length() * row.get(2).length();
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
			Arrays.setAll(both, i -> data[i].length() * data2[i].length() * data3[i].length());
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
			String[] data = random(size);
			Column column = new SimpleObjectColumn<>(FREE_STRING_TYPE, data);
			Transformer transformer = new Transformer(column);
			MutableDouble result = transformer.reduceObjects(String.class, MutableDouble::new,
					(r, v) -> r.value += v.length(),
					(l, r) -> l.value += r.value, CTX);

			MutableDouble expected = Arrays.stream(data).collect(MutableDouble::new,
					(r, v) -> r.value += v.length(),
					(l, r) -> l.value += r.value);

			assertEquals(expected.value, result.value, EPSILON);
		}


		@Test
		public void testThreeColumns() {
			int size = 75;
			String[] data = random(size);
			String[] data2 = random(size);
			String[] data3 = random(size);

			RowTransformer transformer = new RowTransformer(new Column[]{
					new SimpleObjectColumn<>(FREE_STRING_TYPE, data), new SimpleObjectColumn<>(FREE_STRING_TYPE, data2),
					new SimpleObjectColumn<>(FREE_STRING_TYPE, data3)});
			MutableDouble result = transformer.reduceObjects(String.class, MutableDouble::new,
					(d, row) -> d.value += (row.get(0).length() + row.get(1).length() + row.get(2).length()),
					(l, r) -> l.value += r.value, CTX);

			double[] sum = new double[data.length];
			Arrays.setAll(sum, i -> data[i].length() + data2[i].length() + data3[i].length());
			double expected = Arrays.stream(sum).reduce(0, (a, b) -> a + b);
			assertEquals(expected, result.value, EPSILON);
		}

		@Test
		public void testOneColumnZeroHeight() {
			String[] data = random(0);
			Column column = new SimpleObjectColumn<>(FREE_STRING_TYPE, data);
			Transformer transformer = new Transformer(column);
			MutableDouble result = transformer.reduceObjects(String.class, () -> {
						MutableDouble d = new MutableDouble();
						d.value = 1;
						return d;
					},
					(r, v) -> r.value *= v.length(),
					(l, r) -> l.value *= r.value, CTX);

			assertEquals(1, result.value, EPSILON);
		}

		@Test
		public void testThreeColumnsZeroHeight() {
			String[] data = random(0);

			RowTransformer transformer = new RowTransformer(new Column[]{
					new SimpleObjectColumn<>(FREE_STRING_TYPE, data), new SimpleObjectColumn<>(FREE_STRING_TYPE, data),
					new SimpleObjectColumn<>(FREE_STRING_TYPE, data)});
			MutableDouble result = transformer.reduceObjects(String.class, () -> {
						MutableDouble d = new MutableDouble();
						d.value = 1;
						return d;
					},
					(d, row) -> d.value += (row.get(0).length() + row.get(1).length() + row.get(2).length()),
					(l, r) -> l.value += r.value, CTX);

			assertEquals(1, result.value, EPSILON);
		}
	}

}
