/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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
import java.util.Objects;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.buffer.Int32NominalBuffer;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.execution.Context;


/**
 * Tests the {@link MixedColumnsReducer}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class MixedColumnReducerTests {

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

	private static String[] randomStrings(int n) {
		String[] data = new String[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(10) + "");
		return data;
	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	public static class InputValidationMoreColumns {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").reduceMixed(ArrayList::new, (t, r) -> t.add(r.getIndex(0) + r.getIndex(1)),
					ArrayList::addAll, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullSupplier() {
			table.transform("a", "b").reduceMixed((Supplier<ArrayList<Integer>>) null,
					(t, r) -> t.add(r.getIndex(0) + r.getIndex(1)), ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullReducer() {
			table.transform("a", "b").reduceMixed(ArrayList::new, null, ArrayList::addAll, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCombiner() {
			table.transform("a", "b")
					.reduceMixed(ArrayList::new, (t, r) -> t.add(r.getIndex(0) + r.getIndex(1)), null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullProducingSupplier() {
			table.transform("a", "b").reduceMixed((Supplier<ArrayList<Integer>>) (() -> null), (t, r) -> t.add(r
					.getIndex(0) + r.getIndex(1)), ArrayList::addAll, CTX);
		}

	}

	public static class Parts {


		@Test
		public void testTwoColumns() {
			int size = 75;
			String[] data = randomStrings(size);
			double[] data2 = random(size);
			CategoricalColumn column = getColumn(data);
			Column column2 = ColumnTestUtils.getNumericColumn(TypeId.REAL, data2);
			MixedColumnsReducer<int[]> calculator = new MixedColumnsReducer<>(Arrays.asList(column,
					column2), () -> new int[2],
					(t, row) -> {
						t[0] += (int) (row.getNumeric(1) * 5) + row.getIndex(0);
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
			Arrays.setAll(both, i -> ColumnTestUtils.getIntData(column)[i] + (int) (5 * data2[i]));
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
			String[] data = randomStrings(size);
			String[] data2 = randomStrings(size);
			double[] data3 = random(size);
			CategoricalColumn column = getColumn(data);
			CategoricalColumn column2 = getColumn(data2);
			Column column3 = ColumnTestUtils.getNumericColumn(TypeId.REAL, data3);
			MixedColumnsReducer<int[]> calculator = new MixedColumnsReducer<>(Arrays.asList(column,
					column2, column3), () -> new int[2],
					(t, row) -> {
						t[0] += (int) (row.getIndex(1) * Objects.toString(row.getObject(0), "").length() * row
								.getNumeric(2));
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
					i -> (int) (Objects.toString(column.getDictionary().get(ColumnTestUtils.getIntData(column)[i]), "")
					.length() * ColumnTestUtils.getIntData(column2)[i] * data3[i]));
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

		private static class MutableDouble {
			double value = 0;
		}

		@Test
		public void testThreeColumnsWhole() {
			int size = 75;
			String[] data = randomStrings(size);
			CategoricalColumn column = getColumn(data);
			String[] data2 = randomStrings(size);
			CategoricalColumn column2 = getColumn(data2);
			double[] data3 = random(size);
			Column column3 = ColumnTestUtils.getNumericColumn(TypeId.REAL, data3);

			RowTransformer transformer = new RowTransformer(Arrays.asList(column, column2, column3));
			MutableDouble result = transformer.reduceMixed(MutableDouble::new,
					(d, row) -> d.value +=
							(row.getIndex(0) + Objects.toString(row.getObject(1)).length() + row.getNumeric(2)),
					(l, r) -> l.value += r.value, CTX);

			double[] sum = new double[data.length];
			Arrays.setAll(sum,
					i -> ColumnTestUtils.getIntData(column)[i] + Objects.toString(column2.getDictionary().get(ColumnTestUtils.getIntData(column2)
							[i]))
							.length() + data3[i]);
			double expected = Arrays.stream(sum).reduce(0, (a, b) -> a + b);
			assertEquals(expected, result.value, EPSILON);
		}

		@Test
		public void testThreeColumnsWholeZeroHeight() {
			String[] data = randomStrings(0);
			CategoricalColumn column = getColumn(data);

			RowTransformer transformer = new RowTransformer(Arrays.asList(column, column, column));
			MutableDouble result = transformer.reduceMixed(() -> {
						MutableDouble d = new MutableDouble();
						d.value = 1;
						return d;
					},
					(d, row) -> d.value *=
							(row.getIndex(0) + Objects.toString(row.getObject(1)).length() + row.getNumeric(2)),
					(l, r) -> l.value *= r.value, CTX);

			assertEquals(1, result.value, EPSILON);
		}

	}

}
