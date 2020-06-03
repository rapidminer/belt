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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntToDoubleFunction;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.table.TableTestUtils;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.Workload;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;


/**
 * Tests {@link NumericColumnFilterer} and {@link NumericColumnsFilterer}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class NumericColumnFiltererTests {

	private static final double EPSILON = 1e-10;

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	private static final List<String> EMPTY_DICTIONARY = new ArrayList<>();

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


	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		NumericReader reader = Readers.numericReader(table.column(column));
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

	private static final int MAX_VALUES = 30;

	private static List<String> getMappingList() {
		List<String> list = new ArrayList<>(MAX_VALUES);
		list.add(null);
		for (int i = 1; i < MAX_VALUES; i++) {
			list.add("value" + i);
		}
		return list;
	}

	private static int[] randomInt(int n) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(MAX_VALUES));
		return data;
	}

	public static class InputValidationOneColumnNumeric {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterNumeric("b", i -> i > 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFilter() {
			table.filterNumeric("b", null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.filterNumeric("b", i -> i > 0, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterNumeric("x", i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabel() {
			table.filterNumeric((String) null, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterNumeric(123, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterNumeric(-1, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContained(){
			new RowFilterer(Collections.singletonList(null));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyList(){
			new RowFilterer(Collections.emptyList());
		}

	}

	public static class InputValidationOneColumnCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterCategorical("b", i -> i > 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFilter() {
			table.filterCategorical("b", null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.filterCategorical("b", i -> i > 0, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterCategorical("x", i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabel() {
			table.filterCategorical((String) null, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterCategorical(123, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterCategorical(-1, i -> i > 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnObjects {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterObjects("b", Double.class, i -> i > 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.filterObjects("b", null, i -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFilter() {
			table.filterObjects("b", Double.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.filterObjects("b", Double.class, i -> i > 0, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterObjects("x", Double.class, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullLabel() {
			table.filterObjects((String) null, Double.class, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterObjects(123, Double.class, i -> i > 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterObjects(-1, Double.class, i -> i > 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumeric {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterNumeric(Arrays.asList("a", "b"), row -> true, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.filterNumeric(Arrays.asList("a", "b"), null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterNumeric(new int[]{3, 0}, row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterNumeric(Arrays.asList("a", "x"), row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel2() {
			table.filterNumeric("a", "x", (a, b) -> a > b, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterNumeric(new int[]{-1, 0}, row -> false, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterCategorical(Arrays.asList("a", "b"), row -> true, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.filterCategorical(Arrays.asList("a", "b"), null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterCategorical(new int[]{3, 0}, row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterCategorical(Arrays.asList("a", "x"), row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel2() {
			table.filterCategorical("a", "x", (a, b) -> a > b, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterCategorical(new int[]{-1, 0}, row -> false, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjects {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterObjects(Arrays.asList("a", "b"), Double.class, row -> true, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.filterObjects(Arrays.asList("a", "b"), Double.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.filterObjects(Arrays.asList("a", "b"), null, row -> true, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterObjects(new int[]{3, 0}, Double.class, row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterObjects(Arrays.asList("a", "x"), Double.class, row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel2() {
			table.filterObjects("a", "x", Double.class, (a, b) -> a > b, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterObjects(new int[]{-1, 0}, Double.class, row -> false, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneral {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.filterMixed(Arrays.asList("a", "b"), row -> true, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.filterMixed(Arrays.asList("a", "b"), null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testWrongIndex() {
			table.filterMixed(new int[]{3, 0}, row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongLabel() {
			table.filterMixed(Arrays.asList("a", "x"), row -> false, Workload.DEFAULT, CTX);
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testNegativeIndex() {
			table.filterMixed(new int[]{-1, 0}, row -> false, Workload.DEFAULT, CTX);
		}

	}

	public static class Numeric {


		@Test
		public void testOneColumnPart() {
			int size = 75;
			double[] data = random(size);
			NumericColumnFilterer calculator = new NumericColumnFilterer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data),
					i -> i > 0.5);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> data[i] > 0.5).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testOneColumnWhole() {
			int size = 75;
			double[] data = random(size);
			Table table = TableTestUtils.newTable(new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, data)},
					new String[]{"a"});
			Table filtered = table.filterNumeric(0, d -> d > 0.3, Workload.SMALL, CTX);


			double[] expected = Arrays.stream(data).filter(d -> d > 0.3).toArray();
			assertArrayEquals(expected, readColumnToArray(filtered, 0), EPSILON);
		}

		@Test
		public void testMoreColumnsPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			NumericColumnsFilterer calculator =
					new NumericColumnsFilterer(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL, first),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, second),
							ColumnTestUtils.getNumericColumn(TypeId.REAL, third)),
							row -> row.get(0) + row.get(1) + row.get(2) > 1);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> first[i] + second[i] + third[i] > 1).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testMoreColumnsWhole() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			Table table = TableTestUtils.newTable(new Column[]{ColumnTestUtils.getNumericColumn(TypeId.REAL, first),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, second),
					ColumnTestUtils.getNumericColumn(TypeId.REAL, third)}, new String[]{"a", "b", "c"});
			Table filtered = table.filterNumeric(new int[]{0, 1, 2}, row -> row.get(0) + row.get(1) + row.get(2) > 1,
					Workload.DEFAULT, CTX);

			int[] expected = IntStream.range(0, size).filter(i -> first[i] + second[i] + third[i] > 1).toArray();
			double[] expectedFirst = new double[expected.length];
			Arrays.setAll(expectedFirst, i -> first[expected[i]]);
			double[] expectedSecond = new double[expected.length];
			Arrays.setAll(expectedSecond, i -> second[expected[i]]);
			double[] expectedThird = new double[expected.length];
			Arrays.setAll(expectedThird, i -> third[expected[i]]);
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(filtered));
		}

	}

	public static class Categorical {


		@Test
		public void testOneColumnPart() {
			int size = 75;
			int[] data = randomInt(size);
			CategoricalColumnFilterer calculator = new CategoricalColumnFilterer(
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, data, EMPTY_DICTIONARY), i -> i > 5);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> data[i] > 5).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testOneColumnWhole() {
			int size = 75;
			int[] data = randomInt(size);
			Table table = TableTestUtils.newTable(new Column[]{
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, data, EMPTY_DICTIONARY)}, new String[]{"a"});
			Table filtered = table.filterCategorical(0, d -> d > 5, Workload.SMALL, CTX);


			double[] expected = Arrays.stream(data).filter(d -> d > 5).asDoubleStream().toArray();
			assertArrayEquals(expected, readColumnToArray(filtered, 0), EPSILON);
		}

		@Test
		public void testMoreColumnsPart() {
			int size = 75;
			int[] first = randomInt(size);
			int[] second = randomInt(size);
			int[] third = randomInt(size);
			CategoricalColumnsFilterer calculator = new CategoricalColumnsFilterer(Arrays.asList(
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, first, EMPTY_DICTIONARY),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, second, EMPTY_DICTIONARY),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, third, EMPTY_DICTIONARY)),
					row -> row.get(0) + row.get(1) > row.get(2));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> first[i] + second[i] > third[i]).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testMoreColumnsWhole() {
			int size = 75;
			int[] first = randomInt(size);
			int[] second = randomInt(size);
			int[] third = randomInt(size);
			Table table = TableTestUtils.newTable(new Column[]{
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, first, EMPTY_DICTIONARY),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, second, EMPTY_DICTIONARY),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, third, EMPTY_DICTIONARY)},
					new String[]{"a", "b", "c"});
			Table filtered = table.filterCategorical(new int[]{0, 1, 2}, row -> row.get(0) + row.get(1) > row.get(2),
					Workload.DEFAULT, CTX);

			int[] expected = IntStream.range(0, size).filter(i -> first[i] + second[i] > third[i]).toArray();
			double[] expectedFirst = new double[expected.length];
			Arrays.setAll(expectedFirst, i -> first[expected[i]] == 0 ? Double.NaN : first[expected[i]]);
			double[] expectedSecond = new double[expected.length];
			Arrays.setAll(expectedSecond, i -> second[expected[i]] == 0 ? Double.NaN : second[expected[i]]);
			double[] expectedThird = new double[expected.length];
			Arrays.setAll(expectedThird, i -> third[expected[i]] == 0 ? Double.NaN : third[expected[i]]);
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(filtered));
		}

	}

	public static class Objects {


		@Test
		public void testOneColumnPart() {
			int size = 75;
			int[] data = randomInt(size);
			List<String> mappingList = getMappingList();
			ObjectColumnFilterer<String> calculator = new ObjectColumnFilterer<>(
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, data, mappingList), String.class,
					i -> i == null || i.length() > 6);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> mappingList.get(data[i]) == null
					|| mappingList.get(data[i]).length() > 6).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testOneColumnWhole() {
			int size = 75;
			int[] data = randomInt(size);
			List<String> mappingList = getMappingList();
			Table table = TableTestUtils.newTable(new Column[]{
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, data, mappingList)}, new String[]{"a"});
			Table filtered = table.filterObjects(0, String.class, i -> i != null && !"value1".equals(i), Workload.SMALL, CTX);

			double[] expected = Arrays.stream(data).filter(i -> i != 0 && !"value1".equals(mappingList.get(i)))
					.asDoubleStream().toArray();
			assertArrayEquals(expected, readColumnToArray(filtered, 0), EPSILON);
		}

		@Test
		public void testMoreColumnsPart() {
			int size = 75;
			int[] first = randomInt(size);
			int[] second = randomInt(size);
			int[] third = randomInt(size);
			List<String> mappingList = getMappingList();
			ObjectColumnsFilterer<String> calculator = new ObjectColumnsFilterer<>(Arrays.asList(
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, first,
							mappingList),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, second,
							mappingList),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, third, mappingList)), String.class,
					row -> row.get(0) == null || (!row.get(0).equals(row.get(1)) && !row.get(0).equals(row.get(2))));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> first[i] == 0
					|| (first[i] != second[i] && first[i] != third[i])).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testMoreColumnsWhole() {
			int size = 75;
			int[] first = randomInt(size);
			int[] second = randomInt(size);
			int[] third = randomInt(size);
			List<String> mappingList = getMappingList();
			Table table = TableTestUtils.newTable(new Column[]{
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, first,
							mappingList),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, second,
							mappingList),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, third, mappingList)},
					new String[]{"a", "b", "c"});
			Table filtered = table.filterObjects(new int[]{0, 1, 2}, String.class,
					row -> row.get(0) != null && row.get(1) != null && row.get(2) != null,
					Workload.DEFAULT, CTX);

			int[] expected = IntStream.range(0, size).filter(i -> first[i] != 0 && second[i] != 0 && third[i] != 0).toArray();
			double[] expectedFirst = new double[expected.length];
			Arrays.setAll(expectedFirst, i -> first[expected[i]]);
			double[] expectedSecond = new double[expected.length];
			Arrays.setAll(expectedSecond, i -> second[expected[i]]);
			double[] expectedThird = new double[expected.length];
			Arrays.setAll(expectedThird, i -> third[expected[i]]);
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(filtered));
		}

	}

	public static class General {

		@Test
		public void testMoreColumnsPart() {
			int size = 75;
			double[] first = random(size);
			int[] second = randomInt(size);
			int[] third = randomInt(size);
			List<String> mappingList = getMappingList();
			MixedColumnsFilterer calculator = new MixedColumnsFilterer(Arrays.asList(
					ColumnTestUtils.getNumericColumn(TypeId.REAL, first),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, second,
							mappingList),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, third, mappingList)),
					row -> row.getNumeric(0) > 0.5 || row.getIndex(1) == 0 || row.getObject(1).equals(row.getObject(2)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			int[] result = calculator.getResult();

			int[] expected = IntStream.range(10, 30).filter(i -> first[i] > 0.5
					|| (second[i] == 0 || second[i] == third[i])).toArray();
			assertArrayEquals(expected, result);
		}

		@Test
		public void testMoreColumnsWhole() {
			int size = 75;
			double[] first = random(size);
			int[] second = randomInt(size);
			int[] third = randomInt(size);
			List<String> mappingList = getMappingList();
			Table table = TableTestUtils.newTable(new Column[]{
					ColumnTestUtils.getNumericColumn(TypeId.REAL, first),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, second,
							mappingList),
					ColumnTestUtils.getSimpleCategoricalColumn(ColumnType.NOMINAL, third, mappingList)},
					new String[]{"a", "b", "c"});
			Table filtered = table.filterMixed(new int[]{0, 1, 2},
					row -> row.getNumeric(0) > 0.1 && row.getObject(1) != null && row.getObject(2) != null,
					Workload.DEFAULT, CTX);

			int[] expected = IntStream.range(0, size).filter(i -> first[i] > 0.1 && second[i] != 0 && third[i] != 0).toArray();
			double[] expectedFirst = new double[expected.length];
			Arrays.setAll(expectedFirst, i -> first[expected[i]]);
			double[] expectedSecond = new double[expected.length];
			Arrays.setAll(expectedSecond, i -> second[expected[i]]);
			double[] expectedThird = new double[expected.length];
			Arrays.setAll(expectedThird, i -> third[expected[i]]);
			assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(filtered));
		}

	}

}
