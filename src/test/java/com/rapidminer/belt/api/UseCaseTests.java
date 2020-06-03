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

package com.rapidminer.belt.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.stream.IntStream;

import org.junit.Test;

import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.execution.Workload;
import com.rapidminer.belt.util.Order;


/**
 * @author Gisa Schaefer
 */
public class UseCaseTests {

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
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

	private static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	@Test
	public void testReplaceAndRemoveColumnsUseCase() {

		double[] first = random(NUMBER_OF_ROWS);
		double[] second = random(NUMBER_OF_ROWS);
		double[] third = random(NUMBER_OF_ROWS);

		NumericBuffer buffer = Buffers.realBuffer(NUMBER_OF_ROWS);
		for (int i = 0; i < NUMBER_OF_ROWS; i++) {
			buffer.set(i, second[i]);
		}

		Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
				.addReal("one", i -> first[i])
				.add("two", buffer.toColumn())
				.addReal("three", i -> third[i])
				.build(CTX);

		double[] copyOfSecond = Arrays.copyOf(second, second.length);
		copyOfSecond[3] = 42;

		NumericBuffer secondAsBuffer = Buffers.realBuffer(table.column(1));
		secondAsBuffer.set(3, 42);

		Table derivedTable = Builders.newTableBuilder(table)
				.replace("two", secondAsBuffer.toColumn())
				.remove("three")
				.build(CTX);

		assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
		assertArrayEquals(new double[][]{first, copyOfSecond}, readTableToArray(derivedTable));
	}


	@Test
	public void testModifyAndSelectRowsUseCase() {
		double[] first = random(NUMBER_OF_ROWS);
		double[] second = random(NUMBER_OF_ROWS);
		double[] third = random(NUMBER_OF_ROWS);
		double[] fourth = random(NUMBER_OF_ROWS);

		Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
				.addReal("one", i -> first[i])
				.addReal("two", i -> second[i])
				.addReal("three", i -> third[i])
				.addReal("four", i -> fourth[i])
				.build(CTX);

		double[] fifth = random(NUMBER_OF_ROWS);
		Table fifthAsTable = Builders.newTableBuilder(NUMBER_OF_ROWS)
				.addReal("one", i -> fifth[i])
				.build(CTX);

		Table derivedTable = Builders.newTableBuilder(table.columns(Arrays.asList("one", "three", "four")))
				.add("five", fifthAsTable.column(0))
				.build(CTX);

		derivedTable = derivedTable.columns(Arrays.asList("four", "three", "five", "one"));

		Table rowSubset = derivedTable.rows(new int[]{1, 3, 5}, CTX);

		assertArrayEquals(new double[][]{first, second, third, fourth}, readTableToArray(table));
		assertArrayEquals(new double[][]{fourth, third, fifth, first}, readTableToArray(derivedTable));
		assertArrayEquals(new double[][]{
			new double[]{fourth[1], fourth[3], fourth[5]},
			new double[]{third[1], third[3], third[5]},
			new double[]{fifth[1], fifth[3], fifth[5]},
			new double[]{first[1], first[3], first[5]}
		}, readTableToArray(rowSubset));
	}

	@Test
	public void testSimpleSortAfterSelectUseCase() {
		double first[] = { 100, 0, 1, 2, 3, 100, 4, 5, 6, 7, 100, 100 };
		double second[] = { 100, 0, 1, 0, 1, 100, 0, 1, 0, 1, 100, 100 };
		double third[] = { 100, 7, 6, 5, 4, 100, 3, 2, 1, 0, 100, 100 };

		Table table = Builders.newTableBuilder(12)
				.addReal("one", i -> first[i])
				.addReal("two", i -> second[i])
				.addReal("three", i -> third[i])
				.build(CTX);

		Table selected = table.rows(new int[]{1, 2, 3, 4, 6, 7, 8, 9}, CTX);

		Table sorted = selected.sort("two", Order.ASCENDING, CTX);

		double selectedFirst[] = { 0, 1, 2, 3, 4, 5, 6, 7 };
		double selectedSecond[] = { 0, 1, 0, 1, 0, 1, 0, 1 };
		double selectedThird[] = { 7, 6, 5, 4, 3, 2, 1, 0 };

		double expectedFirst[] = {0, 2, 4, 6, 1, 3, 5, 7};
		double expectedSecond[] = {0, 0, 0, 0, 1, 1, 1, 1};
		double expectedThird[] = {7, 5, 3, 1, 6, 4, 2, 0};

		assertArrayEquals(new double[][] { first, second, third }, readTableToArray(table));
		assertArrayEquals(new double[][] { selectedFirst, selectedSecond, selectedThird }, readTableToArray(selected));
		assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(sorted));
	}


	@Test
	public void testMultiCriteriaSortWithDifferentMappingUseCase() {
		double first[] = { 0, 1, 2, 3, 4, 5, 6, 7 };
		double second[] = { 100, -0.001, 0.423, 0.157, 0.423, 100, 0.350, -0.089, 0.157, 0.375, 100, 100 };
		double third[] = { 10, 100, 10, 100, 10, 100, 10, 100 };

		Table tableSecond = Builders.newTableBuilder(12)
				.addReal("two", i -> second[i])
				.build(CTX);

		Table tableSecondSelected = tableSecond.rows(new int[]{1, 2, 3, 4, 6, 7, 8, 9}, CTX);

		double[] secondSelected = { -0.001, 0.423, 0.157, 0.423, 0.350, -0.089, 0.157, 0.375 };

		Table table = Builders.newTableBuilder(8)
				.addReal("one", i -> first[i])
				.add("two", tableSecondSelected.column("two"))
				.addReal("three", i -> third[i])
				.build(CTX);

		Table sorted = table.sort(Arrays.asList("three", "two"),
				Arrays.asList(Order.ASCENDING, Order.DESCENDING), CTX);

		double expectedFirst[] = { 4, 2, 6, 0, 1, 3, 7, 5 };
		double expectedSecond[] = { 0.350, 0.157, 0.157, -0.001, 0.423, 0.423, 0.375, -0.089 };
		double expectedThird[] = { 10, 10, 10, 10, 100, 100, 100, 100 };

		assertArrayEquals(new double[][] { second }, readTableToArray(tableSecond));
		assertArrayEquals(new double[][] { secondSelected }, readTableToArray(tableSecondSelected));
		assertArrayEquals(new double[][] { first, secondSelected, third }, readTableToArray(table));
		assertArrayEquals(new double[][] { expectedFirst, expectedSecond, expectedThird }, readTableToArray(sorted));
	}

	@Test
	public void testReplaceByMappedColumnsUseCase() {

		double[] first = random(NUMBER_OF_ROWS);
		double[] second = random(NUMBER_OF_ROWS);
		double[] third = random(NUMBER_OF_ROWS);

		Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
				.addReal("one", i -> first[i])
				.addReal("two", i -> second[i])
				.addReal("three", i -> third[i])
				.build(CTX);

		NumericBuffer secondTimesTwo = table.transform("two").applyNumericToReal(i -> 2 * i, CTX);
		NumericBuffer onePlusThree = table.transform("one", "three").applyNumericToReal((a, b) -> a + b, CTX);

		Table derivedTable = Builders.newTableBuilder(table)
				.replace("two", secondTimesTwo.toColumn())
				.replace("one", onePlusThree.toColumn())
				.build(CTX);

		double[] secondExpected = new double[NUMBER_OF_ROWS];
		Arrays.setAll(secondExpected, i -> 2 * second[i]);
		double[] firstExpected = new double[NUMBER_OF_ROWS];
		Arrays.setAll(firstExpected, i -> first[i] + third[i]);

		assertArrayEquals(new double[][]{first, second, third}, readTableToArray(table));
		assertArrayEquals(new double[][]{firstExpected, secondExpected, third}, readTableToArray(derivedTable));
	}

	@Test
	public void testSortingOfColumnSubset() {
		double first[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
		double second[] = {2, 2, 0, 0, 7, 7, 3, 3, 1, 1};
		double third[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

		Table table = Builders.newTableBuilder(10)
				.addReal("one", i -> first[i])
				.addReal("two", i -> second[i])
				.addReal("three", i -> third[i])
				.build(CTX);

		Table derived = table.columns(Arrays.asList("one", "two"))
				.sort(Arrays.asList("two", "one"), Order.DESCENDING, CTX);

		derived = Builders.newTableBuilder(derived)
				.add("three", table.column("three"))
				.build(CTX);

		double expectedFirst[] = {5, 4, 7, 6, 1, 0, 9, 8, 3, 2};
		double expectedSecond[] = {7, 7, 3, 3, 2, 2, 1, 1, 0, 0};
		double expectedThird[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

		assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(derived));
	}

	@Test
	public void testSelectionOfTopThree() {
		double first[] = {0.350, 0.157, 0.157, -0.001, 0.427, 0.423, 0.375, -0.089};

		Table table = Builders.newTableBuilder(8)
				.addReal("data", i -> first[i])
				.build(CTX);

		Table derived = table.sort("data", Order.DESCENDING, CTX);
		derived = derived.rows(0, 3, CTX);
		derived = Builders.newTableBuilder(derived)
				.addReal("id", i -> i + 1)
				.build(CTX);

		double expectedFirst[] = {0.427, 0.423, .375};
		double expectedSecond[] = {1, 2, 3};

		assertArrayEquals(new double[][]{expectedFirst, expectedSecond}, readTableToArray(derived));
	}

	@Test
	public void testRowsWithValuesGreaterThanAverage() {
		double[] first = random(NUMBER_OF_ROWS);
		double[] second = random(NUMBER_OF_ROWS);
		double[] third = random(NUMBER_OF_ROWS);

		Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
				.addReal("one", i -> first[i])
				.addReal("two", i -> second[i])
				.addReal("three", i -> third[i])
				.build(CTX);

		double[] averageInfo = table.transform("one").reduceNumeric(() -> new double[2],
				(t, d) -> {
					t[0] += d;
					t[1] += 1;
				},
				(t1, t2) -> {
					t1[0] += t2[0];
					t1[1] += t2[1];
				}, CTX);

		double average = averageInfo[0] / averageInfo[1];

		Table filtered = table.filterNumeric("one", d -> d > average, Workload.DEFAULT, CTX);

		@SuppressWarnings("ConstantConditions")
		double expectedAverage = Arrays.stream(first).average().getAsDouble();
		int[] expectedIndices = IntStream.range(0, first.length).filter(i -> first[i] > average).toArray();
		double[] expectedFirst = new double[expectedIndices.length];
		Arrays.setAll(expectedFirst, i -> first[expectedIndices[i]]);
		double[] expectedSecond = new double[expectedIndices.length];
		Arrays.setAll(expectedSecond, i -> second[expectedIndices[i]]);
		double[] expectedThird = new double[expectedIndices.length];
		Arrays.setAll(expectedThird, i -> third[expectedIndices[i]]);

		assertEquals(expectedAverage, average, 10e-15);
		assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(filtered));
	}

	@Test
	public void testMaxOfPositiveDifference() {
		double[] first = random(NUMBER_OF_ROWS);
		double[] second = random(NUMBER_OF_ROWS);
		double[] third = random(NUMBER_OF_ROWS);

		Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
				.addReal("one", i -> first[i])
				.addReal("two", i -> second[i])
				.addReal("three", i -> third[i])
				.build(CTX);

		Table filtered = table.filterNumeric("two", "three", (two, three) -> three - two > 0, Workload.DEFAULT, CTX);

		double maxLeft = filtered.transform("one").reduceNumeric(-1, Double::max, CTX);

		int[] expectedIndices = IntStream.range(0, first.length).filter(i -> third[i] - second[i] > 0).toArray();
		double[] expectedFirst = new double[expectedIndices.length];
		Arrays.setAll(expectedFirst, i -> first[expectedIndices[i]]);
		double[] expectedSecond = new double[expectedIndices.length];
		Arrays.setAll(expectedSecond, i -> second[expectedIndices[i]]);
		double[] expectedThird = new double[expectedIndices.length];
		Arrays.setAll(expectedThird, i -> third[expectedIndices[i]]);

		@SuppressWarnings("ConstantConditions")
		double expectedMax = Arrays.stream(expectedFirst).max().getAsDouble();

		assertEquals(expectedMax, maxLeft, 10e-15);
		assertArrayEquals(new double[][]{expectedFirst, expectedSecond, expectedThird}, readTableToArray(filtered));
	}


}
