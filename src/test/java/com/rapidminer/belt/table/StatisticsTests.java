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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Statistics;
import com.rapidminer.belt.column.Statistics.Result;
import com.rapidminer.belt.column.Statistics.Statistic;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.transform.ParallelExecutorTests;
import com.rapidminer.belt.util.Belt;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class StatisticsTests {

	private static final double EPSILON = 1e-10;
	private static final Context CTX = Belt.defaultContext();
	private static final int N = ParallelExecutorTests.BATCH_SIZE_MEDIUM * 4;

	private static int[] permutation(Random rng, int n) {
		int[] indices = new int[n];
		Arrays.setAll(indices, i -> i);
		for (int i = 0; i < n; i++) {
			int a = (int) (rng.nextDouble() * n);
			int b = (int) (rng.nextDouble() * n);
			int tmp = indices[a];
			indices[a] = indices[b];
			indices[b] = tmp;
		}
		return indices;
	}

	public static class Numeric {

		@Test
		public void testEmpty() {
			Table table = Builders.newTableBuilder(50)
					.addReal("real", i -> i)
					.build(CTX);
			assertTrue(Statistics.compute(table.column("real"), Collections.emptySet(), CTX).isEmpty());
		}

		@Test
		public void testCounts() {
			Random rng = new Random(8734619283L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i < 300 ? i - 150 : Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(300, stats.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(-150, stats.get(Statistic.MIN).getNumeric(), EPSILON);
			assertEquals(149, stats.get(Statistic.MAX).getNumeric(), EPSILON);
			assertEquals(-0.5, stats.get(Statistic.MEAN).getNumeric(), EPSILON);
		}

		@Test
		public void testMinMaxPositive() {
			Random rng = new Random(2756284756L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i < 300 ? i + 1: Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(1, stats.get(Statistic.MIN).getNumeric(), EPSILON);
			assertEquals(300, stats.get(Statistic.MAX).getNumeric(), EPSILON);
		}

		@Test
		public void testMinMaxNegative() {
			Random rng = new Random(8336494L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i < 300 ? -i - 1 : Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(-300, stats.get(Statistic.MIN).getNumeric(), EPSILON);
			assertEquals(-1, stats.get(Statistic.MAX).getNumeric(), EPSILON);
		}

		@Test
		public void testInfiniteValues() {
			NumericBuffer buffer = Buffers.realBuffer(100);

			Random rng = new Random(987364198367L);
			for (int i = 0; i < buffer.size(); i++) {
				if (rng.nextDouble() < 0.9) {
					buffer.set(i, -100 + rng.nextDouble() * 200);
				} else {
					buffer.set(i, Double.NaN);
				}
			}

			buffer.set(5, Double.NEGATIVE_INFINITY);
			buffer.set(65, Double.NEGATIVE_INFINITY);
			buffer.set(93, Double.NEGATIVE_INFINITY);
			buffer.set(12, Double.POSITIVE_INFINITY);
			buffer.set(73, Double.POSITIVE_INFINITY);

			Table table = Builders.newTableBuilder(100)
					.add("real", buffer.toColumn())
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN,
							Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(Double.NEGATIVE_INFINITY, stats.get(Statistic.MIN).getNumeric(), EPSILON);
			assertEquals(Double.POSITIVE_INFINITY, stats.get(Statistic.MAX).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.MEAN).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.VAR).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.SD).getNumeric(), EPSILON);
		}

		@Test
		public void testMinimumBound() {
			// -Double.MAX_VALUE + -Double.MAX_VALUE = Double.NEGATIVE_INFINITY
			Table table = Builders.newTableBuilder(100)
					.addReal("real", i -> -Double.MAX_VALUE)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("real"),
					EnumSet.of(Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			double min = stats.get(Statistic.MIN).getNumeric();
			double max = stats.get(Statistic.MAX).getNumeric();
			double mean = stats.get(Statistic.MEAN).getNumeric();

			assertEquals(-Double.MAX_VALUE, min, EPSILON);
			assertEquals(-Double.MAX_VALUE, max, EPSILON);
			assertTrue(Double.isNaN(mean) || (mean >= min && max >= mean));
		}

		@Test
		public void testMaximumBound() {
			// Double.MAX_VALUE + Double.MAX_VALUE = Double.POSITIVE_INFINITY
			Table table = Builders.newTableBuilder(100)
					.addReal("real", i -> Double.MAX_VALUE)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("real"),
					EnumSet.of(Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			double min = stats.get(Statistic.MIN).getNumeric();
			double max = stats.get(Statistic.MAX).getNumeric();
			double mean = stats.get(Statistic.MEAN).getNumeric();

			assertEquals(Double.MAX_VALUE, min, EPSILON);
			assertEquals(Double.MAX_VALUE, max, EPSILON);
			assertTrue(Double.isNaN(mean) || (mean >= min && max >= mean));
		}

		@Test
		public void testZeroCounts() {
			Random rng = new Random(2874564387L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(0, stats.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.MIN).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.MAX).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.MEAN).getNumeric(), EPSILON);
		}

		@Test
		public void testSingleCounts() {
			Random rng = new Random(28376549743L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("real");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(stats.get(Statistic.COUNT), Statistics.compute(column, Statistic.COUNT, CTX));
			assertEquals(stats.get(Statistic.MIN), Statistics.compute(column, Statistic.MIN, CTX));
			assertEquals(stats.get(Statistic.MAX), Statistics.compute(column, Statistic.MAX, CTX));
			assertEquals(stats.get(Statistic.MEAN), Statistics.compute(column, Statistic.MEAN, CTX));
		}

		@Test
		public void testDeviation() {
			Random rng = new Random(1237564L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i < 100 ? i + 1 : Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(841.666666666666629, stats.get(Statistic.VAR).getNumeric(), EPSILON);
			assertEquals(29.011491975882016, stats.get(Statistic.SD).getNumeric(), EPSILON);
		}

		@Test
		public void testZeroDeviation() {
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i % 3 == 0 ? Double.NaN : 42.0)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("real"),
					EnumSet.of(Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(0.0, stats.get(Statistic.VAR).getNumeric(), EPSILON);
			assertEquals(0.0, stats.get(Statistic.SD).getNumeric(), EPSILON);
		}

		@Test
		public void testDeviationOfOneValue() {
			Random rng = new Random(74624957643L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i == 42 ? 42.0 : Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(Double.NaN, stats.get(Statistic.VAR).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.SD).getNumeric(), EPSILON);
		}

		@Test
		public void testDeviationOfTwoValues() {
			Random rng = new Random(228472L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> {
						if (i == 42) {
							return 5.0;
						} else if (i == N - 42) {
							return 10.0;
						} else {
							return Double.NaN;
						}
					})
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(12.5, stats.get(Statistic.VAR).getNumeric(), EPSILON);
			assertEquals(3.535533905932738, stats.get(Statistic.SD).getNumeric(), EPSILON);
		}

		@Test
		public void testDeviationOfZeroValues() {
			Random rng = new Random(992748482L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(Double.NaN, stats.get(Statistic.VAR).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.SD).getNumeric(), EPSILON);
		}


		@Test
		public void testSingleDeviation() {
			Random rng = new Random(73293877L);
			Table table = Builders.newTableBuilder(N)
					.addReal("real", i -> i % 3 == 0 ? Double.NaN : i)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("real");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.VAR, Statistic.SD),
					CTX);

			assertEquals(stats.get(Statistic.VAR), Statistics.compute(column, Statistic.VAR, CTX));
			assertEquals(stats.get(Statistic.SD), Statistics.compute(column, Statistic.SD, CTX));
		}

		@Test
		public void testInterpolatedPercentiles() {
			Random rng = new Random(22837009L);
			Table table = Builders.newTableBuilder(100)
					.addReal("real", i -> i + 1)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(25.25, stats.get(Statistic.P25).getNumeric(), EPSILON);
			assertEquals(50.5, stats.get(Statistic.P50).getNumeric(), EPSILON);
			assertEquals(75.75, stats.get(Statistic.P75).getNumeric(), EPSILON);
			assertEquals(stats.get(Statistic.P50).getNumeric(), stats.get(Statistic.MEDIAN).getNumeric(), EPSILON);
		}

		@Test
		public void testExistingPercentiles() {
			Random rng = new Random(237567878273L);
			Table table = Builders.newTableBuilder(403)
					.addReal("real", i -> i + 1)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(101.0, stats.get(Statistic.P25).getNumeric(), EPSILON);
			assertEquals(202.0, stats.get(Statistic.P50).getNumeric(), EPSILON);
			assertEquals(303.0, stats.get(Statistic.P75).getNumeric(), EPSILON);
			assertEquals(stats.get(Statistic.P50).getNumeric(), stats.get(Statistic.MEDIAN).getNumeric(), EPSILON);
		}

		@Test
		public void testPercentilesWithoutValues() {
			Table table = Builders.newTableBuilder(400)
					.addReal("real", i -> Double.NaN)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("real"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(Double.NaN, stats.get(Statistic.P25).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.P50).getNumeric(), EPSILON);
			assertEquals(Double.NaN, stats.get(Statistic.P75).getNumeric(), EPSILON);
			assertEquals(stats.get(Statistic.P50).getNumeric(), stats.get(Statistic.MEDIAN).getNumeric(), EPSILON);
		}

		@Test
		public void testPercentilesWithSingleValue() {
			Table table = Builders.newTableBuilder(100)
					.addReal("real", i -> i == 42 ? 42.0 : Double.NaN)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("real"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(42.0, stats.get(Statistic.P25).getNumeric(), EPSILON);
			assertEquals(42.0, stats.get(Statistic.P50).getNumeric(), EPSILON);
			assertEquals(42.0, stats.get(Statistic.P75).getNumeric(), EPSILON);
			assertEquals(stats.get(Statistic.P50).getNumeric(), stats.get(Statistic.MEDIAN).getNumeric(), EPSILON);
		}

		@Test
		public void testPercentilesWithMissingValues() {
			Random rng = new Random(29845294876L);
			Table table = Builders.newTableBuilder(150)
					.addReal("real", i -> i < 100 ? i + 1 : Double.NaN)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("real"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(25.25, stats.get(Statistic.P25).getNumeric(), EPSILON);
			assertEquals(50.5, stats.get(Statistic.P50).getNumeric(), EPSILON);
			assertEquals(75.75, stats.get(Statistic.P75).getNumeric(), EPSILON);
			assertEquals(stats.get(Statistic.P50).getNumeric(), stats.get(Statistic.MEDIAN).getNumeric(), EPSILON);
		}

		@Test
		public void testSinglePercentiles() {
			Random rng = new Random(294889767L);
			Table table = Builders.newTableBuilder(100)
					.addReal("real", i -> i + 1)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("real");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(stats.get(Statistic.P25), Statistics.compute(column, Statistic.P25, CTX));
			assertEquals(stats.get(Statistic.P50), Statistics.compute(column, Statistic.P50, CTX));
			assertEquals(stats.get(Statistic.P75), Statistics.compute(column, Statistic.P75, CTX));
			assertEquals(stats.get(Statistic.MEDIAN), Statistics.compute(column, Statistic.MEDIAN, CTX));
		}

	}

	public static class Nominal {

		@Test
		public void testEmpty() {
			Table table = Builders.newTableBuilder(50)
					.addNominal("categories", i -> "foo")
					.build(CTX);
			assertTrue(Statistics.compute(table.column("categories"), Collections.emptySet(), CTX).isEmpty());
		}

		@Test
		public void testCounts() {
			Random rng = new Random(2382717L);
			List<String> dictionary = new ArrayList<>(4);
			dictionary.add(null);
			dictionary.add("one");
			dictionary.add("two");
			dictionary.add("three");
			dictionary.add("unused");

			int[] indices = new int[N];
			Arrays.setAll(indices, i -> {
				if (i < 10) {
					return 1;
				} else if (i < 30) {
					return 2;
				} else if (i < 70) {
					return 3;
				} else {
					return 0;
				}
			});

			Table table = Builders.newTableBuilder(N)
					.add("categories", ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices,
							dictionary))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("categories");

			Map<Statistic, Result> multiple = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE),
					CTX);

			assertEquals(70, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(10, multiple.get(Statistic.LEAST).getNumeric(), EPSILON);
			assertEquals("one", multiple.get(Statistic.LEAST).getObject(String.class));
			assertEquals(1, multiple.get(Statistic.LEAST).getCategorical());
			assertEquals(40, multiple.get(Statistic.MODE).getNumeric(), EPSILON);
			assertEquals("three", multiple.get(Statistic.MODE).getObject(String.class));
			assertEquals(3, multiple.get(Statistic.MODE).getCategorical());
		}

		@Test
		public void testAllCounts() {
			Random rng = new Random(2382717L);
			List<String> dictionary = new ArrayList<>(4);
			dictionary.add(null);
			dictionary.add("one");
			dictionary.add("two");
			dictionary.add("three");
			dictionary.add("unused");

			int[] indices = new int[N];
			Arrays.setAll(indices, i -> {
				if (i < 10) {
					return 1;
				} else if (i < 30) {
					return 2;
				} else if (i < 70) {
					return 3;
				} else {
					return 0;
				}
			});

			Table table = Builders.newTableBuilder(N)
					.add("categories", ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices,
							dictionary))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("categories");

			Map<Statistic, Result> multiple = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE, Statistic.INDEX_COUNTS),
					CTX);

			assertEquals(70, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(10, multiple.get(Statistic.LEAST).getNumeric(), EPSILON);
			assertEquals("one", multiple.get(Statistic.LEAST).getObject(String.class));
			assertEquals(1, multiple.get(Statistic.LEAST).getCategorical());
			assertEquals(40, multiple.get(Statistic.MODE).getNumeric(), EPSILON);
			assertEquals("three", multiple.get(Statistic.MODE).getObject(String.class));
			assertEquals(3, multiple.get(Statistic.MODE).getCategorical());
			assertEquals(0, multiple.get(Statistic.INDEX_COUNTS).getCategorical());
			assertEquals(Double.NaN, multiple.get(Statistic.INDEX_COUNTS).getNumeric(), 0);
			Object indexCounts = multiple.get(Statistic.INDEX_COUNTS).getObject();
			assertTrue(indexCounts instanceof Statistics.CategoricalIndexCounts);
			Statistics.CategoricalIndexCounts counts = (Statistics.CategoricalIndexCounts) indexCounts;
			assertEquals(N - 70, counts.countForIndex(0));
			assertEquals(10, counts.countForIndex(1));
			assertEquals(20, counts.countForIndex(2));
			assertEquals(40, counts.countForIndex(3));
			assertEquals(0, counts.countForIndex(4));
			assertEquals(0, counts.countForIndex(5));
		}


		@Test
		public void testZeroCounts() {
			Table table = Builders.newTableBuilder(N)
					.addNominal("categories", i -> null)
					.build(CTX);

			Column column = table.column("categories");

			Map<Statistic, Result> multiple = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE),
					CTX);

			assertEquals(0, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(0, multiple.get(Statistic.COUNT).getCategorical());
			assertNull(multiple.get(Statistic.COUNT).getObject());

			assertEquals(0, multiple.get(Statistic.LEAST).getNumeric(), EPSILON);
			assertEquals(0, multiple.get(Statistic.LEAST).getCategorical());
			assertNull(multiple.get(Statistic.LEAST).getObject());

			assertEquals(0, multiple.get(Statistic.MODE).getNumeric(), EPSILON);
			assertEquals(0, multiple.get(Statistic.MODE).getCategorical());
			assertNull(multiple.get(Statistic.MODE).getObject());
		}

		@Test
		public void testSingleCounts() {
			Random rng = new Random(98287L);
			List<String> dictionary = new ArrayList<>(4);
			dictionary.add(null);
			dictionary.add("one");
			dictionary.add("two");
			dictionary.add("three");

			int[] indices = new int[N];
			Arrays.setAll(indices, i -> {
				if (i < 10) {
					return 1;
				} else if (i < 30) {
					return 2;
				} else if (i < 70) {
					return 3;
				} else {
					return 0;
				}
			});

			Table table = Builders.newTableBuilder(N)
					.add("categories", ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices,
							dictionary))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("categories");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE),
					CTX);

			assertEquals(stats.get(Statistic.COUNT), Statistics.compute(column, Statistic.COUNT, CTX));
			assertEquals(stats.get(Statistic.LEAST), Statistics.compute(column, Statistic.LEAST, CTX));
			assertEquals(stats.get(Statistic.MODE), Statistics.compute(column, Statistic.MODE, CTX));
		}

	}

	public static class ObjectColumn {

		@Test
		public void testEmpty() {
			Table table = Builders.newTableBuilder(50)
					.addTextset("custom", i -> new StringSet(null))
					.build(CTX);
			assertTrue(Statistics.compute(table.column("custom"), Collections.emptySet(), CTX).isEmpty());
		}

		@Test
		public void testCount() {
			Random rng = new Random(1109920L);
			ObjectBuffer<String> buffer = BufferAccessor.get().newObjectBuffer(ColumnType.TEXT, N);
			for (int i = 0; i < 100; i++) {
				buffer.set(i, String.valueOf(i));
			}

			Table table = Builders.newTableBuilder(N)
					.add("freetext", buffer.toColumn())
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("freetext");

			Result single = Statistics.compute(column, Statistic.COUNT, CTX);
			Map<Statistic, Result> multiple = Statistics.compute(column, EnumSet.of(Statistic.COUNT), CTX);

			assertEquals(100, single.getNumeric(), EPSILON);
			assertEquals(100, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
		}

		@Test
		public void testZeroCount() {
			ObjectBuffer<String> buffer = BufferAccessor.get().newObjectBuffer(ColumnType.TEXT, N);

			Table table = Builders.newTableBuilder(N)
					.add("freetext", buffer.toColumn())
					.build(CTX);

			Column column = table.column("freetext");

			Result single = Statistics.compute(column, Statistic.COUNT, CTX);
			Map<Statistic, Result> multiple = Statistics.compute(column, EnumSet.of(Statistic.COUNT), CTX);

			assertEquals(0, single.getNumeric(), EPSILON);
			assertEquals(0, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
		}

	}

	public static class Categorical {

		private static final ColumnType<String> CUSTOM_NOMINAL = ColumnTestUtils.categoricalType(String.class, null);

		@Test
		public void testEmpty() {
			Table table = Builders.newTableBuilder(50)
					.addNominal("categories", i -> "foo")
					.build(CTX);
			assertTrue(Statistics.compute(table.column("categories"), Collections.emptySet(), CTX).isEmpty());
		}

		@Test
		public void testCounts() {
			Random rng = new Random(900072911L);
			List<String> dictionary = new ArrayList<>(5);
			dictionary.add(null);
			dictionary.add("one");
			dictionary.add("two");
			dictionary.add("three");

			int[] indices = new int[N];
			Arrays.setAll(indices, i -> {
				if (i < 10) {
					return 1;
				} else if (i < 30) {
					return 2;
				} else if (i < 70) {
					return 3;
				} else {
					return 0;
				}
			});

			Table table = Builders.newTableBuilder(N)
					.add("categories", ColumnAccessor.get().newCategoricalColumn(CUSTOM_NOMINAL, indices,
							dictionary))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("categories");

			Map<Statistic, Result> multiple = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE),
					CTX);

			assertEquals(70, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(10, multiple.get(Statistic.LEAST).getNumeric(), EPSILON);
			assertEquals("one", multiple.get(Statistic.LEAST).getObject());
			assertEquals(1, multiple.get(Statistic.LEAST).getCategorical());
			assertEquals(40, multiple.get(Statistic.MODE).getNumeric(), EPSILON);
			assertEquals("three", multiple.get(Statistic.MODE).getObject());
			assertEquals(3, multiple.get(Statistic.MODE).getCategorical());
		}

		@Test
		public void testZeroCounts() {
			NominalBuffer buffer = Buffers.nominalBuffer(N);

			Table table = Builders.newTableBuilder(N)
					.add("custom", buffer.toColumn())
					.build(CTX);

			Column column = table.column("custom");

			Map<Statistic, Result> multiple = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE),
					CTX);

			assertEquals(0, multiple.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertNull(multiple.get(Statistic.LEAST).getObject());
			assertEquals(0, multiple.get(Statistic.LEAST).getCategorical());
			assertNull(multiple.get(Statistic.MODE).getObject());
			assertEquals(0, multiple.get(Statistic.MODE).getCategorical());
		}

		@Test
		public void testSingleCounts() {
			Random rng = new Random(77888297L);
			List<String> dictionary = new ArrayList<>(4);
			dictionary.add(null);
			dictionary.add("one");
			dictionary.add("two");
			dictionary.add("three");

			int[] indices = new int[N];
			Arrays.setAll(indices, i -> {
				if (i < 10) {
					return 1;
				} else if (i < 30) {
					return 2;
				} else if (i < 70) {
					return 3;
				} else {
					return 0;
				}
			});

			Table table = Builders.newTableBuilder(N)
					.add("custom", ColumnAccessor.get().newCategoricalColumn(CUSTOM_NOMINAL, indices, dictionary))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("custom");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.LEAST, Statistic.MODE),
					CTX);

			assertEquals(stats.get(Statistic.COUNT), Statistics.compute(column, Statistic.COUNT, CTX));
			assertEquals(stats.get(Statistic.LEAST), Statistics.compute(column, Statistic.LEAST, CTX));
			assertEquals(stats.get(Statistic.MODE), Statistics.compute(column, Statistic.MODE, CTX));
		}

	}

	public static class Time {

		private static final long MINUTE = 60_000_000_000L;
		private static final long HOUR = 3_600_000_000_000L;

		@Test
		public void testEmpty() {
			Table table = Builders.newTableBuilder(50)
					.addTime("time", i -> LocalTime.MIDNIGHT)
					.build(CTX);
			assertTrue(Statistics.compute(table.column("time"), Collections.emptySet(), CTX).isEmpty());
		}

		@Test
		public void testCounts() {
			Random rng = new Random(551723L);
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> i < 100 ? LocalTime.ofNanoOfDay(i * MINUTE) : null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("time"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(100, stats.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(LocalTime.ofNanoOfDay(0), stats.get(Statistic.MIN).getObject(LocalTime.class));
			assertEquals(LocalTime.ofNanoOfDay(99 * MINUTE), stats.get(Statistic.MAX).getObject(LocalTime.class));
			assertEquals(LocalTime.ofNanoOfDay((long) (49.5 * MINUTE)),
					stats.get(Statistic.MEAN).getObject(LocalTime.class));
		}

		@Test
		public void testZeroCounts() {
			Random rng = new Random(7812648738164L);
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("time"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(0, stats.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertNull(stats.get(Statistic.MIN).getObject());
			assertNull(stats.get(Statistic.MAX).getObject());
			assertNull(stats.get(Statistic.MEAN).getObject());
		}

		@Test
		public void testSingleCounts() {
			Random rng = new Random(14560240562L);
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> i < 100 ? LocalTime.ofNanoOfDay(i * MINUTE) : null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("time");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX, Statistic.MEAN),
					CTX);

			assertEquals(stats.get(Statistic.COUNT), Statistics.compute(column, Statistic.COUNT, CTX));
			assertEquals(stats.get(Statistic.MIN), Statistics.compute(column, Statistic.MIN, CTX));
			assertEquals(stats.get(Statistic.MAX), Statistics.compute(column, Statistic.MAX, CTX));
			assertEquals(stats.get(Statistic.MEAN), Statistics.compute(column, Statistic.MEAN, CTX));
		}

		@Test
		public void testZeroDeviation() {
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> i % 3 == 0 ? null : LocalTime.MIDNIGHT)
					.build(CTX);

			Result result = Statistics.compute(table.column("time"), Statistic.SD, CTX);
			assertEquals(LocalTime.ofNanoOfDay(0), result.getObject(LocalTime.class));
		}

		@Test
		public void testDeviationOfZeroValues() {
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> null)
					.build(CTX);

			Result result = Statistics.compute(table.column("time"), Statistic.SD, CTX);
			assertNull(result.getObject());
		}

		@Test
		public void testDeviationOfOneValue() {
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> i == 42 ? LocalTime.NOON : null)
					.build(CTX);

			Result result = Statistics.compute(table.column("time"), Statistic.SD, CTX);
			assertNull(result.getObject());
		}

		@Test
		public void testDeviationOfTwoValues() {
			Random rng = new Random(555353098L);
			LocalTime onepm = LocalTime.of(13, 0, 0);
			LocalTime threepm = LocalTime.of(15, 0, 0);

			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> {
						if (i == 42) {
							return onepm;
						} else if (i == N - 42) {
							return threepm;
						} else {
							return null;
						}
					})
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Result result = Statistics.compute(shuffled.column("time"), Statistic.SD, CTX);
			assertEquals(LocalTime.ofNanoOfDay((long) (1.414213562373095 * HOUR)), result.getObject(LocalTime.class));
		}


		@Test
		public void testSingleDeviation() {
			Random rng = new Random(1429983L);
			Table table = Builders.newTableBuilder(N)
					.addTime("time", i -> i < 60 ? LocalTime.of(12, i, 0) : null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("time");

			Map<Statistic, Result> stats = Statistics.compute(column, EnumSet.of(Statistic.SD), CTX);
			Result result = Statistics.compute(shuffled.column("time"), Statistic.SD, CTX);

			assertEquals(stats.get(Statistic.SD), result);
		}

		@Test
		public void testInterpolatedPercentiles() {
			Random rng = new Random(23576876L);
			Table table = Builders.newTableBuilder(60)
					.addTime("time", i -> LocalTime.of(8, i))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("time"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(LocalTime.of(8, 14, 15), stats.get(Statistic.P25).getObject(LocalTime.class));
			assertEquals(LocalTime.of(8, 29, 30), stats.get(Statistic.P50).getObject(LocalTime.class));
			assertEquals(LocalTime.of(8, 44, 45), stats.get(Statistic.P75).getObject(LocalTime.class));
			assertEquals(stats.get(Statistic.P50), stats.get(Statistic.MEDIAN));
		}

		@Test
		public void testExistingPercentiles() {
			Random rng = new Random(5564766L);
			Table table = Builders.newTableBuilder(31)
					.addTime("time", i -> LocalTime.of(17, i))
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("time"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(LocalTime.of(17, 7), stats.get(Statistic.P25).getObject(LocalTime.class));
			assertEquals(LocalTime.of(17, 15), stats.get(Statistic.P50).getObject(LocalTime.class));
			assertEquals(LocalTime.of(17, 23), stats.get(Statistic.P75).getObject(LocalTime.class));
			assertEquals(stats.get(Statistic.P50), stats.get(Statistic.MEDIAN));
		}

		@Test
		public void testPercentilesWithoutValues() {
			Table table = Builders.newTableBuilder(400)
					.addTime("time", i -> null)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("time"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertNull(stats.get(Statistic.P25).getObject());
			assertNull(stats.get(Statistic.P50).getObject());
			assertNull(stats.get(Statistic.P75).getObject());
			assertEquals(stats.get(Statistic.P50), stats.get(Statistic.MEDIAN));
		}

		@Test
		public void testPercentilesWithSingleValue() {
			Table table = Builders.newTableBuilder(100)
					.addTime("time", i -> i == 42 ? LocalTime.NOON : null)
					.build(CTX);

			Map<Statistic, Result> stats = Statistics.compute(table.column("time"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(LocalTime.NOON, stats.get(Statistic.P25).getObject(LocalTime.class));
			assertEquals(LocalTime.NOON, stats.get(Statistic.P50).getObject(LocalTime.class));
			assertEquals(LocalTime.NOON, stats.get(Statistic.P75).getObject(LocalTime.class));
			assertEquals(stats.get(Statistic.P50), stats.get(Statistic.MEDIAN));
		}

		@Test
		public void testPercentilesWithMissingValues() {
			Random rng = new Random(223008080L);
			Table table = Builders.newTableBuilder(100)
					.addTime("time", i -> i < 60 ? LocalTime.of(8, i) : null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("time"),
					EnumSet.of(Statistic.P25, Statistic.P50, Statistic.P75, Statistic.MEDIAN),
					CTX);

			assertEquals(4, stats.size());
			assertEquals(LocalTime.of(8, 14, 15), stats.get(Statistic.P25).getObject(LocalTime.class));
			assertEquals(LocalTime.of(8, 29, 30), stats.get(Statistic.P50).getObject(LocalTime.class));
			assertEquals(LocalTime.of(8, 44, 45), stats.get(Statistic.P75).getObject(LocalTime.class));
			assertEquals(stats.get(Statistic.P50), stats.get(Statistic.MEDIAN));
		}

	}

	public static class DateTime {

		private static final long PSEUDO_MONTH = 2_592_000;

		@Test
		public void testEmpty() {
			Table table = Builders.newTableBuilder(50)
					.addDateTime("datetime", i -> Instant.now())
					.build(CTX);
			assertTrue(Statistics.compute(table.column("datetime"), Collections.emptySet(), CTX).isEmpty());
		}

		@Test
		public void testCounts() {
			Random rng = new Random(1233217L);
			Table table = Builders.newTableBuilder(N)
					.addDateTime("datetime", i -> i < 100 ? Instant.ofEpochSecond(i * PSEUDO_MONTH) : null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("datetime"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX),
					CTX);

			assertEquals(100, stats.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertEquals(Instant.ofEpochSecond(0), stats.get(Statistic.MIN).getObject(Instant.class));
			assertEquals(Instant.ofEpochSecond(99 * PSEUDO_MONTH), stats.get(Statistic.MAX).getObject(Instant.class));
		}

		@Test
		public void testZeroCounts() {
			Random rng = new Random(6677134153840L);
			Table table = Builders.newTableBuilder(N)
					.addDateTime("datetime", i -> null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);

			Map<Statistic, Result> stats = Statistics.compute(shuffled.column("datetime"),
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX),
					CTX);

			assertEquals(0, stats.get(Statistic.COUNT).getNumeric(), EPSILON);
			assertNull(stats.get(Statistic.MIN).getObject());
			assertNull(stats.get(Statistic.MAX).getObject());
		}

		@Test
		public void testSingleCounts() {
			Random rng = new Random(23649L);
			Table table = Builders.newTableBuilder(N)
					.addDateTime("datetime", i -> i < 100 ? Instant.ofEpochSecond(i * PSEUDO_MONTH) : null)
					.build(CTX);

			Table shuffled = table.map(permutation(rng, table.height()), false);
			Column column = shuffled.column("datetime");

			Map<Statistic, Result> stats = Statistics.compute(column,
					EnumSet.of(Statistic.COUNT, Statistic.MIN, Statistic.MAX),
					CTX);

			assertEquals(stats.get(Statistic.COUNT), Statistics.compute(column, Statistic.COUNT, CTX));
			assertEquals(stats.get(Statistic.MIN), Statistics.compute(column, Statistic.MIN, CTX));
			assertEquals(stats.get(Statistic.MAX), Statistics.compute(column, Statistic.MAX, CTX));

		}

	}

	public static class Validation {

		@Test(expected = NullPointerException.class)
		public void testSupportedNullColumn() {
			Statistics.supported(null, Statistic.COUNT);
		}

		@Test(expected = NullPointerException.class)
		public void testSupportedNullStatistic() {
			Column column = Buffers.realBuffer(100).toColumn();
			Statistics.supported(column, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnSingle() {
			Statistics.compute(null, Statistic.COUNT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnMultiple() {
			Statistics.compute(null, EnumSet.of(Statistic.COUNT), CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullStatisticSingle() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[0]);
			Statistics.compute(column, (Statistic) null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullStatisticMultiple() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[0]);
			Statistics.compute(column, (Set<Statistic>) null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextSingle() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[0]);
			Statistics.compute(column, Statistic.COUNT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextMultiple() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[0]);
			Statistics.compute(column, EnumSet.of(Statistic.COUNT), null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedStatisticSingle() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[0]);
			Statistics.compute(column, Statistic.LEAST, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedStatisticMultiple() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[0]);
			Statistics.compute(column, EnumSet.of(Statistic.LEAST), CTX);
		}

	}

	@RunWith(Parameterized.class)
	public static class Supported {

		@Parameter
		public Column implementation;

		@Parameter(value = 1)
		public String name;

		@Parameters(name = "{1}")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(
					new Object[]{Buffers.realBuffer(100).toColumn(), "real"},
					new Object[]{Buffers.integer53BitBuffer(100).toColumn(), "integer"},
					new Object[]{Buffers.<String>nominalBuffer(100).toColumn(), "nominal"},
					new Object[]{Buffers.dateTimeBuffer(100, false).toColumn(), "datetime"},
					new Object[]{Buffers.timeBuffer(100).toColumn(), "time"}
			);
		}

		@Test
		public void testSupported() {
			for (Statistic statistic: Statistic.values()) {
				if (Statistics.supported(implementation, statistic)) {
					Result single = Statistics.compute(implementation, statistic, CTX);
					Result singletonSet = Statistics.compute(implementation, EnumSet.of(statistic), CTX).get(statistic);
					assertNotNull(single);
					assertNotNull(singletonSet);
					assertEquals(single, singletonSet);
				}
			}
		}

		@Test
		public void testUnsupported() {
			int expectedExceptions = 0;
			int exceptionCount = 0;
			for (Statistic statistic: Statistic.values()) {
				if (!Statistics.supported(implementation, statistic)) {
					expectedExceptions++;
					try {
						Statistics.compute(implementation, statistic, CTX);
					} catch (UnsupportedOperationException e) {
						exceptionCount++;
					}
				}
			}
			assertTrue(exceptionCount > 0);
			assertEquals(expectedExceptions, exceptionCount);
		}

	}

}
