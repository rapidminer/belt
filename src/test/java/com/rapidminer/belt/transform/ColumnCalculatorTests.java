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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.execution.Context;


/**
 * Tests all implementations of {@link Calculator}.
 *
 * @author Gisa Meier
 */
@RunWith(Parameterized.class)
public class ColumnCalculatorTests {

	private static final double EPSILON = 1e-10;

	private static final Context CTX = Belt.defaultContext();


	private static double[] readBufferToArray(NumericBuffer buffer) {
		double[] data = new double[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}


	@Parameter
	public CalculatorTest test;

	@Parameters(name = "{0}")
	public static Iterable<CalculatorTest<?>> tests() {
		List<CalculatorTest<?>> tests = new ArrayList<>(5);

		tests.add(new CalculatorTest<>("reduce_one", (data, data2) ->
				new NumericColumnReducer<>(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), () -> new double[2],
						(t, d) -> {
							t[0] += d;
							t[1] += 1;
						},
						(t1, t2) -> {
							t1[0] += t2[0];
							t1[1] += t2[1];
						}),
				(data, data2) -> Arrays.stream(data).collect(() -> new double[2],
						(t, d) -> {
							t[0] += d;
							t[1] += 1;
						},
						(t1, t2) -> {
							t1[0] += t2[0];
							t1[1] += t2[1];
						}), Calculator::getResult));

		tests.add(new CalculatorTest<>("reduce_one_not_comm", (data, data2) ->
				new NumericColumnReducer<>(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), () -> new double[1],
						(t, d) -> {
							t[0] = d;
						},
						(t1, t2) -> {
							t1[0] = t2[0];
						}),
				(data, data2) -> new double[]{data[data.length - 1]}, Calculator::getResult));

		tests.add(new CalculatorTest<>("map_one",
				(data, data2) -> new ApplierNumericToNumeric(ColumnTestUtils.getNumericColumn(TypeId.REAL, data),
						i -> 2 * i, false),
				(data, data2) -> {
					double[] expected = new double[data.length];
					Arrays.setAll(expected, i -> 2 * data[i]);
					return expected;
				}, calc -> readBufferToArray(calc.getResult())));

		tests.add(new CalculatorTest<>("map_more",
				(data, data2) -> new ApplierNNumericToNumeric(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), ColumnTestUtils.getNumericColumn(TypeId.REAL, data2)),
						row -> 2 * row.get(0) + row.get(1), false),
				(data, data2) -> {
					double[] expected = new double[data.length];
					Arrays.setAll(expected, i -> 2 * data[i] + data2[i]);
					return expected;
				}, calc -> readBufferToArray(calc.getResult())));

		tests.add(new CalculatorTest<>("reduce_double", (data, data2) ->
				new NumericColumnReducerDouble(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), 0, Double::sum),
				(data, data2) -> {
					double result = Arrays.stream(data).sum();
					return new double[]{result};
				},
				calc -> {
					double result = calc.getResult();
					return new double[]{result};
				}));

		tests.add(new CalculatorTest<>("reduce_double_combiner", (data, data2) ->
				new NumericColumnReducerDouble(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), 0,
						(count, d) -> d > 0.5 ? count + 1 : count, (count1, count2) -> count1 + count2),
				(data, data2) -> {
					double result = Arrays.stream(data).reduce(0, (count, d) -> d > 0.5 ? count + 1 : count);
					return new double[]{result};
				},
				calc -> {
					double result = calc.getResult();
					return new double[]{result};
				}));

		tests.add(new CalculatorTest<>("reduce_double_not_comm", (data, data2) ->
				new NumericColumnReducerDouble(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), 1000, (d, e) -> e),
				(data, data2) -> {
					double result = data[data.length - 1];
					return new double[]{result};
				},
				calc -> {
					double result = calc.getResult();
					return new double[]{result};
				}));

		tests.add(new CalculatorTest<>("reduce_more",
				(data, data2) -> new NumericColumnsReducer<>(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL,
						data), ColumnTestUtils.getNumericColumn(TypeId.REAL, data2)),
						() -> new double[2],
						(t, row) -> {
							t[0] += row.get(0) * row.get(1);
							t[1] += row.get(1) * 2;
						},
						(t1, t2) -> {
							t1[0] += t2[0];
							t1[1] += t2[1];
						}),
				(data, data2) -> {
					double[] expected = new double[data.length];
					Arrays.setAll(expected, i -> data[i] * data2[i]);
					double firstResult = Arrays.stream(expected).sum();
					double secondResult = Arrays.stream(data2).map(i -> i * 2).sum();
					return new double[]{firstResult, secondResult};
				}, Calculator::getResult));

		tests.add(new CalculatorTest<>("reduce_more_not_comm",
				(data, data2) -> new NumericColumnsReducer<>(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL,
						data), ColumnTestUtils.getNumericColumn(TypeId.REAL, data2)),
						() -> new double[1],
						(t, row) -> {
							t[0] = row.get(0) * row.get(1);
						},
						(t1, t2) -> {
							t1[0] = t2[0];
						}),
				(data, data2) -> new double[]{data[data.length - 1] * data2[data2.length - 1]}
				, Calculator::getResult));
		tests.add(new CalculatorTest<>("filter_one",
				(data, data2) -> new NumericColumnFilterer(ColumnTestUtils.getNumericColumn(TypeId.REAL, data), d -> d > 0.5),
				(data, data2) -> IntStream.range(0, data.length).filter(i -> data[i] > 0.5).mapToDouble(i -> i).toArray(),
				calc -> {
					int[] mapping = calc.getResult();
					double[] result = new double[mapping.length];
					Arrays.setAll(result, i -> mapping[i]);
					return result;
				}));
		tests.add(new CalculatorTest<>("filter_more",
				(data, data2) -> new NumericColumnsFilterer(Arrays.asList(ColumnTestUtils.getNumericColumn(TypeId.REAL,
						data), ColumnTestUtils.getNumericColumn(TypeId.REAL, data2)), row -> row.get(0) * row.get(1) > 0.3),
				(data, data2) -> IntStream.range(0, data.length).filter(i -> data[i] * data2[i] > 0.3).mapToDouble(i -> i).toArray(),
				calc -> {
					int[] mapping = calc.getResult();
					double[] result = new double[mapping.length];
					Arrays.setAll(result, i -> mapping[i]);
					return result;
				}));

		return tests;
	}

	public static class CalculatorTest<T> {
		final String name;
		final BiFunction<double[], double[], Calculator<T>> calculatorSupplier;
		final BiFunction<double[], double[], double[]> resultCalculator;
		final Function<Calculator<T>, double[]> resultInterpreter;


		private CalculatorTest(String name, BiFunction<double[], double[], Calculator<T>> calculatorSupplier,
							   BiFunction<double[], double[], double[]> resultCalculator,
							   Function<Calculator<T>, double[]> resultInterpreter) {
			this.name = name;
			this.calculatorSupplier = calculatorSupplier;
			this.resultCalculator = resultCalculator;
			this.resultInterpreter = resultInterpreter;
		}

		@Override
		public String toString() {
			return name;
		}
	}

	@Test
	public void testNumberOfOperations() {
		Random random = new Random();
		int size = random.nextInt(100);
		double[] data = new double[size];
		BiFunction<double[], double[], Calculator<?>> supplier = test.calculatorSupplier;
		Calculator<?> calculator = supplier.apply(data, data);
		assertEquals(size, calculator.getNumberOfOperations());
	}


	@Test
	public void testFullPart() {
		int size = 75;
		double[] data = random(size);
		double[] data2 = random(size);
		BiFunction<double[], double[], Calculator<?>> supplier = test.calculatorSupplier;
		Calculator<?> calculator = supplier.apply(data, data2);
		calculator.init(1);
		calculator.doPart(0, size, 0);

		Function<Calculator<?>, double[]> resultInterpreter = test.resultInterpreter;
		double[] result = resultInterpreter.apply(calculator);

		BiFunction<double[], double[], double[]> resultCalculator = test.resultCalculator;
		double[] expected = resultCalculator.apply(data, data2);

		assertArrayEquals(expected, result, EPSILON);
	}

	@Test
	public void testReducingSeveralParts() {
		int batches = 15;
		int factor = 5;
		int size = batches * factor;
		double[] data = random(size);
		double[] data2 = random(size);
		BiFunction<double[], double[], Calculator<?>> supplier = test.calculatorSupplier;
		Calculator<?> calculator = supplier.apply(data, data2);
		calculator.init(batches);
		for (int i = 0; i < size; i += factor) {
			calculator.doPart(i, i + factor, i / factor);
		}

		Function<Calculator<?>, double[]> resultInterpreter = test.resultInterpreter;
		double[] result = resultInterpreter.apply(calculator);

		BiFunction<double[], double[], double[]> resultCalculator = test.resultCalculator;
		double[] expected = resultCalculator.apply(data, data2);

		assertArrayEquals(expected, result, EPSILON);
	}

	@Test
	public void testReducingSeveralPartsOutOfOrder() {
		int batches = 15;
		int factor = 5;
		int size = batches * factor;
		double[] data = random(size);
		double[] data2 = random(size);
		BiFunction<double[], double[], Calculator<?>> supplier = test.calculatorSupplier;
		Calculator<?> calculator = supplier.apply(data, data2);
		calculator.init(batches);
		List<Integer> starts = new ArrayList<>(batches);
		for (int i = 0; i < size; i += factor) {
			starts.add(i);
		}
		Collections.shuffle(starts);
		for (int i : starts) {
			calculator.doPart(i, i + factor, i / factor);
		}

		Function<Calculator<?>, double[]> resultInterpreter = test.resultInterpreter;
		double[] result = resultInterpreter.apply(calculator);

		BiFunction<double[], double[], double[]> resultCalculator = test.resultCalculator;
		double[] expected = resultCalculator.apply(data, data2);

		assertArrayEquals(expected, result, EPSILON);
	}

	@Test
	public void testReducingSeveralPartsInParallel() throws InterruptedException, ExecutionException {
		ExecutorService pool = Executors.newWorkStealingPool();
		int batches = 15;
		int factor = 5;
		int size = batches * factor;
		double[] data = random(size);
		double[] data2 = random(size);
		BiFunction<double[], double[], Calculator<?>> supplier = test.calculatorSupplier;
		Calculator<?> calculator = supplier.apply(data, data2);
		calculator.init(batches);
		CountDownLatch startedLatch = new CountDownLatch(batches);
		CountDownLatch finishedLatch = new CountDownLatch(batches);
		for (int i = 0; i < size; i += factor) {
			int start = i;
			pool.submit(() -> {
				startedLatch.await();
				calculator.doPart(start, start + factor, start / factor);
				finishedLatch.countDown();
				return null;
			});
			startedLatch.countDown();
		}
		finishedLatch.await();
		Function<Calculator<?>, double[]> resultInterpreter = test.resultInterpreter;
		double[] result = resultInterpreter.apply(calculator);

		BiFunction<double[], double[], double[]> resultCalculator = test.resultCalculator;
		double[] expected = resultCalculator.apply(data, data2);

		assertArrayEquals(expected, result, EPSILON);
	}

}



