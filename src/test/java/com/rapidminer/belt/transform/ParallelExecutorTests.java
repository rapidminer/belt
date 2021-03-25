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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleConsumer;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionAbortedException;
import com.rapidminer.belt.execution.Workload;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ParallelExecutorTests {

	public static final int BATCH_SIZE_MEDIUM = ParallelExecutor.BATCH_SIZE_MEDIUM;

	private static final double EPSILON = 1e-10;

	private static final Context CTX = Belt.defaultContext();

	private static final String SEQUENTIAL = "sequential";
	private static final String EQUAL_PARTS = "equal_parts";
	private static final String IN_BATCHES = "in_batches";

	private static final DoubleConsumer NOOP_CALLBACK = i -> {};

	@RunWith(Parameterized.class)
	public static class AllSizes {

		@Parameter
		public String size;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(SEQUENTIAL, EQUAL_PARTS, IN_BATCHES);
		}

		private int number() {
			switch (size) {
				case SEQUENTIAL:
					return 101;
				case EQUAL_PARTS:
					return ParallelExecutor.THRESHOLD_PARALLEL_SMALL * CTX.getParallelism() + 17;
				case IN_BATCHES:
					return ParallelExecutor.BATCH_SIZE_SMALL *
							ParallelExecutor.THRESHOLD_FACTOR_EQUAL_PARTS * CTX.getParallelism() + 11;
				default:
					throw new IllegalStateException("Unknown size");
			}
		}

		private int expectedRuns() {
			switch (size) {
				case SEQUENTIAL:
					return 1;
				case EQUAL_PARTS:
					return CTX.getParallelism();
				case IN_BATCHES:
					return ParallelExecutor.THRESHOLD_FACTOR_EQUAL_PARTS * CTX.getParallelism() + 1;
				default:
					throw new IllegalStateException("Unknown size");
			}
		}

		@Test
		public void testDecision() throws Exception {
			Calculator<Integer> calculator = new Calculator<Integer>() {

				AtomicInteger called = new AtomicInteger();

				@Override
				public void init(int numberOfBatches) {
				}

				@Override
				public int getNumberOfOperations() {
					return number();
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					called.incrementAndGet();
				}

				@Override
				public Integer getResult() {
					return called.get();
				}
			};
			int result = new ParallelExecutor<>(calculator, Workload.SMALL, NOOP_CALLBACK).execute(CTX);
			assertEquals(expectedRuns(), result);
		}

		@Test(expected = ExceptionInUserCode.class)
		public void testExceptionPropagation() throws Exception {
			Calculator<Integer> calculator = new Calculator<Integer>() {

				@Override
				public void init(int numberOfBatches) {
				}

				@Override
				public int getNumberOfOperations() {
					return number();
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					throw new ExceptionInUserCode();
				}

				@Override
				public Integer getResult() {
					return 42;
				}
			};
			new ParallelExecutor<>(calculator, Workload.SMALL, NOOP_CALLBACK).execute(CTX);
		}

		private static class ExceptionInUserCode extends RuntimeException {
			// Placeholder for runtime exception thrown in user code
		}
	}

	@RunWith(Parameterized.class)
	public static class EveryOperationDone {

		@Parameter
		public Workload workload;

		@Parameters(name = "{0}")
		public static Iterable<Workload> workloads() {
			return Arrays.asList(Workload.values());
		}

		private int getThreshold() {
			switch (workload) {
				case HUGE:
					return ParallelExecutor.THRESHOLD_PARALLEL_HUGE;
				case LARGE:
					return ParallelExecutor.THRESHOLD_PARALLEL_LARGE;
				case SMALL:
					return ParallelExecutor.THRESHOLD_PARALLEL_SMALL;
				case MEDIUM:
				case DEFAULT:
				default:
					return ParallelExecutor.THRESHOLD_PARALLEL_MEDIUM;
			}
		}

		private int getBatchSize() {
			switch (workload) {
				case HUGE:
					return ParallelExecutor.BATCH_SIZE_HUGE;
				case LARGE:
					return ParallelExecutor.BATCH_SIZE_LARGE;
				case SMALL:
					return ParallelExecutor.BATCH_SIZE_SMALL;
				case MEDIUM:
				case DEFAULT:
				default:
					return ParallelExecutor.BATCH_SIZE_MEDIUM;
			}
		}

		@Test
		public void testSeparationInParts() throws Exception {
			int nTasks = CTX.getParallelism();
			final int size = getThreshold() * nTasks + 17;
			boolean[] done = new boolean[size];
			AtomicInteger counter = new AtomicInteger();
			int[] initialized = new int[1];
			List<Integer> batchIndices = Collections.synchronizedList(new ArrayList<>());
			Calculator<List<Integer>> calculator
					= new Calculator<List<Integer>>() {

				private List<Integer> partSizes = Collections.synchronizedList(new ArrayList<>());

				@Override
				public void init(int numberOfBatches) {
					initialized[0] = numberOfBatches;
				}

				@Override
				public int getNumberOfOperations() {
					return size;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					partSizes.add(to - from);
					batchIndices.add(batchIndex);
					for (int i = from; i < to; i++) {
						done[i] = true;
						counter.incrementAndGet();
					}
				}

				@Override
				public List<Integer> getResult() {
					return partSizes;
				}
			};

			List<Integer> partSizes = new ParallelExecutor<>(calculator, workload,
					NOOP_CALLBACK).execute(CTX);

			//test number of parts
			assertEquals(initialized[0], partSizes.size());
			assertTrue(initialized[0] <= nTasks);
			// test every index was done
			boolean[] expectedDone = new boolean[size];
			Arrays.fill(expectedDone, true);
			assertArrayEquals(expectedDone, done);
			//test no index was done twice
			assertEquals(size, counter.get());
			//test blocks of similar size
			Collections.sort(partSizes);
			int last = partSizes.get(partSizes.size() - 1);
			for (int i = 1; i < partSizes.size() - 1; i++) {
				assertEquals(last, (int) partSizes.get(i));
			}
			assertTrue(partSizes.get(0) <= last);
			//test batch indices
			Collections.sort(batchIndices);
			int[] usedBatchIndices = batchIndices.stream().mapToInt(i -> i).toArray();
			int[] expectedIndices = new int[partSizes.size()];
			Arrays.setAll(expectedIndices, i -> i);
			assertArrayEquals(expectedIndices, usedBatchIndices);
		}

		@Test
		public void testSeparationInPartsInitialization() throws Exception {
			final int size = getThreshold() * getThreshold() + 1;
			Context context = new Context() {
				@Override
				public boolean isActive() {
					return true;
				}

				@Override
				public int getParallelism() {
					return getThreshold();
				}

				@Override
				public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
					return CTX.call(callables);
				}
			};
			int[] initialized = new int[1];
			AtomicInteger counter = new AtomicInteger();
			Calculator<Void> calculator = new Calculator<Void>() {

				@Override
				public void init(int numberOfBatches) {
					initialized[0] = numberOfBatches;
				}

				@Override
				public int getNumberOfOperations() {
					return size;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					counter.incrementAndGet();
				}

				@Override
				public Void getResult() {
					return null;
				}
			};

			new ParallelExecutor<>(calculator, workload, NOOP_CALLBACK)
					.execute(context);

			//test number of parts matches initialized
			assertEquals(initialized[0], counter.get());
		}

		@Test
		public void testInBatches() throws Exception {
			int nTasks = CTX.getParallelism();
			int size = getBatchSize() * nTasks * 3 + 11;
			int expectedBatches = nTasks * 3 + 1;
			boolean[] done = new boolean[size];
			List<Integer> batchIndices = Collections.synchronizedList(new ArrayList<>());

			Calculator<Integer> calculator = new Calculator<Integer>() {

				AtomicInteger counter = new AtomicInteger();

				@Override
				public void init(int numberOfBatches) {
					assertEquals(expectedBatches, numberOfBatches);
				}

				@Override
				public int getNumberOfOperations() {
					return size;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					batchIndices.add(batchIndex);
					for (int i = from; i < to; i++) {
						done[i] = true;
						counter.incrementAndGet();
					}
				}

				@Override
				public Integer getResult() {
					return counter.get();
				}

			};
			int counts = new ParallelExecutor<>(calculator, workload, NOOP_CALLBACK).execute(CTX);

			// test every index was done
			boolean[] expectedDone = new boolean[size];
			Arrays.fill(expectedDone, true);
			assertArrayEquals(expectedDone, done);
			//test no index was done twice
			assertEquals(size, counts);

			//test right number of batches
			assertEquals(expectedBatches, batchIndices.size());

			//test batch indices
			Collections.sort(batchIndices);
			int[] usedBatchIndices = batchIndices.stream().mapToInt(i -> i).toArray();
			int[] expectedIndices = new int[expectedBatches];
			Arrays.setAll(expectedIndices, i -> i);
			assertArrayEquals(expectedIndices, usedBatchIndices);
		}

	}

	public static class Cancellation {

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
			public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
				if (active.get()) {
					active.set(false);
					return CTX.call(callables);
				} else {
					throw new RejectedExecutionException("You had your one shot");
				}
			}

		}

		Calculator<Integer> unsupportedCalculator = new Calculator<Integer>() {

			@Override
			public void init(int numberOfBatches) {
				// do nothing
			}

			@Override
			public int getNumberOfOperations() {
				return 1000;
			}

			@Override
			public void doPart(int from, int to, int batchIndex) {
				throw new UnsupportedOperationException();
			}

			@Override
			public Integer getResult() {
				throw new UnsupportedOperationException();
			}

		};

		@Test(expected = ExecutionAbortedException.class)
		public void testInactiveContext() throws Exception {
			Context ctx = spy(CTX);
			when(ctx.isActive()).thenReturn(false);
			new ParallelExecutor<>(unsupportedCalculator, Workload.SMALL, NOOP_CALLBACK).execute(ctx);
		}

		@Test
		public void testProgressWithInactiveContext() throws Exception {
			Context ctx = spy(CTX);
			when(ctx.isActive()).thenReturn(false);
			double[] result = new double[]{-1.0};
			try {
				new ParallelExecutor<>(unsupportedCalculator, Workload.SMALL, p -> result[0] = p).execute(ctx);
			} catch (ExecutionAbortedException e) {
				// ignore exception
			}
			assertEquals(-1.0, result[0], EPSILON);
		}

		@Test(expected = ExecutionAbortedException.class)
		public void testAbortionBeforeParts() {
			new ParallelExecutor<>(unsupportedCalculator, Workload.SMALL, NOOP_CALLBACK)
					.execute(new OneShotContext());
		}

		@Test
		public void testProgressWithAbortionBeforeParts() {
			double[] result = new double[]{-1.0};
			try {
				new ParallelExecutor<>(unsupportedCalculator, Workload.SMALL, p -> result[0] = p)
						.execute(new OneShotContext());
			} catch (ExecutionAbortedException e) {
				// ignore exception
			}
			assertNotEquals(1.0, result[0], EPSILON);
		}

	}

	public static class Workloads {

		@Test
		public void testBatchSizes() throws Exception {
			Calculator<Integer> calculator = new Calculator<Integer>() {

				AtomicInteger called = new AtomicInteger();

				@Override
				public void init(int numberOfBatches) {
					called.set(0);
				}

				@Override
				public int getNumberOfOperations() {
					return ParallelExecutor.BATCH_SIZE_SMALL * 4;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					called.incrementAndGet();
				}

				@Override
				public Integer getResult() {
					return called.get();
				}
			};

			Map<Workload, Integer> batchCount = new HashMap<>();
			batchCount.put(Workload.DEFAULT, new ParallelExecutor<>(calculator, Workload.DEFAULT, NOOP_CALLBACK)
					.execute(CTX));
			batchCount.put(Workload.SMALL, new ParallelExecutor<>(calculator, Workload.SMALL, NOOP_CALLBACK)
					.execute(CTX));
			batchCount.put(Workload.MEDIUM, new ParallelExecutor<>(calculator, Workload.MEDIUM, NOOP_CALLBACK)
					.execute(CTX));
			batchCount.put(Workload.LARGE, new ParallelExecutor<>(calculator, Workload.LARGE, NOOP_CALLBACK)
					.execute(CTX));
			batchCount.put(Workload.HUGE, new ParallelExecutor<>(calculator, Workload.HUGE, NOOP_CALLBACK)
					.execute(CTX));

			assertTrue("Fewer batches for default workloads than for huge ones",
					batchCount.get(Workload.DEFAULT) < batchCount.get(Workload.HUGE));
			assertTrue("Fewer batches for small workloads than for medium ones",
					batchCount.get(Workload.SMALL) < batchCount.get(Workload.MEDIUM));
			assertTrue("Fewer batches for medium workloads than for large ones",
					batchCount.get(Workload.MEDIUM) < batchCount.get(Workload.LARGE));
			assertTrue("Fewer batches for large workloads than for huge ones",
					batchCount.get(Workload.LARGE) < batchCount.get(Workload.HUGE));
		}

	}

	@RunWith(Parameterized.class)
	public static class Callbacks {

		@Parameter
		public Workload workload;

		@Parameters(name = "{0}")
		public static Iterable<Workload> workloadConfiguration() {
			return Arrays.asList(Workload.values());
		}

		private static Context NO_POOL = new Context() {

			@Override
			public boolean isActive() {
				return true;
			}

			@Override
			public int getParallelism() {
				return 4;
			}

			@Override
			public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
				// Spawn a new thread for each job to ensure that ThreadLocal data is also job local (see test below).
				List<T> results = new ArrayList<>();
				for(Callable<T> callable: callables) {
					FutureTask<T> future = new FutureTask<>(callable);
					new Thread(future).start();
					try {
						results.add(future.get());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				return results;
			}

		};

		@Test
		public void testLocalMonotonicity() {
			Calculator<Integer> calculator = new Calculator<Integer>() {

				@Override
				public void init(int numberOfBatches) {
				}

				@Override
				public int getNumberOfOperations() {
					return ParallelExecutor.BATCH_SIZE_SMALL * 10;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					// noop
				}

				@Override
				public Integer getResult() {
					return 0;
				}

			};

			// The NO_POOL context runs each job in a new thread. Thus, we know that this thread local field will only
			// see updates from a single worker.
			ThreadLocal<Double> progress = ThreadLocal.withInitial(() -> Double.NaN);
			new ParallelExecutor<>(calculator, Workload.SMALL, p -> {
				double current = progress.get();
				assertTrue(Double.isNaN(current) || Double.isNaN(p) || current <= p);
				progress.set(p);
			}).execute(NO_POOL);
		}

	}

	@RunWith(Parameterized.class)
	public static class BatchSize {

		@Parameter
		public Workload workload;

		@Parameters(name = "{0}")
		public static Iterable<Workload> workloads() {
			return Arrays.asList(Workload.values());
		}

		private int getThreshold() {
			switch (workload) {
				case HUGE:
					return ParallelExecutor.THRESHOLD_PARALLEL_HUGE;
				case LARGE:
					return ParallelExecutor.THRESHOLD_PARALLEL_LARGE;
				case MEDIUM:
					return ParallelExecutor.THRESHOLD_PARALLEL_MEDIUM;
				case SMALL:
				case DEFAULT:
				default:
					return ParallelExecutor.THRESHOLD_PARALLEL_SMALL;
			}
		}

		private int getBatchSize() {
			switch (workload) {
				case HUGE:
					return ParallelExecutor.BATCH_SIZE_HUGE;
				case LARGE:
					return ParallelExecutor.BATCH_SIZE_LARGE;
				case MEDIUM:
					return ParallelExecutor.BATCH_SIZE_MEDIUM;
				case SMALL:
				case DEFAULT:
				default:
					return ParallelExecutor.BATCH_SIZE_SMALL;
			}
		}

		private Context getContext() {
			Random random = new Random();
			int parallelism = random.nextInt(50) + 2;

			return new Context() {
				@Override
				public boolean isActive() {
					return CTX.isActive();
				}

				@Override
				public int getParallelism() {
					return parallelism;
				}

				@Override
				public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
					return CTX.call(callables);
				}
			};
		}

		@Test
		public void testInBatches() {

			Calculator<Void> calculator = new Calculator<Void>() {

				@Override
				public void init(int numberOfBatches) {
				}

				@Override
				public int getNumberOfOperations() {
					Random random = new Random();
					long min = getBatchSize() * (long) ParallelExecutor.THRESHOLD_FACTOR_EQUAL_PARTS * getContext()
							.getParallelism();
					if (min > Integer.MAX_VALUE) {
						return Integer.MAX_VALUE;
					}
					int intMin = (int) min;
					//chose random value so that in batch case
					return random.nextInt(Integer.MAX_VALUE - intMin) + intMin;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					//test that start of batch is divisible by 4, required when using {@link UInt2NominalBuffer}
					assertEquals(0, from % 4);
				}

				@Override
				public Void getResult() {
					return null;
				}

			};
			new ParallelExecutor<>(calculator, workload, NOOP_CALLBACK).execute(getContext());
		}

		@Test
		public void testEqualParts() {

			final Context context = getContext();
			Calculator<Void> calculator = new Calculator<Void>() {

				@Override
				public void init(int numberOfBatches) {
				}

				@Override
				public int getNumberOfOperations() {
					Random random = new Random();
					long max = getBatchSize() * (long) ParallelExecutor.THRESHOLD_FACTOR_EQUAL_PARTS * context
							.getParallelism();
					if (max > Integer.MAX_VALUE) {
						return Integer.MAX_VALUE;
					}
					int intMax = (int) max;
					int min = getThreshold();
					//chose random value so that in equal parts case with more than one part
					return random.nextInt(intMax - min) + min;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					//test that start of batch is divisible by 4, required when using {@link UInt2NominalBuffer}
					assertEquals(0, from % 4);
				}

				@Override
				public Void getResult() {
					return null;
				}

			};
			 new ParallelExecutor<>(calculator, workload, NOOP_CALLBACK).execute(context);
		}

		@Test
		public void testZeroHeight() {

			final Context context = getContext();
			Calculator<Integer> calculator = new Calculator<Integer>() {

				Integer result = null;

				@Override
				public void init(int numberOfBatches) {
					result = numberOfBatches;
				}

				@Override
				public int getNumberOfOperations() {
					return 0;
				}

				@Override
				public void doPart(int from, int to, int batchIndex) {
					assertEquals(0, batchIndex);
				}

				@Override
				public Integer getResult() {
					return result;
				}

			};
			new ParallelExecutor<>(calculator, workload, NOOP_CALLBACK).execute(context);
			assertEquals(1, calculator.getResult().intValue());
		}


	}

}
