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


import static java.lang.Integer.max;
import static java.lang.Integer.min;

import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.rapidminer.belt.util.ScheduledTaskRunner;
import com.rapidminer.belt.util.Task;
import com.rapidminer.belt.util.TaskAbortedException;


/**
 * Handles parallel execution, either in batches or in equal parts.
 *
 * @param <T>
 * 		the type of the result of the parallel calculation
 * @author Michael Knopf, Gisa Meier
 */
class ParallelExecutor<T> {

	/**
	 * A calculator that is called by a {@link ParallelExecutor} in order to do a calculation in parallel.
	 *
	 * @param <T>
	 * 		the type of the result
	 */
	interface Calculator<T> {

		/**
		 * Initializes the calculator. This is called once by the executor before {@link #doPart(int, int, int)} is
		 * called.
		 *
		 * @param numberOfBatches
		 * 		the number of times {@link #doPart(int, int, int)} will be called.
		 */
		void init(int numberOfBatches);

		/**
		 * @return the total number of operations that can be split into parts.
		 */
		int getNumberOfOperations();

		/**
		 * Does a part of the calculation. This method is called one or more times by the executor and every index
		 * between 0 and {@link #getNumberOfOperations()} is part of exactly one interval {@code [from,to)}.
		 *
		 * @param from
		 * 		the index to start from (inclusive)
		 * @param to
		 * 		the end index (exclusive)
		 * @param batchIndex
		 * 		the index of the part
		 */
		void doPart(int from, int to, int batchIndex);

		/**
		 * Returns the result. Is called from the executor once after all calls to {@link #doPart(int, int, int)} are
		 * finished.
		 *
		 * @return the result
		 */
		T getResult();

	}

	/**
	 * only execute in parallel if more than {@code nTasks* batchSize * THRESHOLD_FACTOR_EQUAL_PARTS} values
	 */
	static final int THRESHOLD_FACTOR_EQUAL_PARTS = 2;

	/**
	 * threshold for executing small workloads in parallel
	 */
	static final int THRESHOLD_PARALLEL_SMALL = 1 << 15;

	/**
	 * threshold for executing medium workloads in parallel
	 */
	static final int THRESHOLD_PARALLEL_MEDIUM = 1 << 10;

	/**
	 * threshold for executing large workloads in parallel
	 */
	static final int THRESHOLD_PARALLEL_LARGE = 1 << 5;

	/**
	 * threshold for executing huge workloads in parallel
	 */
	static final int THRESHOLD_PARALLEL_HUGE = 1 << 2;

	/**
	 * batch size for executing small workloads in parallel batches
	 */
	static final int BATCH_SIZE_SMALL = 1 << 20;

	/**
	 * batch size for executing medium workloads in parallel batches
	 */
	static final int BATCH_SIZE_MEDIUM = 1 << 15;

	/**
	 * batch size for executing large workloads in parallel batches
	 */
	static final int BATCH_SIZE_LARGE = 1 << 10;

	/**
	 * batch size for executing huge workloads in parallel batches
	 */
	static final int BATCH_SIZE_HUGE = 1 << 5;

	/**
	 * All batches must start with and index%4==0 because of restricted thread-safety of {@link UInt2CategoricalBuffer}
	 */
	private static final int BATCH_DIVISOR = 4;

	/**
	 * Batch size must be at least 4 because of restricted thread-safety of {@link UInt2CategoricalBuffer}
	 */
	private final int thresholdParallel;

	/**
	 * Batch size must be divisible by 4 because of restricted thread-safety of {@link UInt2CategoricalBuffer}
	 */
	private final int batchSize;
	private final Calculator<T> calculator;
	private ArrayList<Future<Void>> futures;

	ParallelExecutor(Calculator<T> calculator, Workload workload) {
		this.calculator = calculator;
		switch (workload) {
			case HUGE:
				thresholdParallel = THRESHOLD_PARALLEL_HUGE;
				batchSize = BATCH_SIZE_HUGE;
				break;
			case LARGE:
				thresholdParallel = THRESHOLD_PARALLEL_LARGE;
				batchSize = BATCH_SIZE_LARGE;
				break;
			case MEDIUM:
				thresholdParallel = THRESHOLD_PARALLEL_MEDIUM;
				batchSize = BATCH_SIZE_MEDIUM;
				break;
			case SMALL:
			case DEFAULT:
			default:
				thresholdParallel = THRESHOLD_PARALLEL_SMALL;
				batchSize = BATCH_SIZE_SMALL;
				break;
		}
	}

	/**
	 * A {@link Callable} that calls {Calculator#doPart} for one batch and then resubmits itself with the next batch if
	 * there are batches left.
	 */
	private final class Batch implements Callable<Void> {

		private final Context context;
		private final AtomicInteger position;
		private final AtomicBoolean sentinel;

		private int start;
		private int end;

		private Batch(Context context, AtomicBoolean sentinel, AtomicInteger position, int start) {
			this.context = context;
			this.position = position;
			this.sentinel = sentinel;

			this.start = start;
			this.end = min(calculator.getNumberOfOperations(), start + batchSize);
		}

		@Override
		public Void call() {
			do {
				if (!context.isActive() || !sentinel.get()) {
					TaskAbortedException e = new TaskAbortedException("Task aborted by invoker");
					return handleException(sentinel, e);
				}
				try {
					calculator.doPart(start, end, start / batchSize);
				} catch (RuntimeException e) {
					return handleException(sentinel, e);
				}

				int next = position.addAndGet(batchSize);
				start = next - batchSize;
				end = min(next, calculator.getNumberOfOperations());

			} while (start < end);
			return null;
		}

	}


	/**
	 * Creates a scheduled task runner that executes the calculator in parallel.
	 *
	 * @return a scheduled task runner
	 */
	ScheduledTaskRunner<T> create() {
		Task<Void> scheduler = (context, sentinel) -> {
			if (!context.isActive()) {
				throw new TaskAbortedException("Context is inactive");
			}

			int nTasks = max(1, context.getParallelism());

			int expectedNumberOfOperations = calculator.getNumberOfOperations();

			if (expectedNumberOfOperations >= batchSize * (long) THRESHOLD_FACTOR_EQUAL_PARTS * nTasks) {
				int numberOfBatches = expectedNumberOfOperations / batchSize
						+ (expectedNumberOfOperations % batchSize == 0 ? 0 : 1);
				calculator.init(numberOfBatches);
				doInBatches(nTasks, context, sentinel);
				return null;
			}


			if (expectedNumberOfOperations < thresholdParallel) {
				//calculator initialization is done inside doEqualParts
				doEqualParts(1, context, sentinel);
				return null;
			}

			// ensure that a batch is at least threshold parallel big
			nTasks = Math.min(nTasks, expectedNumberOfOperations / thresholdParallel);
			//calculator initialization is done inside doEqualParts
			doEqualParts(nTasks, context, sentinel);
			return null;
		};

		return new ScheduledTaskRunner<>(scheduler, createBlocker());
	}

	/**
	 * @return a blocker that waits for the scheduled tasks to finish
	 */
	private ScheduledTaskRunner.Blocker<T> createBlocker() {
		return () -> {
			for (int i = futures.size(); i > 0; i--) {
				futures.get(i - 1).get();
			}
			return calculator.getResult();
		};
	}

	/**
	 * Calls {#doPart} in batches of size {@link #batchSize}, executing {#nTasks} batches in parallel.
	 */
	private void doInBatches(int nTasks, Context context, AtomicBoolean sentinel) {
		futures = new ArrayList<>(nTasks);

		AtomicInteger position = new AtomicInteger(nTasks * batchSize);
		for (int i = 0; i < nTasks; i++) {
			Batch batch = new Batch(context, sentinel, position, i * batchSize);
			futures.add(context.submit(batch));
		}

	}

	/**
	 * Runs {#nTasks} many {@link Callable}s that call {@link Calculator#doPart(int, int, int)} with the same number of
	 * operations.
	 */
	private void doEqualParts(int nTasks, Context context, AtomicBoolean sentinel) {

		int size = calculator.getNumberOfOperations();

		int targetBatchSize = size % nTasks == 0 ? size / nTasks : size / nTasks + 1;
		if (targetBatchSize % BATCH_DIVISOR != 0) {
			int padding = BATCH_DIVISOR - targetBatchSize % BATCH_DIVISOR;
			targetBatchSize += padding;
		}

		nTasks = size / targetBatchSize + (size % targetBatchSize == 0 ? 0 : 1);
		calculator.init(nTasks);
		futures = new ArrayList<>(nTasks);

		int covered = 0;
		int batch = 0;
		while (covered < size) {
			futures.add(submitBatch(context, sentinel, covered, min(covered + targetBatchSize, size), batch++));
			covered += targetBatchSize;
		}
	}

	/**
	 * Submits a batch to the given context with the given sentinel and defined by the other parameters.
	 */
	private Future<Void> submitBatch(Context context, AtomicBoolean sentinel, int start, int end, int batchIndex) {
		return context.submit(() -> {
			if (!context.isActive() || !sentinel.get()) {
				handleException(sentinel, new TaskAbortedException("Task aborted by invoker"));
			}
			try {
				calculator.doPart(start, end, batchIndex);
			} catch (RuntimeException e) {
				return handleException(sentinel, e);
			}
			return null;
		});
	}

	private static Void handleException(AtomicBoolean sentinel, RuntimeException e) {
		boolean fineBefore = sentinel.getAndSet(false);
		if (fineBefore) {
			throw e;
		}
		return null;
	}
}