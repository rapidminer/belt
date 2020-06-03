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


import static java.lang.Integer.max;
import static java.lang.Integer.min;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.DoubleConsumer;

import com.rapidminer.belt.buffer.UInt2NominalBuffer;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionAbortedException;
import com.rapidminer.belt.execution.ExecutionUtils;
import com.rapidminer.belt.execution.Workload;


/**
 * Handles parallel execution, either in batches or in equal parts.
 *
 * @param <T>
 * 		the type of the result of the parallel calculation
 * @author Michael Knopf, Gisa Meier
 */
class ParallelExecutor<T> {

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
	 * All batches must start with and index%4==0 because of restricted thread-safety of {@link UInt2NominalBuffer}
	 */
	private static final int BATCH_DIVISOR = 4;

	/**
	 * Batch size must be at least 4 because of restricted thread-safety of {@link UInt2NominalBuffer}
	 */
	private final int thresholdParallel;

	/**
	 * Batch size must be divisible by 4 because of restricted thread-safety of {@link UInt2NominalBuffer}
	 */
	private final int batchSize;
	private final Calculator<T> calculator;
	private final DoubleConsumer callback;

	ParallelExecutor(Calculator<T> calculator, Workload workload, DoubleConsumer callback) {
		this.calculator = calculator;
		this.callback = callback;
		switch (workload) {
			case HUGE:
				thresholdParallel = THRESHOLD_PARALLEL_HUGE;
				batchSize = BATCH_SIZE_HUGE;
				break;
			case LARGE:
				thresholdParallel = THRESHOLD_PARALLEL_LARGE;
				batchSize = BATCH_SIZE_LARGE;
				break;
			case SMALL:
				thresholdParallel = THRESHOLD_PARALLEL_SMALL;
				batchSize = BATCH_SIZE_SMALL;
				break;
			case MEDIUM:
			case DEFAULT:
			default:
				thresholdParallel = THRESHOLD_PARALLEL_MEDIUM;
				batchSize = BATCH_SIZE_MEDIUM;
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
					ExecutionAbortedException e = new ExecutionAbortedException("Execution aborted by invoker");
					return handleException(sentinel, e);
				}
				try {
					calculator.doPart(start, end, start / batchSize);
					callback.accept((double) (end - 1) / calculator.getNumberOfOperations());
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
	 * Executes the transform in parallel inside the given context.
	 *
	 * @param context the context to use
	 * @return the result of the transform
	 */
	T execute(Context context) {
		context.requireActive();

		ExecutionUtils.run(buildCallables(context), context);

		T result = calculator.getResult();
		// Invoke getResult() before setting the progress to 100%, since the method need not return instantaneously.
		callback.accept(1);
		return result;
	}

	/**
	 * Creates callables for the tasks depending on the parallelism level and the number of operations.
	 */
	private List<Callable<Void>> buildCallables(Context context) {
		int nTasks = max(1, context.getParallelism());

		int expectedNumberOfOperations = calculator.getNumberOfOperations();

		if (expectedNumberOfOperations >= batchSize * (long) THRESHOLD_FACTOR_EQUAL_PARTS * nTasks) {
			int numberOfBatches = expectedNumberOfOperations / batchSize
					+ (expectedNumberOfOperations % batchSize == 0 ? 0 : 1);
			calculator.init(numberOfBatches);
			return doInBatches(nTasks, context);
		}


		if (expectedNumberOfOperations < thresholdParallel) {
			// transform initialization is done inside doEqualParts
			return doEqualParts(1, context);
		}

		// ensure that a batch is at least threshold parallel big
		nTasks = Math.min(nTasks, expectedNumberOfOperations / thresholdParallel);
		// transform initialization is done inside doEqualParts
		return doEqualParts(nTasks, context);
	}


	/**
	 * Creates {#nTasks} callable that call {#doPart} in batches of size {@link #batchSize}.
	 */
	private List<Callable<Void>> doInBatches(int nTasks, Context context) {
		List<Callable<Void>> callables = new ArrayList<>(nTasks);
		AtomicBoolean sentinel = new AtomicBoolean(true);
		AtomicInteger position = new AtomicInteger(nTasks * batchSize);
		for (int i = 0; i < nTasks; i++) {
			Batch batch = new Batch(context, sentinel, position, i * batchSize);
			callables.add(batch);
		}
		return callables;
	}

	/**
	 * Creates {#nTasks} many {@link Callable}s that call {@link Calculator#doPart(int, int, int)} with the same number
	 * of operations.
	 */
	private List<Callable<Void>> doEqualParts(int nTasks, Context context) {
		List<Callable<Void>> callables;
		AtomicBoolean sentinel = new AtomicBoolean(true);
		int size = calculator.getNumberOfOperations();

		int targetBatchSize = size % nTasks == 0 ? size / nTasks : size / nTasks + 1;
		if (targetBatchSize % BATCH_DIVISOR != 0) {
			int padding = BATCH_DIVISOR - targetBatchSize % BATCH_DIVISOR;
			targetBatchSize += padding;
		}
		// Set progress to indeterminate...
		callback.accept(Double.NaN);
		if (size > 0) {
			nTasks = size / targetBatchSize + (size % targetBatchSize == 0 ? 0 : 1);
			calculator.init(nTasks);
			callables = new ArrayList<>(nTasks);

			int covered = 0;
			int batch = 0;
			while (covered < size) {
				callables.add(makeBatch(context, sentinel, covered, min(covered + targetBatchSize, size), batch++));
				covered += targetBatchSize;
			}
		} else {
			calculator.init(1);
			callables = new ArrayList<>(1);
			callables.add(makeBatch(context, sentinel, 0,0, 0));
		}
		return callables;
	}

	/**
	 * Creates a batch callable with the given context and sentinel and defined by the other parameters.
	 */
	private Callable<Void> makeBatch(Context context, AtomicBoolean sentinel, int start, int end, int batchIndex) {
		return () -> {
			if (!context.isActive() || !sentinel.get()) {
				handleException(sentinel, new ExecutionAbortedException("Execution aborted by invoker"));
			}
			try {
				calculator.doPart(start, end, batchIndex);
			} catch (RuntimeException e) {
				return handleException(sentinel, e);
			}
			return null;
		};
	}

	private static Void handleException(AtomicBoolean sentinel, RuntimeException e) {
		boolean fineBefore = sentinel.getAndSet(false);
		if (fineBefore) {
			throw e;
		}
		return null;
	}
}