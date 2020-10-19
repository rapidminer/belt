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

package com.rapidminer.belt.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;


/**
 * Utility functions for executing {@link Callable}s in a {@link Context}.
 *
 * @author Gisa Meier
 */
public final class ExecutionUtils {

	// Suppress default constructor for noninstantiability
	private ExecutionUtils() {
		throw new AssertionError();
	}

	/**
	 * Runs the given callable in the given context and handles the exceptions under the assumption, that no checked
	 * exceptions come from the callable.
	 *
	 * @param callable
	 * 		a callable that throws no checked exception, e.g. one constructed via a lambda expression
	 * @param context
	 * 		the context to use
	 * @param <T>
	 * 		the result type
	 * @return the result of the callable
	 * @throws ExecutionAbortedException
	 * 		if the callable throws a checked exception
	 */
	public static <T> T run(Callable<T> callable, Context context) {
		return run(Collections.singletonList(callable), context).get(0);
	}


	/**
	 * Runs the given callables in the given context and handles the exceptions under the assumption, that no checked
	 * exceptions come from the callables.
	 *
	 * @param callables
	 * 		a list of callables that throw no checked exceptions, e.g. constructed via lambda expressions
	 * @param context
	 * 		the context to use
	 * @param <T>
	 * 		the result type
	 * @return the list of results of the callables
	 * @throws ExecutionAbortedException
	 * 		if one of the callable throws a checked exception
	 */
	public static <T> List<T> run(List<Callable<T>> callables, Context context) {
		context.requireActive();
		try {
			return context.call(callables);
		} catch (ExecutionException e) {
			// The Callable wraps a Function that must not throw checked exceptions. Thus, the cause is most likely a
			// RuntimeException thrown in the user code.
			Throwable cause = e.getCause();
			if (cause instanceof RuntimeException) {
				throw (RuntimeException) cause;
			} else if (cause instanceof Error) {
				throw (Error) cause;
			} else {
				throw new ExecutionAbortedException("Computation aborted with checked exception", cause);
			}
		}
	}

	/**
	 * Can be used to execute multiple tasks in parallel. The consumer will be fed with the indices in the range {@code
	 * [start, end)}. Executing a method {@code doWork} on every column of a given table can be done, e.g., via
	 * <pre>{@code ExecutionUtils.parallel(0, table.width(), i -> doWork(table.column(i)), context);}</pre>
	 *
	 * @param start
	 * 		start of the index range
	 * @param end
	 * 		end of the index range (exclusively)
	 * @param task
	 * 		an {@link IntConsumer} that consumes an index and executes some task
	 * @param context
	 * 		the {@link Context} used for the parallel execution
	 * @throws ExecutionAbortedException
	 * 		if the context goes inactive before the tasks are done
	 */
	public static void parallel(int start, int end, IntConsumer task, Context context) {
		AtomicBoolean sentinel = new AtomicBoolean(true);
		int tasks = end - start;
		int taskers = Math.min(tasks, context.getParallelism());
		AtomicInteger position = new AtomicInteger(start + taskers - 1);
		List<Callable<Void>> callables = new ArrayList<>(taskers);
		for (int i = 0; i < taskers; i++) {
			callables.add(makeTasker(context, sentinel, position, start + i, end, task));
		}
		ExecutionUtils.run(callables, context);
	}

	/**
	 * Creates a new {@link Callable} used to do parallel computations on multiple tasks in {@link #parallel(int, int,
	 * IntConsumer, Context)}.
	 */
	private static Callable<Void> makeTasker(Context context, AtomicBoolean sentinel, AtomicInteger position,
											 int firstIndex, int end, IntConsumer task) {
		return () -> {
			int currentIndex = firstIndex;
			do {
				if (!context.isActive() || !sentinel.get()) {
					return handleException(sentinel, null);
				}
				try {
					task.accept(currentIndex);
				} catch (RuntimeException e) {
					return handleException(sentinel, e);
				}
				currentIndex = position.incrementAndGet();
			} while (currentIndex < end);
			return null;
		};
	}

	/**
	 * Is used in {@link #makeTasker(Context, AtomicBoolean, AtomicInteger, int, int, IntConsumer)} to handle
	 * exceptions.
	 */
	private static Void handleException(AtomicBoolean sentinel, RuntimeException e) {
		if (sentinel.getAndSet(false)) {
			if (e == null) {
				throw new ExecutionAbortedException("Execution aborted by invoker");
			}
			throw e;
		}
		return null;
	}

}