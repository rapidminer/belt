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

package com.rapidminer.belt.util;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rapidminer.belt.Context;


/**
 * Runs a task with a given execution {@link Context} and blocks until it is done.
 *
 * @param <T>
 * 		the the result type of the method {@link #run}
 * @author Michael Knopf, Gisa Meier
 */
interface TaskRunner<T> {


	/**
	 * Runs the task with the given execution {@link Context}. This operation blocks until the computation of the
	 * table has finished and returns the final table.
	 *
	 * @param context
	 * 		the execution context
	 * @return the final column buffer
	 * @throws NullPointerException
	 * 		if the given context is {@code null}
	 */
	default T run(Context context) {
		Objects.requireNonNull(context, "Context must not be null");
		if (!context.isActive()) {
			throw new TaskAbortedException("Context is inactive");
		}

		// To abort the job without shutting down the entire context, we need a separate job-specific sentinel.
		AtomicBoolean sentinel = new AtomicBoolean(true);
		schedule(context, sentinel);
		try {
			// Block invoking thread until the task has completed.
			return block();
		} catch (InterruptedException e) {
			// The invoking thread (not one of the worker threads) has been interrupted: abort computation and reset
			// thread state.
			sentinel.set(false);
			Thread.currentThread().interrupt();
			throw new TaskAbortedException("Invoking thread was interrupted", e);
		} catch (ExecutionException e) {
			// The Callable wraps a Function that must not throw checked exceptions. Thus, the cause is most likely a
			// RuntimeException thrown in the user code.
			Throwable cause = e.getCause();
			if (cause instanceof RuntimeException) {
				throw (RuntimeException) cause;
			} else {
				throw new TaskAbortedException("Computation aborted with checked exception", cause);
			}
		}
	}

	/**
	 * Runs a calculation inside the context.
	 *
	 * @param context
	 * 		the execution context
	 * @param sentinel
	 * 		the sentinel
	 */
	void schedule(Context context, AtomicBoolean sentinel);

	/**
	 * Blocks until the calculation started with the {@link #schedule(Context, AtomicBoolean)}  methods is finished and
	 * returns the result.
	 *
	 * @return the result
	 * @throws InterruptedException
	 * 		if the blocking is interrupted
	 * @throws ExecutionException
	 * 		if a checked exception occurred
	 */
	T block() throws InterruptedException, ExecutionException;

}