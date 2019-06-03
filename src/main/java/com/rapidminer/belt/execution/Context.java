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

package com.rapidminer.belt.execution;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;


/**
 * Execution context for arbitrary, concurrent background jobs. The context need not utilize all available processors
 * (see {@link #getParallelism()}) and may reject jobs once it becomes inactive (see {@link #isActive()}).
 *
 * @author Michael Knopf
 */
public interface Context {

	/**
	 * Returns {@code true} if the execution context is active. Inactive contexts may reject new jobs.
	 *
	 * @return {@code true} if the execution context is active
	 */
	boolean isActive();

	/**
	 * Checks if the context {@link #isActive()} and throws a {@link ExecutionAbortedException} if it is not.
	 *
	 * @throws ExecutionAbortedException
	 * 		if the context is not active
	 */
	default void requireActive() {
		if(!isActive()){
			throw new ExecutionAbortedException("Execution was aborted");
		}
	}

	/**
	 * Returns the targeted parallelism level of this execution context.
	 *
	 * @return the targeted parallelism level
	 */
	int getParallelism();

	/**
	 * Executes the given {@link Callable}s in parallel, returning their results upon completion.
	 * <p>
	 * If one of the {@code Callable}s encounters an exception, the context attempts to cancel the other {@code
	 * Callable}s and throws an {@link ExecutionException} that wraps the exception.
	 * If more than one {@code Callable} encounters an exception, only the first observed exception is reported.
	 * <p>
	 * The method blocks until all {@code Callables} have completed.
	 * <p>
	 * Note that the cancellation of one of the {@code Callable}s might not interrupt the execution of other {@code
	 * Callable}s that have already been started. As a consequence, it is recommended that all long-running {@code
	 * Callable}s periodically check for the cancellation of the computation via {@link #requireActive()}.
	 *
	 * @param <T>
	 * 		the type of the values returned from the callables
	 * @param callables
	 * 		the {@link Callable}s to execute in parallel
	 * @return a list containing the results of the callables
	 * @throws ExecutionException
	 * 		if the computation threw an exception
	 * @throws ExecutionAbortedException
	 * 		if the execution was stopped before completion
	 * @throws NullPointerException
	 * 		if the given list of {@code Callable}s is or contains {@code null}
	 * @throws RejectedExecutionException
	 * 		is the execution context is inactive
	 */
	<T> List<T> call(List<Callable<T>> callables) throws ExecutionException;


	/**
	 * Derives a {@link Context} with parallelism level {@code 1} from the given context.
	 *
	 * @param context
	 * 		the base context
	 * @return a context with parallelism level 1
	 */
	static Context singleThreaded(Context context){
		if (context instanceof SingleThreadedContext) {
			return context;
		}
		return new SingleThreadedContext(context);
	}
}
