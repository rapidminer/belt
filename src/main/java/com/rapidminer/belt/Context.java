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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;


/**
 * Execution context for arbitrary, concurrent background jobs. The context need not utilize all available processors
 * (see {@link #getParallelism()}) and may reject jobs once it becomes inactive (see {@link #isActive()}).
 *
 * <p>Usage note: Belt makes no assumption on the execution order of submitted jobs. However, if jobs are executed by
 * the context in submission order (FIFO), concurrent Belt operations do not block each other (interleaved processing).
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
	 * Returns the targeted parallelism level of this execution context.
	 *
	 * @return the targeted parallelism level
	 */
	int getParallelism();

	/**
	 * Submits a value-returning job for execution and returns a {@link Future} representing the pending results of the
	 * job. The future's get method will return the job's result upon successful completion.
	 *
	 * @param job
	 * 		the job to submit
	 * @param <T>
	 * 		the type of the job's result
	 * @return a {@link Future}
	 * @throws NullPointerException
	 * 		if the submitted job is {@code null}
	 * @throws RejectedExecutionException
	 * 		is the execution context is inactive
	 */
	<T> Future<T> submit(Callable<T> job);

}
