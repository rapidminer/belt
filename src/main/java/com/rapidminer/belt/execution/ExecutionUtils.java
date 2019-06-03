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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


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

}