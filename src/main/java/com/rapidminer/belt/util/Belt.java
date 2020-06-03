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

package com.rapidminer.belt.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionAbortedException;


/**
 * Utility class for default implementations and registries.
 *
 * @author Michael Knopf
 */
public final class Belt {

	private static Context defaultContext;

	// Suppress default constructor for noninstantiability
	private Belt() {
		throw new AssertionError();
	}

	/**
	 * Returns the default execution context.
	 *
	 * @return the default execution context
	 */
	public static synchronized Context defaultContext() {
		if (defaultContext == null) {
			defaultContext = new SimpleContext();
		}
		return defaultContext;
	}

	// class is not final for testing purposes
	private static class SimpleContext implements Context {

		private final int parallelism;
		private final ForkJoinPool service;
		private final AtomicBoolean sentinel;

		private SimpleContext() {
			parallelism = Runtime.getRuntime().availableProcessors();
			//create fork join pool in fifo mode
			service = new ForkJoinPool(parallelism, ForkJoinPool.defaultForkJoinWorkerThreadFactory,
					null, true);
			sentinel = new AtomicBoolean(true);
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				sentinel.set(false);
				service.shutdownNow();
			}));
		}

		@Override
		public boolean isActive() {
			return sentinel.get();
		}

		@Override
		public int getParallelism() {
			return parallelism;
		}

		@Override
		public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
			if (callables == null) {
				throw new NullPointerException("callables must not be null");
			}

			// nothing to do if list is empty
			if (callables.isEmpty()) {
				return Collections.emptyList();
			}

			// check for null tasks
			for (Callable<T> callable : callables) {
				if (callable == null) {
					throw new NullPointerException("callables must not contain null");
				}
			}

			if (!sentinel.get()) {
				throw new RejectedExecutionException("Context is inactive");
			}

			// handle submissions from inside and outside the pool differently
			Thread currentThread = Thread.currentThread();
			List<Future<T>> futures = new ArrayList<>(callables.size());
			if (currentThread instanceof ForkJoinWorkerThread
					&& ((ForkJoinWorkerThread) currentThread).getPool() == service) {
				for (Callable<T> callable : callables) {
					futures.add(new AdaptedCallable<>(callable));
				}
				for (int i = futures.size() - 1; i >= 0; i--) {
					((ForkJoinTask) futures.get(i)).fork();
				}
			} else {
				for (Callable<T> callable : callables) {
					futures.add(service.submit((ForkJoinTask<T>) new AdaptedCallable<>(callable)));
				}
			}
			return collectResults(futures);
		}

		<T> List<T> collectResults(List<Future<T>> futures) throws ExecutionException {

			List<T> results = new ArrayList<>(futures.size());
			for (Future<T> future : futures) {
				try {
					T result = future.get();
					results.add(result);
				} catch (InterruptedException | RejectedExecutionException e) {
					// The pool's invokeAll() method calls Future.get() internally. If the process is
					// stopped by the user, these calls might be interrupted before calls to
					// requireActive() throw an ExecutionAbortedException. Thus, we need to check the
					// current status again.
					requireActive();
					// InterruptedExceptions are very unlikely to happen at this point, since the above
					// calls to get() will return immediately. A RejectedExectutionException is an
					// extreme corner case as well. In both cases, there is no benefit for the API user
					// if the exception is passed on directly. Thus, we can wrap it within a
					// ExecutionException which is part of the API.
					throw new ExecutionException(e);
				} catch (ExecutionException e) {
					// A ExecutionAbortedException is an internal exception thrown if the user
					// requests the process to stop (see the checkStatus() implementation of this
					// class). This exception should not be wrapped or consumed here, since it is
					// handled by the operator implementation itself.
					if (e.getCause() instanceof ExecutionAbortedException) {
						throw (ExecutionAbortedException) e.getCause();
					} else if (e.getCause() instanceof WrapperRuntimeException) {
						// unwrap checked exceptions from runtime exceptions
						throw new ExecutionException(e.getCause().getCause());
					} else {
						throw e;
					}
				}
			}

			return results;
		}

		@Override
		public String toString() {
			return "Default execution context (" + (isActive() ? "active" : "inactive")
					+ ", parallelism " + parallelism + ")";
		}
	}

	private static final class WrapperRuntimeException extends RuntimeException {

		private WrapperRuntimeException(Exception e) {
			super(e);
		}

	}

	/**
	 * Wrapper for {@link Callable}s that is the same as ForkJoinTask#AdaptedCallable but wraps checked exceptions in
	 * {@link WrapperRuntimeException} instead of generic {@link RuntimeException} for easier
	 * unwrapping.
	 *
	 */
	private static final class AdaptedCallable<T> extends ForkJoinTask<T>
			implements RunnableFuture<T> {
		private static final long serialVersionUID = 2838392045355241008L;

		private final transient Callable<? extends T> callable;
		private transient T result;

		private AdaptedCallable(Callable<? extends T> callable) {
			this.callable = callable;
		}

		@Override
		public final T getRawResult() {
			return result;
		}

		@Override
		public final void setRawResult(T v) {
			result = v;
		}

		@Override
		public final boolean exec() {
			try {
				result = callable.call();
				return true;
			} catch (Error | RuntimeException err) {
				throw err;
			} catch (Exception ex) {
				// the following line is the only difference to ForkJoinTask#AdaptedCallable
				throw new WrapperRuntimeException(ex);
			}
		}

		@Override
		public final void run() {
			super.invoke();
		}
	}

}
