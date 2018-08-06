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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;


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
		private final ExecutorService service;
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
		public <T> Future<T> submit(Callable<T> job) {
			Objects.requireNonNull(job, "Job must not be null");
			if (sentinel.get()) {
				return service.submit(job);
			} else {
				throw new RejectedExecutionException("Context is inactive");
			}
		}

		@Override
		public String toString() {
			return "Default execution context (" + (isActive() ? "active" : "inactive")
					+ ", parallelism " + parallelism + ")";
		}
	}

}
