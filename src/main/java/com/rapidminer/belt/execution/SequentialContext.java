/**
 * Copyright (C) 2001-2021 by RapidMiner and the contributors
 *
 * Complete list of developers available at our web site:
 *
 * http://rapidminer.com
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version 3
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this program.
 * If not, see http://www.gnu.org/licenses/.
 */
package com.rapidminer.belt.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;


/**
 * A simple {@link Context} to use for belt calculations. Executes single-threaded in the current thread. The execution
 * of tasks can be stopped by calling {@link #stop()}.
 *
 * @author Gisa Meier
 * @since 1.0.1
 */
public class SequentialContext implements Context {

	private volatile boolean isActive = true;

	@Override
	public boolean isActive() {
		return isActive;
	}

	@Override
	public int getParallelism() {
		return 1;
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
		List<T> results = new ArrayList<>();
		for (Callable<T> entry : callables) {
			try {
				results.add(entry.call());
			} catch (Exception e) {
				throw new ExecutionException(e);
			}
		}
		return results;
	}

	/**
	 * Stops the execution for this context.
	 */
	public void stop() {
		isActive = false;
	}

}
