/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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


/**
 * A wrapper for a {@link Context} that makes it single threaded. The target parallelism level will be {@code 1} and the
 * {@link #call(List)} method will only run one {@link Callable} at a time. Using {@link #call(List)} several times
 * before {@link #call} returned will still lead to more than one thread of the parent context being used.
 *
 * @author Gisa Meier
 */
final class SingleThreadedContext implements Context {

	private final Context context;

	SingleThreadedContext(Context context) {
		this.context = context;
	}

	@Override
	public boolean isActive() {
		return context.isActive();
	}

	@Override
	public void requireActive() {
		context.requireActive();
	}

	@Override
	public int getParallelism() {
		return 1;
	}

	@Override
	public <T> List<T> call(List<Callable<T>> callables) throws ExecutionException {
		List<T> results = new ArrayList<>(callables.size());
		for (Callable<T> callable : callables) {
			results.add(context.call(Collections.singletonList(callable)).get(0));
		}
		return results;
	}

	@Override
	public String toString() {
		return "Single threaded context on top of " + context.toString();
	}
}


