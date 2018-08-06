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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import com.rapidminer.belt.Context;


/**
 * A {@link TaskRunner} that submits a task into a {@link Context} and waits for the resulting future.
 *
 * @param <T>
 * 		the the result type of the method {@link #block()}
 * @author Michael Knopf, Gisa Meier
 */
public class SimpleTaskRunner<T> implements TaskRunner<T> {

	private final Task<T> task;
	private Future<T> blocker;

	public SimpleTaskRunner(Task<T> task) {
		this.task = task;
	}


	@Override
	public void schedule(Context context, AtomicBoolean sentinel) {
		blocker = context.submit(() -> task.call(context, sentinel));
	}

	@Override
	public T block() throws InterruptedException, ExecutionException {
		return blocker.get();
	}

}