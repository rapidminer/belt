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

import java.util.concurrent.atomic.AtomicBoolean;

import com.rapidminer.belt.Context;


/**
 * A context-aware callable that allows to specify an additional sentinel.
 *
 * @param <T>
 * 		the the result type of the method {@link #call}
 * @author Michael Knopf
 */
@FunctionalInterface
public interface Task<T> {

	/**
	 * Computes the result using the given execution context. Aborts if the given sentinel is set to {@code false}.
	 *
	 * @param ctx
	 * 		the execution context
	 * @param sentinel
	 * 		the sentinel
	 * @return the computed result
	 */
	T call(Context ctx, AtomicBoolean sentinel);

}
