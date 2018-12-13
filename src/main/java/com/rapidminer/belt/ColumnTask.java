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

import com.rapidminer.belt.util.ScheduledTaskRunner;


/**
 * Task to compute a new {@link NumericBuffer}. Requires an active {@link Context} to run.
 *
 * @author Gisa Meier
 */
public class ColumnTask {


	private final int size;
	private final ScheduledTaskRunner<NumericBuffer> task;

	ColumnTask(ScheduledTaskRunner<NumericBuffer> task, int size) {
		this.task = task;
		this.size = size;
	}

	/**
	 * Runs the column task with the given execution {@link Context}. This operation blocks until the computation of the
	 * table has finished and returns the final table.
	 *
	 * @param context
	 * 		the execution context
	 * @return the final column buffer
	 * @throws NullPointerException
	 * 		if the given context is {@code null}
	 */
	public NumericBuffer run(Context context) {
		return task.run(context);
	}

	/**
	 * Returns the size of the column buffer to be computed.
	 *
	 * @return the size of the buffer
	 */
	public int size() {
		return size;
	}

	@Override
	public String toString() {
		return "Column task (" + size + ")";
	}
}