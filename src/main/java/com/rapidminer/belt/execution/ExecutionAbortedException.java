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

/**
 * Runtime exception to indicate that the execution of a job has been aborted.
 *
 * @author Michael Knopf
 */
public class ExecutionAbortedException extends RuntimeException {

	/**
	 * Constructs a new exception with the given detail message.
	 *
	 * @param message
	 * 		the detail message
	 */
	public ExecutionAbortedException(String message) {
		super(message);
	}

	/**
	 * Constructs a new exception with the given detail message and cause.
	 *
	 * @param message
	 * 		the detail message
	 * @param cause
	 * 		the cause
	 */
	public ExecutionAbortedException(String message, Throwable cause) {
		super(message, cause);
	}

}
