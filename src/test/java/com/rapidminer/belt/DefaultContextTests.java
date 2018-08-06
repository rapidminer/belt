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

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;


/**
 * @author Michael Knopf
 */
public class DefaultContextTests {

	@Test
	public void testActiveness() {
		Context ctx = Belt.defaultContext();
		assertTrue(ctx.isActive());
	}

	@Test
	public void testParallelism() {
		Context ctx = Belt.defaultContext();
		int parallelism = Runtime.getRuntime().availableProcessors();
		assertEquals(parallelism, ctx.getParallelism());
	}

	@Test(expected = NullPointerException.class)
	public void testNullJob() {
		Context ctx = Belt.defaultContext();
		ctx.submit(null);
	}

	@Test
	public void testSimpleJob() throws ExecutionException, InterruptedException {
		Context ctx = Belt.defaultContext();
		String result = ctx.submit(() -> "Hello, world!").get();
		assertEquals("Hello, world!", result);
	}

	@Test(expected = ExecutionException.class)
	public void testThrowingJob() throws ExecutionException, InterruptedException {
		Context ctx = Belt.defaultContext();
		Future<Void> future = ctx.submit(() -> {
			throw new RuntimeException();
		});
		future.get();
	}

	@Test
	public void testToString() {
		Context ctx = Belt.defaultContext();
		String expected = "Default execution context (active, parallelism " + ctx.getParallelism() + ")";
		assertEquals(expected, ctx.toString());
	}

}
