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

package com.rapidminer.belt;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.util.Belt;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class ContextTests {

	private static final class TestException extends Exception {
		private static final long serialVersionUID = 4812446284457262805L;

		private TestException() {
			super();
		}
	}

	@RunWith(Parameterized.class)
	public static class ForEveryContext {

		@Parameterized.Parameter
		public Context ctx;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Context> workloads() {
			return Arrays.asList(Belt.defaultContext(), Context.singleThreaded(Belt.defaultContext()));
		}

		@Test
		public void testActiveness() {
			assertTrue(ctx.isActive());
		}


		@Test(expected = TestException.class)
		public void testCallablesExceptionHandling() throws Throwable {
			List<Callable<String>> callables = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				String val = "" + i;
				callables.add(() -> val);
			}
			callables.set(5, () -> {
				throw new TestException();
			});
			try {
				ctx.call(callables);
			} catch (ExecutionException e) {
				throw e.getCause();
			}
		}

		@Test(expected = NullPointerException.class)
		public void testNullCallables() throws ExecutionException {
			ctx.call(null);
		}

		@Test
		public void testEmptyCallables() throws ExecutionException {
			List<String> result = ctx.call(Collections.emptyList());
			assertTrue(result.isEmpty());
		}

		@Test(expected = NullPointerException.class)
		public void testCallablesWithNull() throws ExecutionException {
			ctx.call(Arrays.asList((Callable<Void>) () -> null, null));
		}

		@Test
		public void testCallables() throws ExecutionException {
			List<Callable<String>> callables = new ArrayList<>();
			for (int i = 0; i < 20; i++) {
				String val = "" + i;
				callables.add(() -> val);
			}
			List<String> result = ctx.call(callables);
			List<String> expected = IntStream.range(0, 20).mapToObj(i -> "" + i).collect(Collectors.toList());
			assertEquals(expected, result);
		}

		@Test
		public void testCallablesNested() throws ExecutionException, InterruptedException {
			List<Callable<List<String>>> callables = new ArrayList<>();
			for (int i = 0; i < 20; i += 2) {
				String val = "" + i;
				String val2 = "" + (i + 1);
				callables.add(() -> ctx.call(Arrays.asList(() -> val, () -> val2)));
			}
			List<List<String>> result = ctx.call(callables);
			List<List<String>> expected =
					IntStream.range(0, 10).mapToObj(i -> Arrays.asList(2 * i + "", 2 * i + 1 + "")).collect(Collectors.toList());
			assertEquals(expected, result);
		}

	}

	public static class ContextSpecific {

		@Test
		public void testParallelism() {
			Context ctx = Belt.defaultContext();
			int parallelism = Runtime.getRuntime().availableProcessors();
			assertEquals(parallelism, ctx.getParallelism());
		}

		@Test
		public void testToString() {
			Context ctx = Belt.defaultContext();
			String expected = "Default execution context (active, parallelism " + ctx.getParallelism() + ")";
			assertEquals(expected, ctx.toString());
		}

		@Test
		public void testParallelismSingle() {
			Context ctx = Context.singleThreaded(Belt.defaultContext());
			int parallelism = 1;
			assertEquals(parallelism, ctx.getParallelism());
		}

		@Test
		public void testToStringSingle() {
			Context ctx = Context.singleThreaded(Belt.defaultContext());
			String expected =
					"Single threaded context on top of Default execution context (active, parallelism "
							+ Belt.defaultContext().getParallelism() + ")";
			assertEquals(expected, ctx.toString());
		}

		@Test
		public void testParallelismSingleSingle() {
			Context ctx = Context.singleThreaded(Context.singleThreaded(Belt.defaultContext()));
			assertEquals(1, ctx.getParallelism());
		}
	}

}
