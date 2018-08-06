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
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Parameterized.class)
public class ColumnTaskTests {

	private static final double EPSILON = 1e-10;

	private static Context CTX = Belt.defaultContext();

	private static double[] readBufferToArray(ColumnBuffer buffer) {
		double[] data = new double[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}


	@Parameter
	public ApiTest test;

	@Parameters(name = "{0}")
	public static Iterable<ApiTest> tests() {
		List<ApiTest> tests = new ArrayList<>(3);

		Table input = Table.newTable(100)
				.addReal("a", i -> Math.random())
				.addReal("b", i -> Math.random())
				.addReal("c", i -> Math.random())
				.build(CTX);

		tests.add(new ApiTest("map_one",
				input.transform("a").applyNumericToReal(x -> x * 3 - 1, Workload.DEFAULT),
				input.transform("a").applyNumericToReal(x -> x * 3 - 1, Workload.DEFAULT, CTX),
				100));

		tests.add(new ApiTest("map_two",
				input.transform("a", "b").applyNumericToReal((a, b) -> a * 3 + b, Workload.DEFAULT),
				input.transform("a", "b").applyNumericToReal((a, b) -> a * 3 + b, Workload.DEFAULT, CTX),
				100));

		tests.add(new ApiTest("map_three",
				input.transform("a", "b", "c").applyNumericToReal(row -> row.get(0) * 3 - row.get(1) + 2 * row.get(2),
						Workload.DEFAULT),
				input.transform("a", "b", "c").applyNumericToReal(row -> row.get(0) * 3 - row.get(1) + 2 * row.get(2),
						Workload.DEFAULT, CTX),
				100));

		return tests;
	}

	@Test
	public void testTableTaskSize() {
		assertEquals(test.size, test.task.size());
	}

	@Test
	public void testTableTaskResult() {
		ColumnBuffer buffer = test.task.run(CTX);
		assertEquals(test.size, buffer.size());
		assertArrayEquals(readBufferToArray(test.buffer), readBufferToArray(buffer), EPSILON);
	}

	@Test
	public void testContextInteraction() {
		Context ctx = spy(CTX);
		test.task.run(ctx);
		verify(ctx, atLeast(1)).isActive();
		verify(ctx, times(1)).submit(any());
	}

	@Test
	public void testToString() {
		assertEquals("Column task (" + test.size + ")", test.task.toString());
	}

	public static class ApiTest {
		final String name;
		final ColumnTask task;
		final ColumnBuffer buffer;
		final int size;

		private ApiTest(String name, ColumnTask task, ColumnBuffer buffer, int size) {
			this.name = name;
			this.task = task;
			this.buffer = buffer;
			this.size = size;
		}

		@Override
		public String toString() {
			return name;
		}
	}


}
