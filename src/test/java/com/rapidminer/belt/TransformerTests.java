package com.rapidminer.belt;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class TransformerTests {

	private static final double EPSILON = 1e-10;

	private static final Context CTX = Belt.defaultContext();

	public static class Simple {

		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Builders.newTableBuilder(100)
					.addReal("zero", i -> 0)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.build(CTX);
		}

		@AfterClass
		public static void freeTable() {
			table = null;
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform("zero").workload(null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCallback() {
			table.transform("two").callback(null);
		}

		@Test
		public void testCallbackCompletion() {
			final double[] result = new double[]{-1.0};
			table.transform("one")
					.workload(Workload.SMALL)
					.callback(p -> result[0] = p)
					.applyNumericToReal(i -> 2 * i, CTX);
			assertEquals(1.0, result[0], EPSILON);
		}

		@Test
		public void testCallbackReplacement() {
			final double[] result = new double[]{-1.0, -1.0, -1.0};
			table.transform("one")
					.workload(Workload.SMALL)
					.callback(p -> result[0] = Double.NaN)
					.callback(p -> result[1] = Double.NaN)
					.callback(p -> result[2] = p)
					.applyNumericToReal(i -> 2 * i, CTX);
			assertArrayEquals(new double[]{-1.0, -1.0, 1.0}, result, EPSILON);
		}

	}

	public static class Multi {

		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Builders.newTableBuilder(100)
					.addReal("zero", i -> 0)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.build(CTX);
		}

		@AfterClass
		public static void freeTable() {
			table = null;
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform("zero", "one").workload(null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullCallback() {
			table.transform("zero", "two").callback(null);
		}

		@Test
		public void testCallbackCompletion() {
			final double[] result = new double[]{-1.0};
			table.transform("one", "two")
					.workload(Workload.SMALL)
					.callback(p -> result[0] = p)
					.applyNumericToReal(r -> r.get(0) + r.get(1), CTX);
			assertEquals(1.0, result[0], EPSILON);
		}

		@Test
		public void testCallbackReplacement() {
			final double[] result = new double[]{-1.0, -1.0, -1.0};
			table.transform("one", "zero")
					.workload(Workload.SMALL)
					.callback(p -> result[0] = Double.NaN)
					.callback(p -> result[1] = Double.NaN)
					.callback(p -> result[2] = p)
					.applyNumericToReal(r -> r.get(0) + r.get(1), CTX);
			assertArrayEquals(new double[]{-1.0, -1.0, 1.0}, result, EPSILON);
		}

	}

}
