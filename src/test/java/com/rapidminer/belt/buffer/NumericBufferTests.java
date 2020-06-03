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

package com.rapidminer.belt.buffer;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.column.Column;


/**
 * @author Gisa Schaefer
 */
@RunWith(Enclosed.class)
public class NumericBufferTests {

	private static final double EPSILON = 1e-10;

	private static final String IMPL_FIXED_BUFFER = "fixed_double";

	private static final String IMPL_FIXED_INT_BUFFER = "fixed_integer";

	private static NumericBuffer getBuffer(String impl, int length, boolean initialize) {
		switch (impl) {
			case IMPL_FIXED_BUFFER:
				return Buffers.realBuffer(length, initialize);
			case IMPL_FIXED_INT_BUFFER:
				return Buffers.integer53BitBuffer(length, initialize);
			default:
				throw new IllegalStateException("Unknown column implementation");
		}
	}

	private static NumericBuffer getBuffer(String impl, int length) {
		switch (impl) {
			case IMPL_FIXED_BUFFER:
				return Buffers.realBuffer(length);
			case IMPL_FIXED_INT_BUFFER:
				return Buffers.integer53BitBuffer(length);
			default:
				throw new IllegalStateException("Unknown column implementation");
		}
	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	@RunWith(Parameterized.class)
	public static class Real {

		@Parameter
		public String bufferImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> bufferImplementations() {
			return Arrays.asList(IMPL_FIXED_BUFFER);
		}

		private NumericBuffer buffer(int length) {
			return getBuffer(bufferImplementation, length);
		}


		@Test
		public void testSet() {
			int n = 123;
			double[] testData = random(n);
			NumericBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, testData[i]);
			}
			assertArrayEquals(testData, buffer.getData(), EPSILON);
		}

		@Test
		public void testGet() {
			int n = 173;
			double[] testData = random(n);
			NumericBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, testData[i]);
			}
			double[] resultData = new double[n];
			for (int i = 0; i < n; i++) {
				resultData[i] = buffer.get(i);
			}
			assertArrayEquals(testData, resultData, EPSILON);
		}


		@Test
		public void testToStringSmall() {
			double[] data = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99};
			NumericBuffer buffer = buffer(7);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, data[i]);
			}
			String expected = "Real Buffer (" + data.length + ")\n(" + "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringMaxFit() {
			double[] datablock = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999};
			NumericBuffer buffer = buffer(32);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, datablock[i % datablock.length]);
			}

			String expected = "Real Buffer (" + buffer.size() + ")\n("
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringBigger() {
			double[] datablock = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999};
			int length = 33;
			NumericBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, datablock[i % datablock.length]);
			}
			buffer.set(buffer.size() - 1, 100);

			String expected = "Real Buffer (" + length + ")\n(" + "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, ..., 100.000)";

			assertEquals(expected, buffer.toString());
		}
	}


	@RunWith(Parameterized.class)
	public static class Integer {

		@Parameter
		public String bufferImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> bufferImplementations() {
			return Arrays.asList(IMPL_FIXED_INT_BUFFER);
		}

		private NumericBuffer buffer(int length) {
			return getBuffer(bufferImplementation, length);
		}


		@Test
		public void testSet() {
			int n = 123;
			double[] testData = random(n);
			NumericBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, testData[i]);
			}
			double[] expected = new double[n];
			for (int i = 0; i < n; i++) {
				expected[i] = Math.round(testData[i]);
			}
			assertArrayEquals(expected, buffer.getData(), EPSILON);
		}

		@Test
		public void testGet() {
			int n = 173;
			double[] testData = random(n);
			NumericBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, testData[i]);
			}
			double[] resultData = new double[n];
			for (int i = 0; i < n; i++) {
				resultData[i] = buffer.get(i);
			}
			double[] expected = new double[n];
			for (int i = 0; i < n; i++) {
				expected[i] = Math.round(testData[i]);
			}
			assertArrayEquals(expected, resultData, EPSILON);
		}

		private double[] random(int n) {
			double[] data = new double[n];
			Arrays.setAll(data, i -> 42 * Math.random());
			return data;
		}


		@Test
		public void testToStringSmall() {
			double[] data = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99};
			NumericBuffer buffer = buffer(7);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, data[i]);
			}
			String expected = "Integer Buffer (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringMaxFit() {
			double[] datablock = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999};
			NumericBuffer buffer = buffer(32);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, datablock[i % datablock.length]);
			}

			String expected = "Integer Buffer (" + buffer.size() + ")\n("
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringBigger() {
			double[] datablock = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999};
			int length = 33;
			NumericBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, datablock[i % datablock.length]);
			}
			buffer.set(buffer.size() - 1, 100);

			String expected = "Integer Buffer (" + length + ")\n(" + "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, ..., 100)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testRoundingOfRealColumn() {
			ThreadLocalRandom rng = ThreadLocalRandom.current();

			double[] data = new double[100];
			Arrays.setAll(data, i -> rng.nextDouble(-10, 10));
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, data);

			NumericBuffer buffer = Buffers.integer53BitBuffer(column);
			buffer.freeze();

			double[] expected = new double[100];
			Arrays.setAll(expected, i -> Math.round(data[i]));

			assertArrayEquals(expected, buffer.getData(), EPSILON);
		}

	}


	@RunWith(Parameterized.class)
	public static class Both {

		@Parameter
		public String bufferImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> bufferImplementations() {
			return Arrays.asList(IMPL_FIXED_BUFFER, IMPL_FIXED_INT_BUFFER);
		}

		private NumericBuffer buffer(int length) {
			return getBuffer(bufferImplementation, length);
		}

		@Test
		public void testBufferLength() {
			NumericBuffer buffer = buffer(197);
			assertEquals(197, buffer.size());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLength() {
			buffer(-5);
		}


		@Test
		public void testZeroBufferLength() {
			NumericBuffer buffer = buffer(0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testToColumnToBuffer() {
			int n = 42;
			double[] testData = random(n);
			NumericBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, testData[i]);
			}
			NumericBuffer buffer2 = new RealBuffer(buffer.toColumn());

			double[] resultData = new double[n];
			for (int i = 0; i < n; i++) {
				resultData[i] = buffer2.get(i);
			}
			double[] expected = new double[n];
			for (int i = 0; i < n; i++) {
				expected[i] = buffer.get(i);
			}
			assertArrayEquals(expected, resultData, EPSILON);
		}

		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			double[] testData = random(n);
			NumericBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, testData[i]);
			}
			buffer.freeze();
			buffer.set(5, 42);
		}

		@Test
		public void testNotFinite() {
			double[] data = {Double.NaN, Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
			NumericBuffer buffer = buffer(4);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, data[i]);
			}
			String expected = buffer.type() + " Buffer (" + data.length + ")\n(" + "?, -Infinity, ?, Infinity)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testInitialization() {
			int n = 42;
			NumericBuffer initialized = buffer(n);
			NumericBuffer notInitialized = getBuffer(bufferImplementation, n, false);
			for (int i = 0; i < notInitialized.size(); i++) {
				notInitialized.set(i, Double.NaN);
			}

			double[] resultData = new double[n];
			for (int i = 0; i < n; i++) {
				resultData[i] = notInitialized.get(i);
			}
			double[] expected = new double[n];
			for (int i = 0; i < n; i++) {
				expected[i] = initialized.get(i);
			}
			assertArrayEquals(expected, resultData, EPSILON);
		}

	}

	public static class InputValidation{

		@Test(expected = NullPointerException.class)
		public void testNull(){
			Buffers.realBuffer(null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullInt(){
			Buffers.integer53BitBuffer(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType(){
			Buffers.realBuffer(ColumnAccessor.get().newDateTimeColumn(new long[0], null));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeInt(){
			Buffers.integer53BitBuffer(ColumnAccessor.get().newDateTimeColumn(new long[0], null));
		}
	}
}
