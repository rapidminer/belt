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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.Column.TypeId;
import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class ColumnTests {

	private static final String IMPL_DOUBLE_ARRAY = "DoubleArrayColumn";

	private static final String IMPL_DOUBLE_ARRAY_INT = "DoubleArrayColumn_Int";

	private static final String IMPL_MAPPED_DOUBLE_ARRAY = "MappedDoubleArrayColumn";

	private static final String IMPL_MAPPED_DOUBLE_ARRAY_INT = "MappedDoubleArrayColumn_Int";

	private static final double EPSILON = 1e-10;

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	private static double[] randomWithSame(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random() < 0.2 ? Double.NaN : Math.floor(10 * Math.random()));
		return data;
	}

	private static int[] permutation(int n) {
		int[] indices = new int[n];
		Arrays.setAll(indices, i -> i);
		for (int i = 0; i < n; i++) {
			int a = (int) (Math.random() * n);
			int b = (int) (Math.random() * n);
			int tmp = indices[a];
			indices[a] = indices[b];
			indices[b] = tmp;
		}
		return indices;
	}

	private static Column column(String columnImplementation, double[] data) {
		switch (columnImplementation) {
			case IMPL_DOUBLE_ARRAY:
				return new DoubleArrayColumn(Column.TypeId.REAL, data);
			case IMPL_DOUBLE_ARRAY_INT:
				return new DoubleArrayColumn(Column.TypeId.INTEGER, data);
			case IMPL_MAPPED_DOUBLE_ARRAY:
				// Create random index mapping and apply the inverse such that the values returned by the mapped column
				// match the input data.
				int[] mapping = permutation(data.length);
				double[] mappedData = new double[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
				return new MappedDoubleArrayColumn(Column.TypeId.REAL, mappedData, mapping);
			case IMPL_MAPPED_DOUBLE_ARRAY_INT:
				// Create random index mapping and apply the inverse such that the values returned by the mapped column
				// match the input data.
				mapping = permutation(data.length);
				mappedData = new double[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
				return new MappedDoubleArrayColumn(Column.TypeId.INTEGER, mappedData, mapping);
			default:
				throw new IllegalStateException("Unknown column implementation");
		}
	}

	@RunWith(Parameterized.class)
	public static class ToBuffer {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_DOUBLE_ARRAY_INT, IMPL_MAPPED_DOUBLE_ARRAY,
					IMPL_MAPPED_DOUBLE_ARRAY_INT);
		}

		private Column column(double[] data) {
			return ColumnTests.column(columnImplementation, data);
		}

		private ColumnBuffer toBuffer(Column column) {
			if (column.type().id() == Column.TypeId.INTEGER) {
				return new FixedIntegerBuffer(column);
			} else {
				return new FixedRealBuffer(column);
			}
		}

		@Test
		public void testSimple() {
			int nValues = 1605;
			double[] data = random(nValues);
			Column column = column(data);
			ColumnBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			ColumnReader reader = new ColumnReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}

		@Test
		public void testSubsetMapping() {
			int nValues = 3 * 245;
			double[] data = random(nValues);
			int[] mapping = new int[nValues / 3];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column column = column(data).map(mapping, true);
			ColumnBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			ColumnReader reader = new ColumnReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}

		@Test
		public void testSupersetMapping() {
			int nValues = 2148;
			double[] data = random(nValues);
			int[] mapping = new int[nValues * 2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column column = column(data).map(mapping, true);
			ColumnBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			ColumnReader reader = new ColumnReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}

		@Test
		public void testMappingMissings() {
			int nValues = 2148;
			double[] data = random(nValues);
			int[] mapping = new int[nValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(2 * nValues) - nValues);

			Column column = column(data).map(mapping, true);
			ColumnBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			ColumnReader reader = new ColumnReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}

		@Test
		public void testBufferType() {
			int nValues = 105;
			double[] data = random(nValues);
			Column column = column(data);
			ColumnBuffer buffer = toBuffer(column);
			assertEquals(column.type().id(), buffer.type());

		}

	}

	@RunWith(Parameterized.class)
	public static class Sort {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_MAPPED_DOUBLE_ARRAY);
		}

		private Column column(double[] data) {
			return ColumnTests.column(columnImplementation, data);
		}

		@Test
		public void testAscending() {
			double[] data = randomWithSame(1342);

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			Column column = column(data);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			ColumnBuffer buffer = new FixedRealBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testDescending() {
			double[] data = randomWithSame(1375);

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);
			double[] reversedJdkSorting = new double[jdkSorting.length];
			for (int i = 0; i < jdkSorting.length; i++) {
				reversedJdkSorting[jdkSorting.length - i - 1] = jdkSorting[i];
			}

			Column column = column(data);
			int[] order = column.sort(Order.DESCENDING);
			double[] customSorting = new double[column.size()];
			ColumnBuffer buffer = new FixedRealBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(reversedJdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testSubsetMappingAndAscending() {
			int nValues = 342 * 3;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[nValues / 3];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			double[] jdkSorting = Mapping.apply(data, mapping);
			Arrays.sort(jdkSorting);

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			ColumnBuffer buffer = new FixedRealBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testSubsetMappingAndDescending() {
			int nValues = 375 * 3;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[nValues / 3];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			double[] jdkSorting = Mapping.apply(data, mapping);
			Arrays.sort(jdkSorting);
			double[] reversedJdkSorting = new double[jdkSorting.length];
			for (int i = 0; i < jdkSorting.length; i++) {
				reversedJdkSorting[jdkSorting.length - i - 1] = jdkSorting[i];
			}

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.DESCENDING);
			double[] customSorting = new double[column.size()];
			ColumnBuffer buffer = new FixedRealBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(reversedJdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testSupersetMappingAndAscending() {
			int nValues = 1342;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[nValues * 2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			double[] jdkSorting = Mapping.apply(data, mapping);
			Arrays.sort(jdkSorting);

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			ColumnBuffer buffer = new FixedRealBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testSupersetMappingAndDescending() {
			int nValues = 1375;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[nValues * 2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			double[] jdkSorting = Mapping.apply(data, mapping);
			Arrays.sort(jdkSorting);
			double[] reversedJdkSorting = new double[jdkSorting.length];
			for (int i = 0; i < jdkSorting.length; i++) {
				reversedJdkSorting[jdkSorting.length - i - 1] = jdkSorting[i];
			}

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.DESCENDING);
			double[] customSorting = new double[column.size()];
			ColumnBuffer buffer = new FixedRealBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(reversedJdkSorting, customSorting, EPSILON);
		}
	}

	@RunWith(Parameterized.class)
	public static class ToString {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_MAPPED_DOUBLE_ARRAY);
		}

		private Column column(double[] data) {
			return ColumnTests.column(columnImplementation, data);
		}

		@Test
		public void testSmall() {
			double[] data = { 5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99 };
			Column column = column(data);

			String expected = Column.TypeId.REAL + " Column (" + data.length + ")\n(" + "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testMaxFit() {
			double[] datablock = { 5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999 };
			double[] data = new double[32];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			Column column = column(data);

			String expected = Column.TypeId.REAL + " Column (" + data.length + ")\n(" + "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testBigger() {
			double[] datablock = { 5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999 };
			double[] data = new double[33];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			data[32] = 100;
			Column column = column(data);

			String expected = Column.TypeId.REAL + " Column (" + data.length + ")\n(" + "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, 8.990, 9.900, "
					+ "5.000, 7.100, 3.560, 1.111, 4.000, 4.700, ..., 100.000)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testNotFinite() {
			double[] data = {Double.NaN, Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
			Column column = column(data);
			String expected = TypeId.REAL + " Column (" + data.length + ")\n(" + "?, -Infinity, ?, Infinity)";

			assertEquals(expected, column.toString());
		}
	}

	@RunWith(Parameterized.class)
	public static class ToStringInteger {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY_INT, IMPL_MAPPED_DOUBLE_ARRAY_INT);
		}

		private Column column(double[] data) {
			return ColumnTests.column(columnImplementation, data);
		}

		@Test
		public void testSmall() {
			double[] data = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99};
			Column column = column(data);

			String expected = Column.TypeId.INTEGER + " Column (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testMaxFit() {
			double[] datablock = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999};
			double[] data = new double[32];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			Column column = column(data);

			String expected = Column.TypeId.INTEGER + " Column (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testBigger() {
			double[] datablock = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99, 9.8999};
			double[] data = new double[33];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			data[32] = 100;
			Column column = column(data);

			String expected = TypeId.INTEGER + " Column (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, ..., 100)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testNotFinite() {
			double[] data = {Double.NaN, Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
			Column column = column(data);
			String expected = TypeId.INTEGER + " Column (" + data.length + ")\n(" + "?, -Infinity, ?, Infinity)";

			assertEquals(expected, column.toString());
		}
	}

}
