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

package com.rapidminer.belt.column;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;
import java.util.SplittableRandom;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;


/**
 * @author Michael Knopf, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class NumericColumnTests {

	private static final String IMPL_DOUBLE_ARRAY = "DoubleArrayColumn";

	private static final String IMPL_DOUBLE_SPARSE_COLUMN = "DoubleSparseColumn";

	private static final String IMPL_DOUBLE_SPARSE_COLUMN_INT = "DoubleSparseColumnInt";

	private static final String IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN = "DoubleSparseColumnDefaultIsNaN";

	private static final String IMPL_DOUBLE_SPARSE_COLUMN_INT_DEFAULT_IS_NAN = "DoubleSparseColumnIntDefaultIsNaN";

	private static final String IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_NOT_IN_DATA = "DoubleSparseColumnDefaultNotInData";

	private static final String IMPL_DOUBLE_ARRAY_INT = "DoubleArrayColumn_Int";

	private static final String IMPL_MAPPED_DOUBLE_ARRAY = "MappedDoubleArrayColumn";

	private static final String IMPL_MAPPED_DOUBLE_ARRAY_INT = "MappedDoubleArrayColumn_Int";

	private static final double EPSILON = 1e-10;

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random() - 0.5d);
		return data;
	}

	private static double[] randomInts(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.floor((Math.random() - 0.5d) * 10));
		return data;
	}

	private static double[] randomWithSame(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random() < 0.2 ? Double.NaN : Math.floor(10 * (Math.random() - 0.5d)));
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
				return new DoubleArrayColumn(TypeId.REAL, data);
			case IMPL_DOUBLE_ARRAY_INT:
				return new DoubleArrayColumn(TypeId.INTEGER_53_BIT, data);
			case IMPL_DOUBLE_SPARSE_COLUMN:
				return new DoubleSparseColumn(TypeId.REAL, ColumnTestUtils.getMostFrequentValue(data, -0.00001d), data);
			case IMPL_DOUBLE_SPARSE_COLUMN_INT:
				return new DoubleSparseColumn(TypeId.INTEGER_53_BIT, ColumnTestUtils.getMostFrequentValue(data, 17d), data);
			case IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN:
				return new DoubleSparseColumn(TypeId.REAL, Double.NaN, data);
			case IMPL_DOUBLE_SPARSE_COLUMN_INT_DEFAULT_IS_NAN:
				return new DoubleSparseColumn(TypeId.INTEGER_53_BIT, Double.NaN, data);
			case IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_NOT_IN_DATA:
				return new DoubleSparseColumn(TypeId.REAL, Math.random(), data);
			case IMPL_MAPPED_DOUBLE_ARRAY:
				// Create random index mapping and apply the inverse such that the values returned by the mapped column
				// match the input data.
				int[] mapping = permutation(data.length);
				double[] mappedData = new double[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
				return new MappedDoubleArrayColumn(TypeId.REAL, mappedData, mapping);
			case IMPL_MAPPED_DOUBLE_ARRAY_INT:
				// Create random index mapping and apply the inverse such that the values returned by the mapped column
				// match the input data.
				mapping = permutation(data.length);
				mappedData = new double[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
				return new MappedDoubleArrayColumn(Column.TypeId.INTEGER_53_BIT, mappedData, mapping);
			default:
				throw new IllegalStateException("Unknown column implementation");
		}
	}

	@RunWith(Parameterized.class)
	public static class NewNumericColumn {
		/**
		 * This is used with a seed so that tests results that depend on randomness can be reproduced.
		 */
		private static SplittableRandom random;

		private static final int NUM_ROWS = 6746;

		private static final int ITERATIONS = 50;

		/**
		 * We use values for sparsity that will lead very likely either to sparse columns or to dense columns. Values
		 * close to {@link InternalColumnsImpl#MIN_SPARSITY_64BIT} can lead to both and we are not interested in testing this
		 * here.
		 */
		@Parameter
		public double sparsity;

		@Parameter(value = 1)
		public double paramDefaultValue;

		@Parameters(name = "Sparsity = {0}; default value = {1}")
		public static Iterable<Double[]> setParameters() {
			return Arrays.asList(new Double[][]{
					{0.99, 0d}, {0.99, -1.2d}, {0.99, 64d}, {0.99, Double.NaN},
					{0.85, 0d}, {0.85, -1.2d}, {0.85, 64d}, {0.85, Double.NaN},
					{0.75, 0d}, {0.75, -1.2d}, {0.75, 64d}, {0.75, Double.NaN},
					{0.70, 0d}, {0.70, -1.2d}, {0.70, 64d}, {0.70, Double.NaN},
					{0.55, 0d}, {0.55, -1.2d}, {0.55, 64d}, {0.55, Double.NaN},
					{0.01, 0d}, {0.01, -1.2d}, {0.01, 64d}, {0.01, Double.NaN},});
		}

		@BeforeClass
		public static void setUp() {
			random = new SplittableRandom(13636027794303224l);
		}

		/**
		 * In this test we check if all predictions regarding the sparsity of columns are correct. Caution: If we
		 * weren't using the same seed for every test run there would be a (very small) chance that the test would fail.
		 */
		@Test
		public void testNewNumericColumn() {
			InternalColumnsImpl columns = new InternalColumnsImpl();
			double[] data = new double[NUM_ROWS];
			Arrays.setAll(data, i -> sparsity > random.nextDouble() ? paramDefaultValue : random.nextDouble());
			double dataSparsity = Arrays.stream(data).map(x -> doubleEquals(x, paramDefaultValue) ? 1 : 0).sum() / data.length;
			for (int i = 0; i < ITERATIONS; i++) {
				Column column = columns.createNumericColumn(Math.random() > 0.5 ? TypeId.REAL : TypeId.INTEGER_53_BIT, data, random);
				if (column instanceof DoubleSparseColumn) {
					assertTrue(dataSparsity >= InternalColumnsImpl.MIN_SPARSITY_64BIT);
					assertTrue(doubleEquals(((DoubleSparseColumn) column).getDefaultValue(), paramDefaultValue));
				} else {
					assertTrue(dataSparsity < InternalColumnsImpl.MIN_SPARSITY_64BIT);
				}
			}
		}

		private boolean doubleEquals(double value, double other) {
			return (Double.isNaN(value) && Double.isNaN(other)) || (other == value);
		}

	}

	@RunWith(Parameterized.class)
	public static class StripData {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_DOUBLE_ARRAY_INT, IMPL_MAPPED_DOUBLE_ARRAY,
					IMPL_MAPPED_DOUBLE_ARRAY_INT, IMPL_DOUBLE_SPARSE_COLUMN, IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_NOT_IN_DATA);
		}

		private Column column(double[] data) {
			return NumericColumnTests.column(columnImplementation, data);
		}

		@Test
		public void testStripProperties() {
			int nValues = 1605;
			double[] data = random(nValues);
			Column column = column(data);

			Column stripped = column.stripData();
			assertEquals(0, stripped.size());
			assertEquals(column.type(), stripped.type());
		}

		@Test
		public void testAfterMap() {
			int nValues = 1325;
			double[] data = random(nValues);
			Column column = column(data);

			Column stripped = column.map(new int[]{5,3,17}, true).stripData();
			assertEquals(0, stripped.size());
			assertEquals(column.type(), stripped.type());
		}

	}

	@RunWith(Parameterized.class)
	public static class Read {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_DOUBLE_ARRAY_INT, IMPL_MAPPED_DOUBLE_ARRAY,
					IMPL_MAPPED_DOUBLE_ARRAY_INT, IMPL_DOUBLE_SPARSE_COLUMN, IMPL_DOUBLE_SPARSE_COLUMN_INT,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN, IMPL_DOUBLE_SPARSE_COLUMN_INT_DEFAULT_IS_NAN,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_NOT_IN_DATA);
		}

		private Column column(double[] data) {
			return NumericColumnTests.column(columnImplementation, data);
		}

		@Test
		public void testAllValuesEqual() {
			double[] data = new double[1384];
			Arrays.fill(data, Math.random());
			Column column = column(data);
			NumericReader reader = Readers.numericReader(column);
			double[] readData = new double[data.length];
			int index = 0;
			while (reader.hasRemaining()) {
				readData[index++] = reader.read();
			}
			assertArrayEquals(data, readData, 1e-10);
		}

		@Test
		public void testOneValue() {
			int nValues = 1;
			double[] data = randomInts(nValues);
			Column column = column(data);
			NumericReader reader = Readers.numericReader(column);
			double[] readData = new double[data.length];
			int index = 0;
			while (reader.hasRemaining()) {
				readData[index++] = reader.read();
			}
			assertArrayEquals(data, readData, 1e-10);
		}

		@Test
		public void testEmpty() {
			double[] data = new double[0];
			Column column = column(data);
			NumericReader reader = Readers.numericReader(column);
			double[] readData = new double[data.length];
			int index = 0;
			while (reader.hasRemaining()) {
				readData[index++] = reader.read();
			}
			assertArrayEquals(data, readData, 1e-10);
		}

		@Test
		public void testInts() {
			int nValues = 1605;
			double[] data = randomInts(nValues);
			Column column = column(data);
			NumericReader reader = Readers.numericReader(column);
			double[] readData = new double[data.length];
			int index = 0;
			while (reader.hasRemaining()) {
				readData[index++] = reader.read();
			}
			assertArrayEquals(data, readData, 1e-10);
		}

		@Test
		public void testDoubles() {
			int nValues = 1605;
			double[] data = random(nValues);
			Column column = column(data);
			NumericReader reader = Readers.numericReader(column);
			double[] readData = new double[data.length];
			int index = 0;
			while (reader.hasRemaining()) {
				readData[index++] = reader.read();
			}
			assertArrayEquals(data, readData, 1e-10);
		}

	}

	@RunWith(Parameterized.class)
	public static class ToBuffer {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_DOUBLE_ARRAY_INT, IMPL_MAPPED_DOUBLE_ARRAY,
					IMPL_MAPPED_DOUBLE_ARRAY_INT, IMPL_DOUBLE_SPARSE_COLUMN, IMPL_DOUBLE_SPARSE_COLUMN_INT,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN, IMPL_DOUBLE_SPARSE_COLUMN_INT_DEFAULT_IS_NAN,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_NOT_IN_DATA);
		}

		private Column column(double[] data) {
			return NumericColumnTests.column(columnImplementation, data);
		}

		private NumericBuffer toBuffer(Column column) {
			if (column.type().id() == Column.TypeId.INTEGER_53_BIT) {
				return Buffers.integer53BitBuffer(column);
			} else {
				return Buffers.realBuffer(column);
			}
		}

		@Test
		public void testSimple() {
			int nValues = 1605;
			double[] data = random(nValues);
			Column column = column(data);
			NumericBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			NumericReader reader = Readers.numericReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}

		@Test
		public void testEmpty() {
			double[] data = new double[0];
			Column column = column(data);
			NumericBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			NumericReader reader = Readers.numericReader(column, column.size());
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
			NumericBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			NumericReader reader = Readers.numericReader(column, column.size());
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
			NumericBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			NumericReader reader = Readers.numericReader(column, column.size());
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
			NumericBuffer buffer = toBuffer(column);
			assertEquals(column.size(), buffer.size());

			NumericReader reader = Readers.numericReader(column, column.size());
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
			NumericBuffer buffer = toBuffer(column);
			assertEquals(column.type().id(), buffer.type());

		}

	}

	@RunWith(Parameterized.class)
	public static class Sort {


		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_MAPPED_DOUBLE_ARRAY, IMPL_DOUBLE_SPARSE_COLUMN,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN, IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_NOT_IN_DATA);
		}

		private Column column(double[] data) {
			return NumericColumnTests.column(columnImplementation, data);
		}

		private double[] sparseDoubleData(int n) {
			double[] result = new double[n];
			double[] valuePool = getValuePool(10);
			double defaultValue = Math.random();
			Random random = new Random();
			for (int i = 0; i < n; i++) {
				int nextInt = random.nextInt(valuePool.length * 10);
				if (nextInt < valuePool.length) {
					result[i] = valuePool[nextInt];
				} else {
					result[i] = defaultValue;
				}
			}
			return result;
		}

		/**
		 * Helper method that returns a pool of random values.
		 */
		private static double[] getValuePool(int size) {
			double[] valuePool = new double[size];
			for (int i = 0; i < size; i++) {
				valuePool[i] = Math.random();
			}
			return valuePool;
		}

		@Test
		public void testAllValuesEqual() {
			double[] data = new double[1384];
			Arrays.fill(data, 3.7d);

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			Column column = column(data);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testAscending() {
			double[] data = randomWithSame(1342);

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			Column column = column(data);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testSparse() {
			double[] data = sparseDoubleData(1342);

			double[] jdkSorting = Arrays.copyOf(data, data.length);
			Arrays.sort(jdkSorting);

			Column column = column(data);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
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
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
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
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testEmptyMapping() {
			int nValues = 342;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[0];

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(new double[0], customSorting, EPSILON);
		}

		@Test
		public void testOneElementMapping() {
			int nValues = 342;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[1];
			mapping[0] = new Random().nextInt(nValues);

			double[] jdkSorting = Mapping.apply(data, mapping);
			Arrays.sort(jdkSorting);

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
			Arrays.setAll(customSorting, buffer::get);

			assertArrayEquals(jdkSorting, customSorting, EPSILON);
		}

		@Test
		public void testTwoElementMapping() {
			int nValues = 342;
			double[] data = randomWithSame(nValues);

			int[] mapping = new int[2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			double[] jdkSorting = Mapping.apply(data, mapping);
			Arrays.sort(jdkSorting);

			Column column = column(data).map(mapping, true);
			int[] order = column.sort(Order.ASCENDING);
			double[] customSorting = new double[column.size()];
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
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
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
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
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
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
			NumericBuffer buffer = Buffers.realBuffer(column.map(order, false));
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
			return Arrays.asList(IMPL_DOUBLE_ARRAY, IMPL_MAPPED_DOUBLE_ARRAY, IMPL_DOUBLE_SPARSE_COLUMN,
					IMPL_DOUBLE_SPARSE_COLUMN_DEFAULT_IS_NAN);
		}

		private Column column(double[] data) {
			return NumericColumnTests.column(columnImplementation, data);
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
			return Arrays.asList(IMPL_DOUBLE_ARRAY_INT, IMPL_MAPPED_DOUBLE_ARRAY_INT, IMPL_DOUBLE_SPARSE_COLUMN_INT);
		}

		private Column column(double[] data) {
			return NumericColumnTests.column(columnImplementation, data);
		}

		@Test
		public void testSmall() {
			double[] data = {5, 7.1, 3.56, 1.1111, 4, 4.7, 8.99};
			Column column = column(data);

			String expected = Column.TypeId.INTEGER_53_BIT + " Column (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9)";

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

			String expected = Column.TypeId.INTEGER_53_BIT + " Column (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9, 10, "
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

			String expected = TypeId.INTEGER_53_BIT + " Column (" + data.length + ")\n(" + "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, 9, 10, "
					+ "5, 7, 4, 1, 4, 5, ..., 100)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testNotFinite() {
			double[] data = {Double.NaN, Double.NEGATIVE_INFINITY, Double.NaN, Double.POSITIVE_INFINITY};
			Column column = column(data);
			String expected = TypeId.INTEGER_53_BIT + " Column (" + data.length + ")\n(" + "?, -Infinity, ?, Infinity)";

			assertEquals(expected, column.toString());
		}
	}

}
