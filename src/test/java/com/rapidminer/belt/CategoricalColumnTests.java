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

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * @author Michael Knopf
 */
@RunWith(Enclosed.class)
public class CategoricalColumnTests {

	private static final String IMPL_SIMPLE_CATEGORICAL_STRING = "SimpleCategoricalColumn_String";

	private static final String IMPL_MAPPED_CATEGORICAL_STRING = "MappedCategoricalColumn_String";

	private static final List<Object[]> MINIMAL_CATEGORICAL_COLUMNS = Arrays.asList(
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT2},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT4},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT8},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT16},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, IntegerFormats.Format.SIGNED_INT32},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT2},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT4},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT8},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, IntegerFormats.Format.UNSIGNED_INT16},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, IntegerFormats.Format.SIGNED_INT32},
			new Object[] {IMPL_MAPPED_CATEGORICAL_STRING, IntegerFormats.Format.SIGNED_INT32});

	private static final double EPSILON = 1e-10;
	private static final int MAX_VALUES = 30;

	private static int[] random(int n) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(MAX_VALUES));
		return data;
	}

	private static int[] randomBoolean(int n) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(3));
		return data;
	}

	private static int[] random(int n, IntegerFormats.Format format) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(format.maxValue()));
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

	private static List<String> getMappingList() {
		List<String> list = new ArrayList<>(MAX_VALUES);
		list.add(null);
		for (int i = 1; i < MAX_VALUES; i++) {
			list.add("value" + i);
		}
		return list;
	}

	private static List<String> getBooleanMappingList() {
		List<String> list = new ArrayList<>(3);
		list.add(null);
		for (int i = 1; i < 3; i++) {
			list.add("value" + i);
		}
		return list;
	}

	private static byte[] toByteArray(int[] data, IntegerFormats.Format format) {
		byte[] bytes;
		switch (format) {
			case UNSIGNED_INT2:
				bytes = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
				for (int i = 0; i < data.length; i++) {
					IntegerFormats.writeUInt2(bytes, i, data[i]);
				}
				break;
			case UNSIGNED_INT4:
				bytes = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
				for (int i = 0; i < data.length; i++) {
					IntegerFormats.writeUInt4(bytes, i, data[i]);
				}
				break;
			case UNSIGNED_INT8:
				bytes = new byte[data.length];
				for (int i = 0; i < data.length; i++) {
					bytes[i] = (byte) data[i];
				}
				break;
			default:
				throw new IllegalArgumentException("Unsupported format");
		}
		return bytes;
	}

	private static short[] toShortArray(int[] data) {
		short[] bytes = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			bytes[i] = (short) data[i];
		}
		return bytes;
	}

	private static final ColumnType<String> TYPE = ColumnTypes.categoricalType(
			"com.rapidminer.belt.column.test.stringcolumn", String.class, null);

	private static Column column(String columnImplementation, int[] data, IntegerFormats.Format format) {
		switch (columnImplementation) {
			case IMPL_SIMPLE_CATEGORICAL_STRING:
				switch (format) {
					case UNSIGNED_INT2:
					case UNSIGNED_INT4:
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new SimpleCategoricalColumn<>(TYPE, packed, getMappingList());
					case UNSIGNED_INT16:
						return new SimpleCategoricalColumn<>(TYPE, toShortArray(data), getMappingList());
					case SIGNED_INT32:
					default:
						return new SimpleCategoricalColumn<>(TYPE, data, getMappingList());
				}
			case IMPL_MAPPED_CATEGORICAL_STRING:
				// Create random index mapping and apply the inverse such that the values returned by the mapped column
				// match the input data.
				int[] mapping = permutation(data.length);
				int[] mappedData = new int[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}

				switch (format) {
					case UNSIGNED_INT2:
					case UNSIGNED_INT4:
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(mappedData, format), format, mappedData.length);
						return new MappedCategoricalColumn<>(TYPE, packed, getMappingList(), mapping);
					case UNSIGNED_INT16:
						return new MappedCategoricalColumn<>(TYPE, toShortArray(mappedData), getMappingList(), mapping);
					case SIGNED_INT32:
					default:
						return new MappedCategoricalColumn<>(TYPE, mappedData, getMappingList(), mapping);
				}
			default:
				throw new IllegalStateException("Unknown column implementation");
		}
	}

	private static CategoricalColumn<?> booleanColumn(String columnImplementation, int[] data, IntegerFormats.Format format, int positiveIndex) {
		switch (columnImplementation) {
			case IMPL_SIMPLE_CATEGORICAL_STRING:
				switch (format) {
					case UNSIGNED_INT2:
					case UNSIGNED_INT4:
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new SimpleCategoricalColumn<>(TYPE, packed, getBooleanMappingList(), positiveIndex);
					case UNSIGNED_INT16:
						return new SimpleCategoricalColumn<>(TYPE, toShortArray(data), getBooleanMappingList(), positiveIndex);
					case SIGNED_INT32:
					default:
						return new SimpleCategoricalColumn<>(TYPE, data, getBooleanMappingList(), positiveIndex);
				}
			case IMPL_MAPPED_CATEGORICAL_STRING:
				// Create random index mapping and apply the inverse such that the values returned by the mapped column
				// match the input data.
				int[] mapping = permutation(data.length);
				int[] mappedData = new int[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}

				switch (format) {
					case UNSIGNED_INT2:
					case UNSIGNED_INT4:
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(mappedData, format), format, mappedData.length);
						return new MappedCategoricalColumn<>(TYPE, packed, getBooleanMappingList(), mapping, positiveIndex);
					case UNSIGNED_INT16:
						return new MappedCategoricalColumn<>(TYPE, toShortArray(mappedData), getBooleanMappingList(), mapping, positiveIndex);
					case SIGNED_INT32:
					default:
						return new MappedCategoricalColumn<>(TYPE, mappedData, getBooleanMappingList(), mapping, positiveIndex);
				}
			default:
				throw new IllegalStateException("Unknown column impl");
		}
	}

	@RunWith(Parameterized.class)
	public static class CategoricalMapping {

		@Parameter
		public String columnImplementation;

		@Parameter(value = 1)
		public IntegerFormats.Format format;

		@Parameters(name = "{0}_({1})")
		public static Iterable<Object[]> columnImplementations() {
			return MINIMAL_CATEGORICAL_COLUMNS;
		}

		private Column column(int[] data) {
			return CategoricalColumnTests.column(columnImplementation, data, format);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			int nValues = 1605;
			int[] data = random(nValues, format);
			Column column = column(data);
			column.getDictionary(int[].class);
		}

		@Test
		public void testSuperType() {
			int nValues = 1325;
			int[] data = random(nValues, format);
			Column column = column(data);
			List<Object> superMapping = column.getDictionary(Object.class);
			List<String> stringMapping = column.getDictionary(String.class);
			for (int i = 0; i < superMapping.size(); i++) {
				assertEquals(stringMapping.get(i), superMapping.get(i));
			}
		}

	}


	@RunWith(Parameterized.class)
	public static class ToBuffer {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_SIMPLE_CATEGORICAL_STRING, IMPL_MAPPED_CATEGORICAL_STRING);
		}

		@SuppressWarnings("unchecked")
		private CategoricalColumn<String> column(int[] data) {
			return (CategoricalColumn<String>) CategoricalColumnTests.column(columnImplementation, data, IntegerFormats.Format.SIGNED_INT32);
		}

		private Int32CategoricalBuffer<String> toCategoricalBuffer(Column column) {
			return new Int32CategoricalBuffer<>(column, String.class);
		}

		private FreeColumnBuffer<String> toFreeBuffer(Column column) {
			return new FreeColumnBuffer<>(String.class, column);
		}

		@Test
		public void testCategorical() {
			int nValues = 1605;
			int[] data = random(nValues);
			CategoricalColumn<String> column = column(data);
			Int32CategoricalBuffer buffer = toCategoricalBuffer(column);
			assertEquals(column.size(), buffer.size());

			for (int i = 0; i < column.size(); i++) {
				String expected;
				if (column instanceof MappedCategoricalColumn) {
					expected = column.getDictionary().get(column.getIntData()[((MappedCategoricalColumn) column).getRowMapping()[i]]);
				} else {
					expected = column.getDictionary().get(column.getIntData()[i]);
				}
				assertEquals(expected, buffer.get(i));
			}
		}

		@Test
		public void testDouble() {
			int nValues = 1703;
			int[] data = random(nValues);
			Column column = column(data);
			ColumnBuffer buffer = new FixedIntegerBuffer(column);
			assertEquals(column.size(), buffer.size());

			ColumnReader reader = new ColumnReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}


		@Test
		public void testFree() {
			int nValues = 1404;
			int[] data = random(nValues);
			CategoricalColumn<String> column = column(data);
			FreeColumnBuffer buffer = toFreeBuffer(column);
			assertEquals(column.size(), buffer.size());

			for (int i = 0; i < column.size(); i++) {
				String expected;
				if (column instanceof MappedCategoricalColumn) {
					expected = column.getDictionary().get(column.getIntData()[((MappedCategoricalColumn) column).getRowMapping()[i]]);
				} else {
					expected = column.getDictionary().get(column.getIntData()[i]);
				}
				assertEquals(expected, buffer.get(i));
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class Boolean {

		@Parameter
		public String columnImplementation;

		@Parameter(value = 1)
		public IntegerFormats.Format format;

		@Parameters(name = "{0}_({1})")
		public static Iterable<Object[]> columnImplementations() {
			return MINIMAL_CATEGORICAL_COLUMNS;
		}

		private CategoricalColumn<?> column(int[] data) {
			return CategoricalColumnTests.booleanColumn(columnImplementation, data, format, Math.random() > 0.5 ? 2 : 1);
		}

		@Test
		public void testMappingView() {
			int nValues = 1172;
			int[] data = randomBoolean(nValues);

			CategoricalColumn<?> column = column(data);
			int[] mapping = permutation(nValues);

			Column mappedColumn = column.map(mapping, true);

			assertEquals(column.toBoolean(1), mappedColumn.toBoolean(1));
			assertEquals(column.toBoolean(2), mappedColumn.toBoolean(2));
		}

		@Test
		public void testMappingNonView() {
			int nValues = 1132;
			int[] data = randomBoolean(nValues);

			CategoricalColumn<?> column = column(data);
			int[] mapping = permutation(nValues / 100);

			Column mappedColumn = column.map(mapping, false);

			assertEquals(column.toBoolean(1), mappedColumn.toBoolean(1));
			assertEquals(column.toBoolean(2), mappedColumn.toBoolean(2));
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedIndex() {
			int nValues = 113;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.booleanColumn(columnImplementation, data, format, CategoricalColumn.NOT_BOOLEAN);
			column.toBoolean(1);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedString() {
			int nValues = 112;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.booleanColumn(columnImplementation, data, format, CategoricalColumn.NOT_BOOLEAN);
			column.toBoolean("Maybe");
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedNumber() {
			int nValues = 112;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.booleanColumn(columnImplementation, data, format, CategoricalColumn.NOT_BOOLEAN);
			column.toBoolean(0.5);
		}
	}

}
