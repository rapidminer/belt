/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;

import junit.framework.TestCase;


/**
 * @author Gisa Schaefer
 */
@RunWith(Enclosed.class)
public class NominalBufferTests {

	private static NominalBuffer buffer(int length, IntegerFormats.Format format) {
		switch (format) {
			case UNSIGNED_INT2:
				return new UInt2NominalBuffer(ColumnType.NOMINAL, length);
			case UNSIGNED_INT4:
				return new UInt4NominalBuffer(ColumnType.NOMINAL,length);
			case UNSIGNED_INT8:
				return new UInt8NominalBuffer(ColumnType.NOMINAL,length);
			case UNSIGNED_INT16:
				return new UInt16NominalBuffer(ColumnType.NOMINAL,length);
			case SIGNED_INT32:
				return new Int32NominalBuffer(ColumnType.NOMINAL, length);
			default:
				throw new IllegalArgumentException("Unsupported format");
		}
	}

	private static int[] random(int n, IntegerFormats.Format format) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(format.maxValue()));
		return data;
	}

	private static int[] getData(NominalBuffer buffer, IntegerFormats.Format format) {
		switch (format) {
			case SIGNED_INT32:
				return ((Int32NominalBuffer) buffer).getData();
			case UNSIGNED_INT16:
				return toIntArray(((UInt16NominalBuffer) buffer).getData());
			case UNSIGNED_INT8:
				return toIntArray(((UInt8NominalBuffer) buffer).getData());
			case UNSIGNED_INT4:
				return toIntArray(((UInt4NominalBuffer) buffer).getData());
			case UNSIGNED_INT2:
				return toIntArray(((UInt2NominalBuffer) buffer).getData());
			default:
				throw new IllegalArgumentException("Unsupported format");
		}
	}

	private static int[] toIntArray(short[] data) {
		int[] ints = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			ints[i] = Short.toUnsignedInt(data[i]);
		}
		return ints;
	}

	private static int[] toIntArray(IntegerFormats.PackedIntegers packedIntegers) {
		int[] ints;
		switch (packedIntegers.format()) {
			case UNSIGNED_INT2:
				ints = new int[packedIntegers.size()];
				for (int i = 0; i < ints.length; i++) {
					ints[i] = IntegerFormats.readUInt2(packedIntegers.data(), i);
				}
				break;
			case UNSIGNED_INT4:
				ints = new int[packedIntegers.size()];
				for (int i = 0; i < ints.length; i++) {
					ints[i] = IntegerFormats.readUInt4(packedIntegers.data(), i);
				}
				break;
			case UNSIGNED_INT8:
				ints = new int[packedIntegers.size()];
				for (int i = 0; i < ints.length; i++) {
					ints[i] = Byte.toUnsignedInt(packedIntegers.data()[i]);
				}
				break;
			default:
				throw new IllegalArgumentException("Unsupported format");
		}
		return ints;
	}

	@RunWith(Parameterized.class)
	public static class ValueStoring {

		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> tests() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		private NominalBuffer buffer(int length) {
			return NominalBufferTests.buffer(length, format);
		}

		private int[] random(int n) {
			return NominalBufferTests.random(n, format);
		}

		private int[] getData(NominalBuffer buffer) {
			return NominalBufferTests.getData(buffer, format);
		}

		@Test
		public void testBufferLength() {
			NominalBuffer buffer = buffer(197);
			assertEquals(197, buffer.size());
		}

		@Test
		public void testZeroBufferLength() {
			NominalBuffer buffer = buffer(0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + testData[i]);
			}
			buffer.set(42, null);

			String[] expected = Arrays.stream(testData).mapToObj(i -> "value" + i).toArray(String[]::new);
			expected[42] = null;

			String[] result = new String[n];
			Arrays.setAll(result, buffer::get);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testMappingOrder() {
			int n = Math.min(14, format.maxValue());
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + i);
			}
			List<String> expected = IntStream.range(0, n).mapToObj(i -> "value" + i).collect(Collectors.toList());
			expected.add(0, null);
			assertEquals(expected, buffer.getMapping());
		}


		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			int[] testData = random(n);
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + testData[i]);
			}
			buffer.freeze();
			buffer.set(5, "no");
		}

		@Test
		public void testCategories() {
			int n = Math.min(142, format.maxValue());
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.setSave(i, "value" + i);
			}
			int nullIndex = Math.min(42, format.maxValue() - 1);
			buffer.set(nullIndex, null);
			int[] expectedCategories = new int[n];
			Arrays.setAll(expectedCategories, i -> i + 1);
			expectedCategories[nullIndex] = 0;
			assertArrayEquals(expectedCategories, getData(buffer));
		}

		@Test
		public void testCompressionFormat() {
			NominalBuffer buffer = buffer(123);
			assertEquals(format, buffer.indexFormat());
		}

		@Test
		public void testDifferentValues() {
			int n = Math.min(14, format.maxValue());
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + i);
			}
			assertEquals(n, buffer.differentValues());
		}

		@Test
		public void testSameValueInParallel() throws InterruptedException {
			int n = 17;
			NominalBuffer buffer = buffer(n * 3);

			CyclicBarrier barrier = new CyclicBarrier(n);
			ExecutorService pool = Executors.newFixedThreadPool(n);
			for (int i = 0; i < n; i++) {
				int startIndex = i;
				pool.submit(() -> {
					for (int j = 0; j < 3; j++) {
						try {
							barrier.await();
						} catch (InterruptedException | BrokenBarrierException e) {
							assertFalse("Must not happen.", true);
						}
						buffer.set(startIndex * 3 + j, "value" + j);
						assertEquals(j + 1, buffer.differentValues());
					}
				});
			}
			pool.shutdown();
			pool.awaitTermination(4, TimeUnit.MINUTES);
			assertEquals(3, buffer.differentValues());
		}

		@Test
		public void testGetSetInParallel() throws InterruptedException {
			int n = 5;
			int size = 10_000;
			NominalBuffer buffer = buffer(size);

			CountDownLatch latch = new CountDownLatch(1);
			ExecutorService pool = Executors.newFixedThreadPool(n + 1);
			for (int i = 0; i < n; i++) {
				pool.submit(() -> {
					try {
						latch.await();
					} catch (InterruptedException e) {
						assertFalse("Must not happen.", true);
					}
					Random random = new Random();
					for (int j = 0; j < 1000; j++) {
						int index = random.nextInt(size);
						int value = random.nextInt(format.maxValue());
						buffer.set(index, "value" + value);
					}
				});
			}
			pool.submit(() -> {
				try {
					latch.await();
				} catch (InterruptedException e) {
					assertFalse("Must not happen.", true);
				}
				int sum = 0;
				Random random = new Random();
				for (int j = 0; j < 10000; j++) {
					sum += buffer.get(random.nextInt(size)).length();
				}
				return sum;
			});
			latch.countDown();
			pool.shutdown();
			pool.awaitTermination(4, TimeUnit.MINUTES);
		}

	}


	@RunWith(Parameterized.class)
	public static class MaxValues {

		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> tests() {
			return Arrays.asList(IntegerFormats.Format.UNSIGNED_INT2, IntegerFormats.Format.UNSIGNED_INT4,
					IntegerFormats.Format.UNSIGNED_INT8, IntegerFormats.Format.UNSIGNED_INT16);
		}

		private NominalBuffer buffer(int length) {
			return NominalBufferTests.buffer(length, format);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetMore() {
			int n = format.maxValue() + 1;
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + i);
			}
		}

		@Test
		public void testSetSaveMore() {
			int n = format.maxValue();
			NominalBuffer buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				assertTrue(buffer.setSave(i, "value" + i));
			}
			assertFalse(buffer.setSave(n, "value" + n));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetOverwriteMore() {
			int n = format.maxValue() + 1;
			NominalBuffer buffer = buffer(3);
			for (int i = 0; i < n; i++) {
				buffer.set(i % 3, "value" + i);
			}
		}

		@Test
		public void testSetSaveOverwriteMore() {
			int n = format.maxValue();
			NominalBuffer buffer = buffer(3);
			for (int i = 0; i < n; i++) {
				assertTrue(buffer.setSave(i % 3, "value" + i));
			}
			assertTrue(buffer.setSave(0, null));
			assertFalse(buffer.setSave(1, "value" + n));
		}

		@Test
		public void testSetOverwrite() {
			int n = format.maxValue();
			NominalBuffer buffer = buffer(3);
			for (int i = 0; i < n; i++) {
				buffer.set(i % 3, "value" + i);
			}
		}

	}

	@RunWith(Parameterized.class)
	public static class ToString {

		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> tests() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		private NominalBuffer buffer(int length) {
			return NominalBufferTests.buffer(length, format);
		}

		@Test
		public void testToStringSmall() {
			int[] data = {1, 2, 3, 1, 1, 3, 1};
			NominalBuffer buffer = buffer(8);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, "value" + data[i]);
			}
			String expected = "Categorical Buffer (" + (data.length + 1) + ")\n(" + "value1, value2, value3, value1, value1, value3, value1, ?)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringMaxFit() {
			int[] datablock = {1, 2, 3, 1, 3, 3, 2, 2};
			NominalBuffer buffer = buffer(32);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + datablock[i % datablock.length]);
			}

			String expected = "Categorical Buffer (" + buffer.size() + ")\n("
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringBigger() {
			int[] datablock = {1, 2, 3, 1, 3, 3, 2, 2};
			int length = 33;
			NominalBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, "value" + datablock[i % datablock.length]);
			}
			buffer.set(buffer.size() - 1, "value" + 1);

			String expected = "Categorical Buffer (" + length + ")\n("
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, ..., value1)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringBiggerLastMissing() {
			int[] datablock = {1, 2, 3, 1, 3, 3, 2, 2};
			int length = 33;
			NominalBuffer buffer = buffer(length);
			for (int i = 0; i < buffer.size() - 1; i++) {
				buffer.set(i, "value" + datablock[i % datablock.length]);
			}
			buffer.set(buffer.size() - 1, null);

			String expected = "Categorical Buffer (" + length + ")\n("
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, value2, value2, "
					+ "value1, value2, value3, value1, value3, value3, ..., ?)";

			assertEquals(expected, buffer.toString());
		}
	}

	@RunWith(Parameterized.class)
	public static class Bounds {

		ColumnType<String> TYPE = ColumnTestUtils.categoricalType(String.class, null);

		@Parameter
		public IntegerFormats.Format targetFormat;

		@Parameters(name = "{0}")
		public static Iterable<Object> tests() {
			return Arrays.asList(Format.UNSIGNED_INT2,
					Format.UNSIGNED_INT4,
					Format.UNSIGNED_INT8,
					Format.UNSIGNED_INT16);
		}

		private List<String> getMapping(int elements) {
			Double[] data = new Double[elements];
			Arrays.setAll(data, Double::valueOf);
			data[0] = Double.NaN;
			return Arrays.stream(data).map(Objects::toString).collect(Collectors.toList());
		}

		private Column getColumn(int length, int categories) {
			int[] data = new int[length];
			Arrays.setAll(data, i -> i % categories);
			List<String> mapping = getMapping(categories);
			return ColumnAccessor.get().newCategoricalColumn(TYPE, data, mapping);
		}

		@Test
		public void testBound() {
			int categories = targetFormat.maxValue();
			Column column = getColumn(categories, categories);
			switch (targetFormat) {
				case UNSIGNED_INT2:
				case UNSIGNED_INT4:
				case UNSIGNED_INT8:
					new UInt8NominalBuffer(column, ColumnTestUtils.categoricalType(String.class, null), targetFormat);
					break;
				case UNSIGNED_INT16:
					new UInt16NominalBuffer(column, ColumnTestUtils.categoricalType(String.class, null));
					break;
				default:
					throw new AssertionError();
			}
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testOutOfBounds() {
			int categories = targetFormat.maxValue() + 1;
			Column column = getColumn(categories, categories);
			switch (targetFormat) {
				case UNSIGNED_INT2:
				case UNSIGNED_INT4:
				case UNSIGNED_INT8:
					new UInt8NominalBuffer(column, ColumnTestUtils.categoricalType(String.class, null), targetFormat);
					break;
				case UNSIGNED_INT16:
					new UInt16NominalBuffer(column, ColumnTestUtils.categoricalType(String.class, null));
				default:
					throw new AssertionError();
			}
		}

	}

	@RunWith(Parameterized.class)
	public static class FromColumnAllowed {

		private static final int MAX_VALUES = 30;

		@Parameter
		public IntegerFormats.Format columnFormat;

		@Parameter(value = 1)
		public IntegerFormats.Format bufferFormat;

		@Parameter(value = 2)
		public IntegerFormats.Format targetFormat;

		@Parameters(name = "{0}_to_{2}_via_{1}")
		public static Iterable<Object> tests() {
			return Arrays.asList(
					new Object[]{Format.UNSIGNED_INT2, Format.UNSIGNED_INT8, Format.UNSIGNED_INT2},
					new Object[]{Format.UNSIGNED_INT2, Format.UNSIGNED_INT8, Format.UNSIGNED_INT4},
					new Object[]{Format.UNSIGNED_INT2, Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[]{Format.UNSIGNED_INT2, Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[]{Format.UNSIGNED_INT2, Format.SIGNED_INT32, Format.SIGNED_INT32},
					new Object[]{Format.UNSIGNED_INT4, Format.UNSIGNED_INT8, Format.UNSIGNED_INT4},
					new Object[]{Format.UNSIGNED_INT4, Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[]{Format.UNSIGNED_INT4, Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[]{Format.UNSIGNED_INT4, Format.SIGNED_INT32, Format.SIGNED_INT32},
					new Object[]{Format.UNSIGNED_INT8, Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[]{Format.UNSIGNED_INT8, Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[]{Format.UNSIGNED_INT8, Format.SIGNED_INT32, Format.SIGNED_INT32},
					new Object[]{Format.UNSIGNED_INT16, Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[]{Format.UNSIGNED_INT16, Format.SIGNED_INT32, Format.SIGNED_INT32},
					new Object[]{Format.SIGNED_INT32, Format.SIGNED_INT32, Format.SIGNED_INT32});
		}

		private static final ColumnType<String> TYPE = ColumnTestUtils.categoricalType(
				String.class, null);

		static Column column(int[] data, Format format) {
			switch (format) {
				case UNSIGNED_INT2:
				case UNSIGNED_INT4:
				case UNSIGNED_INT8:
					PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
					return ColumnAccessor.get().newCategoricalColumn(TYPE, packed, getMappingList(format));
				case UNSIGNED_INT16:
					return ColumnAccessor.get().newCategoricalColumn(TYPE, toShortArray(data), getMappingList(format));
				case SIGNED_INT32:
				default:
					return ColumnAccessor.get().newCategoricalColumn(TYPE, data, getMappingList(format));
			}
		}

		static NominalBuffer buffer(Column column, Format bufferFormat, Format targetFormat) {
			switch (bufferFormat) {
				case UNSIGNED_INT2:
				case UNSIGNED_INT4:
					throw new AssertionError();
				case UNSIGNED_INT8:
					return new UInt8NominalBuffer(column, ColumnType.NOMINAL, targetFormat);
				case UNSIGNED_INT16:
					return new UInt16NominalBuffer(column, ColumnType.NOMINAL);
				case SIGNED_INT32:
				default:
					return new Int32NominalBuffer(column, ColumnType.NOMINAL);
			}
		}

		private static List<String> getMappingList(Format format) {
			int nValues = Integer.min(format.maxValue(), MAX_VALUES);
			List<String> list = new ArrayList<>(nValues);
			list.add(null);
			for (int i = 1; i < nValues; i++) {
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


		private int[] random(int n) {
			int[] data = new int[n];
			Random random = new Random();
			Arrays.setAll(data, i -> random.nextInt(Math.min(columnFormat.maxValue(), MAX_VALUES)));
			return data;
		}

		private int[] getData(NominalBuffer buffer) {
			return NominalBufferTests.getData(buffer, bufferFormat);
		}

		@Test
		public void testConversionObjects() {
			int[] data = random(177);
			Column column = column(data, columnFormat);
			NominalBuffer buffer = buffer(column, bufferFormat, targetFormat);
			Object[] expected = new Object[column.size()];
			column.fill(expected, 0);
			Object[] actual = new Object[buffer.size()];
			Arrays.setAll(actual, buffer::get);
			assertArrayEquals(expected, actual);
		}

		@Test
		public void testConversionCategories() {
			int[] data = random(177);
			Column column = column(data, columnFormat);
			NominalBuffer buffer = buffer(column, bufferFormat, targetFormat);
			int[] expected = new int[column.size()];
			column.fill(expected, 0);
			int[] actual = getData(buffer);
			assertArrayEquals(expected, actual);
		}

	}

	@RunWith(Parameterized.class)
	public static class FromColumnType {

		private static final int MAX_VALUES = 30;

		@Parameter
		public IntegerFormats.Format columnFormat;


		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> tests() {
			return Arrays.asList(Format.UNSIGNED_INT8, Format.UNSIGNED_INT16, Format.SIGNED_INT32);
		}


		private int[] random(int n) {
			int[] data = new int[n];
			Random random = new Random();
			Arrays.setAll(data, i -> random.nextInt(Math.min(columnFormat.maxValue(), MAX_VALUES)));
			return data;
		}


		static NominalBuffer buffer(Column column, IntegerFormats.Format format, ColumnType<String> clazz) {
			switch (format) {
				case UNSIGNED_INT2:
				case UNSIGNED_INT4:
					throw new AssertionError();
				case UNSIGNED_INT8:
					return new UInt8NominalBuffer(column, clazz);
				case UNSIGNED_INT16:
					return new UInt16NominalBuffer(column, clazz);
				case SIGNED_INT32:
				default:
					return new Int32NominalBuffer(column, clazz);
			}
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testNumericColumn() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, new double[101]);
			buffer(column, columnFormat, ColumnTestUtils.categoricalType(String.class, null));
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testFreeColumn() {
			Column column = ColumnAccessor.get().newObjectColumn(ColumnTestUtils.categoricalType(
					Object.class, null), new Object[0]
			);
			buffer(column, columnFormat, ColumnType.NOMINAL);
		}

		@Test
		public void testConversionObjects() {
			int[] data = random(177);
			Column column = FromColumnAllowed.column(data, columnFormat);
			NominalBuffer buffer = buffer(column, columnFormat, ColumnTestUtils.categoricalType(String.class, null));
			Object[] expected = new Object[column.size()];
			column.fill(expected, 0);
			Object[] actual = new Object[buffer.size()];
			Arrays.setAll(actual, buffer::get);
			assertArrayEquals(expected, actual);
		}
	}

	public static class FactoryMethods {

		@Test(expected = IllegalArgumentException.class)
		public void testUnlimitedBufferOfInvalidSize() {
			Buffers.nominalBuffer(-1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testLimitedBufferOfInvalidSize() {
			Buffers.nominalBuffer(-1, 100);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidBound() {
			Buffers.nominalBuffer(100, -1);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			Buffers.nominalBuffer(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchingColumn() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER_53_BIT, new double[]{});
			Buffers.nominalBuffer(column);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[]{}, Collections.emptyList());
			new UInt16NominalBuffer(column, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnWithBound() {
			Buffers.nominalBuffer(null, 100);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMismatchingColumnWithBound() {
			Column column = ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER_53_BIT, new double[]{});
			Buffers.nominalBuffer(column, 100);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidBoundWithColumn() {
			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[]{}, Collections.emptyList());
			Buffers.nominalBuffer(column, -1);
		}

		@Test
		public void testUnboundedBuffer() {
			NominalBuffer buffer = Buffers.nominalBuffer(100);
			assertEquals(Format.SIGNED_INT32, buffer.indexFormat());
		}

		@Test
		public void testTinyBuffer() {
			NominalBuffer buffer = Buffers.nominalBuffer(100, 0);
			assertEquals(Format.UNSIGNED_INT8, buffer.indexFormat());
		}

		@Test
		public void testByteBuffer() {
			NominalBuffer buffer = Buffers.nominalBuffer(100, Format.UNSIGNED_INT8.maxValue());
			assertEquals(Format.UNSIGNED_INT8, buffer.indexFormat());
		}

		@Test
		public void testShortBuffer() {
			NominalBuffer buffer = Buffers.nominalBuffer(100, Format.UNSIGNED_INT16.maxValue());
			assertEquals(Format.UNSIGNED_INT16, buffer.indexFormat());
		}

		@Test
		public void testIntBuffer() {
			NominalBuffer buffer = Buffers.nominalBuffer(100, Format.SIGNED_INT32.maxValue());
			assertEquals(Format.SIGNED_INT32, buffer.indexFormat());
		}

		@Test
		public void testBufferFromColumn() {
			int n = 100;
			int nValues = 10;

			int[] indices = new int[n];
			String[] values = new String[nValues];
			Arrays.setAll(indices, i -> i % nValues);
			Arrays.setAll(values, String::valueOf);

			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices, Arrays.asList(values));
			NominalBuffer buffer = Buffers.nominalBuffer(column);

			assertEquals(Format.SIGNED_INT32, buffer.indexFormat());
		}

		@Test
		public void testColumnToTinyBuffer() {
			int n = 100;
			int nValues = 2;

			int[] indices = new int[n];
			String[] values = new String[nValues];
			Arrays.setAll(indices, i -> i % nValues);
			Arrays.setAll(values, String::valueOf);

			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices, Arrays.asList(values));
			NominalBuffer buffer = Buffers.nominalBuffer(column, nValues);

			assertEquals(Format.UNSIGNED_INT8, buffer.indexFormat());
		}

		@Test
		public void testColumnToByteBuffer() {
			int n = 100;
			int nValues = 10;

			int[] indices = new int[n];
			String[] values = new String[nValues];
			Arrays.setAll(indices, i -> i % nValues);
			Arrays.setAll(values, String::valueOf);

			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices, Arrays.asList(values));
			NominalBuffer buffer = Buffers.nominalBuffer(column,
					Format.UNSIGNED_INT8.maxValue());

			assertEquals(Format.UNSIGNED_INT8, buffer.indexFormat());
		}

		@Test
		public void testColumnToShortBuffer() {
			int n = 100;
			int nValues = 10;

			int[] indices = new int[n];
			String[] values = new String[nValues];
			Arrays.setAll(indices, i -> i % nValues);
			Arrays.setAll(values, String::valueOf);

			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices, Arrays.asList(values));
			NominalBuffer buffer = Buffers.nominalBuffer(column,
					Format.UNSIGNED_INT16.maxValue());

			assertEquals(Format.UNSIGNED_INT16, buffer.indexFormat());
		}

		@Test
		public void testColumnToIntBuffer() {
			int n = 100;
			int nValues = 10;

			int[] indices = new int[n];
			String[] values = new String[nValues];
			Arrays.setAll(indices, i -> i % nValues);
			Arrays.setAll(values, String::valueOf);

			Column column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices, Arrays.asList(values));
			NominalBuffer buffer = Buffers.nominalBuffer(column,
					Format.SIGNED_INT32.maxValue());

			assertEquals(Format.SIGNED_INT32, buffer.indexFormat());
		}

		@Test
		public void testTooSmallBound() {
			// Create a dictionary too large to fit into a 8Bit buffer.
			int n = Format.UNSIGNED_INT16.maxValue();
			int nValues = Format.UNSIGNED_INT8.maxValue() + 10;

			int[] indices = new int[n];
			String[] values = new String[nValues];
			Arrays.setAll(indices, i -> i % nValues);
			Arrays.setAll(values, String::valueOf);

			// Request a buffer with support for only a few categories.
			CategoricalColumn column = ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, indices,
					Arrays.asList(values));
			NominalBuffer buffer = Buffers.nominalBuffer(column, 10);

			// Factory method should ignore the specified number of categories and select the format based on the given
			// columns' dictionary size instead.
			assertEquals(Format.UNSIGNED_INT16, buffer.indexFormat());
		}

	}

	@RunWith(Parameterized.class)
	public static class ToColumn {

		private static int BUFFER_LENGTH = 100;

		@Parameter
		public Format bufferFormat;

		@Parameter(value = 1)
		public Format targetFormat;


		@Parameters(name = "{0}_{1}")
		public static Iterable<Object[]> tests() {
			return Arrays.asList(
					new Object[] {Format.UNSIGNED_INT2, Format.UNSIGNED_INT2},
					new Object[] {Format.UNSIGNED_INT4, Format.UNSIGNED_INT4},
					new Object[] {Format.UNSIGNED_INT8, Format.UNSIGNED_INT2},
					new Object[] {Format.UNSIGNED_INT8, Format.UNSIGNED_INT4},
					new Object[] {Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[] {Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[] {Format.SIGNED_INT32, Format.SIGNED_INT32}
			);
		}

		private NominalBuffer getBuffer(ColumnType<String> columnType) {
			switch (bufferFormat) {
				case UNSIGNED_INT2:
					return new UInt2NominalBuffer(columnType, BUFFER_LENGTH);
				case UNSIGNED_INT4:
					return new UInt4NominalBuffer(columnType, BUFFER_LENGTH);
				case UNSIGNED_INT8:
					return new UInt8NominalBuffer(columnType, BUFFER_LENGTH, targetFormat);
				case UNSIGNED_INT16:
					return new UInt16NominalBuffer(columnType, BUFFER_LENGTH);
				case SIGNED_INT32:
					return new Int32NominalBuffer(columnType, BUFFER_LENGTH);
				default:
					throw new AssertionError();
			}
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnType() {
			NominalBuffer buffer = getBuffer(null);
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toColumn();
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonCategoricalColumnType() {
			NominalBuffer buffer = getBuffer(ColumnType.TEXT);
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toColumn();
		}

		@Test
		public void testEmptyMapping() {
			NominalBuffer buffer = getBuffer(ColumnType.NOMINAL);
			assertEquals(buffer.indexFormat(), bufferFormat);

			CategoricalColumn column = buffer.toColumn();
			assertEquals(column.getFormat(), targetFormat);

			assertEquals(0, column.getDictionary().size());
		}

	}

	@RunWith(Parameterized.class)
	public static class ToBooleanColumn {

		private static int BUFFER_LENGTH = 100;

		@Parameter
		public Format bufferFormat;

		@Parameter(value = 1)
		public Format targetFormat;


		@Parameters(name = "{0}_{1}")
		public static Iterable<Object[]> tests() {
			return Arrays.asList(
					new Object[] {Format.UNSIGNED_INT2, Format.UNSIGNED_INT2},
					new Object[] {Format.UNSIGNED_INT4, Format.UNSIGNED_INT4},
					new Object[] {Format.UNSIGNED_INT8, Format.UNSIGNED_INT2},
					new Object[] {Format.UNSIGNED_INT8, Format.UNSIGNED_INT4},
					new Object[] {Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[] {Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[] {Format.SIGNED_INT32, Format.SIGNED_INT32}
			);
		}

		private NominalBuffer getBuffer() {
			switch (bufferFormat) {
				case UNSIGNED_INT2:
					return new UInt2NominalBuffer(ColumnType.NOMINAL, BUFFER_LENGTH);
				case UNSIGNED_INT4:
					return new UInt4NominalBuffer(ColumnType.NOMINAL, BUFFER_LENGTH);
				case UNSIGNED_INT8:
					return new UInt8NominalBuffer(ColumnType.NOMINAL, BUFFER_LENGTH, targetFormat);
				case UNSIGNED_INT16:
					return new UInt16NominalBuffer(ColumnType.NOMINAL, BUFFER_LENGTH);
				case SIGNED_INT32:
					return new Int32NominalBuffer(ColumnType.NOMINAL, BUFFER_LENGTH);
				default:
					throw new AssertionError();
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonCategoricalColumnType() {
			NominalBuffer buffer = getBuffer();
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toBooleanColumn("positive");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMissingPositiveValue() {
			NominalBuffer buffer = getBuffer();
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toBooleanColumn("positive");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTooManyValues() {
			NominalBuffer buffer = getBuffer();
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.set(0, "one");
			buffer.set(1, "two");
			buffer.set(2, "three");

			buffer.toBooleanColumn(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNullOfTwoPositiveValue() {
			NominalBuffer buffer = getBuffer();
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.set(0, "one");
			buffer.set(1, "two");

			buffer.toBooleanColumn(null);
		}

		@Test
		public void testFirstOfTwoPositiveValue() {
			NominalBuffer buffer = getBuffer();

			buffer.set(0, "one");
			buffer.set(1, "two");

			CategoricalColumn column = buffer.toBooleanColumn("one");
			assertEquals(column.getFormat(), targetFormat);

			Dictionary dictionary = column.getDictionary();
			assertTrue(dictionary.isBoolean());
			assertTrue(dictionary.hasNegative());
			assertTrue(dictionary.hasPositive());
			assertEquals(1, dictionary.getPositiveIndex());
			assertEquals(2, dictionary.getNegativeIndex());
		}

		@Test
		public void testSecondOfTwoPositiveValue() {
			NominalBuffer buffer = getBuffer();

			buffer.set(0, "one");
			buffer.set(1, "two");

			CategoricalColumn column = buffer.toBooleanColumn("two");
			assertEquals(column.getFormat(), targetFormat);

			Dictionary dictionary = column.getDictionary();
			assertTrue(dictionary.isBoolean());
			assertTrue(dictionary.hasNegative());
			assertTrue(dictionary.hasPositive());
			assertEquals(2, dictionary.getPositiveIndex());
			assertEquals(1, dictionary.getNegativeIndex());
		}

		@Test
		public void testNullOfOnePositiveValue() {
			NominalBuffer buffer = getBuffer();

			buffer.set(0, "one");

			CategoricalColumn column = buffer.toBooleanColumn(null);
			assertEquals(column.getFormat(), targetFormat);

			Dictionary dictionary = column.getDictionary();
			assertTrue(dictionary.isBoolean());
			assertTrue(dictionary.hasNegative());
			assertFalse(dictionary.hasPositive());
			TestCase.assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getPositiveIndex());
			assertEquals(1, dictionary.getNegativeIndex());
		}

		@Test
		public void testFirstOfOnePositiveValue() {
			NominalBuffer buffer = getBuffer();

			buffer.set(0, "one");

			CategoricalColumn column = buffer.toBooleanColumn("one");
			assertEquals(column.getFormat(), targetFormat);

			Dictionary dictionary = column.getDictionary();
			assertTrue(dictionary.isBoolean());
			assertFalse(dictionary.hasNegative());
			assertTrue(dictionary.hasPositive());
			assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getNegativeIndex());
			assertEquals(1, dictionary.getPositiveIndex());
		}

		@Test
		public void testNoValuePositiveValue() {
			NominalBuffer buffer = getBuffer();

			CategoricalColumn column = buffer.toBooleanColumn(null);
			assertEquals(column.getFormat(), targetFormat);

			Dictionary dictionary = column.getDictionary();
			assertTrue(dictionary.isBoolean());
			assertFalse(dictionary.hasNegative());
			assertFalse(dictionary.hasPositive());
			assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getNegativeIndex());
			assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getPositiveIndex());
		}

	}

}
