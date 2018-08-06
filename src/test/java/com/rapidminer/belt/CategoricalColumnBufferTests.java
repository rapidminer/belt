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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;

/**
 * @author Gisa Schaefer
 */
@RunWith(Enclosed.class)
public class CategoricalColumnBufferTests {

	private static AbstractCategoricalColumnBuffer<String> buffer(int length, IntegerFormats.Format format) {
		switch (format) {
			case UNSIGNED_INT2:
				return new UInt2CategoricalBuffer<>(length);
			case UNSIGNED_INT4:
				return new UInt4CategoricalBuffer<>(length);
			case UNSIGNED_INT8:
				return new UInt8CategoricalBuffer<>(length);
			case UNSIGNED_INT16:
				return new UInt16CategoricalBuffer<>(length);
			case SIGNED_INT32:
				return new Int32CategoricalBuffer<>(length);
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

	private static int[] getData(AbstractCategoricalColumnBuffer buffer, IntegerFormats.Format format) {
		switch (format) {
			case SIGNED_INT32:
				return ((Int32CategoricalBuffer) buffer).getData();
			case UNSIGNED_INT16:
				return toIntArray(((UInt16CategoricalBuffer) buffer).getData());
			case UNSIGNED_INT8:
				return toIntArray(((UInt8CategoricalBuffer) buffer).getData());
			case UNSIGNED_INT4:
				return toIntArray(((UInt4CategoricalBuffer) buffer).getData());
			case UNSIGNED_INT2:
				return toIntArray(((UInt2CategoricalBuffer) buffer).getData());
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

		private AbstractCategoricalColumnBuffer<String> buffer(int length) {
			return CategoricalColumnBufferTests.buffer(length, format);
		}

		private int[] random(int n) {
			return CategoricalColumnBufferTests.random(n, format);
		}

		private int[] getData(AbstractCategoricalColumnBuffer buffer) {
			return CategoricalColumnBufferTests.getData(buffer, format);
		}

		@Test
		public void testBufferLength() {
			AbstractCategoricalColumnBuffer buffer = buffer(197);
			assertEquals(197, buffer.size());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testBufferNegativeLength() {
			buffer(-5);
		}


		@Test
		public void testZeroBufferLength() {
			AbstractCategoricalColumnBuffer buffer = buffer(0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
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
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
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
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + testData[i]);
			}
			buffer.freeze();
			buffer.set(5, "no");
		}

		@Test
		public void testCategories() {
			int n = Math.min(142, format.maxValue());
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
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
			CategoricalColumnBuffer<String> buffer = buffer(123);
			assertEquals(format, buffer.indexFormat());
		}

		@Test
		public void testDifferentValues() {
			int n = Math.min(14, format.maxValue());
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + i);
			}
			assertEquals(n, buffer.differentValues());
		}

		@Test
		public void testSameValueInParallel() throws InterruptedException {
			int n = 17;
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n * 3);

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
			AbstractCategoricalColumnBuffer<String> buffer = buffer(size);

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

		private AbstractCategoricalColumnBuffer<String> buffer(int length) {
			return CategoricalColumnBufferTests.buffer(length, format);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetMore() {
			int n = format.maxValue() + 1;
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				buffer.set(i, "value" + i);
			}
		}

		@Test
		public void testSetSaveMore() {
			int n = format.maxValue();
			AbstractCategoricalColumnBuffer<String> buffer = buffer(n);
			for (int i = 0; i < n; i++) {
				assertTrue(buffer.setSave(i, "value" + i));
			}
			assertFalse(buffer.setSave(n, "value" + n));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetOverwriteMore() {
			int n = format.maxValue() + 1;
			AbstractCategoricalColumnBuffer<String> buffer = buffer(3);
			for (int i = 0; i < n; i++) {
				buffer.set(i % 3, "value" + i);
			}
		}

		@Test
		public void testSetSaveOverwriteMore() {
			int n = format.maxValue();
			AbstractCategoricalColumnBuffer<String> buffer = buffer(3);
			for (int i = 0; i < n; i++) {
				assertTrue(buffer.setSave(i % 3, "value" + i));
			}
			assertTrue(buffer.setSave(0, null));
			assertFalse(buffer.setSave(1, "value" + n));
		}

		@Test
		public void testSetOverwrite() {
			int n = format.maxValue();
			AbstractCategoricalColumnBuffer<String> buffer = buffer(3);
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

		private AbstractCategoricalColumnBuffer<String> buffer(int length) {
			return CategoricalColumnBufferTests.buffer(length, format);
		}

		@Test
		public void testToStringSmall() {
			int[] data = {1, 2, 3, 1, 1, 3, 1};
			AbstractCategoricalColumnBuffer<String> buffer = buffer(8);
			for (int i = 0; i < data.length; i++) {
				buffer.set(i, "value" + data[i]);
			}
			String expected = "Categorical Buffer (" + (data.length + 1) + ")\n(" + "value1, value2, value3, value1, value1, value3, value1, ?)";

			assertEquals(expected, buffer.toString());
		}

		@Test
		public void testToStringMaxFit() {
			int[] datablock = {1, 2, 3, 1, 3, 3, 2, 2};
			AbstractCategoricalColumnBuffer<String> buffer = buffer(32);
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
			AbstractCategoricalColumnBuffer<String> buffer = buffer(length);
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
			AbstractCategoricalColumnBuffer<String> buffer = buffer(length);
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
	public static class FromColumnAllowed {

		private static final int MAX_VALUES = 30;

		@Parameter
		public IntegerFormats.Format columnFormat;

		@Parameter(value = 1)
		public IntegerFormats.Format targetFormat;

		@Parameters(name = "{0}_to_{1}")
		public static Iterable<Object> tests() {
			return Arrays.asList(
					new Object[]{IntegerFormats.Format.UNSIGNED_INT2, IntegerFormats.Format.UNSIGNED_INT2},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT2, IntegerFormats.Format.UNSIGNED_INT4},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT2, IntegerFormats.Format.UNSIGNED_INT8},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT2, IntegerFormats.Format.UNSIGNED_INT16},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT2, IntegerFormats.Format.SIGNED_INT32},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT4, IntegerFormats.Format.UNSIGNED_INT4},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT4, IntegerFormats.Format.UNSIGNED_INT8},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT4, IntegerFormats.Format.UNSIGNED_INT16},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT4, IntegerFormats.Format.SIGNED_INT32},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT8, IntegerFormats.Format.UNSIGNED_INT8},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT8, IntegerFormats.Format.UNSIGNED_INT16},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT8, IntegerFormats.Format.SIGNED_INT32},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT16, IntegerFormats.Format.UNSIGNED_INT16},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT16, IntegerFormats.Format.SIGNED_INT32},
					new Object[]{IntegerFormats.Format.SIGNED_INT32, IntegerFormats.Format.SIGNED_INT32});
		}

		private static final ColumnType<String> TYPE = ColumnTypes.categoricalType(
				"com.rapidminer.belt.column.test.stringcolumn", String.class, null);

		static Column column(int[] data, IntegerFormats.Format format) {
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
		}

		static AbstractCategoricalColumnBuffer<String> buffer(Column column, IntegerFormats.Format format) {
			switch (format) {
				case UNSIGNED_INT2:
					return new UInt2CategoricalBuffer<>(column, String.class);
				case UNSIGNED_INT4:
					return new UInt4CategoricalBuffer<>(column, String.class);
				case UNSIGNED_INT8:
					return new UInt8CategoricalBuffer<>(column, String.class);
				case UNSIGNED_INT16:
					return new UInt16CategoricalBuffer<>(column, String.class);
				case SIGNED_INT32:
				default:
					return new Int32CategoricalBuffer<>(column, String.class);
			}
		}

		private static List<String> getMappingList() {
			List<String> list = new ArrayList<>(MAX_VALUES);
			list.add(null);
			for (int i = 1; i < MAX_VALUES; i++) {
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

		private int[] getData(AbstractCategoricalColumnBuffer buffer) {
			return CategoricalColumnBufferTests.getData(buffer, targetFormat);
		}

		@Test
		public void testConversionObjects() {
			int[] data = random(177);
			Column column = column(data, columnFormat);
			AbstractCategoricalColumnBuffer<String> buffer = buffer(column, targetFormat);
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
			AbstractCategoricalColumnBuffer<String> buffer = buffer(column, targetFormat);
			int[] expected = new int[column.size()];
			column.fill(expected, 0);
			int[] actual = getData(buffer);
			assertArrayEquals(expected, actual);
		}

	}


	@RunWith(Parameterized.class)
	public static class FromColumnNotAllowed {

		private static final int MAX_VALUES = 30;

		@Parameter
		public IntegerFormats.Format columnFormat;

		@Parameter(value = 1)
		public IntegerFormats.Format targetFormat;

		@Parameters(name = "{0}_to_{1}")
		public static Iterable<Object> tests() {
			return Arrays.asList(
					new Object[]{IntegerFormats.Format.SIGNED_INT32, IntegerFormats.Format.UNSIGNED_INT2},
					new Object[]{IntegerFormats.Format.SIGNED_INT32, IntegerFormats.Format.UNSIGNED_INT4},
					new Object[]{IntegerFormats.Format.SIGNED_INT32, IntegerFormats.Format.UNSIGNED_INT8},
					new Object[]{IntegerFormats.Format.SIGNED_INT32, IntegerFormats.Format.UNSIGNED_INT16},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT16, IntegerFormats.Format.UNSIGNED_INT2},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT16, IntegerFormats.Format.UNSIGNED_INT4},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT16, IntegerFormats.Format.UNSIGNED_INT8},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT8, IntegerFormats.Format.UNSIGNED_INT2},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT8, IntegerFormats.Format.UNSIGNED_INT4},
					new Object[]{IntegerFormats.Format.UNSIGNED_INT4, IntegerFormats.Format.UNSIGNED_INT2});
		}


		private int[] random(int n) {
			int[] data = new int[n];
			Random random = new Random();
			Arrays.setAll(data, i -> random.nextInt(Math.min(columnFormat.maxValue(), MAX_VALUES)));
			return data;
		}


		@Test(expected = UnsupportedOperationException.class)
		public void testConversion() {
			int[] data = random(177);
			Column column = FromColumnAllowed.column(data, columnFormat);
			FromColumnAllowed.buffer(column, targetFormat);
		}
	}


	@RunWith(Parameterized.class)
	public static class FromColumnType {

		private static final int MAX_VALUES = 30;

		@Parameter
		public IntegerFormats.Format columnFormat;


		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> tests() {
			return Arrays.asList(IntegerFormats.Format.values());
		}


		private int[] random(int n) {
			int[] data = new int[n];
			Random random = new Random();
			Arrays.setAll(data, i -> random.nextInt(Math.min(columnFormat.maxValue(), MAX_VALUES)));
			return data;
		}


		static AbstractCategoricalColumnBuffer<?> buffer(Column column, IntegerFormats.Format format, Class<?> clazz) {
			switch (format) {
				case UNSIGNED_INT2:
					return new UInt2CategoricalBuffer<>(column, clazz);
				case UNSIGNED_INT4:
					return new UInt4CategoricalBuffer<>(column, clazz);
				case UNSIGNED_INT8:
					return new UInt8CategoricalBuffer<>(column, clazz);
				case UNSIGNED_INT16:
					return new UInt16CategoricalBuffer<>(column, clazz);
				case SIGNED_INT32:
				default:
					return new Int32CategoricalBuffer<>(column, clazz);
			}
		}


		@Test(expected = IllegalArgumentException.class)
		public void testNotSuperclass() {
			int[] data = random(177);
			Column column = FromColumnAllowed.column(data, columnFormat);
			buffer(column, columnFormat, Integer.class);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testNumericColumn() {
			Column column = new DoubleArrayColumn(new double[101]);
			buffer(column, columnFormat, Integer.class);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testFreeColumn() {
			Column column = new SimpleFreeColumn<>(ColumnTypes.categoricalType("com.rapidminer.belt.column.test.objectcolumn",
					Object.class, null), new Object[0]
			);
			buffer(column, columnFormat, String.class);
		}

		@Test
		public void testConversionObjects() {
			int[] data = random(177);
			Column column = FromColumnAllowed.column(data, columnFormat);
			AbstractCategoricalColumnBuffer<?> buffer = buffer(column, columnFormat, Object.class);
			Object[] expected = new Object[column.size()];
			column.fill(expected, 0);
			Object[] actual = new Object[buffer.size()];
			Arrays.setAll(actual, buffer::get);
			assertArrayEquals(expected, actual);
		}
	}

	public static class ToBoolean {

		@Test
		public void testPositiveIndex1() {
			UInt2CategoricalBuffer<String> buffer = new UInt2CategoricalBuffer<>(50);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, i % 2 == 0 ? "Yes" : "No");
			}
			buffer.set(42, null);

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "Yes");
			assertTrue(column.hasCapability(Column.Capability.BOOLEAN));
			assertTrue(column.toBoolean(1));
			assertFalse(column.toBoolean(2));
			assertFalse(column.toBoolean(0));
			assertTrue(column.toBoolean("Yes"));
			assertFalse(column.toBoolean("No"));
			assertFalse(column.toBoolean(null));
			assertTrue(column.toBoolean(1.0));
			assertFalse(column.toBoolean(2.0));
			assertFalse(column.toBoolean(0.0));
		}

		@Test
		public void testPositiveIndex2() {
			UInt2CategoricalBuffer<String> buffer = new UInt2CategoricalBuffer<>(50);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, i % 2 == 0 ? "No" : "Yes");
			}
			buffer.set(42, null);

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "Yes");
			assertTrue(column.hasCapability(Column.Capability.BOOLEAN));
			assertTrue(column.toBoolean(2));
			assertFalse(column.toBoolean(1));
			assertTrue(column.toBoolean("Yes"));
			assertFalse(column.toBoolean("No"));
			assertTrue(column.toBoolean(2.0));
			assertFalse(column.toBoolean(1.0));
		}

		@Test(expected = IllegalArgumentException.class)
		public void test3Values() {
			UInt2CategoricalBuffer<String> buffer = new UInt2CategoricalBuffer<>(50);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, i % 3 == 0 ? "No" : i % 3 == 1 ? "Yes" : "Maybe");
			}

			buffer.toBooleanColumn(ColumnTypes.NOMINAL, "Yes");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongPositiveValue() {
			UInt2CategoricalBuffer<String> buffer = new UInt2CategoricalBuffer<>(50);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, i % 2 == 0 ? "No" : "Yes");
			}
			buffer.toBooleanColumn(ColumnTypes.NOMINAL, "Maybe");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNullPositiveValue() {
			UInt2CategoricalBuffer<String> buffer = new UInt2CategoricalBuffer<>(50);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, i % 2 == 0 ? "No" : "Yes");
			}
			buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
		}

	}

}
