/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
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
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.column.Dictionary;
import com.rapidminer.belt.util.IntegerFormats.Format;

import junit.framework.TestCase;


/**
 * Tests {@link UInt8CategoricalBuffer}, {@link UInt16CategoricalBufferSparse} and {@link
 * Int32CategoricalBufferSparse}.
 *
 * @author Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class CategoricalBufferSparseTests {

	private static final double SPARSITY = 0.1;

	private static CategoricalBufferSparse<String> buffer(String defaultValue, int length, Format format) {
		switch (format) {
			case UNSIGNED_INT8:
				return new UInt8CategoricalBufferSparse<>(defaultValue, length);
			case UNSIGNED_INT16:
				return new UInt16CategoricalBufferSparse<>(defaultValue, length);
			case SIGNED_INT32:
				return new Int32CategoricalBufferSparse<>(defaultValue, length);
			default:
				throw new IllegalArgumentException("Unsupported format");
		}
	}

	private static int[] random(int n, Format format) {
		int[] data = new int[n];
		SplittableRandom random = new SplittableRandom();
		int defaultValue = random.nextInt(format.maxValue());
		Arrays.setAll(data, i -> Math.random() < SPARSITY ? defaultValue : random.nextInt(format.maxValue()));
		return data;
	}

	private static int[] getData(CategoricalBufferSparse buffer) {
		CategoricalColumn column = buffer.toColumn(ColumnTypes.NOMINAL);
		int[] data = new int[column.size()];
		column.fill(data, 0);
		return data;
	}

	@RunWith(Parameterized.class)
	public static class ValueStoring {

		@Parameter
		public Format format;

		@Parameters(name = "{0}")
		public static Iterable<Format> tests() {
			return Arrays.asList(Format.UNSIGNED_INT8, Format.UNSIGNED_INT16, Format.SIGNED_INT32);
		}

		private CategoricalBufferSparse<String> buffer(String defaultValue, int length) {
			return CategoricalBufferSparseTests.buffer(defaultValue, length, format);
		}

		private int[] random(int n) {
			return CategoricalBufferSparseTests.random(n, format);
		}

		private int[] getData(CategoricalBufferSparse buffer) {
			return CategoricalBufferSparseTests.getData(buffer);
		}

		@Test
		public void testBufferLength() {
			CategoricalBufferSparse buffer = buffer("defaultValue", 197);
			assertEquals(197, buffer.size());
		}

		@Test
		public void testZeroBufferLength() {
			CategoricalBufferSparse buffer = buffer("defaultValue", 0);
			assertEquals(0, buffer.size());
		}

		@Test
		public void testSet() {
			int n = 1234;
			int[] testData = random(n);
			CategoricalBufferSparse<String> buffer = buffer("value" +
					ColumnTestUtils.getMostFrequentValue(testData, 0), n);
			int nullIndex = 42;
			for (int i = 0; i < n; i++) {
				buffer.setNext(i == nullIndex ? null : "value" + testData[i]);
			}

			String[] expected = Arrays.stream(testData).mapToObj(i -> "value" + i).toArray(String[]::new);
			expected[nullIndex] = null;

			String[] result = new String[n];
			buffer.toColumn(ColumnTypes.NOMINAL).fill(result, 0);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testSetInterleaved() {
			int n = 100;
			String defaultValue = "defaultValue";
			CategoricalBufferSparse<String> buffer = buffer(defaultValue, n);
			buffer.setNext(17, defaultValue);
			buffer.setNext(20, "someNonDefault");
			buffer.setNext(21, "whatever");
			buffer.setNext(87, "yep");
			buffer.setNext(99, "");
			Object[] testData = new Object[n];
			Arrays.fill(testData, defaultValue);
			testData[20] = "someNonDefault";
			testData[21] = "whatever";
			testData[87] = "yep";
			testData[99] = "";
			String[] result = new String[n];
			buffer.toColumn(ColumnTypes.NOMINAL).fill(result, 0);
			assertArrayEquals(testData, result);
		}

		@Test
		public void testNumberOfNonDefaults() {
			int n = 100;
			int[] testData = random(n);
			// default value will not be part of test data
			String defaultValue = "defaultValue";
			CategoricalBufferSparse<String> buffer = buffer(defaultValue, n);
			// add default value one..
			buffer.setNext(defaultValue);
			for (int i = 1; i < n / 2; i++) {
				buffer.setNext("value" + testData[i]);
			}
			// two..
			buffer.setNext(defaultValue);
			for (int i = n / 2 + 1; i < n - 1; i++) {
				buffer.setNext("value" + testData[i]);
			}
			// three times
			buffer.setNext(defaultValue);
			// check if default value has been detected three times
			assertEquals(n - 3, buffer.getNumberOfNonDefaults());
		}

		@Test
		public void testGrow() {
			int n = 1_000_000;
			int[] testData = random(n);
			CategoricalBufferSparse<String> buffer = buffer("value" +
					ColumnTestUtils.getMostFrequentValue(testData, 0), n);
			int nullIndex = 42;
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, i == nullIndex ? null : "value" + testData[i]);
			}

			String[] expected = Arrays.stream(testData).mapToObj(i -> "value" + i).toArray(String[]::new);
			expected[nullIndex] = null;

			String[] result = new String[n];
			buffer.toColumn(ColumnTypes.NOMINAL).fill(result, 0);

			assertArrayEquals(expected, result);
		}

		@Test
		public void testMappingOrder() {
			int n = 14;
			CategoricalBufferSparse<String> buffer = buffer("value" + 0, n);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, "value" + i);
			}
			List<String> expected = IntStream.range(0, n).mapToObj(i -> "value" + i).collect(Collectors.toList());
			expected.add(0, null);
			assertEquals(expected, buffer.getMapping());
		}


		@Test(expected = IllegalStateException.class)
		public void testSetAfterFreeze() {
			int n = 12;
			int[] testData = random(n);
			CategoricalBufferSparse<String> buffer = buffer("value" +
					ColumnTestUtils.getMostFrequentValue(testData, 0), n);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, "value" + testData[i]);
			}
			buffer.toColumn(ColumnTypes.NOMINAL);
			buffer.setNext(5, "no");
		}

		@Test
		public void testCategories() {
			int n = 142;
			CategoricalBufferSparse<String> buffer = buffer("value" + 0, n);
			int nullIndex = 42;
			for (int i = 0; i < n; i++) {
				buffer.setNextSave(i, i == nullIndex ? null : "value" + i);
			}
			int[] expectedCategories = new int[n];
			Arrays.setAll(expectedCategories, i -> i > nullIndex ? i : i + 1);
			expectedCategories[nullIndex] = 0;
			assertArrayEquals(expectedCategories, getData(buffer));
		}

		@Test
		public void testCompressionFormat() {
			CategoricalBufferSparse<String> buffer = buffer("defaultValue", 123);
			assertEquals(format, buffer.indexFormat());
		}

		@Test
		public void testDifferentValues() {
			int n = 14;
			CategoricalBufferSparse<String> buffer = buffer("value" + 0, n);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, "value" + i);
			}
			assertEquals(n, buffer.differentValues());
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testIndexOutOfUpperBounds() {
			int length = 100;
			CategoricalBufferSparse<String> buffer = buffer("defaultValue", length);
			buffer.setNext(length, "someValue");
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testIndexOutOfUpperBoundsDefaultValue() {
			int length = 100;
			CategoricalBufferSparse<String> buffer = buffer("defaultValue", length);
			buffer.setNext(length, "defaultValue");
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testIndexOutOfLowerBounds() {
			int length = 100;
			CategoricalBufferSparse<String> buffer = buffer("defaultValue", length);
			buffer.setNext(-1, "someValue");
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testIndexOutOfLowerBoundsDefaultValue() {
			int length = 100;
			CategoricalBufferSparse<String> buffer = buffer("defaultValue", length);
			buffer.setNext(-1, "defaultValue");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetOverwrite() {
			int n = 10;
			CategoricalBufferSparse<String> buffer = buffer("value" + format.maxValue(), 3);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i % 3, "value" + i);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeLength() {
			buffer("defaultValue", -1);
		}
	}

	@RunWith(Parameterized.class)
	public static class MaxValues {

		@Parameter
		public Format format;

		@Parameters(name = "{0}")
		public static Iterable<Format> tests() {
			return Arrays.asList(Format.UNSIGNED_INT8, Format.UNSIGNED_INT16);
		}

		private CategoricalBufferSparse<String> buffer(String defaultValue, int length) {
			return CategoricalBufferSparseTests.buffer(defaultValue, length, format);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetMore() {
			int n = format.maxValue() + 2;
			CategoricalBufferSparse<String> buffer = buffer("value" + 0, n);
			for (int i = 0; i < n; i++) {
				buffer.setNext(i, "value" + i);
			}
		}

		@Test
		public void testSetSaveMore() {
			int n = format.maxValue();
			CategoricalBufferSparse<String> buffer = buffer("value" + 1, n + 1);
			for (int i = 0; i < n; i++) {
				assertTrue(buffer.setNextSave(i, "value" + i));
			}
			assertFalse(buffer.setNextSave(n, "value" + n));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testSetSaveOverwrite() {
			int n = 10;
			CategoricalBufferSparse<String> buffer = buffer("value" + 1, 3);
			for (int i = 0; i < n; i++) {
				assertTrue(buffer.setNextSave(i % 3, "value" + i));
			}
		}

	}

	public static class FactoryMethods {

		@Test(expected = IllegalArgumentException.class)
		public void testUnlimitedBufferOfInvalidSize() {
			Buffers.sparseCategoricalBuffer("defaultValue", -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testLimitedBufferOfInvalidSize() {
			Buffers.sparseCategoricalBuffer("defaultValue", -1, 100);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testInvalidBound() {
			Buffers.sparseCategoricalBuffer("defaultValue", 100, -1);
		}

		@Test
		public void testUnboundedBuffer() {
			CategoricalBufferSparse<String> buffer = Buffers.sparseCategoricalBuffer("defaultValue", 100);
			assertEquals(Format.SIGNED_INT32, buffer.indexFormat());
		}

		@Test
		public void testTinyBuffer() {
			CategoricalBufferSparse<String> buffer = Buffers.sparseCategoricalBuffer("defaultValue",
					100, 0);
			assertEquals(Format.UNSIGNED_INT8, buffer.indexFormat());
		}

		@Test
		public void testByteBuffer() {
			CategoricalBufferSparse<String> buffer = Buffers.sparseCategoricalBuffer("defaultValue", 100,
					Format.UNSIGNED_INT8.maxValue());
			assertEquals(Format.UNSIGNED_INT8, buffer.indexFormat());
		}

		@Test
		public void testShortBuffer() {
			CategoricalBufferSparse<String> buffer = Buffers.sparseCategoricalBuffer("defaultValue", 100,
					Format.UNSIGNED_INT16.maxValue());
			assertEquals(Format.UNSIGNED_INT16, buffer.indexFormat());
		}

		@Test
		public void testIntBuffer() {
			CategoricalBufferSparse<String> buffer = Buffers.sparseCategoricalBuffer("defaultValue", 100,
					Format.SIGNED_INT32.maxValue());
			assertEquals(Format.SIGNED_INT32, buffer.indexFormat());
		}
	}

	@RunWith(Parameterized.class)
	public static class ToColumn {

		private static int BUFFER_LENGTH = 100;

		private static final ColumnType<String> TYPE_FREE_TEXT =
				ColumnTypes.objectType("custom", String.class, null);

		@Parameter
		public Format bufferFormat;

		@Parameter(value = 1)
		public Format targetFormat;


		@Parameters(name = "{0}_{1}")
		public static Iterable<Object[]> tests() {
			return Arrays.asList(
					new Object[]{Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[]{Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[]{Format.SIGNED_INT32, Format.SIGNED_INT32}
			);
		}

		private CategoricalBufferSparse<String> getBuffer(String defaultValue) {
			switch (bufferFormat) {
				case UNSIGNED_INT8:
					return new UInt8CategoricalBufferSparse<>(defaultValue, BUFFER_LENGTH);
				case UNSIGNED_INT16:
					return new UInt16CategoricalBufferSparse<>(defaultValue, BUFFER_LENGTH);
				case SIGNED_INT32:
					return new Int32CategoricalBufferSparse<>(defaultValue, BUFFER_LENGTH);
				default:
					throw new AssertionError();
			}
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnType() {
			CategoricalBufferSparse<String> buffer = getBuffer("defaultValue");
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toColumn(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonCategoricalColumnType() {
			CategoricalBufferSparse<String> buffer = getBuffer("defaultValue");
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toColumn(TYPE_FREE_TEXT);
		}

		@Test
		public void testEmptyMapping() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);
			assertEquals(buffer.indexFormat(), bufferFormat);

			CategoricalColumn<String> column = buffer.toColumn(ColumnTypes.NOMINAL);
			assertEquals(column.getFormat(), targetFormat);

			assertEquals(0, column.getDictionary(String.class).size());
		}

	}

	@RunWith(Parameterized.class)
	public static class ToBooleanColumn {

		private static int BUFFER_LENGTH = 100;

		private static final ColumnType<String> TYPE_FREE_TEXT = ColumnTypes.objectType("custom",
				String.class, null);

		@Parameter
		public Format bufferFormat;

		@Parameter(value = 1)
		public Format targetFormat;


		@Parameters(name = "{0}_{1}")
		public static Iterable<Object[]> tests() {
			return Arrays.asList(
					new Object[]{Format.UNSIGNED_INT8, Format.UNSIGNED_INT8},
					new Object[]{Format.UNSIGNED_INT16, Format.UNSIGNED_INT16},
					new Object[]{Format.SIGNED_INT32, Format.SIGNED_INT32}
			);
		}

		private CategoricalBufferSparse<String> getBuffer(String defaultValue) {
			switch (bufferFormat) {
				case UNSIGNED_INT8:
					return new UInt8CategoricalBufferSparse<>(defaultValue, BUFFER_LENGTH);
				case UNSIGNED_INT16:
					return new UInt16CategoricalBufferSparse<>(defaultValue, BUFFER_LENGTH);
				case SIGNED_INT32:
					return new Int32CategoricalBufferSparse<>(defaultValue, BUFFER_LENGTH);
				default:
					throw new AssertionError();
			}
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnType() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toBooleanColumn(null, "positive");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNonCategoricalColumnType() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toBooleanColumn(TYPE_FREE_TEXT, "positive");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMissingPositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.toBooleanColumn(ColumnTypes.NOMINAL, "positive");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTooManyValues() {
			CategoricalBufferSparse<String> buffer = getBuffer("three");
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.setNext(0, "one");
			buffer.setNext(1, "two");
			buffer.setNext(2, "three");

			buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNullOfTwoPositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);
			assertEquals(buffer.indexFormat(), bufferFormat);

			buffer.setNext(0, "one");
			buffer.setNext(1, "two");

			buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
		}

		@Test
		public void testFirstOfTwoPositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer("one");

			buffer.setNext(0, "one");
			buffer.setNext(1, "two");

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "one");
			assertEquals(column.getFormat(), targetFormat);

			Dictionary<String> dictionary = column.getDictionary(String.class);
			assertTrue(dictionary.isBoolean());
			assertTrue(dictionary.hasNegative());
			assertTrue(dictionary.hasPositive());
			assertEquals(1, dictionary.getPositiveIndex());
			assertEquals(2, dictionary.getNegativeIndex());
		}

		@Test
		public void testSecondOfTwoPositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);

			buffer.setNext(0, "one");
			buffer.setNext(1, "two");

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "two");
			assertEquals(column.getFormat(), targetFormat);

			Dictionary<String> dictionary = column.getDictionary(String.class);
			assertTrue(dictionary.isBoolean());
			assertTrue(dictionary.hasNegative());
			assertTrue(dictionary.hasPositive());
			assertEquals(2, dictionary.getPositiveIndex());
			assertEquals(1, dictionary.getNegativeIndex());
		}

		@Test
		public void testNullOfOnePositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer("one");

			buffer.setNext(0, "one");

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
			assertEquals(column.getFormat(), targetFormat);

			Dictionary<String> dictionary = column.getDictionary(String.class);
			assertTrue(dictionary.isBoolean());
			assertTrue(dictionary.hasNegative());
			assertFalse(dictionary.hasPositive());
			TestCase.assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getPositiveIndex());
			assertEquals(1, dictionary.getNegativeIndex());
		}

		@Test
		public void testFirstOfOnePositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer("one");

			buffer.setNext(0, "one");

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "one");
			assertEquals(column.getFormat(), targetFormat);

			Dictionary<String> dictionary = column.getDictionary(String.class);
			assertTrue(dictionary.isBoolean());
			assertFalse(dictionary.hasNegative());
			assertTrue(dictionary.hasPositive());
			assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getNegativeIndex());
			assertEquals(1, dictionary.getPositiveIndex());
		}

		@Test
		public void testNoValuePositiveValue() {
			CategoricalBufferSparse<String> buffer = getBuffer(null);

			CategoricalColumn<String> column = buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
			assertEquals(column.getFormat(), targetFormat);

			Dictionary<String> dictionary = column.getDictionary(String.class);
			assertTrue(dictionary.isBoolean());
			assertFalse(dictionary.hasNegative());
			assertFalse(dictionary.hasPositive());
			assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getNegativeIndex());
			assertEquals(BooleanDictionary.NO_ENTRY, dictionary.getPositiveIndex());
		}

	}

}
