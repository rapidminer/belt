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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.buffer.BufferTestUtils;
import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.Int32NominalBuffer;
import com.rapidminer.belt.buffer.NumericBuffer;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * @author Michael Knopf, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class CategoricalColumnTests {

	private static final String IMPL_SIMPLE_CATEGORICAL_STRING = "SimpleCategoricalColumn_String";

	private static final String IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING = "SimpleCategoricalSparseColumn_String";

	private static final String IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING = "SimpleCategoricalSparseColumn_DefaultIsMissing_String";

	private static final String IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING = "RemappedCategoricalSparseColumn_String";

	private static final String IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING = "RemappedCategoricalSparseColumn_DefaultIsMissing_String";

	private static final String IMPL_MAPPED_CATEGORICAL_STRING = "MappedCategoricalColumn_String";

	private static final String IMPL_REMAPPED_CATEGORICAL_STRING = "RemappedCategoricalColumn_String";

	private static final String IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING = "RemappedMappedCategoricalColumn_String";

	private static final List<Object[]> MINIMAL_CATEGORICAL_COLUMNS = Arrays.asList(
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, Format.UNSIGNED_INT2},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, Format.UNSIGNED_INT4},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_STRING, Format.SIGNED_INT32},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING, Format.SIGNED_INT32},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, Format.SIGNED_INT32},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING, Format.SIGNED_INT32},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, Format.SIGNED_INT32},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT2},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT4},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_MAPPED_CATEGORICAL_STRING, Format.SIGNED_INT32},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT2},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT4},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_REMAPPED_CATEGORICAL_STRING, Format.SIGNED_INT32},
			new Object[]{IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT2},
			new Object[]{IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT4},
			new Object[]{IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT8},
			new Object[]{IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING, Format.UNSIGNED_INT16},
			new Object[]{IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING, Format.SIGNED_INT32});

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

	private static int[] random(int n, Format format) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(format.maxValue()));
		return data;
	}

	private static int[] randomMax(int n, Format format) {
		int[] data = new int[n];
		Random random = new Random();
		int max = Math.min(format.maxValue(), MAX_VALUES);
		Arrays.setAll(data, i -> random.nextInt(max));
		return data;
	}

	private static int[] zeroFixPermutation(int n) {
		int[] indices = new int[n];
		Arrays.setAll(indices, i -> i);
		for (int i = 0; i < n; i++) {
			int a = (int) (Math.random() * n);
			int b = (int) (Math.random() * n);
			if (a != 0 && b != 0) {
				int tmp = indices[a];
				indices[a] = indices[b];
				indices[b] = tmp;
			}
		}
		return indices;
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

	private static byte[] toByteArray(int[] data, Format format) {
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

	private static final ColumnType<String> TYPE = ColumnTestUtils.categoricalType(
			String.class, null);

	private static Column column(String columnImplementation, int[] data, Format format) {
		return getColumn(columnImplementation, data, format, -1, false);
	}

	private static CategoricalColumn booleanColumn(String columnImplementation, int[] data, Format format, int positiveIndex) {
		return getColumn(columnImplementation, data, format, positiveIndex, true);
	}

	private static CategoricalColumn getColumn(String columnImplementation, int[] data, Format format,
												  int positiveIndex, boolean booleanMapping) {
		Dictionary mappingList = booleanMapping ? new BooleanDictionary(getBooleanMappingList(),
				positiveIndex) : new Dictionary(getMappingList());
		switch (columnImplementation) {
			case IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING:
				switch (format) {
					case UNSIGNED_INT2:
						Assert.fail("Test case INT2 does not make sense for sparse categorical column.");
					case UNSIGNED_INT4:
						Assert.fail("Test case INT4 does not make sense for sparse categorical column.");
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new CategoricalSparseColumn(TYPE, packed, mappingList, packed.data().length > 0 ? packed.data()[0] : 0);
					case UNSIGNED_INT16:
						return new CategoricalSparseColumn(TYPE, toShortArray(data), mappingList, data.length > 0 ? (short) data[0] : 0);
					case SIGNED_INT32:
					default:
						return new CategoricalSparseColumn(TYPE, data, mappingList, data.length > 0 ? data[0] : 0);
				}
			case IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING:
				switch (format) {
					case UNSIGNED_INT2:
						Assert.fail("Test case INT2 does not make sense for sparse categorical column.");
					case UNSIGNED_INT4:
						Assert.fail("Test case INT4 does not make sense for sparse categorical column.");
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new CategoricalSparseColumn(TYPE, packed, mappingList, (byte) 0);
					case UNSIGNED_INT16:
						return new CategoricalSparseColumn(TYPE, toShortArray(data), mappingList, (short) 0);
					case SIGNED_INT32:
					default:
						return new CategoricalSparseColumn(TYPE, data, mappingList, 0);
				}
			case IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING: {
				// Create random dictionary mapping and apply the inverse such that the values returned by the
				// remapped column match the input data.
				List<String> dictList = mappingList.getValueList();
				int[] remapping = zeroFixPermutation(dictList.size());
				List<String> remappedDictionary = new ArrayList<>(dictList);
				for (int i = 1; i < remapping.length; i++) {
					remappedDictionary.set(remapping[i], dictList.get(i));
				}
				Dictionary remappedDic = booleanMapping ? new BooleanDictionary(remappedDictionary,
						positiveIndex) : new Dictionary(remappedDictionary);
				switch (format) {
					case UNSIGNED_INT2:
						Assert.fail("Test case INT2 does not make sense for sparse categorical column.");
					case UNSIGNED_INT4:
						Assert.fail("Test case INT4 does not make sense for sparse categorical column.");
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new RemappedCategoricalSparseColumn(TYPE, packed, remappedDic, remapping, packed.data().length > 0 ? packed.data()[0] : 0);
					case UNSIGNED_INT16:
						return new RemappedCategoricalSparseColumn(TYPE, toShortArray(data), remappedDic, remapping, data.length > 0 ? (short) data[0] : 0);
					case SIGNED_INT32:
					default:
						return new RemappedCategoricalSparseColumn(TYPE, data, remappedDic, remapping, data.length > 0 ? data[0] : 0);
				}
			}
			case IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING: {
				// Create random dictionary mapping and apply the inverse such that the values returned by the
				// remapped column match the input data.
				List<String> dictList = mappingList.getValueList();
				int[] remapping = zeroFixPermutation(dictList.size());
				List<String> remappedDictionary = new ArrayList<>(dictList);
				for (int i = 1; i < remapping.length; i++) {
					remappedDictionary.set(remapping[i], dictList.get(i));
				}
				Dictionary remappedDic = booleanMapping ? new BooleanDictionary(remappedDictionary,
						positiveIndex) : new Dictionary(remappedDictionary);
				switch (format) {
					case UNSIGNED_INT2:
						Assert.fail("Test case INT2 does not make sense for sparse categorical column.");
					case UNSIGNED_INT4:
						Assert.fail("Test case INT4 does not make sense for sparse categorical column.");
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new RemappedCategoricalSparseColumn(TYPE, packed, remappedDic, remapping, (byte) 0);
					case UNSIGNED_INT16:
						return new RemappedCategoricalSparseColumn(TYPE, toShortArray(data), remappedDic, remapping, (short) 0);
					case SIGNED_INT32:
					default:
						return new RemappedCategoricalSparseColumn(TYPE, data, remappedDic, remapping, 0);
				}
			}
			case IMPL_SIMPLE_CATEGORICAL_STRING:
				switch (format) {
					case UNSIGNED_INT2:
					case UNSIGNED_INT4:
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new SimpleCategoricalColumn(TYPE, packed, mappingList);
					case UNSIGNED_INT16:
						return new SimpleCategoricalColumn(TYPE, toShortArray(data), mappingList);
					case SIGNED_INT32:
					default:
						return new SimpleCategoricalColumn(TYPE, data, mappingList);
				}
			case IMPL_REMAPPED_CATEGORICAL_STRING: {
				// Create random dictionary mapping and apply the inverse such that the values returned by the
				// remapped column match the input data.
				List<String> dictList = mappingList.getValueList();
				int[] remapping = zeroFixPermutation(dictList.size());
				List<String> remappedDictionary = new ArrayList<>(dictList);
				for (int i = 1; i < remapping.length; i++) {
					remappedDictionary.set(remapping[i], dictList.get(i));
				}
				Dictionary remappedDic = booleanMapping ? new BooleanDictionary(remappedDictionary,
						positiveIndex) : new Dictionary(remappedDictionary);

				switch (format) {
					case UNSIGNED_INT2:
					case UNSIGNED_INT4:
					case UNSIGNED_INT8:
						PackedIntegers packed = new PackedIntegers(toByteArray(data, format), format, data.length);
						return new RemappedCategoricalColumn(TYPE, packed, remappedDic, remapping);
					case UNSIGNED_INT16:
						return new RemappedCategoricalColumn(TYPE, toShortArray(data), remappedDic, remapping);
					case SIGNED_INT32:
					default:
						return new RemappedCategoricalColumn(TYPE, data, remappedDic, remapping);
				}
			}
			case IMPL_MAPPED_CATEGORICAL_STRING: {
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
						return new MappedCategoricalColumn(TYPE, packed, mappingList, mapping);
					case UNSIGNED_INT16:
						return new MappedCategoricalColumn(TYPE, toShortArray(mappedData), mappingList, mapping);
					case SIGNED_INT32:
					default:
						return new MappedCategoricalColumn(TYPE, mappedData, mappingList, mapping);
				}
			}
			case IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING: {
				// Create random dictionary mapping and apply the inverse such that the values returned by the
				// remapped column match the input data.
				List<String> dictList = mappingList.getValueList();
				int[] remapping = zeroFixPermutation(dictList.size());
				List<String> remappedDictionary = new ArrayList<>(dictList);
				for (int i = 1; i < remapping.length; i++) {
					remappedDictionary.set(remapping[i], dictList.get(i));
				}
				Dictionary remappedDic = booleanMapping ? new BooleanDictionary(remappedDictionary,
						positiveIndex) : new Dictionary(remappedDictionary);

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
						PackedIntegers packed = new PackedIntegers(toByteArray(mappedData, format), format,
								mappedData.length);
						return new RemappedMappedCategoricalColumn(TYPE, packed, remappedDic, remapping,
								mapping);
					case UNSIGNED_INT16:
						return new RemappedMappedCategoricalColumn(TYPE, toShortArray(mappedData),
								remappedDic, remapping, mapping);
					case SIGNED_INT32:
					default:
						return new RemappedMappedCategoricalColumn(TYPE, mappedData, remappedDic, remapping,
								mapping);
				}
			}
			default:
				throw new IllegalStateException("Unknown column impl");
		}
	}

	@RunWith(Parameterized.class)
	public static class StripData {

		@Parameter
		public String columnImplementation;

		@Parameter(value = 1)
		public Format format;

		@Parameters(name = "{0}_({1})")
		public static Iterable<Object[]> columnImplementations() {
			return MINIMAL_CATEGORICAL_COLUMNS;
		}

		private Column column(int[] data) {
			return CategoricalColumnTests.column(columnImplementation, data, format);
		}

		@Test
		public void testStripProperties() {
			int nValues = 1605;
			int[] data = random(nValues);
			Column column = column(data);

			Column stripped = column.stripData();
			assertEquals(0, stripped.size());
			assertEquals(column.type(), stripped.type());
			assertEquals(column.getDictionary(), stripped.getDictionary());
		}

		@Test
		public void testAfterMap() {
			int nValues = 1325;
			int[] data = random(nValues);
			Column column = column(data);

			Column stripped = column.map(new int[]{5, 3, 17}, true).stripData();
			assertEquals(0, stripped.size());
			assertEquals(column.type(), stripped.type());
			assertEquals(column.getDictionary(), stripped.getDictionary());
		}

	}

	@RunWith(Parameterized.class)
	public static class Remap {

		@Parameter
		public String columnImplementation;

		@Parameter(value = 1)
		public Format format;

		@Parameters(name = "{0}_({1})")
		public static Iterable<Object[]> columnImplementations() {
			return MINIMAL_CATEGORICAL_COLUMNS;
		}

		private CategoricalColumn column(int[] data) {
			return (CategoricalColumn) CategoricalColumnTests.column(columnImplementation, data, format);
		}

		@Test
		public void testIdentity() {
			int nValues = 1325;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);
			int[] remapping = new int[MAX_VALUES];
			Arrays.setAll(remapping, i -> i);

			Column remappedColumn = column.remap(column.getDictionary(), remapping);
			int[] remapped = new int[nValues];
			remappedColumn.fill(remapped, 0);

			int[] expected = new int[nValues];
			column.fill(expected, 0);
			assertArrayEquals(expected, remapped);

			Object[] remappedObjects = new Object[nValues];
			remappedColumn.fill(remappedObjects, 0);

			Object[] expectedObjects = new Object[nValues];
			List<String> dict = getMappingList();
			Arrays.setAll(expectedObjects, i -> dict.get(data[i]));
			assertArrayEquals(expectedObjects, remappedObjects);
		}

		@Test
		public void testSameDirectory() {
			int nValues = 1325;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			Column remappedColumn = column.remap(column.getDictionary());

			int[] expected = new int[nValues];
			column.fill(expected, 0);
			int[] mapped = new int[nValues];
			remappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSameDirectoryMerge() {
			int nValues = 1325;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			Column remappedColumn = column.mergeDictionaries(column.getDictionary());

			int[] expected = new int[nValues];
			column.fill(expected, 0);
			int[] mapped = new int[nValues];
			remappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testRandom() {
			int nValues = 1289;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);
			int[] reordering = zeroFixPermutation(MAX_VALUES);

			Column remappedColumn = column.remap(column.getDictionary(), reordering);
			int[] remapped = new int[nValues];
			remappedColumn.fill(remapped, 0);

			int[] expected = new int[nValues];
			column.fill(expected, 0);
			Arrays.setAll(expected, i -> reordering[expected[i]]);
			assertArrayEquals(expected, remapped);
		}

		@Test
		public void testSubset() {
			int nValues = 2375;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> dictionary = getMappingList();

			List<String> newDictionary = new ArrayList<>(dictionary.size() / 2);
			for (int i = 0; i < dictionary.size(); i += 2) {
				newDictionary.add(dictionary.get(i));
			}

			Column remapped = column.remap(new Dictionary(newDictionary));

			assertEquals(nValues, remapped.size());
			assertEquals(newDictionary, ((CategoricalColumn) remapped).getDictionary().getValueList());

			int[] remappedValues = new int[nValues];
			remapped.fill(remappedValues, 0);
			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> data[i] % 2 == 0 ? data[i] / 2 : 0);
			assertArrayEquals(expected, remappedValues);

			Object[] remappedObjectValues = new Object[nValues];
			remapped.fill(remappedObjectValues, 0);
			Object[] expectedObjects = new Object[nValues];
			Arrays.setAll(expectedObjects, i -> data[i] % 2 == 0 ? dictionary.get(data[i]) : null);
			assertArrayEquals(expectedObjects, remappedObjectValues);
		}

		@Test
		public void testSubsetMerge() {
			int nValues = 2375;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> dictionary = getMappingList();

			List<String> newDictionary = new ArrayList<>(dictionary.size() / 2);
			for (int i = 0; i < dictionary.size(); i += 2) {
				newDictionary.add(dictionary.get(i));
			}

			Column remapped = column.mergeDictionaries(new Dictionary(newDictionary));

			assertEquals(nValues, remapped.size());

			Object[] remappedObjectValues = new Object[nValues];
			remapped.fill(remappedObjectValues, 0);
			Object[] expectedObjects = new Object[nValues];
			Arrays.setAll(expectedObjects, i -> dictionary.get(data[i]));
			assertArrayEquals(expectedObjects, remappedObjectValues);
		}

		@Test
		public void testSuperset() {
			int nValues = 1243;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> dictionary = getMappingList();

			List<String> newDictionary = new ArrayList<>(dictionary.size() * 2);
			for (int i = 0; i < dictionary.size() * 2; i++) {
				if (i % 2 == 0) {
					newDictionary.add(dictionary.get(i / 2));
				} else {
					newDictionary.add("bla" + i);
				}
			}

			Column remapped = column.remap(new Dictionary(newDictionary));

			assertEquals(nValues, remapped.size());
			assertEquals(newDictionary, ((CategoricalColumn) remapped).getDictionary().getValueList());

			int[] remappedValues = new int[nValues];
			remapped.fill(remappedValues, 0);
			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> data[i] * 2);
			assertArrayEquals(expected, remappedValues);

			Object[] remappedObjectValues = new Object[nValues];
			remapped.fill(remappedObjectValues, 0);
			Object[] expectedObjects = new Object[nValues];
			Arrays.setAll(expectedObjects, i -> dictionary.get(data[i]));
			assertArrayEquals(expectedObjects, remappedObjectValues);
		}

		@Test
		public void testSupersetMerge() {
			int nValues = 1243;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> dictionary = getMappingList();

			List<String> newDictionary = new ArrayList<>(dictionary.size() * 2);
			for (int i = 0; i < dictionary.size() * 2; i++) {
				if (i % 2 == 0) {
					newDictionary.add(dictionary.get(i / 2));
				} else {
					newDictionary.add("bla" + i);
				}
			}

			Column remapped = column.mergeDictionaries(new Dictionary(newDictionary));

			assertEquals(nValues, remapped.size());
			assertEquals(newDictionary, ((CategoricalColumn) remapped).getDictionary().getValueList());

			int[] remappedValues = new int[nValues];
			remapped.fill(remappedValues, 0);
			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> data[i] * 2);
			assertArrayEquals(expected, remappedValues);

			Object[] remappedObjectValues = new Object[nValues];
			remapped.fill(remappedObjectValues, 0);
			Object[] expectedObjects = new Object[nValues];
			Arrays.setAll(expectedObjects, i -> dictionary.get(data[i]));
			assertArrayEquals(expectedObjects, remappedObjectValues);
		}

		@Test
		public void testMinimalDictionary() {
			int nValues = 133;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> newDictionary = Collections.singletonList(null);
			Column remappedColumn = column.remap(new Dictionary(newDictionary));

			int[] expected = new int[nValues];
			int[] remapped = new int[nValues];
			remappedColumn.fill(remapped, 0);
			assertArrayEquals(expected, remapped);
		}

		@Test
		public void testMinimalDictionaryMerge() {
			int nValues = 133;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> newDictionary = Collections.singletonList(null);
			Column remappedColumn = column.mergeDictionaries(new Dictionary(newDictionary));

			assertEquals(column.getDictionary(), ((CategoricalColumn) remappedColumn).getDictionary());

			Object[] remappedObjectValues = new Object[nValues];
			remappedColumn.fill(remappedObjectValues, 0);
			Object[] expectedObjects = new Object[nValues];
			List<String> dict = getMappingList();
			Arrays.setAll(expectedObjects, i -> dict.get(data[i]));
			assertArrayEquals(expectedObjects, remappedObjectValues);
		}


		@Test
		public void testRemappedType() {
			int nValues = 12;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			int[] identity = new int[MAX_VALUES];
			Column mappedColumn = column.remap(new Dictionary(getMappingList()), identity);

			assertEquals(column.type(), mappedColumn.type());
		}


		@Test
		public void testChained() {
			int nValues = 3237;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			// Apply reverse mapping four times
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);
			Column mappedColumn = column.remap(new Dictionary(Collections.emptyList()), mapping)
					.remap(new Dictionary(Collections.emptyList()), mapping)
					.remap(new Dictionary(Collections.emptyList()), mapping)
					.remap(new Dictionary(Collections.emptyList()), mapping);

			int[] expected = new int[nValues];
			column.fill(expected, 0);

			int[] mapped = new int[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testCommutativityWithMap() {
			int nValues = 1257;
			int[] data = randomMax(nValues, format);
			CategoricalColumn column = column(data);

			List<String> dictionary = column.getDictionary().getValueList();
			int[] permutation = zeroFixPermutation(dictionary.size());
			List<String> remappedDictionary = new ArrayList<>(dictionary);
			for (int i = 1; i < permutation.length; i++) {
				remappedDictionary.set(permutation[i], dictionary.get(i));
			}

			int[] mapping = permutation(nValues);
			mapping[52] = -1;

			Column mappedRemapped = column.remap(new Dictionary(remappedDictionary), permutation).map(mapping, true);
			CategoricalColumn map = (CategoricalColumn) column.map(mapping, true);
			Column remappedMapped = map.remap(new Dictionary(remappedDictionary), permutation);

			int[] expected = new int[nValues];
			mappedRemapped.fill(expected, 0);
			int[] actual = new int[nValues];
			remappedMapped.fill(actual, 0);
			assertArrayEquals(expected, actual);

			Object[] expectedObjects = new Object[nValues];
			mappedRemapped.fill(expectedObjects, 0);
			Object[] actualObjects = new Object[nValues];
			remappedMapped.fill(actualObjects, 0);
			assertArrayEquals(expectedObjects, actualObjects);
		}

	}


	@RunWith(Parameterized.class)
	public static class ToBuffer {

		@Parameter
		public String columnImplementation;

		@Parameters(name = "{0}")
		public static Iterable<String> columnImplementations() {
			return Arrays.asList(IMPL_SIMPLE_CATEGORICAL_STRING, IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING,
					IMPL_REMAPPED_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING,
					IMPL_SIMPLE_CATEGORICAL_SPARSE_STRING_DEFAULT_IS_MISSING, IMPL_MAPPED_CATEGORICAL_STRING,
					IMPL_REMAPPED_CATEGORICAL_STRING, IMPL_REMAPPED_MAPPED_CATEGORICAL_STRING);
		}

		@SuppressWarnings("unchecked")
		private CategoricalColumn column(int[] data) {
			return (CategoricalColumn) CategoricalColumnTests.column(columnImplementation, data, Format.SIGNED_INT32);
		}

		private Int32NominalBuffer toCategoricalBuffer(Column column) {
			return BufferTestUtils.getInt32Buffer(column, ColumnType.NOMINAL);
		}

		private ObjectBuffer<String> toFreeBuffer(Column column) {
			return BufferTestUtils.getObjectBuffer(column, ColumnType.TEXT);
		}

		@Test
		public void testCategorical() {
			int nValues = 1605;
			int[] data = random(nValues);
			CategoricalColumn column = column(data);
			Int32NominalBuffer buffer = toCategoricalBuffer(column);
			assertEquals(column.size(), buffer.size());

			ObjectReader reader = Readers.objectReader(column, String.class);
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++));
			}
		}

		@Test
		public void testDouble() {
			int nValues = 1703;
			int[] data = random(nValues);
			Column column = column(data);
			NumericBuffer buffer = Buffers.integer53BitBuffer(column);
			assertEquals(column.size(), buffer.size());

			NumericReader reader = Readers.numericReader(column, column.size());
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++), EPSILON);
			}
		}


		@Test
		public void testFree() {
			int nValues = 1404;
			int[] data = random(nValues);
			CategoricalColumn column = column(data);
			ObjectBuffer<String> buffer = toFreeBuffer(column);
			assertEquals(column.size(), buffer.size());

			ObjectReader reader = Readers.objectReader(column, String.class);
			int index = 0;
			while (reader.hasRemaining()) {
				assertEquals(reader.read(), buffer.get(index++));
			}
		}
	}

	@RunWith(Parameterized.class)
	public static class Boolean {

		@Parameter
		public String columnImplementation;

		@Parameter(value = 1)
		public Format format;

		@Parameters(name = "{0}_({1})")
		public static Iterable<Object[]> columnImplementations() {
			return MINIMAL_CATEGORICAL_COLUMNS;
		}

		private CategoricalColumn column(int[] data) {
			return CategoricalColumnTests.booleanColumn(columnImplementation, data, format, Math.random() > 0.5 ? 2 : 1);
		}

		@Test
		public void testMappingView() {
			int nValues = 1172;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = column(data);
			int[] mapping = permutation(nValues);

			Column mappedColumn = column.map(mapping, true);
			Dictionary dictionary = column.getDictionary();
			Dictionary mappedDictionary = mappedColumn.getDictionary();
			assertEquals(dictionary.getNegativeIndex(), mappedDictionary.getNegativeIndex());
			assertEquals(dictionary.getPositiveIndex(), mappedDictionary.getPositiveIndex());
		}

		@Test
		public void testMappingNonView() {
			int nValues = 1132;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = column(data);
			int[] mapping = permutation(nValues / 100);

			Column mappedColumn = column.map(mapping, false);

			Dictionary dictionary = column.getDictionary();
			Dictionary mappedDictionary = mappedColumn.getDictionary();
			assertEquals(dictionary.getNegativeIndex(), mappedDictionary.getNegativeIndex());
			assertEquals(dictionary.getPositiveIndex(), mappedDictionary.getPositiveIndex());
		}

		@Test
		public void testRemapping() {
			int nValues = 117;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = (CategoricalColumn) column(data);

			List<String> dictionary = getBooleanMappingList();
			String pos1 = dictionary.get(1);
			dictionary.set(1, dictionary.get(2));
			dictionary.set(2, pos1);

			Column remapped = column.remap(new BooleanDictionary(dictionary, 1));
			assertTrue(remapped.getDictionary().isBoolean());
			assertEquals(1, remapped.getDictionary().getPositiveIndex());
		}

		@Test
		public void testMergeDictionary() {
			int nValues = 115;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = (CategoricalColumn) column(data);

			List<String> dictionary = getBooleanMappingList();
			dictionary.set(1, "x");

			Column remapped = column.mergeDictionaries(new BooleanDictionary(dictionary, 1));
			assertFalse(remapped.getDictionary().isBoolean());
		}

		@Test
		public void testMergeDictionaryBooleanAgain() {
			int nValues = 115;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = (CategoricalColumn) column(data);

			List<String> dictionary = getBooleanMappingList();
			dictionary.set(1, dictionary.get(2));
			dictionary.remove(2);

			Column remapped = column.mergeDictionaries(new BooleanDictionary(dictionary, 1));
			assertTrue(remapped.getDictionary().isBoolean());
			assertEquals(1, remapped.getDictionary().getPositiveIndex());
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedPositiveIndex() {
			int nValues = 113;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.column(columnImplementation, data, format);
			column.getDictionary().hasPositive();
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedNegativeIndex() {
			int nValues = 112;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.column(columnImplementation, data, format);
			column.getDictionary().hasNegative();
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedGetPositiveIndex() {
			int nValues = 113;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.column(columnImplementation, data, format);
			column.getDictionary().getPositiveIndex();
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupportedgetNegativeIndex() {
			int nValues = 112;
			int[] data = randomBoolean(nValues);

			Column column = CategoricalColumnTests.column(columnImplementation, data, format);
			assertFalse(column.getDictionary().isBoolean());
			column.getDictionary().getNegativeIndex();
		}

		@Test
		public void testToBoolean() {
			int nValues = 172;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = (CategoricalColumn) column(data);

			Dictionary dictionary = column.getDictionary();
			CategoricalColumn booleanColumn = column.toBoolean(dictionary.get(1));

			Dictionary booleanDictionary = booleanColumn.getDictionary();
			assertEquals(2, booleanDictionary.getNegativeIndex());
			assertEquals(1, booleanDictionary.getPositiveIndex());
		}

		@Test
		public void testToBooleanTwice() {
			int nValues = 132;
			int[] data = randomBoolean(nValues);

			CategoricalColumn column = (CategoricalColumn) column(data);

			Dictionary dictionary = column.getDictionary();
			CategoricalColumn booleanColumn = column.toBoolean(dictionary.get(1));
			booleanColumn = booleanColumn.toBoolean(dictionary.get(2));

			Dictionary booleanDictionary = booleanColumn.getDictionary();
			assertEquals(1, booleanDictionary.getNegativeIndex());
			assertEquals(2, booleanDictionary.getPositiveIndex());
		}
	}

}
