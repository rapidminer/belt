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

package com.rapidminer.belt.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.column.CacheMappedColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Tests functionality of columns that support filling {@code int} buffers, i.e., the functionality implemented by all
 * categorical columns.
 *
 * @author Michael Knopf, Gisa Meier, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class IndexReadableTests {

	private static final List<String> EMPTY_DICTIONARY = Collections.emptyList();

	private enum Implementation {
		SIMPLE_CATEGORICAL_2BIT("SimpleCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				IndexReadableTests::categorical2BitColumn),
		SIMPLE_CATEGORICAL_4BIT("SimpleCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				IndexReadableTests::categorical4BitColumn),
		SIMPLE_CATEGORICAL_8BIT("SimpleCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				IndexReadableTests::categoricalDense8BitColumn),
		SIMPLE_CATEGORICAL_16BIT("SimpleCategorical16Bit",
				(seed, n) -> randomIntegers(seed, 65536, n),
				IndexReadableTests::categoricalDense16BitColumn),
		SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
				(seed, n) -> randomIntegers(seed, Integer.MAX_VALUE, n),
				IndexReadableTests::categoricalDense32BitColumn),
		CATEGORICAL_SPARSE_8BIT("CategoricalSparse8Bit",
				(seed, n) -> sparseRandomInts(seed, 256, n, false, 0.9),
				IndexReadableTests::categoricalSparse8BitColumn),
		CATEGORICAL_SPARSE_16BIT("CategoricalSparse16Bit",
				(seed, n) -> sparseRandomInts(seed, 65536, n, false, 0.8),
				IndexReadableTests::categoricalSparse16BitColumn),
		CATEGORICAL_SPARSE_32BIT("CategoricalSparse32Bit",
				(seed, n) -> sparseRandomInts(seed, Integer.MAX_VALUE, n, false, 0.75),
				IndexReadableTests::categoricalSparse32BitColumn),
		CATEGORICAL_SPARSE_8BIT_DEFAULT_IS_MISSING("CategoricalSparse8Bit_DEFAULT_IS_MISSING",
				(seed, n) -> sparseRandomInts(seed, 256, n, true, 0.9),
				IndexReadableTests::categoricalSparse8BitColumn),
		CATEGORICAL_SPARSE_16BIT_DEFAULT_IS_MISSING("CategoricalSparse16Bit_DEFAULT_IS_MISSING",
				(seed, n) -> sparseRandomInts(seed, 65536, n, true, 0.8),
				IndexReadableTests::categoricalSparse16BitColumn),
		CATEGORICAL_SPARSE_32BIT_DEFAULT_IS_MISSING("CategoricalSparse32Bit_DEFAULT_IS_MISSING",
				(seed, n) -> sparseRandomInts(seed, Integer.MAX_VALUE, n, true, 0.75),
				IndexReadableTests::categoricalSparse32BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_8BIT("REMAPPED_CategoricalSparse8Bit",
				(seed, n) -> sparseRandomInts(seed, 256, n, false, 0.9),
				IndexReadableTests::remappedCategoricalSparse8BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_16BIT("REMAPPED_CategoricalSparse16Bit",
				(seed, n) -> sparseRandomInts(seed, 655, n, false, 0.8),
				IndexReadableTests::remappedCategoricalSparse16BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_32BIT("REMAPPED_CategoricalSparse32Bit",
				(seed, n) -> sparseRandomInts(seed, 713, n, false, 0.75),
				IndexReadableTests::remappedCategoricalSparse32BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_8BIT_DEFAULT_IS_MISSING("REMAPPED_CategoricalSparse8Bit_DEFAULT_IS_MISSING",
				(seed, n) -> sparseRandomInts(seed, 256, n, true, 0.9),
				IndexReadableTests::remappedCategoricalSparse8BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_16BIT_DEFAULT_IS_MISSING("REMAPPED_CategoricalSparse16Bit_DEFAULT_IS_MISSING",
				(seed, n) -> sparseRandomInts(seed, 655, n, true, 0.8),
				IndexReadableTests::remappedCategoricalSparse16BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_32BIT_DEFAULT_IS_MISSING("REMAPPED_CategoricalSparse32Bit_DEFAULT_IS_MISSING",
				(seed, n) -> sparseRandomInts(seed, 713, n, true, 0.75),
				IndexReadableTests::remappedCategoricalSparse32BitColumn),
		MAPPED_CATEGORICAL_2BIT("MappedCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				IndexReadableTests::mappedCategorical2BitColumn),
		MAPPED_CATEGORICAL_4BIT("MappedCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				IndexReadableTests::mappedCategorical4BitColumn),
		MAPPED_CATEGORICAL_8BIT("MappedCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				IndexReadableTests::mappedCategorical8BitColumn),
		MAPPED_CATEGORICAL_16BIT("MappedCategorical16Bit",
				(seed, n) -> randomIntegers(seed, 65536, n),
				IndexReadableTests::mappedCategorical16BitColumn),
		MAPPED_CATEGORICAL_32BIT("MappedCategorical32Bit",
				(seed, n) -> randomIntegers(seed, Integer.MAX_VALUE, n),
				IndexReadableTests::mappedCategorical32BitColumn),
		REMAPPED_CATEGORICAL_2BIT("RemappedCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				IndexReadableTests::remappedCategorical2BitColumn),
		REMAPPED_CATEGORICAL_4BIT("RemappedCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				IndexReadableTests::remappedCategorical4BitColumn),
		REMAPPED_CATEGORICAL_8BIT("RemappedCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				IndexReadableTests::remappedCategoricalDense8BitColumn),
		REMAPPED_CATEGORICAL_16BIT("RemappedCategorical16Bit",
				(seed, n) -> randomIntegers(seed, 655, n),
				IndexReadableTests::remappedCategoricalDense16BitColumn),
		REMAPPED_CATEGORICAL_32BIT("RemappedCategorical32Bit",
				(seed, n) -> randomIntegers(seed, 713, n),
				IndexReadableTests::remappedCategoricalDense32BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_2BIT("RemappedMappedCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				IndexReadableTests::remappedMappedCategorical2BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_4BIT("RemappedMappedCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				IndexReadableTests::remappedMappedCategorical4BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_8BIT("RemappedMappedCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				IndexReadableTests::remappedMappedCategorical8BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_16BIT("RemappedMappedCategorical16Bit",
				(seed, n) -> randomIntegers(seed, 561, n),
				IndexReadableTests::remappedMappedCategorical16BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_32BIT("RemappedMappedCategorical32Bit",
				(seed, n) -> randomIntegers(seed, 1237, n),
				IndexReadableTests::remappedMappedCategorical32BitColumn);

		private final String name;
		private final ArrayGenerator generator;
		private final ColumnBuilder builder;

		Implementation(String name, ArrayGenerator generator, ColumnBuilder builder) {
			this.name = name;
			this.generator = generator;
			this.builder = builder;
		}

		@Override
		public String toString() {
			return name;
		}

		public ArrayGenerator getGenerator() {
			return generator;
		}

		public ColumnBuilder getBuilder() {
			return builder;
		}
	}

	@FunctionalInterface
	private interface ArrayGenerator {
		int[] generate(long seed, int n);
	}

	@FunctionalInterface
	private interface ColumnBuilder {
		Column build(int[] data);
	}

	private static int[] randomIntegers(long seed, int bound, int n) {
		Random rng = new Random(seed);
		int[] data = new int[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextInt(bound);
		}
		return data;
	}

	/**
	 * Returns an array with sparse random integer values.
	 *
	 * @param seed
	 * 		a seed for the random number generator.
	 * @param bound
	 * 		the upper bound for the random numbers.
	 * @param n
	 * 		the number of random numbers to generate.
	 * @param defaultIsMissing
	 * 		iff this value is set to {@code true}, the default value will be set to {@link
	 *        com.rapidminer.belt.reader.CategoricalReader#MISSING_CATEGORY}.
	 * @param sparsity
	 * 		the approximate sparsity of the resulting data.
	 * @return an array with sparse random integer values.
	 */
	private static int[] sparseRandomInts(long seed, int bound, int n, boolean defaultIsMissing,
										  double sparsity) {
		SplittableRandom random = new SplittableRandom(seed);
		int defaultValue = defaultIsMissing ? 0 : 1 + random.nextInt(bound - 1);
		return Arrays.stream(ColumnTestUtils.sparseRandomLongs(random, bound, n, defaultValue, sparsity)).
				mapToInt(i -> (int) i).toArray();
	}

	private static final ColumnType<String> STRING_TYPE = ColumnTestUtils.categoricalType(
			String.class, null);

	private static Column categorical2BitColumn(int[] data) {
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return ColumnAccessor.get().newCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical2BitColumn(int[] data) {
		int max = -1;
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			IntegerFormats.writeUInt2(byteData, i, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping);
	}

	private static Column categorical4BitColumn(int[] data) {
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return ColumnAccessor.get().newCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical4BitColumn(int[] data) {
		int max = -1;
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			IntegerFormats.writeUInt4(byteData, i, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping);
	}

	private static Column categoricalDense8BitColumn(int[] data) {
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getSimpleCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY);
	}

	private static Column categoricalSparse8BitColumn(int[] data) {
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getSparseCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY,
				ColumnTestUtils.getMostFrequentValue(byteData, (byte) 0));
	}

	private static Column remappedCategoricalDense8BitColumn(int[] data) {
		int max = -1;
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			byteData[i] = (byte) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping);
	}

	private static Column remappedCategoricalSparse8BitColumn(int[] data) {
		int max = -1;
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			byteData[i] = (byte) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedCategoricalSparseColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping,
				ColumnTestUtils.getMostFrequentValue(byteData, (byte) 0));
	}

	private static Column categoricalDense16BitColumn(int[] data) {
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) data[i];
		}
		return ColumnTestUtils.getSimpleCategoricalColumn(STRING_TYPE, shortData, EMPTY_DICTIONARY);
	}

	private static Column categoricalSparse16BitColumn(int[] data) {
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) data[i];
		}
		return ColumnTestUtils.getSparseCategoricalColumn(STRING_TYPE, shortData, EMPTY_DICTIONARY,
				ColumnTestUtils.getMostFrequentValue(shortData, (short) 0));
	}

	private static Column remappedCategoricalDense16BitColumn(int[] data) {
		int max = -1;
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			shortData[i] = (short) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, shortData, EMPTY_DICTIONARY, remapping);
	}

	private static Column remappedCategoricalSparse16BitColumn(int[] data) {
		int max = -1;
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			shortData[i] = (short) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalSparseColumn(STRING_TYPE, shortData, EMPTY_DICTIONARY, remapping,
				ColumnTestUtils.getMostFrequentValue(shortData, (short) 0));
	}

	private static Column categoricalDense32BitColumn(int[] data) {
		return ColumnTestUtils.getSimpleCategoricalColumn(STRING_TYPE, data, EMPTY_DICTIONARY);
	}

	private static Column categoricalSparse32BitColumn(int[] data) {
		return ColumnTestUtils.getSparseCategoricalColumn(STRING_TYPE, data, EMPTY_DICTIONARY,
				ColumnTestUtils.getMostFrequentValue(data, 0));
	}

	private static Column remappedCategoricalDense32BitColumn(int[] data) {
		int max = -1;
		int[] intData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			intData[i] = value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, intData, EMPTY_DICTIONARY, remapping);
	}

	private static Column remappedCategoricalSparse32BitColumn(int[] data) {
		int max = -1;
		int[] intData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			max = Math.max(max, value);
			intData[i] = value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalSparseColumn(STRING_TYPE, intData, EMPTY_DICTIONARY, remapping,
				ColumnTestUtils.getMostFrequentValue(data, 0));
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

	private static Column mappedCategorical2BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical2BitColumn(int[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], value);
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping,
				mapping);
	}

	private static Column mappedCategorical4BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical4BitColumn(int[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], value);
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping, mapping);
	}


	private static Column mappedCategorical8BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedByteData[mapping[i]] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical8BitColumn(int[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			mappedByteData[mapping[i]] = (byte) value;
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, packed, EMPTY_DICTIONARY, remapping, mapping);
	}

	private static Column mappedCategorical16BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedShortData[mapping[i]] = (short) data[i];
		}
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, mappedShortData, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical16BitColumn(int[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			mappedShortData[mapping[i]] = (short) value;
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, mappedShortData, EMPTY_DICTIONARY, remapping,
				mapping);
	}

	private static Column mappedCategorical32BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedIntData[mapping[i]] = data[i];
		}
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, mappedIntData, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical32BitColumn(int[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = data[i];
			mappedIntData[mapping[i]] = value;
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, mappedIntData, EMPTY_DICTIONARY, remapping,
				mapping);
	}

	@RunWith(Parameterized.class)
	public static class ContinuousFill {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test
		public void testContinuous() {
			int nValues = 1605;
			int end = 1214;

			int[] data = impl.getGenerator().generate(81217778L, nValues);
			int[] expected = Arrays.copyOfRange(data, 0, end);

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end];
			column.fill(buffer, 0);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testPartialContinuous() {
			int nValues = 1605;
			int start = 80;
			int end = nValues + start;

			int[] data = impl.getGenerator().generate(56120220L, nValues);

			// ignore the part that is out of bounds because it is undefined (see javadoc of fill)
			int[] expected = Arrays.copyOfRange(data, start, data.length);

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, buffer.length - start));
		}

		@Test
		public void testOutOfBoundsContinuous() {
			int nValues = 1703;
			int start = nValues - 10;
			int end = nValues + 10;

			int[] data = impl.getGenerator().generate(43534871L, nValues);
			int[] expected = Arrays.copyOfRange(data, start, data.length);

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end - start];
			column.fill(buffer, start);

			// The first 10 elements that have been read should be equal to the last 10 elements of data.
			// The rest of the read elements is undefined (see javadoc of fill).
			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, 10));
		}

		@Test
		public void testContinuousIdempotence() {
			int nReads = 3;
			int nValues = 2043;
			int start = 83;
			int end = nValues - start;

			int[] data = impl.getGenerator().generate(79407786L, nValues);
			int[] expected = Arrays.copyOfRange(data, start, end);

			Column column = impl.getBuilder().build(data);

			for (int i = 0; i < nReads; i++) {
				int[] buffer = new int[end - start];
				column.fill(buffer, start);
				assertArrayEquals(expected, buffer);
			}
		}

		@Test
		public void testContinuousWithEmptyBuffer() {
			int nValues = 1332;
			int[] data = impl.getGenerator().generate(40100703L, nValues);
			Column column = impl.getBuilder().build(data);
			column.fill(new int[0], 0);
		}

	}

	@RunWith(Parameterized.class)
	public static class InterleavedFill {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		private int[][] generateTable(long seed, int nColumns, int nRows) {
			Random rng = new Random(seed);
			int[][] table = new int[nColumns][];
			Arrays.setAll(table, i -> impl.getGenerator().generate(rng.nextLong(), nRows));
			return table;
		}

		private int[] flattenTable(int[][] table) {
			int[] flattened = new int[table.length * table[0].length];
			for (int i = 0; i < table.length; i++) {
				int[] column = table[i];
				for (int j = 0; j < column.length; j++) {
					flattened[i + table.length * j] = column[j];
				}
			}
			return flattened;
		}

		@Test
		public void testInterleavedOfCompleteRowsFromStart() {
			int nColumns = 4;
			int nRows = 2012;
			int bufferSize = 128;

			int[][] table = generateTable(76431884L, nColumns, nRows);
			int[] expected = Arrays.copyOf(flattenTable(table), 128);

			int[] buffer = new int[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, 0, i, nColumns);
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedOfCompleteRows() {
			int nColumns = 4;
			int nRows = 2012;
			int bufferSize = 128;
			int startIndex = 256;

			int[][] table = generateTable(19378560L, nColumns, nRows);
			int[] expected = Arrays.copyOfRange(flattenTable(table), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			int[] buffer = new int[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, startIndex, i, nColumns);
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedOfIncompleteRowsFromStart() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 2012;
			int bufferSize = 128;

			int[][] table = generateTable(82926115L, nColumns, nRows);
			int expected[] = Arrays.copyOf(flattenTable(table), bufferSize);

			int[] buffer = new int[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, 0, i, nColumns);
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedOfIncompleteRows() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 2012;
			int bufferSize = 128;
			int startIndex = 384;

			int[][] table = generateTable(51022682L, nColumns, nRows);
			int[] expected = Arrays.copyOfRange(flattenTable(table), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			int[] buffer = new int[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, startIndex, i, nColumns);
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedIdempotence() {
			int nColumns = 4;
			int nRows = 2012;
			int bufferSize = 128;
			int nRuns = 3;

			int[][] table = generateTable(51022682L, nColumns, nRows);
			int[] expected = Arrays.copyOf(flattenTable(table), 128);

			int[] buffer = new int[bufferSize];
			for (int run = 0; run < nRuns; run++) {
				for (int i = 0; i < nColumns; i++) {
					Column column = impl.getBuilder().build(table[i]);
					column.fill(buffer, 0, i, nColumns);
				}
				assertArrayEquals(expected, buffer);
			}
		}

		@Test
		public void testPartialInterleaved() {
			int nValues = 2048;

			int[] data = impl.getGenerator().generate(51022682L, nValues);

			int[] expected = new int[nValues / 2];
			System.arraycopy(data, nValues / 2, expected, 0, nValues / 2);

			int[] buffer = new int[nValues];
			Column column = impl.getBuilder().build(data);
			column.fill(buffer, nValues / 2, 0, 1);

			// The values that are out of range are undefined (see javadoc of fill).
			// Therefore, we only check the first half of the arrays.
			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, nValues / 2));
		}

		@Test
		public void testOutOfBoundsInterleaved() {
			int nValues = 1703;
			int start = nValues - 10;
			int end = nValues + 10;

			int[] data = impl.getGenerator().generate(39223391L, nValues);
			int[] expected = Arrays.copyOfRange(data, start, data.length);

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end - start];
			column.fill(buffer, start, 0, 1);

			// The first 10 elements that have been read should be equal to the last 10 elements of data.
			// The rest of the read elements is undefined (see javadoc of fill).
			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, 10));
		}

		@Test
		public void testInterleavedWithEmptyBuffer() {
			int nValues = 1332;
			int[] data = impl.getGenerator().generate(46821694L, nValues);
			Column column = impl.getBuilder().build(data);
			column.fill(new int[0], 0, 0, 1);
		}

	}

	@RunWith(Parameterized.class)
	public static class SimpleMap {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test
		public void testConstant() {
			int nValues = 1172;
			int constant = 78;
			int[] data = impl.generator.generate(46153408L, nValues);
			int[] mapping = new int[nValues];
			Arrays.fill(mapping, constant);

			int[] expected = new int[nValues];
			Arrays.fill(expected, data[constant]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testIdentity() {
			int nValues = 2948;
			int[] data = impl.generator.generate(16191611L, nValues);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(data, mapped);
		}

		@Test
		public void testReverse() {
			int nValues = 1883;
			int[] data = impl.generator.generate(71181325L, nValues);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);

			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testRandom() {
			int nValues = 1289;
			int[] data = impl.generator.generate(43495824L, nValues);
			int[] mapping = permutation(nValues);
			Arrays.setAll(mapping, i -> nValues - i - 1);

			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSubset() {
			int nValues = 2375;
			int nSubsetValues = 25;
			int[] data = impl.generator.generate(88282498L, nValues);
			int[] mapping = new int[nSubsetValues];
			Arrays.setAll(mapping, i -> i);

			int[] expected = new int[nSubsetValues];
			System.arraycopy(data, 0, expected, 0, nSubsetValues);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nSubsetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSubsetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSuperset() {
			int nValues = 1243;
			int nSupersetValues = 2465;
			int[] data = impl.generator.generate(48618327L, nValues);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % nValues);

			int[] expected = new int[nSupersetValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForNegative() {
			int nValues = 124;
			int nSupersetValues = 1465;
			int[] data = impl.generator.generate(75220537L, nValues);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % (nValues + 1) - 1);

			int[] expected = new int[nSupersetValues];
			Arrays.setAll(expected, i -> mapping[i] < 0 ? 0 : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForOutOfBounds() {
			int nValues = 124;
			int nSupersetValues = 1465;
			int[] data = impl.generator.generate(53243935L, nValues);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			int[] expected = new int[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? 0 : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForOutOfBoundsInterleaved() {
			int nValues = 124;
			int nSupersetValues = 1465;
			int[] data = impl.generator.generate(40727149L, nValues);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			int[] expected = new int[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? 0 : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] mapped = new int[nSupersetValues];
			mappedColumn.fill(mapped, 0, 0, 1);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testEmpty() {
			int nValues = 1243;
			int[] data = impl.generator.generate(42359664L, nValues);
			int[] mapping = new int[0];
			Arrays.setAll(mapping, i -> i % nValues);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			assertEquals(0, mappedColumn.size());

			mappedColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, true);
			assertEquals(0, mappedColumn.size());
		}

		@Test
		public void testChained() {
			int nValues = 3237;
			int[] data = impl.generator.generate(82962299L, nValues);

			// Apply reverse mapping four times
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);
			Column mappedColumn =
					ColumnAccessor.get().map(ColumnAccessor.get().map(ColumnAccessor.get().map(ColumnAccessor.get().map(impl.builder.build(data), mapping, false)
					, mapping, false)
					, mapping, false)
					, mapping, false);

			int[] mapped = new int[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(data, mapped);
		}

		@Test
		public void testViewHint() {
			int nValues = 2248;
			int[] data = impl.generator.generate(82161611L, nValues);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] defaultMapping = new int[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, true);
			int[] viewHintMapping = new int[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testViewHintSuperset() {
			int nValues = 2148;
			int[] data = impl.generator.generate(23412980L, nValues);
			int[] mapping = new int[nValues * 2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] defaultMapping = new int[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, true);
			int[] viewHintMapping = new int[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testViewHintSubset() {
			int nValues = 10 * 245;
			int[] data = impl.generator.generate(71100508L, nValues);
			int[] mapping = new int[nValues / 10];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, false);
			int[] defaultMapping = new int[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(data), mapping, true);
			int[] viewHintMapping = new int[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testMappedType() {
			int nValues = 12;
			int[] data = impl.generator.generate(56548366L, nValues);

			Column column = impl.builder.build(data);
			Column mappedColumn = ColumnAccessor.get().map(column, new int[]{2, 3, 4, 6, 7, 11}, false);

			assertEquals(column.type(), mappedColumn.type());
		}

	}

	@RunWith(Parameterized.class)
	public static class CachedMap {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.MAPPED_CATEGORICAL_2BIT, Implementation.MAPPED_CATEGORICAL_4BIT,
					Implementation.MAPPED_CATEGORICAL_8BIT, Implementation.MAPPED_CATEGORICAL_16BIT,
					Implementation.MAPPED_CATEGORICAL_32BIT, Implementation.REMAPPED_MAPPED_CATEGORICAL_2BIT,
					Implementation.REMAPPED_MAPPED_CATEGORICAL_4BIT, Implementation.REMAPPED_MAPPED_CATEGORICAL_8BIT,
					Implementation.REMAPPED_MAPPED_CATEGORICAL_16BIT,
					Implementation.REMAPPED_MAPPED_CATEGORICAL_32BIT);
		}

		@Test
		public void testCache() throws ExecutionException, InterruptedException {
			int nValues = 1238;

			int[] data = impl.generator.generate(28736437L, nValues);
			Column column = impl.builder.build(data);

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			// Create cache and entry for the above mapping
			ConcurrentHashMap<int[], CompletableFuture<int[]>> cache = new ConcurrentHashMap<>();
			((CacheMappedColumn) column).map(mapping, false, cache);
			assertEquals(1, cache.size());
			int[] remapping = new ArrayList<>(cache.values()).get(0).get();

			// Modify cache to test whether it is used (invalidate every fourth value)
			Arrays.setAll(remapping, i -> i % 4 == 0 ? -1 : remapping[i]);


			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> i % 4 == 0 ? 0 : data[i]);

			int[] buffer = new int[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, false, cache);
			mapped.fill(buffer, 0);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testCachePreferView() throws ExecutionException, InterruptedException {
			int nValues = 1238;

			int[] data = impl.generator.generate(28736437L, nValues);
			Column column = impl.builder.build(data);

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			// Create cache and entry for the above mapping
			ConcurrentHashMap<int[], CompletableFuture<int[]>> cache = new ConcurrentHashMap<>();
			((CacheMappedColumn) column).map(mapping, true, cache);
			assertEquals(1, cache.size());
			int[] remapping = new ArrayList<>(cache.values()).get(0).get();

			// Modify cache to test whether it is used (invalidate every fourth value)
			Arrays.setAll(remapping, i -> i % 4 == 0 ? -1 : remapping[i]);


			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> i % 4 == 0 ? 0 : data[i]);

			int[] buffer = new int[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, true, cache);
			mapped.fill(buffer, 0);

			assertArrayEquals(expected, buffer);
		}


	}

	@RunWith(Parameterized.class)
	public static class Fill {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test(expected = NullPointerException.class)
		public void testNullArray() {
			int nValues = 1238;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill((int[]) null, 0);

		}

		@Test
		public void testEmptyArray() {
			int nValues = 1238;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill(new int[0], 0);

		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRow() {
			int nValues = 1238;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill(new int[10], -5);

		}

		@Test
		public void testTooBigRow() {
			int nValues = 12;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, nValues - 1);
			assertEquals(data[nValues - 1], array[0]);
		}

		@Test
		public void testAlmostTooBigRow() {
			int nValues = 32;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, nValues - 4);

			int[] all = new int[nValues];
			column.fill(all, 0);
			int[] expected = new int[4];
			System.arraycopy(all, nValues - 4, expected, 0, 4);

			assertArrayEquals(expected, Arrays.copyOfRange(array, 0, 4));
		}

		@Test(expected = NullPointerException.class)
		public void testNullArrayInterleaved() {
			int nValues = 1238;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill((int[]) null, 0, 0, 1);

		}

		@Test
		public void testEmptyArrayInterleaved() {
			int nValues = 123;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill(new int[0], 0, 0, 1);

		}

		@Test(expected = IllegalArgumentException.class)
		public void testNullStepSizeInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 0, 0, 0);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRowInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, -5, 5, 1);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRowInterleavedStepSize() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, -5, 5, 2);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeOffsetInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 0, -5, 1);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeOffsetInterleavedStepSize() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 0, -5, 2);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeStepSizeInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 10, 0, -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeStepSizeBiggerInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 10, 0, -3);
		}

		@Test
		public void testOffsetInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[100];
			column.fill(array, 0, 50, 1);

			int[] normal = new int[128];
			column.fill(normal, 0);
			int[] expected = new int[100];
			System.arraycopy(normal, 0, expected, 50, 50);
			assertArrayEquals(expected, array);
		}

		@Test
		public void testOffsetInterleavedStepSize() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[100];
			column.fill(array, 0, 50, 2);
			int[] normal = new int[128];
			column.fill(normal, 0);
			int[] expected = new int[100];
			Arrays.setAll(expected, i -> i < 50 ? 0 : (i%2==0 ? normal[(i - 50)/2] : 0));
			assertArrayEquals(expected, array);
		}

		@Test
		public void testOffsetBiggerSizeInterleaved() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 0, 100, 1);
			assertArrayEquals(new int[10], array);
		}

		@Test
		public void testOffsetBiggerSizeInterleavedStepSize() {
			int nValues = 128;

			int[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			int[] array = new int[10];
			column.fill(array, 0, 110, 2);
			assertArrayEquals(new int[10], array);
		}

	}

}
