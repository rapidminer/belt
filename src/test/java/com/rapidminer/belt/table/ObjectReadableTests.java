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

package com.rapidminer.belt.table;

import static com.rapidminer.belt.util.IntegerFormats.Format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.column.CacheMappedColumn;
import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Tests functionality of columns with capability {@link Column.Capability#OBJECT_READABLE}. In particular, the
 * corresponding methods to fill the buffers of {@link ObjectReader}s.
 *
 * @author Michael Knopf, Gisa Meier, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class ObjectReadableTests {

	private static int MAX_MAPPING = 512;

	private enum Implementation {
		SIMPLE_CATEGORICAL_2BIT("SimpleCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				() -> identityMapping(4),
				ObjectReadableTests::categorical2BitColumn),
		SIMPLE_CATEGORICAL_4BIT("SimpleCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				() -> identityMapping(16),
				ObjectReadableTests::categorical4BitColumn),
		SIMPLE_CATEGORICAL_8BIT("SimpleCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				() -> identityMapping(256),
				ObjectReadableTests::denseCategorical8BitColumn),
		SIMPLE_CATEGORICAL_16BIT("SimpleCategorical16Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::denseCategorical16BitColumn),
		SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::denseCategorical32BitColumn),
		SIMPLE_CATEGORICAL_SPARSE_8BIT("SimpleCategoricalSparse8Bit",
				(seed, n) -> sparseTimeRandomIntegers(seed, 256, n, 0.9d, false),
				() -> identityMapping(256),
				ObjectReadableTests::sparseCategorical8BitColumn),
		SIMPLE_CATEGORICAL_SPARSE_16BIT("SimpleCategoricalSparse16Bit",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING,  n, 0.8, false),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::sparseCategorical16BitColumn),
		SIMPLE_CATEGORICAL_SPARSE_32BIT("SimpleCategoricalSparse32Bit",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75, false),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::sparseCategorical32BitColumn),
		SIMPLE_CATEGORICAL_SPARSE_8BIT_DEFAULT_IS_MISSING("SimpleCategoricalSparse8BitDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, 256, n, 0.9d, true),
				() -> identityMapping(256),
				ObjectReadableTests::sparseCategorical8BitColumn),
		SIMPLE_CATEGORICAL_SPARSE_16BIT_DEFAULT_IS_MISSING("SimpleCategoricalSparse16BitDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING,  n, 0.8, true),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::sparseCategorical16BitColumn),
		SIMPLE_CATEGORICAL_SPARSE_32BIT_DEFAULT_IS_MISSING("SimpleCategoricalSparse32BitDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75, true),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::sparseCategorical32BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_8BIT("RemappedCategoricalSparse8Bit",
				(seed, n) -> sparseTimeRandomIntegers(seed, 256, n, 0.9d, false),
				() -> identityMapping(256),
				ObjectReadableTests::remappedCategoricalSparse8BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_16BIT("RemappedCategoricalSparse16Bit",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING,  n, 0.8, false),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedCategoricalSparse16BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_32BIT("RemappedCategoricalSparse32Bit",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75, false),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedCategoricalSparse32BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_8BIT_DEFAULT_IS_MISSING("RemappedCategoricalSparse8BitDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, 256, n, 0.9d, true),
				() -> identityMapping(256),
				ObjectReadableTests::remappedCategoricalSparse8BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_16BIT_DEFAULT_IS_MISSING("RemappedCategoricalSparse16BitDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING,  n, 0.8, true),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedCategoricalSparse16BitColumn),
		REMAPPED_CATEGORICAL_SPARSE_32BIT_DEFAULT_IS_MISSING("RemappedCategoricalSparse32BitDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75, true),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedCategoricalSparse32BitColumn),
		MAPPED_CATEGORICAL_2BIT("MappedCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				() -> identityMapping(4),
				ObjectReadableTests::mappedCategorical2BitColumn),
		MAPPED_CATEGORICAL_4BIT("MappedCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				() -> identityMapping(16),
				ObjectReadableTests::mappedCategorical4BitColumn),
		MAPPED_CATEGORICAL_8BIT("MappedCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				() -> identityMapping(256),
				ObjectReadableTests::mappedCategorical8BitColumn),
		MAPPED_CATEGORICAL_16BIT("MappedCategorical16Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::mappedCategorical16BitColumn),
		MAPPED_CATEGORICAL_32BIT("MappedCategorical32Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::mappedCategorical32BitColumn),
		REMAPPED_CATEGORICAL_2BIT("RemappedCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				() -> identityMapping(4),
				ObjectReadableTests::remappedCategorical2BitColumn),
		REMAPPED_CATEGORICAL_4BIT("RemappedCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				() -> identityMapping(16),
				ObjectReadableTests::remappedCategorical4BitColumn),
		REMAPPED_CATEGORICAL_8BIT("RemappedCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				() -> identityMapping(256),
				ObjectReadableTests::remappedCategorical8BitColumn),
		REMAPPED_CATEGORICAL_16BIT("RemappedCategorical16Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedCategorical16BitColumn),
		REMAPPED_CATEGORICAL_32BIT("RemappedCategorical32Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedCategorical32BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_2BIT("RemappedMappedCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				() -> identityMapping(4),
				ObjectReadableTests::remappedMappedCategorical2BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_4BIT("RemappedMappedCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				() -> identityMapping(16),
				ObjectReadableTests::remappedMappedCategorical4BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_8BIT("RemappedMappedCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				() -> identityMapping(256),
				ObjectReadableTests::remappedMappedCategorical8BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_16BIT("RemappedMappedCategorical16Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedMappedCategorical16BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_32BIT("RemappedMappedCategorical32Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::remappedMappedCategorical32BitColumn),
		SIMPLE_FREE("SimpleFree",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::simpleFree),
		MAPPED_FREE("MappedFree",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::mappedFree),
		DATE_TIME_LOW_PRECISION("DateTimeLowPrecision",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> lowPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::denseDateTimeLowPrecision),
		DATE_TIME_HIGH_PRECISION("DateTimeHighPrecision",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> highPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::denseDateTimeHighPrecision),
		SPARSE_DATE_TIME_LOW_PRECISION("SparseDateTimeLowPrecision",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, false),
				() -> lowPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::sparseDateTimeLowPrecision),
		SPARSE_DATE_TIME_HIGH_PRECISION("SparseDateTimeHighPrecision",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, false),
				() -> highPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::sparseDateTimeHighPrecision),
		SPARSE_DATE_TIME_LOW_PRECISION_DEFAULT_IS_MISSING("SparseDateTimeLowPrecisionDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, true),
				() -> lowPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::sparseDateTimeLowPrecision),
		SPARSE_DATE_TIME_HIGH_PRECISION_DEFAULT_IS_MISSING("SparseDateTimeHighPrecisionDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, true),
				() -> highPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::sparseDateTimeHighPrecision),
		MAPPED_DATE_TIME_LOW_PRECISION("MappedDateTimeLowPrecision",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> lowPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::mappedDateTimeLowPrecision),
		MAPPED_DATE_TIME_HIGH_PRECISION("MappedDateTimeHighPrecision",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> highPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::mappedDateTimeHighPrecision),
		TIME("Time",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> timeMapping(MAX_MAPPING),
				ObjectReadableTests::denseTime),
		SPARSE_TIME("SparseTime",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, false),
				() -> timeMapping(MAX_MAPPING),
				ObjectReadableTests::sparseTime),
		SPARSE_TIME_DEFAULT_IS_MISSING("SparseTimeDefaultIsMissing",
				(seed, n) -> sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, true),
				() -> timeMapping(MAX_MAPPING),
				ObjectReadableTests::sparseTime),
		MAPPED_TIME("MappedTime",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> timeMapping(MAX_MAPPING),
				ObjectReadableTests::mappedTime);

		private final String name;
		private final ArrayGenerator generator;
		private final MappingGenerator mapping;
		private final ColumnBuilder builder;

		Implementation(String name, ArrayGenerator generator, MappingGenerator mapping, ColumnBuilder builder) {
			this.name = name;
			this.generator = generator;
			this.mapping = mapping;
			this.builder = builder;
		}

		@Override
		public String toString() {
			return name;
		}

		public ArrayGenerator getGenerator() {
			return generator;
		}

		public MappingGenerator getMapping() {
			return mapping;
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
	private interface MappingGenerator {
		List<Object> generate();
	}

	@FunctionalInterface
	private interface ColumnBuilder {
		Column build(int[] indices, List<Object> mapping);
	}

	private static int[] randomIntegers(long seed, int bound, int n) {
		SplittableRandom rng = new SplittableRandom(seed);
		int[] data = new int[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextInt(bound);
		}
		return data;
	}

	private static int[] sparseTimeRandomIntegers(long seed, int bound, int n, double sparsity, boolean defaultIsMissing) {
		SplittableRandom rng = new SplittableRandom(seed);
		int[] data = new int[n];
		// 0 ist the index of the missing value in the time mapping (see timeMapping(int n))
		int defaultValue = defaultIsMissing ? 0 : 1 + rng.nextInt(bound - 1);
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < sparsity ? defaultValue : rng.nextInt(bound);
		}
		return data;
	}

	private static List<Object> identityMapping(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(i);
		}
		return mapping;
	}

	private static List<Object> lowPrecisionDateTimeMapping(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(Instant.ofEpochSecond(29854120540790L + 10_101_000_111L * i));
		}
		return mapping;
	}

	private static List<Object> highPrecisionDateTimeMapping(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(Instant.ofEpochSecond(29854120540790L + 10_101_000_111L * i, 10 * i));
		}
		return mapping;
	}

	private static List<Object> timeMapping(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(LocalTime.ofNanoOfDay(29854120540790L + 10_101_000_111L * i));
		}
		return mapping;
	}

	private final static ColumnType<String> STRING_TYPE = ColumnTestUtils.categoricalType(
			String.class, null);

	private static Column categorical2BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return ColumnAccessor.get().newCategoricalColumn(STRING_TYPE, packed, newMapping);
	}

	private static Column remappedCategorical2BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] remapping = zeroFixPermutation(mapping.size());
		List<String> reMapping = reDictionary(newMapping, remapping);

		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, packed, reMapping, remapping);
	}

	private static List<String> reDictionary(List<String> mapping, int[] remapping) {
		List<String> remappedDictionary = new ArrayList<>(mapping);
		for (int i = 1; i < remapping.length; i++) {
			remappedDictionary.set(remapping[i], mapping.get(i));
		}
		return remappedDictionary;
	}

	private static Column categorical4BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return ColumnAccessor.get().newCategoricalColumn(STRING_TYPE, packed, newMapping);
	}

	private static Column remappedCategorical4BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] remapping = zeroFixPermutation(mapping.size());
		List<String> reMapping = reDictionary(newMapping, remapping);
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, packed, reMapping, remapping);
	}

	private static Column sparseCategorical8BitColumn(int[] data, List<Object> mapping) {
		CategoricalColumn column = categorical8BitColumn(data, mapping);
		assertTrue(ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column denseCategorical8BitColumn(int[] data, List<Object> mapping) {
		CategoricalColumn column = categorical8BitColumn(data, mapping);
		assertFalse(ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column sparseCategorical16BitColumn(int[] data, List<Object> mapping) {
		CategoricalColumn column = categorical16BitColumn(data, mapping);
		assertTrue(ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column denseCategorical16BitColumn(int[] data, List<Object> mapping) {
		CategoricalColumn column = categorical16BitColumn(data, mapping);
		assertFalse(ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column sparseCategorical32BitColumn(int[] data, List<Object> mapping) {
		CategoricalColumn column = categorical32BitColumn(data, mapping);
		assertTrue(ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column denseCategorical32BitColumn(int[] data, List<Object> mapping) {
		CategoricalColumn column = categorical32BitColumn(data, mapping);
		assertFalse(ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column remappedCategoricalSparse16BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int max = -1;
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			long value = Math.round(data[i]);
			max = Math.max(max, (int) value);
			shortData[i] = (short) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalSparseColumn(STRING_TYPE, shortData, newMapping, remapping, ColumnTestUtils.getMostFrequentValue(shortData, (short) 0));
	}

	private static Column remappedCategoricalSparse8BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int max = -1;
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			long value = Math.round(data[i]);
			max = Math.max(max, (int) value);
			byteData[i] = (byte) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedCategoricalSparseColumn(STRING_TYPE, packed, newMapping, remapping, ColumnTestUtils.getMostFrequentValue(byteData, (byte) 0));
	}

	private static Column remappedCategoricalSparse32BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int max = -1;
		for (int i = 0; i < data.length; i++) {
			max = Math.max(max, i);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalSparseColumn(STRING_TYPE, data, newMapping, remapping, ColumnTestUtils.getMostFrequentValue(data, 0));
	}

	private static CategoricalColumn categorical8BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		long seed = 7704491377044913L;
		return ColumnTestUtils.getCategoricalColumn(seed, STRING_TYPE, packed, newMapping);
	}

	private static Column remappedCategorical8BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) data[i];
		}
		int[] remapping = zeroFixPermutation(mapping.size());
		List<String> reMapping = reDictionary(newMapping, remapping);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, packed, reMapping, remapping);
	}

	private static CategoricalColumn categorical16BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) data[i];
		}
		long seed = 7704491377044913L;
		return ColumnTestUtils.getCategoricalColumn(seed, STRING_TYPE, shortData, newMapping);
	}

	private static Column remappedCategorical16BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) data[i];
		}
		int[] remapping = zeroFixPermutation(mapping.size());
		List<String> reMapping = reDictionary(newMapping, remapping);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, shortData, reMapping, remapping);
	}

	private static CategoricalColumn categorical32BitColumn(int[] data, List<Object> mapping) {
		long seed = 7704491377044913L;
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		return ColumnTestUtils.getCategoricalColumn(seed, STRING_TYPE, data, newMapping);
	}

	private static Column remappedCategorical32BitColumn(int[] data, List<Object> mapping) {
		List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] remapping = zeroFixPermutation(mapping.size());
		List<String> reMapping = reDictionary(newMapping, remapping);
		return ColumnTestUtils.getRemappedCategoricalColumn(STRING_TYPE, data, reMapping, remapping);
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

	private static Column mappedCategorical2BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary =
				dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, packed, newDictionary, mapping);
	}

	private static Column remappedMappedCategorical2BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
		}
		int[] remapping = zeroFixPermutation(dictionary.size());
		List<String> reDictionary = reDictionary(newDictionary, remapping);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, packed, reDictionary, remapping,
				mapping);
	}

	private static Column mappedCategorical4BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, packed, newDictionary, mapping);
	}

	private static Column remappedMappedCategorical4BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
		}
		int[] remapping = zeroFixPermutation(dictionary.size());
		List<String> reDictionary = reDictionary(newDictionary, remapping);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, packed, reDictionary, remapping, mapping);
	}

	private static Column mappedCategorical8BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedByteData[mapping[i]] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, packed, newDictionary, mapping);
	}

	private static Column remappedMappedCategorical8BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedByteData[mapping[i]] = (byte) data[i];
		}
		int[] remapping = zeroFixPermutation(dictionary.size());
		List<String> reDictionary = reDictionary(newDictionary, remapping);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, packed, reDictionary, remapping, mapping);
	}

	private static Column mappedCategorical16BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedShortData[mapping[i]] = (short) data[i];
		}
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, mappedShortData, newDictionary, mapping);
	}

	private static Column remappedMappedCategorical16BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedShortData[mapping[i]] = (short) data[i];
		}
		int[] remapping = zeroFixPermutation(dictionary.size());
		List<String> reDictionary = reDictionary(newDictionary, remapping);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, mappedShortData, reDictionary,
				remapping, mapping);
	}

	private static Column mappedCategorical32BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedIntData[mapping[i]] = data[i];
		}
		return ColumnTestUtils.getMappedCategoricalColumn(STRING_TYPE, mappedIntData, newDictionary, mapping);
	}

	private static Column remappedMappedCategorical32BitColumn(int[] data, List<Object> dictionary) {
		List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedIntData[mapping[i]] = data[i];
		}
		int[] remapping = zeroFixPermutation(dictionary.size());
		List<String> reDictionary = reDictionary(newDictionary, remapping);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(STRING_TYPE, mappedIntData, reDictionary,
				remapping, mapping);
	}

	private static Column simpleFree(int[] data, List<Object> categoricalMapping) {
		Object[] objectData = new Object[data.length];
		for (int i = 0; i < data.length; i++) {
			objectData[i] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
		}
		return ColumnAccessor.get().newObjectColumn(ColumnTestUtils.OBJECT_DUMMY_TYPE, objectData);
	}

	private static Column mappedFree(int[] data, List<Object> categoricalMapping) {
		int[] mapping = permutation(data.length);
		Object[] mappedObjectData = new Object[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedObjectData[mapping[i]] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
		}
		return ColumnTestUtils.getMappedObjectColumn(ColumnTestUtils.OBJECT_DUMMY_TYPE,
				mappedObjectData, mapping);
	}

	private static DateTimeColumn dateTimeLowPrecision(int[] data, List<Object> categoricalMapping) {
		long[] seconds = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			Instant instant = (Instant) categoricalMapping.get(data[i]);
			if (instant != null) {
				seconds[i] = instant.getEpochSecond();
			} else {
				seconds[i] = DateTimeColumn.MISSING_VALUE;
			}
		}
		long seed = 556750021571630234L;
		return ColumnTestUtils.getDateTimeColumn(seed, seconds, null);
	}

	private static Column denseDateTimeLowPrecision(int[] data, List<Object> categoricalMapping) {
		DateTimeColumn column = dateTimeLowPrecision(data, categoricalMapping);
		assertFalse("Expected dense column but was sparse!", ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column denseDateTimeHighPrecision(int[] data, List<Object> categoricalMapping) {
		DateTimeColumn column = dateTimeHighPrecision(data, categoricalMapping);
		assertFalse("Expected dense column but was sparse!", ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column sparseDateTimeLowPrecision(int[] data, List<Object> categoricalMapping) {
		DateTimeColumn column = dateTimeLowPrecision(data, categoricalMapping);
		assertTrue(ColumnTestUtils.isSparseLowPrecisionDateTime(column));
		return column;
	}

	private static Column sparseDateTimeHighPrecision(int[] data, List<Object> categoricalMapping) {
		DateTimeColumn column = dateTimeHighPrecision(data, categoricalMapping);
		assertTrue(ColumnTestUtils.isSparseHighPrecisionDateTime(column));
		return column;
	}

	private static DateTimeColumn dateTimeHighPrecision(int[] data, List<Object> categoricalMapping) {
		long[] seconds = new long[data.length];
		int[] nanos = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			Instant instant = (Instant) categoricalMapping.get(data[i]);
			if (instant != null) {
				seconds[i] = instant.getEpochSecond();
				nanos[i] = instant.getNano();
			} else {
				seconds[i] = DateTimeColumn.MISSING_VALUE;
			}
		}
		long seed = 738485315248003629L;
		return ColumnTestUtils.getDateTimeColumn(seed, seconds, nanos);
	}

	private static Column time(int[] data, List<Object> categoricalMapping) {
		long[] nanos = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			LocalTime instant = (LocalTime) categoricalMapping.get(data[i]);
			if (instant != null) {
				nanos[i] = instant.toNanoOfDay();
			} else {
				nanos[i] = TimeColumn.MISSING_VALUE;
			}
		}
		long seed = 452597057738839021L;
		return ColumnTestUtils.getTimeColumn(seed, nanos);
	}

	private static Column denseTime(int[] data, List<Object> categoricalMapping) {
		TimeColumn column = (TimeColumn) time(data, categoricalMapping);
		assertFalse("Expected dense column but was sparse!", ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column sparseTime(int[] data, List<Object> categoricalMapping) {
		TimeColumn column = (TimeColumn) time(data, categoricalMapping);
		assertTrue("Expected sparse column but was dense!", ColumnTestUtils.isSparse(column));
		return column;
	}

	private static Column mappedDateTimeLowPrecision(int[] data, List<Object> categoricalMapping) {
		int[] mapping = permutation(data.length);
		long[] seconds = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			Instant instant = (Instant) categoricalMapping.get(data[i]);
			if (instant != null) {
				seconds[mapping[i]] = instant.getEpochSecond();
			} else {
				seconds[mapping[i]] = DateTimeColumn.MISSING_VALUE;
			}
		}
		return ColumnTestUtils.getMappedDateTimeColumn(seconds, null, mapping);
	}

	private static Column mappedDateTimeHighPrecision(int[] data, List<Object> categoricalMapping) {
		int[] mapping = permutation(data.length);
		long[] seconds = new long[data.length];
		int[] nanos = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			Instant instant = (Instant) categoricalMapping.get(data[i]);
			if (instant != null) {
				seconds[mapping[i]] = instant.getEpochSecond();
				nanos[mapping[i]] = instant.getNano();
			} else {
				seconds[mapping[i]] = DateTimeColumn.MISSING_VALUE;
			}
		}
		return ColumnTestUtils.getMappedDateTimeColumn(seconds, nanos, mapping);
	}

	private static Column mappedTime(int[] data, List<Object> categoricalMapping) {
		int[] mapping = permutation(data.length);
		long[] nanos = new long[data.length];

		for (int i = 0; i < data.length; i++) {
			LocalTime instant = (LocalTime) categoricalMapping.get(data[i]);
			if (instant != null) {
				nanos[mapping[i]] = instant.toNanoOfDay();
			} else {
				nanos[mapping[i]] = TimeColumn.MISSING_VALUE;
			}
		}
		return ColumnTestUtils.getMappedTimeColumn(nanos, mapping);
	}

	@RunWith(Parameterized.class)
	public static class ContinuousFill {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		private Object[] fill(int[] indices, List<Object> mapping) {
			Object[] data = new Object[indices.length];
			for (int i = 0; i < indices.length; i++) {
				data[i] = mapping.get(indices[i]);
			}
			return data;
		}

		@Test
		public void testContinuous() {
			int nValues = 4504;
			int end = 1214;

			int[] indices = impl.getGenerator().generate(75699122L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(fill(indices, mapping), 0, end);

			Column column = impl.getBuilder().build(indices, mapping);
			Object[] buffer = new Object[end];
			column.fill(buffer, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testPartialContinuous() {
			int nValues = 4504;
			int start = 80;
			int end = nValues + start;

			int[] indices = impl.getGenerator().generate(30093219L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			// ignore the part that is out of bounds because it is undefined (see javadoc of fill)
			Object[] expected = Arrays.copyOfRange(fill(indices, mapping), start, nValues);

			Column column = impl.getBuilder().build(indices, mapping);
			Object[] buffer = new Object[end - start];
			column.fill(buffer, start);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, buffer.length - start));
		}

		@Test
		public void testOutOfBoundsContinuous() {
			int nValues = 4704;
			int start = nValues - 10;
			int end = start + 10;

			int[] indices = impl.getGenerator().generate(48710914L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.stream(Arrays.copyOfRange(indices, start, indices.length))
					.mapToObj(i -> mapping.get(i)).toArray();

			Column column = impl.getBuilder().build(indices, mapping);
			Object[] buffer = new Object[end - start];
			column.fill(buffer, start);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			// The first 10 elements that have been read should be equal to the last 10 elements of data.
			// The rest of the read elements is undefined (see javadoc of fill).
			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testContinuousIdempotence() {
			int nReads = 3;
			int nValues = 5504;
			int start = 83;
			int end = nValues - start;

			int[] indices = impl.getGenerator().generate(96709684L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(fill(indices, mapping), start, end);

			Column column = impl.getBuilder().build(indices, mapping);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			for (int i = 0; i < nReads; i++) {
				Object[] buffer = new Object[end - start];
				column.fill(buffer, start);
				assertArrayEquals(expected, buffer);
			}
		}

		@Test
		public void testContinuousWithEmptyBuffer() {
			int nValues = 3904;
			int[] indices = impl.getGenerator().generate(43953429L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(indices, mapping);
			column.fill(new Object[0], 0);
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

		private Object[] flattenAndFillTable(int[][] table, List<Object> mapping) {
			Object[] flattened = new Object[table.length * table[0].length];
			for (int i = 0; i < table.length; i++) {
				int[] column = table[i];
				for (int j = 0; j < column.length; j++) {
					flattened[i + table.length * j] = mapping.get(column[j]);
				}
			}
			return flattened;
		}

		@Test
		public void testInterleavedOfCompleteRowsFromStart() {
			int nColumns = 4;
			int nRows = 4504;
			int bufferSize = 128;

			int[][] table = generateTable(41376101L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOf(flattenAndFillTable(table, mapping), 128);

			Object[] buffer = new Object[bufferSize];
			Column column = null;
			for (int i = 0; i < nColumns; i++) {
				column = impl.getBuilder().build(table[i], mapping);
				column.fill(buffer, 0, i, nColumns);
			}

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testAllValuesEqual() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 4504;
			int bufferSize = 128;

			int[] columnData = new int[nRows];
			// set all entries to one single value
			Arrays.fill(columnData, impl.getGenerator().generate(41376101L, 1)[0]);
			int[][] table = new int[nColumns][];
			Arrays.fill(table, columnData);
			List<Object> mapping = impl.getMapping().generate();
			Object expected[] = Arrays.copyOf(flattenAndFillTable(table, mapping), bufferSize);

			Object[] buffer = new Object[bufferSize];
			Column column = null;
			try {
				for (int i = 0; i < nColumns; i++) {
					column = impl.getBuilder().build(table[i], mapping);
					column.fill(buffer, 0, i, nColumns);
				}
			} catch (AssertionError e) {
				// The data is sparse in this test regardless of the implementation's generator which will always
				// lead to sparse columns.
				// Therefore, the test would fail for non-sparse columns at this point.
				// We do not care about these cases as we want to test the sparse columns with this test.
				return;
			}

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}
			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testAllValuesInOneColumnEqual() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 4504;
			int bufferSize = 128;

			int[] columnData = new int[nRows];
			// set all entries to one single value for this special column
			Arrays.fill(columnData, impl.getGenerator().generate(41376101L, 1)[0]);
			int[][] table = generateTable(415865227689485L, nColumns, nRows);
			table[nColumns/2] = columnData; // add the special column to the table
			List<Object> mapping = impl.getMapping().generate();
			Object expected[] = Arrays.copyOf(flattenAndFillTable(table, mapping), bufferSize);

			Object[] buffer = new Object[bufferSize];
			Column column = null;
			try {
				for (int i = 0; i < nColumns; i++) {
					column = impl.getBuilder().build(table[i], mapping);
					column.fill(buffer, 0, i, nColumns);
				}
			} catch (AssertionError e) {
				// The data is sparse in this test regardless of the implementation's generator which will always
				// lead to sparse columns.
				// Therefore, the test would fail for non-sparse columns at this point.
				// We do not care about these cases as we want to test the sparse columns with this test.
				return;
			}

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedOfCompleteRows() {
			int nColumns = 4;
			int nRows = 4504;
			int bufferSize = 128;
			int startIndex = 256;

			int[][] table = generateTable(96966414L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(flattenAndFillTable(table, mapping), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			Object[] buffer = new Object[bufferSize];
			Column column = null;
			for (int i = 0; i < nColumns; i++) {
				column = impl.getBuilder().build(table[i], mapping);
				column.fill(buffer, startIndex, i, nColumns);
			}

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedOfIncompleteRowsFromStart() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 4504;
			int bufferSize = 128;

			int[][] table = generateTable(77492141L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object expected[] = Arrays.copyOf(flattenAndFillTable(table, mapping), bufferSize);

			Object[] buffer = new Object[bufferSize];
			Column column = null;
			for (int i = 0; i < nColumns; i++) {
				column = impl.getBuilder().build(table[i], mapping);
				column.fill(buffer, 0, i, nColumns);
			}

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedOfIncompleteRows() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 4504;
			int bufferSize = 128;
			int startIndex = 384;

			int[][] table = generateTable(91169525L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(flattenAndFillTable(table, mapping), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			Object[] buffer = new Object[bufferSize];
			Column column = null;
			for (int i = 0; i < nColumns; i++) {
				column = impl.getBuilder().build(table[i], mapping);
				column.fill(buffer, startIndex, i, nColumns);
			}

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedIdempotence() {
			int nColumns = 4;
			int nRows = 4504;
			int bufferSize = 128;
			int nRuns = 3;

			int[][] table = generateTable(97074107L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOf(flattenAndFillTable(table, mapping), 128);

			//fix for categorical columns that cannot have integer element type any longer
			Column col = impl.getBuilder().build(table[0], mapping);
			if (col.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			Object[] buffer = new Object[bufferSize];
			for (int run = 0; run < nRuns; run++) {
				for (int i = 0; i < nColumns; i++) {
					Column column = impl.getBuilder().build(table[i], mapping);
					column.fill(buffer, 0, i, nColumns);
				}
				assertArrayEquals(expected, buffer);
			}
		}

		@Test
		public void testPartialInterleaved() {
			int nValues = 4504;

			int[][] table = generateTable(68720420L, 1, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] data = flattenAndFillTable(table, mapping);

			Object[] expected = new Object[nValues / 2];
			System.arraycopy(data, nValues / 2, expected, 0, nValues / 2);

			Object[] buffer = new Object[nValues];
			Column column = impl.getBuilder().build(table[0], mapping);
			column.fill(buffer, nValues / 2, 0, 1);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			// The values that are out of range are undefined (see javadoc of fill).
			// Therefore, we only check the values that are in bounds.
			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, nValues / 2));
		}

		@Test
		public void testOutOfBoundsInterleaved() {
			int nValues = 4504;
			int start = nValues - 10;
			int end = nValues + 10;

			int[][] table = generateTable(14208233L, 1, nValues);
			List<Object> mapping = impl.getMapping().generate();

			Object[] expected = Arrays.stream(Arrays.copyOfRange(table[0], start, table[0].length))
					.mapToObj(i -> mapping.get(i)).toArray();

			Column column = impl.getBuilder().build(table[0], mapping);
			Object[] buffer = new Object[end - start];
			column.fill(buffer, start, 0, 1);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			// The values that are out of range are undefined (see javadoc of fill).
			// Therefore, we only check the values that are in bounds.
			assertArrayEquals(expected, Arrays.copyOfRange(buffer, 0, 10));
		}

		@Test
		public void testInterleavedWithEmptyBuffer() {
			int nValues = 4504;
			int[] data = impl.generator.generate(49262564L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(data, mapping);
			column.fill(new Object[0], 0, 0, 1);
		}

	}

	@RunWith(Parameterized.class)
	public static class StripData {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}


		@Test
		public void testStripProperties() {
			int nValues = 4504;
			int[] indices = impl.generator.generate(59795422L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();
			Column column = impl.builder.build(indices, categoricalMapping);

			Column stripped = column.stripData();
			assertEquals(0, stripped.size());
			assertEquals(column.type(), stripped.type());
		}

		@Test
		public void testAfterMap() {
			int nValues = 4504;
			int[] indices = impl.generator.generate(59195422L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();
			Column column = impl.builder.build(indices, categoricalMapping);

			Column stripped = ColumnAccessor.get().map(column, new int[]{5, 3, 17}, true).stripData();
			assertEquals(0, stripped.size());
			assertEquals(column.type(), stripped.type());
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

		private Object[] fill(int[] indices, List<Object> mapping) {
			Object[] data = new Object[indices.length];
			for (int i = 0; i < indices.length; i++) {
				data[i] = mapping.get(indices[i]);
			}
			return data;
		}

		@Test
		public void testConstant() {
			int nValues = 4504;
			int constant = 78;

			int[] indices = impl.generator.generate(59795422L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nValues];
			Arrays.fill(mapping, constant);

			Object[] expected = new Object[nValues];
			Arrays.fill(expected, data[constant]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping,
					false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testIdentity() {
			int nValues = 2948;

			int[] indices = impl.generator.generate(41957001L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(data, i -> Objects.toString(data[i], null));
			}

			assertArrayEquals(data, mapped);
		}

		@Test
		public void testReverse() {
			int nValues = 4504;

			int[] indices = impl.generator.generate(11258146L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);

			Object[] expected = new Object[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testRandom() {
			int nValues = 4504;

			int[] indices = impl.generator.generate(75148415L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = permutation(nValues);
			Arrays.setAll(mapping, i -> nValues - i - 1);

			Object[] expected = new Object[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSubset() {
			int nValues = 7504;
			int nSubsetValues = 25;

			int[] indices = impl.generator.generate(74584365L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSubsetValues];
			Arrays.setAll(mapping, i -> i);

			// The values that are out of bounds are undefined. Therefore we do not care about them.
			Object[] expected = new Object[nSubsetValues];
			System.arraycopy(data, 0, expected, 0, nSubsetValues);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nSubsetValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertEquals(nSubsetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSuperset() {
			int nValues = 4504;
			int nSupersetValues = 10465;

			int[] indices = impl.generator.generate(24186771L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % nValues);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForNegative() {
			int nValues = 4504;
			int nSupersetValues = 10465;

			int[] indices = impl.generator.generate(23690049L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % (nValues + 1) - 1);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> mapping[i] < 0 ? null : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForOutOfBounds() {
			int nValues = 4504;
			int nSupersetValues = 10465;

			int[] indices = impl.generator.generate(66098955L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? null : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForOutOfBoundsInterleaved() {
			int nValues = 4504;
			int nSupersetValues = 10465;

			int[] indices = impl.generator.generate(49302245L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? null : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0, 0, 1);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testEmpty() {
			int nValues = 4504;

			int[] indices = impl.generator.generate(15643113L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			int[] mapping = new int[0];

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			assertEquals(0, mappedColumn.size());

			mappedColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, true);
			assertEquals(0, mappedColumn.size());
		}

		@Test
		public void testChained() {
			int nValues = 3237;

			int[] indices = impl.generator.generate(34995095L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);

			// Apply reverse mapping four times
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);
			Column mappedColumn =
					ColumnAccessor.get().map(ColumnAccessor.get().map(ColumnAccessor.get().map(ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false)
					, mapping, false)
					, mapping, false)
					, mapping, false);

			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (mappedColumn.type().elementType().equals(String.class)) {
				Arrays.setAll(data, i -> Objects.toString(data[i], null));
			}

			assertArrayEquals(data, mapped);
		}

		@Test
		public void testViewHint() {
			int nValues = 2248;

			int[] indices = impl.generator.generate(75975556L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] defaultMapping = new Object[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, true);
			Object[] viewHintMapping = new Object[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testViewHintSuperset() {
			int nValues = 2148;

			int[] indices = impl.generator.generate(55891423L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			int[] mapping = new int[nValues * 2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] defaultMapping = new Object[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, true);
			Object[] viewHintMapping = new Object[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testViewHintSubset() {
			int nValues = 10 * 245;

			int[] indices = impl.generator.generate(27427410L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			int[] mapping = new int[nValues / 10];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, false);
			Object[] defaultMapping = new Object[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(indices, categoricalMapping), mapping, true);
			Object[] viewHintMapping = new Object[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testMappedType() {
			int nValues = 4504;

			int[] indices = impl.generator.generate(29020070L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Column column = impl.builder.build(indices, categoricalMapping);
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
					Implementation.REMAPPED_MAPPED_CATEGORICAL_16BIT, Implementation.REMAPPED_MAPPED_CATEGORICAL_32BIT,
					Implementation.MAPPED_FREE,	Implementation.MAPPED_DATE_TIME_LOW_PRECISION,
					Implementation.MAPPED_DATE_TIME_HIGH_PRECISION,	Implementation.MAPPED_TIME);
		}

		private Object[] fill(int[] indices, List<Object> mapping) {
			Object[] data = new Object[indices.length];
			for (int i = 0; i < indices.length; i++) {
				data[i] = mapping.get(indices[i]);
			}
			return data;
		}

		@Test
		public void testCache() {
			int nValues = 4504;

			int[] indices = impl.generator.generate(62455892L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(indices, categoricalMapping);

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			// Create cache and entry for the above mapping
			Map<int[], int[]> cache = new HashMap<>();
			((CacheMappedColumn) column).map(mapping, false, cache);
			assertEquals(1, cache.size());
			int[] remapping = new ArrayList<>(cache.values()).get(0);

			// Modify cache to test whether it is used (invalidate every fourth value)
			Arrays.setAll(remapping, i -> i % 4 == 0 ? -1 : remapping[i]);

			Object[] expected = fill(indices, categoricalMapping);
			Arrays.setAll(expected, i -> i % 4 == 0 ? null : expected[i]);

			Object[] buffer = new Object[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, false, cache);
			mapped.fill(buffer, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testCachePreferView() {
			int nValues = 4504;

			int[] indices = impl.generator.generate(83991112L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(indices, categoricalMapping);

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			// Create cache and entry for the above mapping
			java.util.Map<int[], int[]> cache = new HashMap<>();
			((CacheMappedColumn) column).map(mapping, true, cache);
			assertEquals(1, cache.size());
			int[] remapping = new ArrayList<>(cache.values()).get(0);

			// Modify cache to test whether it is used (invalidate every fourth value)
			Arrays.setAll(remapping, i -> i % 4 == 0 ? -1 : remapping[i]);

			Object[] expected = fill(indices, categoricalMapping);
			Arrays.setAll(expected, i -> i % 4 == 0 ? null : expected[i]);

			Object[] buffer = new Object[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, true, cache);
			mapped.fill(buffer, 0);

			//fix for categorical columns that cannot have integer element type any longer
			if (column.type().elementType().equals(String.class)) {
				Arrays.setAll(expected, i -> Objects.toString(expected[i], null));
			}

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
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			column.fill((Object[]) null, 0);

		}

		@Test
		public void testEmptyArray() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			column.fill(new Object[0], 0);

		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRow() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			column.fill(new Object[10], -5);

		}

		@Test
		public void testTooBigRow() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, nValues - 1);
			Object actual = array[0];
			actual = actual instanceof String ? Integer.parseInt((String) actual) : actual;
			assertEquals(categoricalMapping.get(data[nValues - 1]), actual);
		}

		@Test
		public void testAlmostTooBigRow() {
			int nValues = 7504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, nValues - 4);

			Object[] all = new Object[nValues];
			column.fill(all, 0);
			Object[] expected = new Object[4];
			System.arraycopy(all, nValues - 4, expected, 0, 4);

			assertArrayEquals(expected, Arrays.copyOfRange(array, 0, 4));
		}

		@Test(expected = NullPointerException.class)
		public void testNullArrayInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			column.fill((Object[]) null, 0, 0, 1);

		}

		@Test
		public void testEmptyArrayInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			column.fill(new Object[0], 0, 0, 1);

		}

		@Test(expected = IllegalArgumentException.class)
		public void testNullStepSizeInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 0, 0, 0);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRowInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, -5, 5, 1);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRowInterleavedStepSize() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, -5, 5, 2);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeOffsetInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 0, -5, 1);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeOffsetInterleavedStepSize() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 0, -5, 2);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeStepSizeInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 10, 0, -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeStepSizeBiggerInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 10, 0, -3);
		}

		@Test
		public void testOffsetInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[100];
			column.fill(array, 0, 50, 1);

			Object[] normal = new Object[128];
			column.fill(normal, 0);
			Object[] expected = new Object[100];
			System.arraycopy(normal, 0, expected, 50, 50);
			assertArrayEquals(expected, array);
		}

		@Test
		public void testOffsetInterleavedStepSize() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[100];
			column.fill(array, 0, 50, 2);
			Object[] normal = new Object[128];
			column.fill(normal, 0);
			Object[] expected = new Object[100];
			Arrays.setAll(expected, i -> i < 50 ? null : (i % 2 == 0 ? normal[(i - 50) / 2] : null));
			assertArrayEquals(expected, array);
		}

		@Test
		public void testOffsetBiggerSizeInterleaved() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 0, 100, 1);
			assertArrayEquals(new Object[10], array);
		}

		@Test
		public void testOffsetBiggerSizeInterleavedStepSize() {
			int nValues = 4504;

			int[] data = impl.generator.generate(82375234L, nValues);
			List<Object> categoricalMapping = impl.mapping.generate();
			Column column = impl.builder.build(data, categoricalMapping);
			Object[] array = new Object[10];
			column.fill(array, 0, 110, 2);
			assertArrayEquals(new Object[10], array);
		}

	}

	@RunWith(Parameterized.class)
	public static class ToString {

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.SIMPLE_CATEGORICAL_32BIT, Implementation.MAPPED_CATEGORICAL_32BIT,
					Implementation.SIMPLE_FREE, Implementation.MAPPED_FREE);
		}

		private Column column(int[] data) {
			List<Object> categoricalMapping = impl.mapping.generate();
			return impl.builder.build(data, categoricalMapping);
		}

		@Test
		public void testSmall() {
			int[] data = {5, 7, 3, 1, 0, 5, 4};
			Column column = column(data);

			String expected = Column.TypeId.TEXT_SET + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testMaxFit() {
			int[] datablock = {5, 7, 3, 1, 0, 5, 4, 11};
			int[] data = new int[32];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			Column column = column(data);

			String expected = Column.TypeId.TEXT_SET + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testBigger() {
			int[] datablock = {5, 7, 3, 1, 0, 5, 4, 11};
			int[] data = new int[33];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			data[32] = 23;
			Column column = column(data);

			String expected = Column.TypeId.TEXT_SET + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, ..., 23)";

			assertEquals(expected, column.toString());
		}

		@Test
		public void testBiggerLastMissing() {
			int[] datablock = {5, 7, 3, 1, 0, 5, 4, 11};
			int[] data = new int[33];
			for (int i = 0; i < 4; i++) {
				System.arraycopy(datablock, 0, data, i * datablock.length, datablock.length);
			}
			data[32] = 0;
			Column column = column(data);

			String expected = Column.TypeId.TEXT_SET + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, ..., ?)";

			assertEquals(expected, column.toString());
		}

	}

}
