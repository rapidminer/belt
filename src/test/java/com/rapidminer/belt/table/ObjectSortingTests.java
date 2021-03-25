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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.SplittableRandom;
import java.util.function.LongFunction;
import java.util.stream.Collectors;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.DateTimeColumn;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import com.rapidminer.belt.util.Order;


/**
 * @author Michael Knopf, Kevin Majchrzak
 */
@RunWith(Enclosed.class)
public class ObjectSortingTests {

	@FunctionalInterface
	private interface MappingGenerator {
		List<Object> generate();
	}

	@FunctionalInterface
	private interface ColumnBuilder {
		Column build(long seed, List<Object> mapping, int n);
	}

	private static List<Object> identityMapping(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(i);
		}
		return mapping;
	}

	private static List<Object> lowPrecisionInstants(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(Instant.ofEpochSecond(29854120540790L + 10_101_000_111L * i));
		}
		return mapping;
	}

	private static List<Object> highPrecisionInstants(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(Instant.ofEpochSecond(29854120540790L + 10_101_000_111L * i, 10 * i));
		}
		return mapping;
	}

	private static List<Object> times(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(LocalTime.ofNanoOfDay(29854120540790L + 10_101_000_111L * i));
		}
		return mapping;
	}

	private static int[] randomIntegers(long seed, int bound, int n) {
		Random rng = new Random(seed);
		int[] data = new int[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < 0.4 ? 0 : rng.nextInt(bound);
		}
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

	@RunWith(Parameterized.class)
	public static class WithComparator {

		private static int MAX_MAPPING = 512;

		private static final Comparator<Object> INTEGER_COMPARATOR = Comparator.nullsLast(
				Comparator.comparing(x -> Integer.parseInt(Objects.toString(x))));

		private static final Comparator<String> STRING_INTEGER_COMPARATOR = Comparator.nullsLast(
				Comparator.comparing(x -> Integer.parseInt(Objects.toString(x))));

		private static final Comparator<Object> INSTANT_COMPARATOR = Comparator.nullsLast(
				Comparator.comparing(x -> (Instant) x));

		private static final Comparator<Object> TIME_COMPARATOR = Comparator.nullsLast(
				Comparator.comparing(x -> (LocalTime) x));

		enum Implementation {
			SIMPLE_CATEGORICAL_2BIT("SimpleCategorical2Bit",
					() -> identityMapping(4),
					INTEGER_COMPARATOR,
					WithComparator::categorical2BitColumn),
			SIMPLE_CATEGORICAL_4BIT("SimpleCategorical4Bit",
					() -> identityMapping(16),
					INTEGER_COMPARATOR,
					WithComparator::categorical4BitColumn),
			SIMPLE_CATEGORICAL_8BIT("SimpleCategorical8Bit",
					() -> identityMapping(256),
					INTEGER_COMPARATOR,
					WithComparator::categorical8BitColumn),
			SIMPLE_CATEGORICAL_16BIT("SimpleCategorical16Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::categorical16BitColumn),
			SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::categorical32BitColumn),
			SIMPLE_CATEGORICAL_SPARSE_8BIT("SimpleCategoricalSparse8Bit",
					() -> identityMapping(256),
					INTEGER_COMPARATOR,
					WithComparator::categoricalSparse8BitColumn),
			SIMPLE_CATEGORICAL_SPARSE_16BIT("SimpleCategoricalSparse16Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::categoricalSparse16BitColumn),
			SIMPLE_CATEGORICAL_SPARSE_32BIT("SimpleCategoricalSparse32Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::categoricalSparse32BitColumn),
			REMAPPED_CATEGORICAL_SPARSE_8BIT("RemappedCategoricalSparse8Bit",
					() -> identityMapping(256),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategoricalSparse8BitColumn),
			REMAPPED_CATEGORICAL_SPARSE_16BIT("RemappedCategoricalSparse16Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategoricalSparse16BitColumn),
			REMAPPED_CATEGORICAL_SPARSE_32BIT("RemappedCategoricalSparse32Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategoricalSparse32BitColumn),
			MAPPED_CATEGORICAL_2BIT("MappedCategorical2Bit",
					() -> identityMapping(4),
					INTEGER_COMPARATOR,
					WithComparator::mappedCategorical2BitColumn),
			MAPPED_CATEGORICAL_4BIT("MappedCategorical4Bit",
					() -> identityMapping(16),
					INTEGER_COMPARATOR,
					WithComparator::mappedCategorical4BitColumn),
			MAPPED_CATEGORICAL_8BIT("MappedCategorical8Bit",
					() -> identityMapping(256),
					INTEGER_COMPARATOR,
					WithComparator::mappedCategorical8BitColumn),
			MAPPED_CATEGORICAL_16BIT("MappedCategorical16Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::mappedCategorical16BitColumn),
			MAPPED_CATEGORICAL_32BIT("MappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::mappedCategorical32BitColumn),
			REMAPPED_CATEGORICAL_2BIT("RemappedCategorical2Bit",
					() -> identityMapping(4),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategorical2BitColumn),
			REMAPPED_CATEGORICAL_4BIT("RemappedCategorical4Bit",
					() -> identityMapping(16),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategorical4BitColumn),
			REMAPPED_CATEGORICAL_8BIT("RemappedCategorical8Bit",
					() -> identityMapping(256),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategorical8BitColumn),
			REMAPPED_CATEGORICAL_16BIT("RemappedCategorical16Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategorical16BitColumn),
			REMAPPED_CATEGORICAL_32BIT("RemappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::remappedCategorical32BitColumn),
			REMAPPED_MAPPED_CATEGORICAL_2BIT("RemappedMappedCategorical2Bit",
					() -> identityMapping(4),
					INTEGER_COMPARATOR,
					WithComparator::remappedMappedCategorical2BitColumn),
			REMAPPED_MAPPED_CATEGORICAL_4BIT("RemappedMappedCategorical4Bit",
					() -> identityMapping(16),
					INTEGER_COMPARATOR,
					WithComparator::remappedMappedCategorical4BitColumn),
			REMAPPED_MAPPED_CATEGORICAL_8BIT("RemappedMappedCategorical8Bit",
					() -> identityMapping(256),
					INTEGER_COMPARATOR,
					WithComparator::remappedMappedCategorical8BitColumn),
			REMAPPED_MAPPED_CATEGORICAL_16BIT("RemappedMappedCategorical16Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::remappedMappedCategorical16BitColumn),
			REMAPPED_MAPPED_CATEGORICAL_32BIT("RemappedMappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::remappedMappedCategorical32BitColumn),
			SIMPLE_FREE("SimpleFree",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::simpleFree),
			MAPPED_FREE("MappedFree",
					() -> identityMapping(MAX_MAPPING),
					INTEGER_COMPARATOR,
					WithComparator::mappedFree),
			DATE_TIME_LOW_PRECISION("DateTimeLowPrecision",
					() -> lowPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					WithComparator::dateTimeLowPrecision),
			DATE_TIME_HIGH_PRECISION("DateTimeHighPrecision",
					() -> highPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					WithComparator::dateTimeHighPrecision),
			SPARSE_DATE_TIME_LOW_PRECISION("SparseDateTimeLowPrecision",
					() -> lowPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					(seed, mapping, n) -> sparseDateTimeLowPrecision(seed, mapping, n, false)),
			SPARSE_DATE_TIME_HIGH_PRECISION("SparseDateTimeHighPrecision",
					() -> highPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					(seed, mapping, n) -> sparseDateTimeHighPrecision(seed, mapping, n, false)),
			SPARSE_DATE_TIME_LOW_PRECISION_DEFAULT_IS_NAN("SparseDateTimeLowPrecisionDefaultIsNan",
					() -> lowPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					(seed, mapping, n) -> sparseDateTimeLowPrecision(seed, mapping, n, true)),
			SPARSE_DATE_TIME_HIGH_PRECISION_DEFAULT_IS_NAN("SparseDateTimeHighPrecisionDefaultIsNan",
					() -> highPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					(seed, mapping, n) -> sparseDateTimeHighPrecision(seed, mapping, n, true)),
			MAPPED_DATE_TIME_LOW_PRECISION("MappedDateTimeLowPrecision",
					() -> lowPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					WithComparator::mappedDateTimeLowPrecision),
			MAPPED_DATE_TIME_HIGH_PRECISION("MappedDateTimeHighPrecision",
					() -> highPrecisionInstants(MAX_MAPPING),
					INSTANT_COMPARATOR,
					WithComparator::mappedDateTimeHighPrecision),
			TIME("Time",
					() -> times(MAX_MAPPING),
					TIME_COMPARATOR,
					WithComparator::time),
			SPARSE_TIME("SparseTime",
					Collections::emptyList, // mapping is not used
					TIME_COMPARATOR,
					WithComparator::sparseTime),
			SPARSE_TIME_DEFAULT_IS_MISSING("SparseTimeDefaultIsMissing",
					Collections::emptyList, // mapping is not used
					TIME_COMPARATOR,
					WithComparator::sparseTimeDefaultIsMissing),
			MAPPED_TIME("MappedTime",
					() -> times(MAX_MAPPING),
					TIME_COMPARATOR,
					WithComparator::mappedTime);

			private final String name;
			private final MappingGenerator mapping;
			private final Comparator<Object> comparator;
			private final ColumnBuilder builder;

			Implementation(String name, MappingGenerator mapping, Comparator<Object> comparator, ColumnBuilder builder) {
				this.name = name;
				this.mapping = mapping;
				this.comparator = comparator;
				this.builder = builder;
			}

			@Override
			public String toString() {
				return name;
			}

			public MappingGenerator getMapping() {
				return mapping;
			}

			public Comparator<Object> getComparator() {
				return comparator;
			}

			public ColumnBuilder getBuilder() {
				return builder;
			}

		}

		private static final ColumnType<String> HIDDEN_INT_TYPE = ColumnTestUtils.categoricalType(
				String.class, STRING_INTEGER_COMPARATOR);

		private static Column categorical2BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, 4, n);
			byte[] byteData = new byte[n % 4 == 0 ? n / 4 : n / 4 + 1];
			for (int i = 0; i < n; i++) {
				IntegerFormats.writeUInt2(byteData, i, indices[i]);
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT2, n);
			return ColumnAccessor.get().newCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping);
		}

		private static Column remappedCategorical2BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, 4, n);
			byte[] byteData = new byte[n % 4 == 0 ? n / 4 : n / 4 + 1];
			for (int i = 0; i < n; i++) {
				IntegerFormats.writeUInt2(byteData, i, indices[i]);
			}
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT2, n);
			return ColumnTestUtils.getRemappedCategoricalColumn(HIDDEN_INT_TYPE, data, reDictionary, remapping);
		}

		private static List<String> reDictionary(List<String> mapping, int[] remapping) {
			List<String> remappedDictionary = new ArrayList<>(mapping);
			for (int i = 1; i < remapping.length; i++) {
				remappedDictionary.set(remapping[i], mapping.get(i));
			}
			return remappedDictionary;
		}

		static int[] zeroFixPermutation(int n) {
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

		private static Column categorical4BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, 16, n);
			byte[] byteData = new byte[n % 2 == 0 ? n / 2 : n / 2 + 1];
			for (int i = 0; i < n; i++) {
				IntegerFormats.writeUInt4(byteData, i, indices[i]);
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT4, n);
			return ColumnAccessor.get().newCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping);
		}

		private static Column remappedCategorical4BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, 16, n);
			byte[] byteData = new byte[n % 2 == 0 ? n / 2 : n / 2 + 1];
			for (int i = 0; i < n; i++) {
				IntegerFormats.writeUInt4(byteData, i, indices[i]);
			}
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT4, n);
			return ColumnTestUtils.getRemappedCategoricalColumn(HIDDEN_INT_TYPE, data, reDictionary, remapping);
		}

		private static Column categorical8BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, 16, n);
			byte[] byteData = new byte[n];
			for (int i = 0; i < n; i++) {
				byteData[i] = (byte) indices[i];
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT8, n);
			return ColumnTestUtils.getSimpleCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping);
		}

		private static Column categoricalSparse8BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, 16, n, random.nextInt(16), 0.9);
			byte[] byteData = new byte[n];
			for (int i = 0; i < n; i++) {
				byteData[i] = (byte) indices[i];
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT8, n);
			return ColumnTestUtils.getSparseCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping, ColumnTestUtils.getMostFrequentValue(byteData, (byte) 0));
		}

		private static Column categoricalSparse16BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, MAX_MAPPING, n, random.nextInt(MAX_MAPPING), 0.9);
			short[] data = new short[n];
			for (int i = 0; i < n; i++) {
				data[i] = (short) indices[i];
			}
			return ColumnTestUtils.getSparseCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping, ColumnTestUtils.getMostFrequentValue(data, (short) 0));
		}

		private static Column categoricalSparse32BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] data = ColumnTestUtils.sparseRandomInts(random, MAX_MAPPING, n, random.nextInt(MAX_MAPPING), 0.9);
			return ColumnTestUtils.getSparseCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping, ColumnTestUtils.getMostFrequentValue(data, 0));
		}

		private static Column remappedCategorical8BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, 16, n);
			byte[] byteData = new byte[n];
			for (int i = 0; i < n; i++) {
				byteData[i] = (byte) indices[i];
			}
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT8, n);
			return ColumnTestUtils.getRemappedCategoricalColumn(HIDDEN_INT_TYPE, data, reDictionary, remapping);
		}

		private static Column remappedCategoricalSparse8BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, 16, n, random.nextInt(16), 0.9);
			byte[] byteData = new byte[n];
			for (int i = 0; i < n; i++) {
				byteData[i] = (byte) indices[i];
			}
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT8, n);
			return ColumnTestUtils.getRemappedCategoricalSparseColumn(HIDDEN_INT_TYPE, data, reDictionary, remapping,
					ColumnTestUtils.getMostFrequentValue(byteData, (byte)0));
		}

		private static Column categorical16BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			short[] shortData = new short[n];
			for (int i = 0; i < n; i++) {
				shortData[i] = (short) indices[i];
			}
			return ColumnTestUtils.getSimpleCategoricalColumn(HIDDEN_INT_TYPE, shortData, newMapping);
		}

		private static Column remappedCategorical16BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			short[] shortData = new short[n];
			for (int i = 0; i < n; i++) {
				shortData[i] = (short) indices[i];
			}
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			return ColumnTestUtils.getRemappedCategoricalColumn(HIDDEN_INT_TYPE, shortData, reDictionary,
					remapping);
		}

		private static Column remappedCategoricalSparse16BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, MAX_MAPPING, n, random.nextInt(MAX_MAPPING), 0.9);
			short[] shortData = new short[n];
			for (int i = 0; i < n; i++) {
				shortData[i] = (short) indices[i];
			}
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			return ColumnTestUtils.getRemappedCategoricalSparseColumn(HIDDEN_INT_TYPE, shortData, reDictionary,
					remapping, ColumnTestUtils.getMostFrequentValue(shortData, (short)0));
		}

		private static Column categorical32BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			return ColumnTestUtils.getSimpleCategoricalColumn(HIDDEN_INT_TYPE, indices, newMapping);
		}

		private static Column remappedCategorical32BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			return ColumnTestUtils.getRemappedCategoricalColumn(HIDDEN_INT_TYPE, indices, reDictionary,
					remapping);
		}

		private static Column remappedCategoricalSparse32BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, MAX_MAPPING, n, random.nextInt(MAX_MAPPING), 0.9);
			int[] remapping = zeroFixPermutation(mapping.size());
			List<String> reDictionary = reDictionary(newMapping, remapping);
			return ColumnTestUtils.getRemappedCategoricalSparseColumn(HIDDEN_INT_TYPE, indices, reDictionary,
					remapping, ColumnTestUtils.getMostFrequentValue(indices, 0));
		}

		private static Column mappedCategorical2BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, 4, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
			for (int i = 0; i < data.length; i++) {
				IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
			}
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
			return ColumnTestUtils.getMappedCategoricalColumn(HIDDEN_INT_TYPE, packed, newDictionary, mapping);
		}

		private static Column remappedMappedCategorical2BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, 4, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
			for (int i = 0; i < data.length; i++) {
				IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
			}
			int[] remapping = zeroFixPermutation(dictionary.size());
			List<String> reDictionary = reDictionary(newDictionary, remapping);
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
			return ColumnTestUtils.getRemappedMappedCategoricalColumn(HIDDEN_INT_TYPE, packed, reDictionary,
					remapping, mapping);
		}

		private static Column mappedCategorical4BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, 16, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
			for (int i = 0; i < data.length; i++) {
				IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
			}
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
			return ColumnTestUtils.getMappedCategoricalColumn(HIDDEN_INT_TYPE, packed, newDictionary, mapping);
		}

		private static Column remappedMappedCategorical4BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, 16, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
			for (int i = 0; i < data.length; i++) {
				IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
			}
			int[] remapping = zeroFixPermutation(dictionary.size());
			List<String> reDictionary = reDictionary(newDictionary, remapping);
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
			return ColumnTestUtils.getRemappedMappedCategoricalColumn(HIDDEN_INT_TYPE, packed, reDictionary,
					remapping, mapping);
		}

		private static Column mappedCategorical8BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, 256, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedByteData[mapping[i]] = (byte) data[i];
			}
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
			return ColumnTestUtils.getMappedCategoricalColumn(HIDDEN_INT_TYPE, packed, newDictionary, mapping);
		}

		private static Column remappedMappedCategorical8BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, 256, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedByteData[mapping[i]] = (byte) data[i];
			}
			int[] remapping = zeroFixPermutation(dictionary.size());
			List<String> reDictionary = reDictionary(newDictionary, remapping);
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
			return ColumnTestUtils.getRemappedMappedCategoricalColumn(HIDDEN_INT_TYPE, packed, reDictionary,
					remapping, mapping);
		}

		private static Column mappedCategorical16BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			short[] mappedShortData = new short[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedShortData[mapping[i]] = (short) data[i];
			}
			return ColumnTestUtils.getMappedCategoricalColumn(HIDDEN_INT_TYPE, mappedShortData, newDictionary,
					mapping);
		}

		private static Column remappedMappedCategorical16BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			short[] mappedShortData = new short[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedShortData[mapping[i]] = (short) data[i];
			}
			int[] remapping = zeroFixPermutation(dictionary.size());
			List<String> reDictionary = reDictionary(newDictionary, remapping);
			return ColumnTestUtils.getRemappedMappedCategoricalColumn(HIDDEN_INT_TYPE, mappedShortData,
					reDictionary, remapping,
					mapping);
		}

		private static Column mappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			int[] mappedIntData = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedIntData[mapping[i]] = data[i];
			}
			return ColumnTestUtils.getMappedCategoricalColumn(HIDDEN_INT_TYPE, mappedIntData, newDictionary, mapping);
		}

		private static Column remappedMappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			int[] mappedIntData = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedIntData[mapping[i]] = data[i];
			}
			int[] remapping = zeroFixPermutation(dictionary.size());
			List<String> reDictionary = reDictionary(newDictionary, remapping);
			return ColumnTestUtils.getRemappedMappedCategoricalColumn(HIDDEN_INT_TYPE, mappedIntData,
					reDictionary, remapping,
					mapping);
		}

		private static Column simpleFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return ColumnAccessor.get().newObjectColumn(ColumnTestUtils.categoricalType(
					Object.class, INTEGER_COMPARATOR),
					objectData);
		}

		private static Column mappedFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			Object[] mappedObjectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedObjectData[mapping[i]] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return ColumnTestUtils.getMappedObjectColumn(ColumnTestUtils.categoricalType(
					Object.class, INTEGER_COMPARATOR),
					mappedObjectData, mapping);
		}

		private static Column dateTimeLowPrecision(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			long[] seconds = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				Instant instant = (Instant) categoricalMapping.get(data[i]);
				if (instant != null) {
					seconds[i] = instant.getEpochSecond();
				} else {
					seconds[i] = DateTimeColumn.MISSING_VALUE;
				}
			}
			DateTimeColumn column = ColumnTestUtils.getDateTimeColumn(seed, seconds, null);
			assertFalse(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column sparseDateTimeLowPrecision(long seed, List<Object> categoricalMapping, int n, boolean defaultIsMissing) {
			int[] data = sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, defaultIsMissing);
			long[] seconds = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				Instant instant = (Instant) categoricalMapping.get(data[i]);
				if (instant != null) {
					seconds[i] = instant.getEpochSecond();
				} else {
					seconds[i] = DateTimeColumn.MISSING_VALUE;
				}
			}
			DateTimeColumn column = ColumnTestUtils.getDateTimeColumn(seed, seconds, null);
			assertTrue(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column sparseDateTimeHighPrecision(long seed, List<Object> categoricalMapping, int n, boolean defaultIsMissing) {
			int[] data = sparseTimeRandomIntegers(seed, MAX_MAPPING, n, 0.75d, defaultIsMissing);
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
			DateTimeColumn column = ColumnTestUtils.getDateTimeColumn(seed, seconds, nanos);
			assertTrue(ColumnTestUtils.isSparse(column));
			return column;
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

		private static Column dateTimeHighPrecision(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
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
			DateTimeColumn column = ColumnTestUtils.getDateTimeColumn(seed, seconds, nanos);
			assertFalse(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column time(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			long[] nanos = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				LocalTime instant = (LocalTime) categoricalMapping.get(data[i]);
				if (instant != null) {
					nanos[i] = instant.toNanoOfDay();
				} else {
					nanos[i] = TimeColumn.MISSING_VALUE;
				}
			}
			TimeColumn column = ColumnTestUtils.getTimeColumn(seed, nanos);
			assertFalse(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column sparseTime(long seed, List<Object> categoricalMapping, int n) {
			SplittableRandom random = new SplittableRandom(seed);
			long defaultValue = random.nextLong(LocalTime.MAX.toNanoOfDay());
			long[] data = ColumnTestUtils.sparseRandomLongs(random, LocalTime.MAX.toNanoOfDay(), n, defaultValue, 0.75d);
			TimeColumn column = ColumnTestUtils.getTimeColumn(seed, data);
			assertTrue(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column sparseTimeDefaultIsMissing(long seed, List<Object> categoricalMapping, int n) {
			SplittableRandom random = new SplittableRandom(seed);
			long defaultValue = TimeColumn.MISSING_VALUE;
			long[] data = ColumnTestUtils.sparseRandomLongs(random, LocalTime.MAX.toNanoOfDay(), n, defaultValue, 0.75d);
			TimeColumn column = ColumnTestUtils.getTimeColumn(seed, data);
			assertTrue(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column mappedDateTimeLowPrecision(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
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

		private static Column mappedDateTimeHighPrecision(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
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

		private static Column mappedTime(long seed, List<Object> categoricalMapping, int n) {
			Random random = new Random(seed);
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			long[] nanos = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				LocalTime instant = (LocalTime) categoricalMapping.get(data[i]);
				if (instant != null) {
					nanos[mapping[i]] = instant.toNanoOfDay();
				} else {
					double threshold = random.nextDouble();
					if (threshold < 0.4) {
						mapping[i] = -1;
					} else if (threshold > 0.6) {
						mapping[i] = nanos.length + 1;
					} else {
						nanos[mapping[i]] = TimeColumn.MISSING_VALUE;
					}
				}
			}
			return ColumnTestUtils.getMappedTimeColumn(nanos, mapping);
		}

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		private Object[] fill(Column column) {
			Object[] data = new Object[column.size()];
			column.fill(data, 0);
			return data;
		}

		@Test
		public void ascending() {
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(123, mapping, 16204);
			Comparator<Object> comparator = impl.getComparator();

			Object[] expected = fill(column);
			Arrays.sort(expected, comparator);

			Column sortedColumn = ColumnAccessor.get().map(column,column.sort(Order.ASCENDING), false);
			Object[] sorted = fill(sortedColumn);

			assertArrayEquals(expected, sorted);
		}

		@Test
		public void descending() {
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(123, mapping, 16204);
			Comparator<Object> comparator = impl.getComparator();

			Object[] expected = fill(column);
			Arrays.sort(expected, comparator);
			for (int i = 0; i < expected.length / 2; i++) {
				Object tmp = expected[i];
				expected[i] = expected[expected.length - 1 - i];
				expected[expected.length - 1 - i] = tmp;
			}

			Column sortedColumn = ColumnAccessor.get().map(column,column.sort(Order.DESCENDING), false);
			Object[] sorted = fill(sortedColumn);

			assertArrayEquals(expected, sorted);
		}
	}

	@RunWith(Parameterized.class)
	public static class WithoutComparator {
		private static int MAX_MAPPING = 512;

		enum Implementation {
			SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::categorical32BitColumn),
			SPARSE_CATEGORICAL_32BIT("SparseCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::categoricalSparse32BitColumn),
			SPARSE_CATEGORICAL_16BIT("SparseCategorical16Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::categoricalSparse16BitColumn),
			SPARSE_CATEGORICAL_8BIT("SparseCategorical8Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::categoricalSparse8BitColumn),
			MAPPED_CATEGORICAL_32BIT("MappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::mappedCategorical32BitColumn),
			REMAPPED_CATEGORICAL_32BIT("RemappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::remappedCategorical32BitColumn),
			REMAPPED_MAPPED_CATEGORICAL_32BIT("RemappedMappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::remappedMappedCategorical32BitColumn),
			SIMPLE_FREE("SimpleFree",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::simpleFree),
			MAPPED_FREE("MappedFree",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::mappedFree);

			private final String name;
			private final MappingGenerator mapping;
			private final ColumnBuilder builder;

			Implementation(String name, MappingGenerator mapping, ColumnBuilder builder) {
				this.name = name;
				this.mapping = mapping;
				this.builder = builder;
			}

			@Override
			public String toString() {
				return name;
			}

			public MappingGenerator getMapping() {
				return mapping;
			}

			public ColumnBuilder getBuilder() {
				return builder;
			}

		}

		private static final ColumnType<String> HIDDEN_INT_TYPE = ColumnTestUtils.categoricalType(
				String.class, null);

		private static Column categorical32BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			return ColumnAccessor.get().newCategoricalColumn(HIDDEN_INT_TYPE, indices, newMapping);
		}

		private static Column categoricalSparse32BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] data = ColumnTestUtils.sparseRandomInts(random, MAX_MAPPING, n, random.nextInt(MAX_MAPPING), 0.9);
			return ColumnTestUtils.getSparseCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping, ColumnTestUtils.getMostFrequentValue(data, 0));
		}

		private static Column categoricalSparse8BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, 16, n, random.nextInt(16), 0.9);
			byte[] byteData = new byte[n];
			for (int i = 0; i < n; i++) {
				byteData[i] = (byte) indices[i];
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT8, n);
			return ColumnTestUtils.getSparseCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping, ColumnTestUtils.getMostFrequentValue(byteData, (byte) 0));
		}

		private static Column categoricalSparse16BitColumn(long seed, List<Object> mapping, int n) {
			List<String> newMapping = mapping.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			SplittableRandom random = new SplittableRandom(seed);
			int[] indices = ColumnTestUtils.sparseRandomInts(random, MAX_MAPPING, n, random.nextInt(MAX_MAPPING), 0.9);
			short[] data = new short[n];
			for (int i = 0; i < n; i++) {
				data[i] = (short) indices[i];
			}
			return ColumnTestUtils.getSparseCategoricalColumn(HIDDEN_INT_TYPE, data, newMapping, ColumnTestUtils.getMostFrequentValue(data, (short) 0));
		}

		private static Column remappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			int[] remapping = WithComparator.zeroFixPermutation(dictionary.size());
			List<String> reDictionary =  WithComparator.reDictionary(newDictionary, remapping);
			return ColumnTestUtils.getRemappedCategoricalColumn(HIDDEN_INT_TYPE, indices, reDictionary, remapping);
		}

		private static Column mappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			int[] mappedIntData = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedIntData[mapping[i]] = data[i];
			}
			return ColumnTestUtils.getMappedCategoricalColumn(HIDDEN_INT_TYPE, mappedIntData, newDictionary, mapping);
		}

		private static Column remappedMappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			List<String> newDictionary = dictionary.stream().map(o -> Objects.toString(o, null)).collect(Collectors.toList());
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			int[] mappedIntData = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedIntData[mapping[i]] = data[i];
			}
			int[] remapping = WithComparator.zeroFixPermutation(dictionary.size());
			List<String> reDictionary =  WithComparator.reDictionary(newDictionary, remapping);
			return ColumnTestUtils.getRemappedMappedCategoricalColumn(HIDDEN_INT_TYPE, mappedIntData,
					reDictionary, remapping, mapping);
		}

		private static Column simpleFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return ColumnAccessor.get().newObjectColumn(ColumnTestUtils.OBJECT_DUMMY_TYPE, objectData);
		}

		private static Column mappedFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			Object[] mappedObjectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedObjectData[mapping[i]] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return ColumnTestUtils.getMappedObjectColumn(ColumnTestUtils.OBJECT_DUMMY_TYPE,
					mappedObjectData, mapping);
		}

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}


		@Test(expected = UnsupportedOperationException.class)
		public void testUnsupported() {
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(123, mapping, 16204);
			column.sort(Order.ASCENDING);
		}
	}

	@RunWith(Parameterized.class)
	public static class TimeTests {

		private static final Comparator<Object> TIME_COMPARATOR = Comparator.nullsLast(
				Comparator.comparing(x -> (LocalTime) x));

		@FunctionalInterface
		private interface LongColumnBuilder {
			Column build(long[] data);
		}

		enum Implementation {
			TIME("Time",
					LocalTime::ofNanoOfDay,
					TIME_COMPARATOR,
					TimeTests::time,
					LocalTime.MIN.toNanoOfDay(),
					LocalTime.MAX.toNanoOfDay()),
			SPARSE_TIME("SparseTime",
					LocalTime::ofNanoOfDay,
					TIME_COMPARATOR,
					TimeTests::sparseTime,
					LocalTime.MIN.toNanoOfDay(),
					LocalTime.MAX.toNanoOfDay());

			private final String name;
			private final LongFunction map;
			private final LongColumnBuilder builder;
			private final Comparator<Object> comparator;
			private final long maxValue;
			private final long minValue;

			Implementation(String name, LongFunction map, Comparator<Object> comparator, LongColumnBuilder builder, long minValue, long maxValue) {
				this.name = name;
				this.map = map;
				this.builder = builder;
				this.comparator = comparator;
				this.minValue = minValue;
				this.maxValue = maxValue;
			}

			@Override
			public String toString() {
				return name;
			}

			public LongFunction getMap() {
				return map;
			}

			public LongColumnBuilder getBuilder() {
				return builder;
			}

			public Comparator<Object> getComparator() {
				return comparator;
			}

		}

		private static Column sparseTime(long[] data) {
			TimeColumn column = ColumnTestUtils.getSparseTimeColumn(data);
			assertTrue(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static Column time(long[] data) {
			TimeColumn column = ColumnTestUtils.getDenseTimeColumn(data);
			assertFalse(ColumnTestUtils.isSparse(column));
			return column;
		}

		private static long[] sparseRandomLongs(long minValue, long maxValue, int n, double sparsity, long defaultValue) {
			return Arrays.stream(ColumnTestUtils.sparseRandomLongs(new SplittableRandom(),
					maxValue - minValue, n, defaultValue, sparsity)).map(x -> x != defaultValue ? x + minValue : x).toArray();
		}

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test
		public void testAllValuesEqual() {
			long[] data = new long[4563];
			SplittableRandom random = new SplittableRandom();
			Arrays.fill(data, impl.minValue + random.nextLong(impl.maxValue - impl.minValue));
			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testSparseAscending() {
			int nValues = 4342;
			long defaultValue = impl.minValue + new SplittableRandom().nextLong(impl.maxValue - impl.minValue);
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, defaultValue);

			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMinValueAscending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.minValue);

			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMaxValueAscending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.maxValue);

			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testSparseDescending() {
			int nValues = 4342;
			long defaultValue = impl.minValue + new SplittableRandom().nextLong(impl.maxValue - impl.minValue);
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, defaultValue);

			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator().reversed());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.DESCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMinValueDescending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.minValue);

			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator().reversed());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.DESCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMaxValueDescending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.maxValue);

			Object[] objectData = Arrays.stream(data).mapToObj(x -> impl.getMap().apply(x)).toArray();

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator().reversed());

			Column column = impl.getBuilder().build(data);
			int[] order = column.sort(Order.DESCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}
	}

	@RunWith(Parameterized.class)
	public static class DateTimeTests {

		private static final Comparator<Object> INSTANT_COMPARATOR = Comparator.nullsLast(
				Comparator.comparing(x -> (Instant) x));

		@FunctionalInterface
		private interface LongIntColumnBuilder {
			Column build(long[] dataOne, int[] dataTwo);
		}

		@FunctionalInterface
		private interface LongIntFunction {
			Object build(long valueOne, int valueTwo);
		}

		enum Implementation {
			DATE_TIME_HIGH_PREC("DateTimeHighPrec",
					Instant::ofEpochSecond,
					INSTANT_COMPARATOR,
					ColumnTestUtils::getDenseHighPrecDateTimeColumn,
					Instant.MIN.getEpochSecond(),
					Instant.MAX.getEpochSecond()),
			DATE_TIME_LOW_PREC("DateTimeLowPrec",
					(a,b) -> Instant.ofEpochSecond(a),
					INSTANT_COMPARATOR,
					(a, b) -> ColumnTestUtils.getDenseLowPrecDateTimeColumn(a),
					Instant.MIN.getEpochSecond(),
					Instant.MAX.getEpochSecond()),
			SPARSE_DATE_TIME_LOW_PREC("SparseDateTimeLowPrec",
					(a,b) -> Instant.ofEpochSecond(a),
					INSTANT_COMPARATOR,
					(a, b) -> ColumnTestUtils.getSparseLowPrecDateTimeColumn(a),
					Instant.MIN.getEpochSecond(),
					Instant.MAX.getEpochSecond()),
			SPARSE_DATE_TIME_HIGH_PREC("SparseDateTimeHighPrec",
					Instant::ofEpochSecond,
					INSTANT_COMPARATOR,
					ColumnTestUtils::getSparseHighPrecDateTimeColumn,
					Instant.MIN.getEpochSecond(),
					Instant.MAX.getEpochSecond());

			private final String name;
			private final LongIntFunction map;
			private final LongIntColumnBuilder builder;
			private final Comparator<Object> comparator;
			private final long maxValue;
			private final long minValue;

			Implementation(String name, LongIntFunction map, Comparator<Object> comparator, LongIntColumnBuilder builder, long minValue, long maxValue) {
				this.name = name;
				this.map = map;
				this.builder = builder;
				this.comparator = comparator;
				this.minValue = minValue;
				this.maxValue = maxValue;
			}

			@Override
			public String toString() {
				return name;
			}

			public LongIntFunction getMap() {
				return map;
			}

			public LongIntColumnBuilder getBuilder() {
				return builder;
			}

			public Comparator<Object> getComparator() {
				return comparator;
			}

		}

		private static long[] sparseRandomLongs(long minValue, long maxValue, int n, double sparsity, long defaultValue) {
			return Arrays.stream(ColumnTestUtils.sparseRandomLongs(new SplittableRandom(),
					maxValue - minValue, n, defaultValue, sparsity)).map(x -> x != defaultValue ? x + minValue : x).toArray();
		}

		@Parameterized.Parameter
		public Implementation impl;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test
		public void testAllValuesEqual() {
			long[] data = new long[4563];
			SplittableRandom random = new SplittableRandom();
			Arrays.fill(data, impl.minValue + random.nextLong(impl.maxValue - impl.minValue));
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testSparseAscending() {
			int nValues = 4342;
			long defaultValue = impl.minValue + new SplittableRandom().nextLong(impl.maxValue - impl.minValue);
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, defaultValue);
			SplittableRandom random = new SplittableRandom();
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMinValueAscending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.minValue);

			SplittableRandom random = new SplittableRandom();
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMaxValueAscending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.maxValue);

			SplittableRandom random = new SplittableRandom();
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.ASCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testSparseDescending() {
			int nValues = 4342;
			long defaultValue = impl.minValue + new SplittableRandom().nextLong(impl.maxValue - impl.minValue);
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, defaultValue);

			SplittableRandom random = new SplittableRandom();
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator().reversed());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.DESCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMinValueDescending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.minValue);

			SplittableRandom random = new SplittableRandom();
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator().reversed());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.DESCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}

		@Test
		public void testDefaultIsMaxValueDescending() {
			int nValues = 4342;
			long[] data = sparseRandomLongs(impl.minValue, impl.maxValue, nValues, 0.75, impl.maxValue);

			SplittableRandom random = new SplittableRandom();
			int[] dataTwo = new int[data.length];
			Arrays.setAll(dataTwo, i -> random.nextInt(1_000_000));
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = impl.getMap().build(data[i], dataTwo[i]);
			}

			Object[] jdkSorting = Arrays.copyOf(objectData, objectData.length);
			Arrays.sort(jdkSorting, impl.getComparator().reversed());

			Column column = impl.getBuilder().build(data, dataTwo);
			int[] order = column.sort(Order.DESCENDING);
			Column sortedColumn = column.rows(order, false);
			Object[] customSorting = new Object[column.size()];
			sortedColumn.fill(customSorting, 0);

			assertArrayEquals(jdkSorting, customSorting);
		}
	}



}
