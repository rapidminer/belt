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

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import com.rapidminer.belt.util.Order;


/**
 * @author Michael Knopf
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
			mapping.add(Instant.ofEpochSecond(1526287027 + i));
		}
		return mapping;
	}

	private static List<Object> highPrecisionInstants(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(Instant.ofEpochSecond(1526287027 + i, 10 * i));
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
				Comparator.comparing(x -> (Integer) x));

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

		private static final ColumnType<Object> HIDDEN_INT_TYPE = ColumnTypes.categoricalType(
				"com.rapidminer.belt.column.test.objectcolumn", Object.class, INTEGER_COMPARATOR);

		private static Column categorical2BitColumn(long seed, List<Object> mapping, int n) {
			int[] indices = randomIntegers(seed, 4, n);
			byte[] byteData = new byte[n % 4 == 0 ? n / 4 : n / 4 + 1];
			for (int i = 0; i < n; i++) {
				IntegerFormats.writeUInt2(byteData, i, indices[i]);
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT2, n);
			return new SimpleCategoricalColumn<>(HIDDEN_INT_TYPE, data, mapping);
		}

		private static Column categorical4BitColumn(long seed, List<Object> mapping, int n) {
			int[] indices = randomIntegers(seed, 16, n);
			byte[] byteData = new byte[n % 2 == 0 ? n / 2 : n / 2 + 1];
			for (int i = 0; i < n; i++) {
				IntegerFormats.writeUInt4(byteData, i, indices[i]);
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT4, n);
			return new SimpleCategoricalColumn<>(HIDDEN_INT_TYPE, data, mapping);
		}

		private static Column categorical8BitColumn(long seed, List<Object> mapping, int n) {
			int[] indices = randomIntegers(seed, 16, n);
			byte[] byteData = new byte[n];
			for (int i = 0; i < n; i++) {
				byteData[i] = (byte) indices[i];
			}
			PackedIntegers data = new PackedIntegers(byteData, Format.UNSIGNED_INT8, n);
			return new SimpleCategoricalColumn<>(HIDDEN_INT_TYPE, data, mapping);
		}

		private static Column categorical16BitColumn(long seed, List<Object> mapping, int n) {
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			short[] shortData = new short[n];
			for (int i = 0; i < n; i++) {
				shortData[i] = (short) indices[i];
			}
			return new SimpleCategoricalColumn<>(HIDDEN_INT_TYPE, shortData, mapping);
		}

		private static Column categorical32BitColumn(long seed, List<Object> mapping, int n) {
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			return new SimpleCategoricalColumn<>(HIDDEN_INT_TYPE, indices, mapping);
		}

		private static Column mappedCategorical2BitColumn(long seed, List<Object> dictionary, int n) {
			int[] data = randomIntegers(seed, 4, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
			for (int i = 0; i < data.length; i++) {
				IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
			}
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
			return new MappedCategoricalColumn<>(HIDDEN_INT_TYPE, packed, dictionary, mapping);
		}

		private static Column mappedCategorical4BitColumn(long seed, List<Object> dictionary, int n) {
			int[] data = randomIntegers(seed, 16, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
			for (int i = 0; i < data.length; i++) {
				IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
			}
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
			return new MappedCategoricalColumn<>(HIDDEN_INT_TYPE, packed, dictionary, mapping);
		}

		private static Column mappedCategorical8BitColumn(long seed, List<Object> dictionary, int n) {
			int[] data = randomIntegers(seed, 256, n);
			int[] mapping = permutation(data.length);
			byte[] mappedByteData = new byte[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedByteData[mapping[i]] = (byte) data[i];
			}
			PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
			return new MappedCategoricalColumn<>(HIDDEN_INT_TYPE, packed,  dictionary, mapping);
		}

		private static Column mappedCategorical16BitColumn(long seed, List<Object> dictionary, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			short[] mappedShortData = new short[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedShortData[mapping[i]] = (short) data[i];
			}
			return new MappedCategoricalColumn<>(HIDDEN_INT_TYPE, mappedShortData, dictionary, mapping);
		}

		private static Column mappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			int[] mappedIntData = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedIntData[mapping[i]] = data[i];
			}
			return new MappedCategoricalColumn<>(HIDDEN_INT_TYPE, mappedIntData, dictionary, mapping);
		}

		private static Column simpleFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return new SimpleFreeColumn<>(ColumnTypes.categoricalType(
					"com.rapidminer.belt.column.test.objectcolumn", Object.class, INTEGER_COMPARATOR),
					objectData);
		}

		private static Column mappedFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			Object[] mappedObjectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedObjectData[mapping[i]] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return new MappedFreeColumn<>(ColumnTypes.categoricalType(
					"com.rapidminer.belt.column.test.objectcolumn", Object.class, INTEGER_COMPARATOR),
					mappedObjectData, mapping);
		}

		private static Column dateTimeLowPrecision(int[] data, List<Object> categoricalMapping) {
			long[] seconds = new long[data.length];
			for (int i = 0; i < data.length; i++) {
				Instant instant = (Instant) categoricalMapping.get(data[i]);
				if (instant != null) {
					seconds[i] = instant.getEpochSecond();
				} else {
					seconds[i] = DateTimeColumn.MISSING_VALUE;
				}
			}
			return new DateTimeColumn(seconds);
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
			return new DateTimeColumn(seconds);
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
			return new DateTimeColumn(seconds, nanos);
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
			return new TimeColumn(nanos);
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
			return new MappedDateTimeColumn(seconds, mapping);
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
			return new MappedDateTimeColumn(seconds, nanos, mapping);
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
			return new MappedTimeColumn(nanos, mapping);
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
			Column column = impl.getBuilder().build(123, mapping, 32);
			Comparator<Object> comparator = impl.getComparator();

			Object[] expected = fill(column);
			Arrays.sort(expected, comparator);

			Column sortedColumn = column.map(column.sort(Order.ASCENDING), false);
			Object[] sorted = fill(sortedColumn);

			assertArrayEquals(expected, sorted);
		}

		@Test
		public void descending() {
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(123, mapping, 32);
			Comparator<Object> comparator = impl.getComparator();

			Object[] expected = fill(column);
			Arrays.sort(expected, comparator);
			for (int i = 0; i < expected.length / 2; i++) {
				Object tmp = expected[i];
				expected[i] = expected[expected.length - 1 - i];
				expected[expected.length - 1 - i] = tmp;
			}


			Column sortedColumn = column.map(column.sort(Order.DESCENDING), false);
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
			MAPPED_CATEGORICAL_32BIT("MappedCategorical32Bit",
					() -> identityMapping(MAX_MAPPING),
					WithoutComparator::mappedCategorical32BitColumn),
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

		private static final ColumnType<Object> HIDDEN_INT_TYPE = ColumnTypes.categoricalType(
				"com.rapidminer.belt.column.test.objectcolumn", Object.class, null);

		private static Column categorical32BitColumn(long seed, List<Object> mapping, int n) {
			int[] indices = randomIntegers(seed, MAX_MAPPING, n);
			return new SimpleCategoricalColumn<>(HIDDEN_INT_TYPE, indices, mapping);
		}

		private static Column mappedCategorical32BitColumn(long seed, List<Object> dictionary, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			int[] mappedIntData = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedIntData[mapping[i]] = data[i];
			}
			return new MappedCategoricalColumn<>(HIDDEN_INT_TYPE, mappedIntData, dictionary, mapping);
		}

		private static Column simpleFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			Object[] objectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				objectData[i] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return new SimpleFreeColumn<>(ColumnTypes.freeType(
					"com.rapidminer.belt.column.test.objectcolumn", Object.class, null),
					objectData);
		}

		private static Column mappedFree(long seed, List<Object> categoricalMapping, int n) {
			int[] data = randomIntegers(seed, MAX_MAPPING, n);
			int[] mapping = permutation(data.length);
			Object[] mappedObjectData = new Object[data.length];
			for (int i = 0; i < data.length; i++) {
				mappedObjectData[mapping[i]] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
			}
			return new MappedFreeColumn<>(ColumnTypes.freeType(
					"com.rapidminer.belt.column.test.objectcolumn", Object.class, null),
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
			Column column = impl.getBuilder().build(123, mapping, 11);
			column.sort(Order.ASCENDING);
		}
	}

}
