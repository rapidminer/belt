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

import static com.rapidminer.belt.util.IntegerFormats.Format;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Tests functionality of columns with capability {@link Column.Capability#OBJECT_READABLE}. In particular, the
 * corresponding methods to fill the buffers of {@link ObjectColumnReader}s.
 *
 * @author Michael Knopf, Gisa Meier
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
				ObjectReadableTests::categorical8BitColumn),
		SIMPLE_CATEGORICAL_16BIT("SimpleCategorical16Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::categorical16BitColumn),
		SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> identityMapping(MAX_MAPPING),
				ObjectReadableTests::categorical32BitColumn),
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
				ObjectReadableTests::dateTimeLowPrecision),
		DATE_TIME_HIGH_PRECISION("DateTimeHighPrecision",
				(seed, n) -> randomIntegers(seed, MAX_MAPPING, n),
				() -> highPrecisionDateTimeMapping(MAX_MAPPING),
				ObjectReadableTests::dateTimeHighPrecision),
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
				ObjectReadableTests::time),
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
		Random rng = new Random(seed);
		int[] data = new int[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextInt(bound);
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
			mapping.add(Instant.ofEpochSecond(1526287027 + i));
		}
		return mapping;
	}

	private static List<Object> highPrecisionDateTimeMapping(int n) {
		List<Object> mapping = new ArrayList<>(n);
		mapping.add(null);
		for (int i = 1; i < n; i++) {
			mapping.add(Instant.ofEpochSecond(1526287027 + i, 10 * i));
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

	private final static ColumnType<Object> OBJECT_TYPE = ColumnTypes.categoricalType(
			"com.rapidminer.belt.column.test.objectcolumn", Object.class, null);

	private static Column categorical2BitColumn(int[] data, List<Object> mapping) {
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return new SimpleCategoricalColumn<>(OBJECT_TYPE, packed, mapping);
	}

	private static Column categorical4BitColumn(int[] data, List<Object> mapping) {
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return new SimpleCategoricalColumn<>(OBJECT_TYPE, packed, mapping);
	}

	private static Column categorical8BitColumn(int[] data, List<Object> mapping) {
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return new SimpleCategoricalColumn<>(OBJECT_TYPE, packed, mapping);
	}

	private static Column categorical16BitColumn(int[] data, List<Object> mapping) {
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) data[i];
		}
		return new SimpleCategoricalColumn<>(OBJECT_TYPE, shortData, mapping);
	}

	private static Column categorical32BitColumn(int[] data, List<Object> mapping) {
		return new SimpleCategoricalColumn<>(OBJECT_TYPE, data, mapping);
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

	private static Column mappedCategorical2BitColumn(int[] data, List<Object> dictionary) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return new MappedCategoricalColumn<>(OBJECT_TYPE, packed, dictionary, mapping);
	}

	private static Column mappedCategorical4BitColumn(int[] data, List<Object> dictionary) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return new MappedCategoricalColumn<>(OBJECT_TYPE, packed, dictionary, mapping);
	}

	private static Column mappedCategorical8BitColumn(int[] data, List<Object> dictionary) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedByteData[mapping[i]] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return new MappedCategoricalColumn<>(OBJECT_TYPE, packed, dictionary, mapping);
	}

	private static Column mappedCategorical16BitColumn(int[] data, List<Object> dictionary) {
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedShortData[mapping[i]] = (short) data[i];
		}
		return new MappedCategoricalColumn<>(OBJECT_TYPE, mappedShortData, dictionary, mapping);
	}

	private static Column mappedCategorical32BitColumn(int[] data, List<Object> dictionary) {
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedIntData[mapping[i]] = data[i];
		}
		return new MappedCategoricalColumn<>(OBJECT_TYPE, mappedIntData, dictionary, mapping);
	}

	private static Column simpleFree(int[] data, List<Object> categoricalMapping) {
		Object[] objectData = new Object[data.length];
		for (int i = 0; i < data.length; i++) {
			objectData[i] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
		}
		return new SimpleFreeColumn<>(ColumnTypes.categoricalType("com.rapidminer.belt.column.test.objectcolumn",
				Object.class, null), objectData);
	}

	private static Column mappedFree(int[] data, List<Object> categoricalMapping) {
		int[] mapping = permutation(data.length);
		Object[] mappedObjectData = new Object[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedObjectData[mapping[i]] = data[i] == 0 ? null : categoricalMapping.get(data[i]);
		}
		return new MappedFreeColumn<>(ColumnTypes.freeType(
				"com.rapidminer.belt.column.test.objectcolumn", Object.class, null),
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

	private static Column dateTimeHighPrecision(int[] data, List<Object> categoricalMapping) {
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
		return new TimeColumn(nanos);
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
		return new MappedDateTimeColumn(seconds, mapping);
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
		return new MappedDateTimeColumn(seconds, nanos, mapping);
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
		return new MappedTimeColumn(nanos, mapping);
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
			int nValues = 1605;
			int end = 1214;

			int[] indices = impl.getGenerator().generate(75699122L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(fill(indices, mapping), 0, end);

			Column column = impl.getBuilder().build(indices, mapping);
			Object[] buffer = new Object[end];
			column.fill(buffer, 0);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testPartialContinuous() {
			int nValues = 1605;
			int start = 80;
			int end = nValues + start;

			int[] indices = impl.getGenerator().generate(30093219L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(fill(indices, mapping), start, end);

			Column column = impl.getBuilder().build(indices, mapping);
			Object[] buffer = new Object[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testOutOfBoundsContinuous() {
			int nValues = 1703;
			int start = nValues + 69;
			int end = start + 110;

			int[] indices = impl.getGenerator().generate(48710914L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = new Object[end - start];

			Column column = impl.getBuilder().build(indices, mapping);
			Object[] buffer = new Object[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testContinuousIdempotence() {
			int nReads = 3;
			int nValues = 2043;
			int start = 83;
			int end = nValues - start;

			int[] indices = impl.getGenerator().generate(96709684L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(fill(indices, mapping), start, end);

			Column column = impl.getBuilder().build(indices, mapping);

			for (int i = 0; i < nReads; i++) {
				Object[] buffer = new Object[end - start];
				column.fill(buffer, start);
				assertArrayEquals(expected, buffer);
			}
		}

		@Test
		public void testContinuousWithEmptyBuffer() {
			int nValues = 1332;
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
			int nRows = 2012;
			int bufferSize = 128;

			int[][] table = generateTable(41376101L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOf(flattenAndFillTable(table, mapping), 128);

			Object[] buffer = new Object[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i], mapping);
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

			int[][] table = generateTable(96966414L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(flattenAndFillTable(table, mapping), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			Object[] buffer = new Object[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i], mapping);
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

			int[][] table = generateTable(77492141L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object expected[] = Arrays.copyOf(flattenAndFillTable(table, mapping), bufferSize);

			Object[] buffer = new Object[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i], mapping);
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

			int[][] table = generateTable(91169525L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOfRange(flattenAndFillTable(table, mapping), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			Object[] buffer = new Object[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i], mapping);
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

			int[][] table = generateTable(97074107L, nColumns, nRows);
			List<Object> mapping = impl.getMapping().generate();
			Object[] expected = Arrays.copyOf(flattenAndFillTable(table, mapping), 128);

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
			int nValues = 2048;

			int[][] table = generateTable(68720420L, 1, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Object[] data = flattenAndFillTable(table, mapping);

			Object[] expected = new Object[nValues];
			System.arraycopy(data, nValues / 2, expected, 0, nValues / 2);

			Object[] buffer = new Object[nValues];
			Column column = impl.getBuilder().build(table[0], mapping);
			column.fill(buffer, nValues / 2, 0, 1);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testOutOfBoundsInterleaved() {
			int nValues = 1703;
			int start = nValues + 69;
			int end = start + 110;

			int[][] table = generateTable(14208233L, 1, nValues);
			List<Object> mapping = impl.getMapping().generate();

			Object[] expected = new Object[end - start];

			Column column = impl.getBuilder().build(table[0], mapping);
			Object[] buffer = new Object[end - start];
			column.fill(buffer, start, 0, 1);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testInterleavedWithEmptyBuffer() {
			int nValues = 1332;
			int[] data = impl.generator.generate(49262564L, nValues);
			List<Object> mapping = impl.getMapping().generate();
			Column column = impl.getBuilder().build(data, mapping);
			column.fill(new Object[0], 0, 0, 1);
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
			int nValues = 1172;
			int constant = 78;

			int[] indices = impl.generator.generate(59795422L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nValues];
			Arrays.fill(mapping, constant);

			Object[] expected = new Object[nValues];
			Arrays.fill(expected, data[constant]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

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

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(data, mapped);
		}

		@Test
		public void testReverse() {
			int nValues = 1883;

			int[] indices = impl.generator.generate(11258146L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);

			Object[] expected = new Object[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testRandom() {
			int nValues = 1289;

			int[] indices = impl.generator.generate(75148415L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = permutation(nValues);
			Arrays.setAll(mapping, i -> nValues - i - 1);

			Object[] expected = new Object[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSubset() {
			int nValues = 2375;
			int nSubsetValues = 25;

			int[] indices = impl.generator.generate(74584365L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSubsetValues];
			Arrays.setAll(mapping, i -> i);

			// Use buffer larger than mapped column to test bounds of subset.
			Object[] expected = new Object[nValues];
			System.arraycopy(data, 0, expected, 0, nSubsetValues);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSubsetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testSuperset() {
			int nValues = 1243;
			int nSupersetValues = 2465;

			int[] indices = impl.generator.generate(24186771L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % nValues);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForNegative() {
			int nValues = 124;
			int nSupersetValues = 1465;

			int[] indices = impl.generator.generate(23690049L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % (nValues + 1) - 1);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> mapping[i] < 0 ? null : data[mapping[i]]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForOutOfBounds() {
			int nValues = 124;
			int nSupersetValues = 1465;

			int[] indices = impl.generator.generate(66098955L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? null : data[mapping[i]]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testMissingForOutOfBoundsInterleaved() {
			int nValues = 124;
			int nSupersetValues = 1465;

			int[] indices = impl.generator.generate(49302245L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Object[] data = fill(indices, categoricalMapping);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			Object[] expected = new Object[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? null : data[mapping[i]]);

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] mapped = new Object[nSupersetValues];
			mappedColumn.fill(mapped, 0, 0, 1);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped);
		}

		@Test
		public void testEmpty() {
			int nValues = 1243;

			int[] indices = impl.generator.generate(15643113L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			int[] mapping = new int[0];

			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			assertEquals(0, mappedColumn.size());

			mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, true);
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
			Column mappedColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false)
					.map(mapping, false)
					.map(mapping, false)
					.map(mapping, false);

			Object[] mapped = new Object[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(data, mapped);
		}

		@Test
		public void testViewHint() {
			int nValues = 2248;

			int[] indices = impl.generator.generate(75975556L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column defaultMappingColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] defaultMapping = new Object[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = impl.builder.build(indices, categoricalMapping).map(mapping, true);
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

			Column defaultMappingColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] defaultMapping = new Object[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = impl.builder.build(indices, categoricalMapping).map(mapping, true);
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

			Column defaultMappingColumn = impl.builder.build(indices, categoricalMapping).map(mapping, false);
			Object[] defaultMapping = new Object[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = impl.builder.build(indices, categoricalMapping).map(mapping, true);
			Object[] viewHintMapping = new Object[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testMappedType() {
			int nValues = 12;

			int[] indices = impl.generator.generate(29020070L, nValues);
			List<Object> categoricalMapping = impl.getMapping().generate();

			Column column = impl.builder.build(indices, categoricalMapping);
			Column mappedColumn = column.map(new int[]{2, 3, 4, 6, 7, 11}, false);

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
					Implementation.MAPPED_CATEGORICAL_32BIT, Implementation.MAPPED_FREE,
					Implementation.MAPPED_DATE_TIME_LOW_PRECISION, Implementation.MAPPED_DATE_TIME_HIGH_PRECISION,
					Implementation.MAPPED_TIME);
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
			int nValues = 1238;

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

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testCachePreferView() {
			int nValues = 1238;

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

			assertArrayEquals(expected, buffer);
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

			String expected = Column.TypeId.CUSTOM + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4)";

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

			String expected = Column.TypeId.CUSTOM + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4, 11, "
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

			String expected = Column.TypeId.CUSTOM + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4, 11, "
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

			String expected = Column.TypeId.CUSTOM + " Column (" + data.length + ")\n(" + "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, 4, 11, "
					+ "5, 7, 3, 1, ?, 5, ..., ?)";

			assertEquals(expected, column.toString());
		}

	}

}
