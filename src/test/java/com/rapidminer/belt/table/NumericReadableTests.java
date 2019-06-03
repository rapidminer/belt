/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.column.CacheMappedColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;

/**
 * Tests functionality of columns with capability {@link Column.Capability#NUMERIC_READABLE}. In particular, the
 * corresponding methods to fill the buffers of {@link NumericReader}s.
 *
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class NumericReadableTests {

	private static final double EPSILON = 1e-10;

	private static final double MISSING_FREQUENCY = 0.1;
	private static final List<Void> EMPTY_DICTIONARY = Collections.emptyList();

	private enum Implementation {
		DOUBLE_ARRAY("DoubleArray",
				NumericReadableTests::randomDoubles,
				a -> ColumnAccessor.get().newNumericColumn(Column.TypeId.REAL, a)),
		MAPPED_DOUBLE_ARRAY("MappedDoubleArray",
				NumericReadableTests::randomDoubles,
				a -> mappedDoubleColumn(a, Column.TypeId.REAL)),
		INTEGER_ARRAY("IntegerArray",
				NumericReadableTests::random32BitIntegers,
				a -> ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER, a)),
		MAPPED_INTEGER_ARRAY("MappedIntegerArray",
				NumericReadableTests::random32BitIntegers,
				a -> mappedDoubleColumn(a, Column.TypeId.INTEGER)),
		SIMPLE_CATEGORICAL_2BIT("SimpleCategorical2Bit",
				NumericReadableTests::random2BitIntegers,
				NumericReadableTests::categorical2BitColumn),
		SIMPLE_CATEGORICAL_4BIT("SimpleCategorical4Bit",
				NumericReadableTests::random4BitIntegers,
				NumericReadableTests::categorical4BitColumn),
		SIMPLE_CATEGORICAL_8BIT("SimpleCategorical8Bit",
				NumericReadableTests::random8BitIntegers,
				NumericReadableTests::categorical8BitColumn),
		SIMPLE_CATEGORICAL_16BIT("SimpleCategorical16Bit",
				NumericReadableTests::random16BitIntegers,
				NumericReadableTests::categorical16BitColumn),
		SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
				NumericReadableTests::random32BitIntegers,
				NumericReadableTests::categorical32BitColumn),
		MAPPED_CATEGORICAL_2BIT("MappedCategorical2Bit",
				NumericReadableTests::random2BitIntegers,
				NumericReadableTests::mappedCategorical2BitColumn),
		MAPPED_CATEGORICAL_4BIT("MappedCategorical4Bit",
				NumericReadableTests::random4BitIntegers,
				NumericReadableTests::mappedCategorical4BitColumn),
		MAPPED_CATEGORICAL_8BIT("MappedCategorical8Bit",
				NumericReadableTests::random8BitIntegers,
				NumericReadableTests::mappedCategorical8BitColumn),
		MAPPED_CATEGORICAL_16BIT("MappedCategorical16Bit",
				NumericReadableTests::random16BitIntegers,
				NumericReadableTests::mappedCategorical16BitColumn),
		MAPPED_CATEGORICAL_32BIT("MappedCategorical32Bit",
				NumericReadableTests::random32BitIntegers,
				NumericReadableTests::mappedCategorical32BitColumn),
		REMAPPED_CATEGORICAL_2BIT("RemappedCategorical2Bit",
				NumericReadableTests::random2BitIntegers,
				NumericReadableTests::remappedCategorical2BitColumn),
		REMAPPED_CATEGORICAL_4BIT("RemappedCategorical4Bit",
				NumericReadableTests::random4BitIntegers,
				NumericReadableTests::remappedCategorical4BitColumn),
		REMAPPED_CATEGORICAL_8BIT("RemappedCategorical8Bit",
				NumericReadableTests::random8BitIntegers,
				NumericReadableTests::remappedCategorical8BitColumn),
		REMAPPED_CATEGORICAL_16BIT("RemappedCategorical16Bit",
				NumericReadableTests::random8BitIntegers, //cannot use too big values because of remapping
				NumericReadableTests::remappedCategorical16BitColumn),
		REMAPPED_CATEGORICAL_32BIT("RemappedCategorical32Bit",
				NumericReadableTests::random8BitIntegers, //cannot use too big values because of remapping
				NumericReadableTests::remappedCategorical32BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_2BIT("RemappedMappedCategorical2Bit",
				NumericReadableTests::random2BitIntegers,
				NumericReadableTests::remappedMappedCategorical2BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_4BIT("RemappedMappedCategorical4Bit",
				NumericReadableTests::random4BitIntegers,
				NumericReadableTests::remappedMappedCategorical4BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_8BIT("RemappedMappedCategorical8Bit",
				NumericReadableTests::random8BitIntegers,
				NumericReadableTests::remappedMappedCategorical8BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_16BIT("RemappedMappedCategorical16Bit",
				NumericReadableTests::random8BitIntegers, //cannot use too big values because of remapping
				NumericReadableTests::remappedMappedCategorical16BitColumn),
		REMAPPED_MAPPED_CATEGORICAL_32BIT("RemappedMappedCategorical32Bit",
				NumericReadableTests::random8BitIntegers, //cannot use too big values because of remapping
				NumericReadableTests::remappedMappedCategorical32BitColumn),
		TIME("Time",
				NumericReadableTests::randomTimeLongs,
				NumericReadableTests::timeColumn),
		MAPPED_TIME("MappedTime",
				NumericReadableTests::randomTimeLongs,
				NumericReadableTests::mappedTimeColumn);

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
		double[] generate(long seed, int n);
	}

	@FunctionalInterface
	private interface ColumnBuilder {
		Column build(double[] data);
	}

	private static double[] randomDoubles(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : rng.nextDouble() * 512d - 256d;
		}
		return data;
	}

	private static double[] random2BitIntegers(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : rng.nextInt(3) + 1;
		}
		return data;
	}

	private static double[] random4BitIntegers(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : rng.nextInt(15) + 1;
		}
		return data;
	}

	private static double[] random8BitIntegers(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : rng.nextInt(255) + 1;
		}
		return data;
	}

	private static double[] random16BitIntegers(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : rng.nextInt(65535) + 1;
		}
		return data;
	}

	private static double[] random32BitIntegers(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : rng.nextInt(Integer.MAX_VALUE - 1) + 1;
		}
		return data;
	}

	private static double[] randomTimeLongs(long seed, int n) {
		Random rng = new Random(seed);
		double[] data = new double[n];
		for (int i = 0; i < n; i++) {
			data[i] = rng.nextDouble() < MISSING_FREQUENCY ? Double.NaN : 12 * 60 * 60 * 1_000_000_000L + rng
					.nextInt();
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

	private static Column mappedDoubleColumn(double[] data, Column.TypeId type) {
		int[] mapping = permutation(data.length);
		double[] mappedData = new double[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedData[mapping[i]] = data[i];
		}
		return ColumnTestUtils.getMappedNumericColumn(type, mappedData, mapping);
	}

	private static final ColumnType<Void> VOID_TYPE = ColumnTypes.categoricalType(
			"com.rapidminer.belt.column.voidcolumn", Void.class, null);

	private static Column categorical2BitColumn(double[] data) {
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(byteData, i, (int) Math.round(data[i]));
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return ColumnAccessor.get().newCategoricalColumn(VOID_TYPE, packed, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical2BitColumn(double[] data) {
		int max = -1;
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = (int) Math.round(data[i]);
			max = Math.max(max, value);
			IntegerFormats.writeUInt2(byteData, i, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(VOID_TYPE, packed, Collections.emptyList(), remapping);
	}

	private static Column categorical4BitColumn(double[] data) {
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(byteData, i, (int) Math.round(data[i]));
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return ColumnAccessor.get().newCategoricalColumn(VOID_TYPE, packed, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical4BitColumn(double[] data) {
		int max = -1;
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = (int) Math.round(data[i]);
			max = Math.max(max, value);
			IntegerFormats.writeUInt4(byteData, i, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getRemappedCategoricalColumn(VOID_TYPE, packed, Collections.emptyList(), remapping);
	}

	private static Column categorical8BitColumn(double[] data) {
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) Math.round(data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return ColumnAccessor.get().newCategoricalColumn(VOID_TYPE, packed, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical8BitColumn(double[] data) {
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
		return ColumnTestUtils.getRemappedCategoricalColumn(VOID_TYPE, packed, Collections.emptyList(), remapping);
	}

	private static Column categorical16BitColumn(double[] data) {
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) Math.round(data[i]);
		}
		return ColumnAccessor.get().newCategoricalColumn(VOID_TYPE, shortData, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical16BitColumn(double[] data) {
		int max = -1;
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			long value = Math.round(data[i]);
			max = Math.max(max, (int) value);
			shortData[i] = (short) value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalColumn(VOID_TYPE, shortData, Collections.emptyList(), remapping);
	}

	private static Column categorical32BitColumn(double[] data) {
		int[] intData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			intData[i] = (int) Math.round(data[i]);
		}
		return ColumnAccessor.get().newCategoricalColumn(VOID_TYPE, intData, EMPTY_DICTIONARY);
	}

	private static Column remappedCategorical32BitColumn(double[] data) {
		int max = -1;
		int[] intData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = (int) Math.round(data[i]);
			max = Math.max(max, value);
			intData[i] = value;
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedCategoricalColumn(VOID_TYPE, intData, Collections.emptyList(), remapping);
	}

	private static Column mappedCategorical2BitColumn(double[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], (int) Math.round(data[i]));
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(VOID_TYPE, packed, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical2BitColumn(double[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = (int) Math.round(data[i]);
			IntegerFormats.writeUInt2(mappedByteData, mapping[i], value);
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT2, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(VOID_TYPE, packed, Collections.emptyList(),
				remapping, mapping);
	}

	private static Column mappedCategorical4BitColumn(double[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], (int) Math.round(data[i]));
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(VOID_TYPE, packed, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical4BitColumn(double[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			int value = (int) Math.round(data[i]);
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], value);
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(VOID_TYPE, packed, Collections.emptyList(),
				remapping, mapping);
	}

	private static Column mappedCategorical8BitColumn(double[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedByteData[mapping[i]] = (byte) Math.round(data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getMappedCategoricalColumn(VOID_TYPE, packed, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical8BitColumn(double[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			long value = Math.round(data[i]);
			mappedByteData[mapping[i]] = (byte) value;
			max = Math.max(max, (int) value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(VOID_TYPE, packed, Collections.emptyList(),
				remapping, mapping);
	}

	private static Column mappedCategorical16BitColumn(double[] data) {
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedShortData[mapping[i]] = (short) Math.round(data[i]);
		}
		return ColumnTestUtils.getMappedCategoricalColumn(VOID_TYPE, mappedShortData, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical16BitColumn(double[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			long value = Math.round(data[i]);
			mappedShortData[mapping[i]] = (short) value;
			max = Math.max(max, (int) value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(VOID_TYPE, mappedShortData, Collections.emptyList(), remapping,
				mapping);
	}

	private static Column mappedCategorical32BitColumn(double[] data) {
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedIntData[mapping[i]] = (int) Math.round(data[i]);
		}
		return ColumnTestUtils.getMappedCategoricalColumn(VOID_TYPE, mappedIntData, EMPTY_DICTIONARY, mapping);
	}

	private static Column remappedMappedCategorical32BitColumn(double[] data) {
		int max = -1;
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			int value = (int) Math.round(data[i]);
			mappedIntData[mapping[i]] = value;
			max = Math.max(max, value);
		}
		int[] remapping = new int[max + 1];
		Arrays.setAll(remapping, i -> i);
		return ColumnTestUtils.getRemappedMappedCategoricalColumn(VOID_TYPE, mappedIntData, Collections.emptyList(), remapping,
				mapping);
	}

	private static Column timeColumn(double[] data) {
		long[] longData = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			double datum = data[i];
			if (Double.isNaN(datum)) {
				longData[i] = TimeColumn.MISSING_VALUE;
			} else {
				longData[i] = (long) datum;
			}
		}
		return ColumnAccessor.get().newTimeColumn(longData);
	}

	private static Column mappedTimeColumn(double[] data) {
		int[] mapping = permutation(data.length);
		long[] mappedLongData = new long[data.length];
		for (int i = 0; i < data.length; i++) {
			double datum = data[i];
			if (Double.isNaN(datum)) {
				mappedLongData[mapping[i]] = TimeColumn.MISSING_VALUE;
			} else {
				mappedLongData[mapping[i]] = (long) datum;
			}
		}
		return ColumnTestUtils.getMappedTimeColumn(mappedLongData, mapping);
	}

	@RunWith(Parameterized.class)
	public static class ContinuousFill {

		@Parameter
		public Implementation impl;

		@Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test
		public void testContinuous() {
			int nValues = 1605;
			int end = 1214;

			double[] data = impl.getGenerator().generate(12573062L, nValues);
			double[] expected = Arrays.copyOfRange(data, 0, end);

			Column column = impl.getBuilder().build(data);
			double[] buffer = new double[end];
			column.fill(buffer, 0);

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testPartialContinuous() {
			int nValues = 1605;
			int start = 80;
			int end = nValues + start;

			double[] data = impl.getGenerator().generate(55406729L, nValues);
			double[] expected = Arrays.copyOfRange(data, start, end);

			Column column = impl.getBuilder().build(data);
			double[] buffer = new double[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testOutOfBoundsContinuous() {
			int nValues = 1703;
			int start = nValues + 69;
			int end = start + 110;

			double[] data = impl.getGenerator().generate(61555865L, nValues);
			double[] expected = new double[end - start];

			Column column = impl.getBuilder().build(data);
			double[] buffer = new double[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testContinuousIdempotence() {
			int nReads = 3;
			int nValues = 2043;
			int start = 83;
			int end = nValues - start;

			double[] data = impl.getGenerator().generate(15568007L, nValues);
			double[] expected = Arrays.copyOfRange(data, start, end);

			Column column = impl.getBuilder().build(data);

			for (int i = 0; i < nReads; i++) {
				double[] buffer = new double[end - start];
				column.fill(buffer, start);
				assertArrayEquals(expected, buffer, EPSILON);
			}
		}

		@Test
		public void testContinuousWithEmptyBuffer() {
			int nValues = 1332;
			double[] data = impl.getGenerator().generate(59174053L, nValues);
			Column column = impl.getBuilder().build(data);
			column.fill(new double[0], 0);
		}

	}

	@RunWith(Parameterized.class)
	public static class InterleavedFill {

		@Parameter
		public Implementation impl;

		@Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		private double[][] generateTable(long seed, int nColumns, int nRows) {
			Random rng = new Random(seed);
			double[][] table = new double[nColumns][];
			Arrays.setAll(table, i -> impl.getGenerator().generate(rng.nextLong(), nRows));
			return table;
		}

		private double[] flattenTable(double[][] table) {
			double[] flattened = new double[table.length * table[0].length];
			for (int i = 0; i < table.length; i++) {
				double[] column = table[i];
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

			double[][] table = generateTable(16120336L, nColumns, nRows);
			double[] expected = Arrays.copyOf(flattenTable(table), 128);

			double[] buffer = new double[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, 0, i, nColumns);
			}

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testInterleavedOfCompleteRows() {
			int nColumns = 4;
			int nRows = 2012;
			int bufferSize = 128;
			int startIndex = 256;

			double[][] table = generateTable(72283810L, nColumns, nRows);
			double[] expected = Arrays.copyOfRange(flattenTable(table), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			double[] buffer = new double[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, startIndex, i, nColumns);
			}

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testInterleavedOfIncompleteRowsFromStart() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 2012;
			int bufferSize = 128;

			double[][] table = generateTable(55544997L, nColumns, nRows);
			double expected[] = Arrays.copyOf(flattenTable(table), bufferSize);

			double[] buffer = new double[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, 0, i, nColumns);
			}

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testInterleavedOfIncompleteRows() {
			// Buffer size is not a multiple of number of columns: last row is incomplete!
			int nColumns = 3;
			int nRows = 2012;
			int bufferSize = 128;
			int startIndex = 384;

			double[][] table = generateTable(22420167L, nColumns, nRows);
			double[] expected = Arrays.copyOfRange(flattenTable(table), startIndex * nColumns,
					startIndex * nColumns + bufferSize);

			double[] buffer = new double[bufferSize];
			for (int i = 0; i < nColumns; i++) {
				Column column = impl.getBuilder().build(table[i]);
				column.fill(buffer, startIndex, i, nColumns);
			}

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testInterleavedIdempotence() {
			int nColumns = 4;
			int nRows = 2012;
			int bufferSize = 128;
			int nRuns = 3;

			double[][] table = generateTable(11160850L, nColumns, nRows);
			double[] expected = Arrays.copyOf(flattenTable(table), 128);

			double[] buffer = new double[bufferSize];
			for (int run = 0; run < nRuns; run++) {
				for (int i = 0; i < nColumns; i++) {
					Column column = impl.getBuilder().build(table[i]);
					column.fill(buffer, 0, i, nColumns);
				}
				assertArrayEquals(expected, buffer, EPSILON);
			}
		}

		@Test
		public void testPartialInterleaved() {
			int nValues = 2048;

			double[] data = impl.getGenerator().generate(93603515L, nValues);

			double[] expected = new double[nValues];
			System.arraycopy(data, nValues / 2, expected, 0, nValues / 2);

			double[] buffer = new double[nValues];
			Column column = impl.getBuilder().build(data);
			column.fill(buffer, nValues / 2, 0, 1);

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testOutOfBoundsInterleaved() {
			int nValues = 1703;
			int start = nValues + 69;
			int end = start + 110;

			double[] data = impl.getGenerator().generate(77273138L, nValues);
			double[] expected = new double[end - start];

			Column column = impl.getBuilder().build(data);
			double[] buffer = new double[end - start];
			column.fill(buffer, start, 0, 1);

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testInterleavedWithEmptyBuffer() {
			int nValues = 1332;
			double[] data = impl.getGenerator().generate(39728324L, nValues);
			Column column = impl.getBuilder().build(data);
			column.fill(new double[0], 0, 0, 1);
		}

	}

	@RunWith(Parameterized.class)
	public static class SimpleMap {

		@Parameter
		public Implementation impl;

		@Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test
		public void testConstant() {
			int nValues = 1172;
			int constant = 78;
			double[] data = impl.generator.generate(65580975L, nValues);
			int[] mapping = new int[nValues];
			Arrays.fill(mapping, constant);

			double[] expected = new double[nValues];
			Arrays.fill(expected, data[constant]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testIdentity() {
			int nValues = 2948;
			double[] data = impl.generator.generate(72297556L, nValues);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(data, mapped, EPSILON);
		}

		@Test
		public void testReverse() {
			int nValues = 1883;
			double[] data = impl.generator.generate(68533020L, nValues);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);

			double[] expected = new double[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testRandom() {
			int nValues = 1289;
			double[] data = impl.generator.generate(61815550L, nValues);
			int[] mapping = permutation(nValues);
			Arrays.setAll(mapping, i -> nValues - i - 1);

			double[] expected = new double[nValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testSubset() {
			int nValues = 2375;
			int nSubsetValues = 25;
			double[] data = impl.generator.generate(65490909L, nValues);
			int[] mapping = new int[nSubsetValues];
			Arrays.setAll(mapping, i -> i);

			// Use buffer larger than mapped column to test bounds of subset.
			double[] expected = new double[nValues];
			System.arraycopy(data, 0, expected, 0, nSubsetValues);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSubsetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testSuperset() {
			int nValues = 1243;
			int nSupersetValues = 2465;
			double[] data = impl.generator.generate(47717323L, nValues);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % nValues);

			double[] expected = new double[nSupersetValues];
			Arrays.setAll(expected, i -> data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testMissingForNegative() {
			int nValues = 124;
			int nSupersetValues = 1465;
			double[] data = impl.generator.generate(67172573L, nValues);
			int[] mapping = new int[nSupersetValues];
			Arrays.setAll(mapping, i -> i % (nValues + 1) - 1);

			double[] expected = new double[nSupersetValues];
			Arrays.setAll(expected, i -> mapping[i] < 0 ? Double.NaN : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testMissingForOutOfBounds() {
			int nValues = 124;
			int nSupersetValues = 1465;
			double[] data = impl.generator.generate(14929440L, nValues);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			double[] expected = new double[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? Double.NaN : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nSupersetValues];
			mappedColumn.fill(mapped, 0);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testMissingForOutOfBoundsInterleaved() {
			int nValues = 124;
			int nSupersetValues = 1465;
			double[] data = impl.generator.generate(84362221L, nValues);
			int[] mapping = new int[nSupersetValues];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues * 2) - nValues);

			double[] expected = new double[nSupersetValues];
			Arrays.setAll(expected, i -> (mapping[i] < 0 || mapping[i] >= nValues) ? Double.NaN : data[mapping[i]]);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] mapped = new double[nSupersetValues];
			mappedColumn.fill(mapped, 0, 0, 1);

			assertEquals(nSupersetValues, mappedColumn.size());
			assertArrayEquals(expected, mapped, EPSILON);
		}

		@Test
		public void testEmpty() {
			int nValues = 1243;
			double[] data = impl.generator.generate(51033485L, nValues);
			int[] mapping = new int[0];
			Arrays.setAll(mapping, i -> i % nValues);

			Column mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			assertEquals(0, mappedColumn.size());

			mappedColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, true);
			assertEquals(0, mappedColumn.size());
		}

		@Test
		public void testChained() {
			int nValues = 3237;
			double[] data = impl.generator.generate(59099926L, nValues);

			// Apply reverse mapping four times
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);
			Column mappedColumn = ColumnAccessor.get().map(ColumnAccessor.get().map(ColumnAccessor.get().map(ColumnAccessor.get().map(impl.builder.build(data),mapping, false)
					,mapping, false)
					,mapping, false)
					,mapping, false);

			double[] mapped = new double[nValues];
			mappedColumn.fill(mapped, 0);

			assertArrayEquals(data, mapped, EPSILON);
		}

		@Test
		public void testViewHint() {
			int nValues = 2248;
			double[] data = impl.generator.generate(41400228L, nValues);
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] defaultMapping = new double[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, true);
			double[] viewHintMapping = new double[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping, EPSILON);
		}

		@Test
		public void testViewHintSuperset() {
			int nValues = 2148;
			double[] data = impl.generator.generate(63012879L, nValues);
			int[] mapping = new int[nValues * 2];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] defaultMapping = new double[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, true);
			double[] viewHintMapping = new double[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping, EPSILON);
		}

		@Test
		public void testViewHintSubset() {
			int nValues = 10 * 245;
			double[] data = impl.generator.generate(59898639L, nValues);
			int[] mapping = new int[nValues / 10];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(nValues));

			Column defaultMappingColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, false);
			double[] defaultMapping = new double[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = ColumnAccessor.get().map(impl.builder.build(data),mapping, true);
			double[] viewHintMapping = new double[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping, EPSILON);
		}

		@Test
		public void testMappedType() {
			int nValues = 12;
			double[] data = impl.generator.generate(63772273L, nValues);

			Column column = impl.builder.build(data);
			Column mappedColumn = ColumnAccessor.get().map(column,new int[]{2, 3, 4, 6, 7, 11}, false);

			assertEquals(column.type(), mappedColumn.type());
		}

	}

	@RunWith(Parameterized.class)
	public static class CachedMap {

		@Parameter
		public Implementation impl;

		@Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.MAPPED_DOUBLE_ARRAY, Implementation.MAPPED_INTEGER_ARRAY,
					Implementation.MAPPED_CATEGORICAL_2BIT, Implementation.MAPPED_CATEGORICAL_4BIT,
					Implementation.MAPPED_CATEGORICAL_8BIT, Implementation.MAPPED_CATEGORICAL_16BIT,
					Implementation.MAPPED_CATEGORICAL_32BIT, Implementation.REMAPPED_MAPPED_CATEGORICAL_2BIT,
					Implementation.REMAPPED_MAPPED_CATEGORICAL_4BIT, Implementation.REMAPPED_MAPPED_CATEGORICAL_8BIT,
					Implementation.REMAPPED_MAPPED_CATEGORICAL_16BIT, Implementation.REMAPPED_MAPPED_CATEGORICAL_32BIT,
					Implementation.MAPPED_TIME);
		}

		@Test
		public void testCache() {
			int nValues = 1238;

			double[] data = impl.generator.generate(28378892L, nValues);
			Column column = impl.builder.build(data);

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			// Create cache and entry for the above mapping
			Map<int[], int[]> cache = new HashMap<>();
			((CacheMappedColumn) column).map(mapping, false, cache);
			assertEquals(1, cache.size());
			int[] remapping = new ArrayList<>(cache.values()).get(0);

			// Modify cache to test whether it is used (invalidate every fourth value)
			Arrays.setAll(remapping, i -> i % 4 == 0 ? -1 : remapping[i]);


			double[] expected = new double[nValues];
			Arrays.setAll(expected, i -> i % 4 == 0 ? Double.NaN : data[i]);

			double[] buffer = new double[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, false, cache);
			mapped.fill(buffer, 0);

			assertArrayEquals(expected, buffer, EPSILON);
		}

		@Test
		public void testCachePreferView() {
			int nValues = 1238;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);

			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> i);

			// Create cache and entry for the above mapping
			Map<int[], int[]> cache = new HashMap<>();
			((CacheMappedColumn) column).map(mapping, true, cache);
			assertEquals(1, cache.size());
			int[] remapping = new ArrayList<>(cache.values()).get(0);

			// Modify cache to test whether it is used (invalidate every fourth value)
			Arrays.setAll(remapping, i -> i % 4 == 0 ? -1 : remapping[i]);


			double[] expected = new double[nValues];
			Arrays.setAll(expected, i -> i % 4 == 0 ? Double.NaN : data[i]);

			double[] buffer = new double[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, true, cache);
			mapped.fill(buffer, 0);

			assertArrayEquals(expected, buffer, EPSILON);
		}

	}

	@RunWith(Parameterized.class)
	public static class Fill {

		@Parameter
		public Implementation impl;

		@Parameters(name = "{0}")
		public static Iterable<Implementation> columnImplementations() {
			return Arrays.asList(Implementation.values());
		}

		@Test(expected = NullPointerException.class)
		public void testNullArray() {
			int nValues = 1238;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill((double[]) null, 0);

		}

		@Test
		public void testEmptyArray() {
			int nValues = 1238;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill(new double[0], 0);

		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRow() {
			int nValues = 1238;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill(new double[10], -5);

		}

		@Test
		public void testTooBigRow() {
			int nValues = 12;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, nValues + 1);
			assertArrayEquals(new double[10], array, 1e-15);
		}

		@Test
		public void testAlmostTooBigRow() {
			int nValues = 32;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, nValues - 4);

			double[] all = new double[nValues];
			column.fill(all, 0);
			double[] expected = new double[10];
			System.arraycopy(all, nValues - 4, expected, 0, 4);

			assertArrayEquals(expected, array, 1e-20);
		}

		@Test(expected = NullPointerException.class)
		public void testNullArrayInterleaved() {
			int nValues = 1238;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill((double[]) null, 0, 0, 1);

		}

		@Test
		public void testEmptyArrayInterleaved() {
			int nValues = 123;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			column.fill(new double[0], 0, 0, 1);

		}

		@Test(expected = IllegalArgumentException.class)
		public void testNullStepSizeInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 0, 0, 0);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRowInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, -5, 5, 1);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeRowInterleavedStepSize() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, -5, 5, 2);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeOffsetInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 0, -5, 1);
		}

		@Test(expected = ArrayIndexOutOfBoundsException.class)
		public void testNegativeOffsetInterleavedStepSize() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 0, -5, 2);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeStepSizeInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 10, 0, -1);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeStepSizeBiggerInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 10, 0, -3);
		}

		@Test
		public void testOffsetInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[100];
			column.fill(array, 0, 50, 1);

			double[] normal = new double[128];
			column.fill(normal, 0);
			double[] expected = new double[100];
			System.arraycopy(normal, 0, expected, 50, 50);
			assertArrayEquals(expected, array, 0);
		}

		@Test
		public void testOffsetInterleavedStepSize() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[100];
			column.fill(array, 0, 50, 2);
			double[] normal = new double[128];
			column.fill(normal, 0);
			double[] expected = new double[100];
			Arrays.setAll(expected, i -> i < 50 ? 0 : (i%2==0 ? normal[(i - 50)/2] : 0));
			assertArrayEquals(expected, array, 0);
		}

		@Test
		public void testOffsetBiggerSizeInterleaved() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 0, 100, 1);
			assertArrayEquals(new double[10], array, 0);
		}

		@Test
		public void testOffsetBiggerSizeInterleavedStepSize() {
			int nValues = 128;

			double[] data = impl.generator.generate(82375234L, nValues);
			Column column = impl.builder.build(data);
			double[] array = new double[10];
			column.fill(array, 0, 110, 2);
			assertArrayEquals(new double[10], array, 0);
		}

	}

}
