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
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.util.IntegerFormats;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;


/**
 * Tests functionality of columns that support filling {@code int} buffers, i.e., the functionality implemented by all
 * categorical columns.
 *
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class IndexReadableTests {

	private enum Implementation {
		SIMPLE_CATEGORICAL_2BIT("SimpleCategorical2Bit",
				(seed, n) -> randomIntegers(seed, 4, n),
				IndexReadableTests::categorical2BitColumn),
		SIMPLE_CATEGORICAL_4BIT("SimpleCategorical4Bit",
				(seed, n) -> randomIntegers(seed, 16, n),
				IndexReadableTests::categorical4BitColumn),
		SIMPLE_CATEGORICAL_8BIT("SimpleCategorical8Bit",
				(seed, n) -> randomIntegers(seed, 256, n),
				IndexReadableTests::categorical8BitColumn),
		SIMPLE_CATEGORICAL_16BIT("SimpleCategorical16Bit",
				(seed, n) -> randomIntegers(seed, 65536, n),
				IndexReadableTests::categorical16BitColumn),
		SIMPLE_CATEGORICAL_32BIT("SimpleCategorical32Bit",
				(seed, n) -> randomIntegers(seed, Integer.MAX_VALUE, n),
				IndexReadableTests::categorical32BitColumn),
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
				IndexReadableTests::mappedCategorical32BitColumn);

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

	private static final ColumnType<Void> VOID_TYPE = ColumnTypes.categoricalType(
			"com.rapidminer.belt.column.test.voidcolumn", Void.class, null);

	private static Column categorical2BitColumn(int[] data) {
		byte[] byteData = new byte[data.length % 4 == 0 ? data.length / 4 : data.length / 4 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT2, data.length);
		return new SimpleCategoricalColumn<>(VOID_TYPE, packed, Collections.emptyList());
	}

	private static Column categorical4BitColumn(int[] data) {
		byte[] byteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(byteData, i, data[i]);
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT4, data.length);
		return new SimpleCategoricalColumn<>(VOID_TYPE, packed, Collections.emptyList());
	}

	private static Column categorical8BitColumn(int[] data) {
		byte[] byteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			byteData[i] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(byteData, Format.UNSIGNED_INT8, data.length);
		return new SimpleCategoricalColumn<>(VOID_TYPE, packed, Collections.emptyList());
	}

	private static Column categorical16BitColumn(int[] data) {
		short[] shortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			shortData[i] = (short) data[i];
		}
		return new SimpleCategoricalColumn<>(VOID_TYPE, shortData, Collections.emptyList());
	}

	private static Column categorical32BitColumn(int[] data) {
		return new SimpleCategoricalColumn<>(VOID_TYPE, data, Collections.emptyList());
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
		return new MappedCategoricalColumn<>(VOID_TYPE, packed, Collections.emptyList(), mapping);
	}

	private static Column mappedCategorical4BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length % 2 == 0 ? data.length / 2 : data.length / 2 + 1];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(mappedByteData, mapping[i], data[i]);
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT4, data.length);
		return new MappedCategoricalColumn<>(VOID_TYPE, packed, Collections.emptyList(), mapping);
	}

	private static Column mappedCategorical8BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		byte[] mappedByteData = new byte[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedByteData[mapping[i]] = (byte) data[i];
		}
		PackedIntegers packed = new PackedIntegers(mappedByteData, Format.UNSIGNED_INT8, data.length);
		return new MappedCategoricalColumn<>(VOID_TYPE, packed, Collections.emptyList(), mapping);
	}

	private static Column mappedCategorical16BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		short[] mappedShortData = new short[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedShortData[mapping[i]] = (short) data[i];
		}
		return new MappedCategoricalColumn<>(VOID_TYPE, mappedShortData, Collections.emptyList(), mapping);
	}

	private static Column mappedCategorical32BitColumn(int[] data) {
		int[] mapping = permutation(data.length);
		int[] mappedIntData = new int[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedIntData[mapping[i]] = data[i];
		}
		return new MappedCategoricalColumn<>(VOID_TYPE, mappedIntData, Collections.emptyList(), mapping);
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
			int[] expected = Arrays.copyOfRange(data, start, end);

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testOutOfBoundsContinuous() {
			int nValues = 1703;
			int start = nValues + 69;
			int end = start + 110;

			int[] data = impl.getGenerator().generate(43534871L, nValues);
			int[] expected = new int[end - start];

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end - start];
			column.fill(buffer, start);

			assertArrayEquals(expected, buffer);
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

			int[] expected = new int[nValues];
			System.arraycopy(data, nValues / 2, expected, 0, nValues / 2);

			int[] buffer = new int[nValues];
			Column column = impl.getBuilder().build(data);
			column.fill(buffer, nValues / 2, 0, 1);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testOutOfBoundsInterleaved() {
			int nValues = 1703;
			int start = nValues + 69;
			int end = start + 110;

			int[] data = impl.getGenerator().generate(39223391L, nValues);
			int[] expected = new int[end - start];

			Column column = impl.getBuilder().build(data);
			int[] buffer = new int[end - start];
			column.fill(buffer, start, 0, 1);

			assertArrayEquals(expected, buffer);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			// Use buffer larger than mapped column to test bounds of subset.
			int[] expected = new int[nValues];
			System.arraycopy(data, 0, expected, 0, nSubsetValues);

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
			int[] mapped = new int[nValues];
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
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

			Column mappedColumn = impl.builder.build(data).map(mapping, false);
			assertEquals(0, mappedColumn.size());

			mappedColumn = impl.builder.build(data).map(mapping, true);
			assertEquals(0, mappedColumn.size());
		}

		@Test
		public void testChained() {
			int nValues = 3237;
			int[] data = impl.generator.generate(82962299L, nValues);

			// Apply reverse mapping four times
			int[] mapping = new int[nValues];
			Arrays.setAll(mapping, i -> nValues - i - 1);
			Column mappedColumn = impl.builder.build(data).map(mapping, false)
					.map(mapping, false)
					.map(mapping, false)
					.map(mapping, false);

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

			Column defaultMappingColumn = impl.builder.build(data).map(mapping, false);
			int[] defaultMapping = new int[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = impl.builder.build(data).map(mapping, true);
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

			Column defaultMappingColumn = impl.builder.build(data).map(mapping, false);
			int[] defaultMapping = new int[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = impl.builder.build(data).map(mapping, true);
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

			Column defaultMappingColumn = impl.builder.build(data).map(mapping, false);
			int[] defaultMapping = new int[nValues];
			defaultMappingColumn.fill(defaultMapping, 0);

			Column viewHintColumn = impl.builder.build(data).map(mapping, true);
			int[] viewHintMapping = new int[nValues];
			viewHintColumn.fill(viewHintMapping, 0);

			assertArrayEquals(defaultMapping, viewHintMapping);
		}

		@Test
		public void testMappedType() {
			int nValues = 12;
			int[] data = impl.generator.generate(56548366L, nValues);

			Column column = impl.builder.build(data);
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
					Implementation.MAPPED_CATEGORICAL_32BIT);
		}

		@Test
		public void testCache() {
			int nValues = 1238;

			int[] data = impl.generator.generate(28736437L, nValues);
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


			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> i % 4 == 0 ? 0 : data[i]);

			int[] buffer = new int[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, false, cache);
			mapped.fill(buffer, 0);

			assertArrayEquals(expected, buffer);
		}

		@Test
		public void testCachePreferView() {
			int nValues = 1238;

			int[] data = impl.generator.generate(28736437L, nValues);
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


			int[] expected = new int[nValues];
			Arrays.setAll(expected, i -> i % 4 == 0 ? 0 : data[i]);

			int[] buffer = new int[nValues];
			Column mapped = ((CacheMappedColumn) column).map(mapping, true, cache);
			mapped.fill(buffer, 0);

			assertArrayEquals(expected, buffer);
		}


	}

}
