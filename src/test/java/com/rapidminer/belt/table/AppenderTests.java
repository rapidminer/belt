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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.DateTimeBuffer;
import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.reader.MixedRowReader;
import com.rapidminer.belt.reader.NumericReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.util.ColumnRole;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class AppenderTests {

	private static final double EPSILON = 1e-10;


	@RunWith(Parameterized.class)
	public static class ColumnsAppend {

		@Parameterized.Parameter
		public double factor;

		@Parameterized.Parameters(name = "{0}")
		public static Iterable<Double> columnImplementations() {
			return Arrays.asList(0.6, 1.0, 1.43);
		}

		@Test
		public void testOneColumn() {
			final double[] callbackResult = new double[]{-1.0};
			Column newColumn =
					Appender.append(Arrays.asList(ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10])),
							(int) (factor * 10),
							p -> callbackResult[0] = p, Belt.defaultContext());
			assertEquals(ColumnType.REAL, newColumn.type());
			assertEquals(1.0, callbackResult[0], EPSILON);
		}

		@Test
		public void testIntegerAndReal() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = i % 3 == 0 ? randomInteger(size) : random(size);
				columns.add(
						i % 3 == 0 ? ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER_53_BIT, data) :
								ColumnAccessor.get().newNumericColumn(TypeId.REAL, data));
			}
			totalSize = (int) (factor * totalSize);
			final double[] callbackResult = new double[]{-1.0};
			Column newColumn = Appender.append(columns, totalSize, p -> callbackResult[0] = p, Belt.defaultContext());
			assertArrayEquals(readToArrayNumeric(columns, totalSize), readToArrayNumeric(newColumn, totalSize),
					EPSILON);
			assertEquals(ColumnType.REAL, newColumn.type());
			assertEquals(1.0, callbackResult[0], EPSILON);
		}

		@Test
		public void testInteger() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = randomInteger(size);
				columns.add(ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER_53_BIT, data));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayNumeric(columns, totalSize), readToArrayNumeric(newColumn, totalSize),
					EPSILON);
			assertEquals(ColumnType.INTEGER_53_BIT, newColumn.type());
		}

		@Test
		public void testIntegerAndRealMapped() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = i % 3 == 0 ? randomInteger(size) : random(size);
				Column column =
						i % 3 == 0 ? ColumnAccessor.get().newNumericColumn(Column.TypeId.INTEGER_53_BIT, data) :
								ColumnAccessor.get().newNumericColumn(TypeId.REAL, data);
				columns.add(ColumnAccessor.get().map(column, mapping(data.length), true));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayNumeric(columns, totalSize), readToArrayNumeric(newColumn, totalSize),
					EPSILON);
			assertEquals(ColumnType.REAL, newColumn.type());
		}

		@Test
		public void testTextSetObjects() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = randomInteger(size);
				Object[] values = new Object[data.length];
				Arrays.setAll(values, j -> new StringSet(Collections.singletonList("" + data[j])));
				columns.add(ColumnAccessor.get().newObjectColumn(ColumnType.TEXTSET, values));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, null, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.TEXTSET, newColumn.type());
		}

		@Test
		public void testTextSetObjectsMapped() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = randomInteger(size);
				Object[] values = new Object[data.length];
				Arrays.setAll(values, j -> new StringSet(Collections.singletonList("" + data[j])));
				Column column = ColumnAccessor.get().map(ColumnAccessor.get().newObjectColumn(ColumnType.TEXTSET,
						values), mapping(data.length), true);
				columns.add(column);
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.TEXTSET, newColumn.type());
		}

		@Test
		public void testNominalSameDictionary() {
			int numberOfCategories = 13;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, dictionary));
			}
			totalSize = (int) (factor * totalSize);
			final double[] callbackResult = new double[]{-1.0};
			Column newColumn = Appender.append(columns, totalSize, p -> callbackResult[0] = p, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertEquals(1.0, callbackResult[0], EPSILON);
		}

		@Test
		public void testNominalSameDictionaryMapped() {
			int numberOfCategories = 13;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				Column column =
						ColumnAccessor.get().map(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
								dictionary), mapping(size), true);
				columns.add(column);
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
		}

		@Test
		public void testNominalDifferentDictionaries() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100) + 1;
				totalSize += size;
				int numberOfCategories = random.nextInt(size);
				List<String> dictionary = getMappingList(numberOfCategories);
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, dictionary));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
		}

		@Test
		public void testNominalDifferentDictionariesMapped() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100) + 1;
				totalSize += size;
				int numberOfCategories = random.nextInt(size);
				List<String> dictionary = getMappingList(numberOfCategories);
				int[] data = randomCategories(size, numberOfCategories);
				Column column =
						ColumnAccessor.get().map(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
								dictionary), mapping(size), true);
				columns.add(column);
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
		}

		@Test
		public void testNominalFirstBoolean() {
			int numberOfCategories = 3;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(i % 5 == 0 ? ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
						dictionary, 2) :
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, dictionary));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertFalse(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testNominalAndBoolean() {
			int numberOfCategories = 3;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, dictionary, 2));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testNominalAndBooleanDifferentPositives() {
			int numberOfCategories = 3;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
						dictionary, i % 2 + 1));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertFalse(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testNominalAndNegative() {
			int numberOfCategories = 2;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, dictionary,
						BooleanDictionary.NO_ENTRY));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testNominalAndFirstFewer() {
			int numberOfCategories = 2;
			List<String> dictionary = getMappingList(numberOfCategories);
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(i != 7 ? ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
						dictionary,
						BooleanDictionary.NO_ENTRY) : ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL,
						randomCategories(size, 3), getMappingList(3),
						2));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testNominalPositiveNotInData() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, randomCategories(size, 2),
						getMappingList(3),
						2));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testBinominal() {
			int numberOfCategories = 3;
			List<String> dictionary = Arrays.asList(null, "yes", "no");
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				int[] data = randomCategories(size, numberOfCategories);
				columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data, dictionary, 1));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
		}

		@Test
		public void testBinominalFirstNoPositive() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(2);
			int totalSize = 0;

			int size = random.nextInt(100) + 2;
			totalSize += size;
			int[] data = randomCategories(size, 2);
			columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
					Arrays.asList(null, "no"), -1));
			size = random.nextInt(100) + 2;
			totalSize += size;
			data = randomCategories(size, 3);
			columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
					Arrays.asList(null, "no", "yes"), 2));

			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {
			}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
			assertEquals("yes",
					newColumn.getDictionary().get(newColumn.getDictionary().getPositiveIndex()));
		}

		@Test
		public void testBinominalFirstNoNegative() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(2);
			int totalSize = 0;

			int size = random.nextInt(100) + 2;
			totalSize += size;
			int[] data = randomCategories(size, 2);
			columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
					Arrays.asList(null, "yes"), 1));
			size = random.nextInt(100) + 2;
			totalSize += size;
			data = randomCategories(size, 3);
			columns.add(ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, data,
					Arrays.asList(null, "yes", "no"), 1));

			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {
			}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.NOMINAL, newColumn.type());
			assertTrue(newColumn.getDictionary().isBoolean());
			assertEquals("yes", newColumn.getDictionary().get(newColumn.getDictionary().getPositiveIndex()));
		}

		@Test
		public void testStringObjects() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = random(size);
				Object[] strings = new Object[data.length];
				Arrays.setAll(strings, j -> data[j] + "");
				columns.add(ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, strings));
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, null, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.TEXT, newColumn.type());
		}

		@Test
		public void testStringObjectsMapped() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = random(size);
				Object[] strings = new Object[data.length];
				Arrays.setAll(strings, j -> data[j] + "");
				Column column = ColumnAccessor.get().map(ColumnAccessor.get().newObjectColumn(ColumnType.TEXT,
						strings), mapping(data.length), true);
				columns.add(column);
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.TEXT, newColumn.type());
		}

		@Test
		public void testTimeObjects() {
			long max = LocalTime.MAX.toNanoOfDay();
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = random(size);
				long[] times = new long[data.length];
				Arrays.setAll(times, j -> Math.round(data[j] * max));
				columns.add(ColumnAccessor.get().newTimeColumn(times));
			}
			totalSize = (int) (factor * totalSize);
			final double[] callbackResult = new double[]{-1.0};
			Column newColumn = Appender.append(columns, totalSize, p -> callbackResult[0] = p, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.TIME, newColumn.type());
			assertEquals(1.0, callbackResult[0], EPSILON);
		}

		@Test
		public void testTimeObjectsMapped() {
			long max = LocalTime.MAX.toNanoOfDay();
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = random(size);
				long[] times = new long[data.length];
				Arrays.setAll(times, j -> Math.round(data[j] * max));
				Column column = ColumnAccessor.get().map(ColumnAccessor.get().newTimeColumn(times),
						mapping(data.length), true);
				columns.add(column);
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.TIME, newColumn.type());
		}

		@Test
		public void testDateTimeObjects() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				DateTimeBuffer buffer = Buffers.dateTimeBuffer(size, false);
				for (int j = 0; j < buffer.size(); j++) {
					buffer.set(j, Instant.ofEpochMilli(random.nextLong()));
				}
				columns.add(buffer.toColumn());
			}
			totalSize = (int) (factor * totalSize);
			final double[] callbackResult = new double[]{-1.0};
			Column newColumn = Appender.append(columns, totalSize, p -> callbackResult[0] = p, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.DATETIME, newColumn.type());
		}

		@Test
		public void testDateTimeObjectsDiffPrecision() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				DateTimeBuffer buffer =
						i % 3 == 0 ? Buffers.dateTimeBuffer(size, false) : Buffers.dateTimeBuffer(size, true);
				for (int j = 0; j < buffer.size(); j++) {
					buffer.set(j, Instant.ofEpochMilli(random.nextLong()));
				}
				columns.add(buffer.toColumn());
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, null, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.DATETIME, newColumn.type());
		}

		@Test
		public void testDateTimeObjectsDiffPrecisionMapped() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 10; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				DateTimeBuffer buffer =
						i % 3 == 0 ? Buffers.dateTimeBuffer(size, false) : Buffers.dateTimeBuffer(size, true);
				for (int j = 0; j < buffer.size(); j++) {
					buffer.set(j, Instant.ofEpochMilli(random.nextLong()));
				}
				Column column = ColumnAccessor.get().map(buffer.toColumn(), mapping(buffer.size()), true);
				columns.add(column);
			}
			totalSize = (int) (factor * totalSize);
			Column newColumn = Appender.append(columns, totalSize, p -> {}, Belt.defaultContext());
			assertArrayEquals(readToArrayObjects(columns, totalSize), readToArrayObjects(newColumn, totalSize));
			assertEquals(ColumnType.DATETIME, newColumn.type());
		}

		@Test
		public void testCallbackMonotonicity() {
			Random random = new Random();
			List<Column> columns = new ArrayList<>(10);
			int totalSize = 0;
			for (int i = 0; i < 100; i++) {
				int size = random.nextInt(100);
				totalSize += size;
				double[] data = random(size);
				columns.add(ColumnAccessor.get().newNumericColumn(TypeId.REAL, data));
			}
			totalSize = (int) (factor * totalSize);
			final double[] callbackResult = new double[]{-1.0};
			Appender.append(columns, totalSize, p -> {
				if (callbackResult[0] > p) {
					throw new IllegalStateException();
				} else {
					callbackResult[0] = p;
				}
			}, Belt.defaultContext());
			assertEquals(1.0, callbackResult[0], EPSILON);
		}

	}

	public static class ColumnsAppendInput {

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeLength() {
			Appender.append(Arrays.asList(ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[3]),
					ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[5])),
					-1, null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumns() {
			Appender.append(null, 5, v->{}, Belt.defaultContext());
		}

		@Test(expected = IllegalArgumentException.class)
		public void testEmptyColumns() {
			Appender.append(Collections.emptyList(), 5, v->{}, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testColumnsFirstNull() {
			Appender.append(
					Arrays.asList(null, ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[3]),
							ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[5])),
					11,null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testColumnsContainNull() {
			Appender.append(
					Arrays.asList(ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[3]), null,
							ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[5])),
					11,null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testColumnsContainNullCategorical() {
			Appender.append(
					Arrays.asList(
							ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4],
									Arrays.asList(null, "bla")),
							null,
							ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4], Arrays.asList(null, "bla"))),
					11, null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testColumnsContainNullObject() {
			Appender.append(
					Arrays.asList(
							ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, new Object[4]),
							null,
							ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, new Object[4])),
					11, null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testColumnsContainNullTime() {
			Appender.append(
					Arrays.asList(
							ColumnAccessor.get().newTimeColumn(new long[5]),
							null,
							ColumnAccessor.get().newTimeColumn(new long[5])),
					11, null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testColumnsContainNullDateTime() {
			Appender.append(
					Arrays.asList(
							ColumnAccessor.get().newDateTimeColumn(new long[5], null),
							null,
							ColumnAccessor.get().newDateTimeColumn(new long[5], null)),
					11, null, Belt.defaultContext());
		}

		@Test
		public void testNotNumeric() {
			try {
				Appender.append(Arrays.asList(ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[3]),
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4], Arrays.asList(null,
								"bla")),
						ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[5])), 11, null, Belt.defaultContext());
				fail();
			}catch(Appender.IncompatibleTypesException e){
				assertEquals(1,e.getIndex());
				assertEquals(ColumnType.REAL.toString(), e.getDesiredType());
				assertEquals(ColumnType.NOMINAL.toString(),e.getActualType());
			}
		}

		@Test
		public void testNotTime() {
			try {
				Appender.append(Arrays.asList(ColumnAccessor.get().newTimeColumn(new long[3]),
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4], Arrays.asList(null,
								"bla")),
						ColumnAccessor.get().newTimeColumn(new long[5])), 11, null, Belt.defaultContext());
				fail();
			}catch(Appender.IncompatibleTypesException e){
				assertEquals(1,e.getIndex());
				assertEquals(ColumnType.TIME.toString(), e.getDesiredType());
				assertEquals(ColumnType.NOMINAL.toString(),e.getActualType());
			}
		}

		@Test
		public void testNotDateTime() {
			try {
				Appender.append(Arrays.asList(ColumnAccessor.get().newDateTimeColumn(new long[3], null),
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4], Arrays.asList(null,
								"bla")),
						ColumnAccessor.get().newDateTimeColumn(new long[5], null)), 11, null, Belt.defaultContext());
				fail();
			}catch(Appender.IncompatibleTypesException e){
				assertEquals(1,e.getIndex());
				assertEquals(ColumnType.DATETIME.toString(), e.getDesiredType());
				assertEquals(ColumnType.NOMINAL.toString(),e.getActualType());
			}
		}

		@Test
		public void testNotCategorical() {
			try {
				Appender.append(Arrays.asList(
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4], Arrays.asList(null,
								"bla")),
						ColumnAccessor.get().newDateTimeColumn(new long[5], null),
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[3], Arrays.asList(null, "blup"))),
						11, null, Belt.defaultContext());
				fail();
			} catch (Appender.IncompatibleTypesException e) {
				assertEquals(1, e.getIndex());
				assertEquals(ColumnType.DATETIME.toString(), e.getActualType());
				assertEquals(ColumnType.NOMINAL.toString(), e.getDesiredType());
			}
		}

		@Test
		public void testNotObject() {
			try {
				Appender.append(Arrays.asList(
						ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, new Object[4]),
						ColumnAccessor.get().newDateTimeColumn(new long[5], null),
						ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, new Object[2])),
						11, null, Belt.defaultContext());
				fail();
			} catch (Appender.IncompatibleTypesException e) {
				assertEquals(1, e.getIndex());
				assertEquals(ColumnType.DATETIME.toString(), e.getActualType());
				assertEquals(ColumnType.TEXT.toString(), e.getDesiredType());
			}
		}

		@Test
		public void testCategoricalDifferentType() {
			try {
				Appender.append(Arrays.asList(
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[4], Arrays.asList(null,
								"bla")),
						ColumnAccessor.get().newCategoricalColumn(ColumnTestUtils.categoricalType(String.class,
								null)
								, new int[3], Arrays.asList(null, "blup")),
						ColumnAccessor.get().newCategoricalColumn(ColumnType.NOMINAL, new int[3], Arrays.asList(null, "blup"))),
						11, null, Belt.defaultContext());
				fail();
			} catch (Appender.IncompatibleTypesException e) {
				assertEquals(1, e.getIndex());
				assertEquals(ColumnTestUtils.categoricalType(String.class, null).toString(), e.getActualType());
				assertEquals(ColumnType.NOMINAL.toString(), e.getDesiredType());
			}
		}

		@Test
		public void testObjectDifferentType() {
			try {
				Appender.append(Arrays.asList(
						ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, new Object[4]),
						ColumnAccessor.get().newObjectColumn(ColumnType.TEXTSET, new Object[2]),
						ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, new Object[2])),
						11, null, Belt.defaultContext());
				fail();
			} catch (Appender.IncompatibleTypesException e) {
				assertEquals(1, e.getIndex());
				assertEquals(ColumnType.TEXTSET.toString(), e.getActualType());
				assertEquals(ColumnType.TEXT.toString(), e.getDesiredType());
			}
		}
	}


	public static class TableAppend {

		@Test
		public void testTable() {
			TableBuilder builder = Builders.newTableBuilder(17);
			builder.addTime("time", i -> LocalTime.of(i, i));
			builder.addNominal("nominal", i -> "value_" + i);
			builder.addMetaData("nominal", ColumnRole.ID);
			builder.addDateTime("date-time", i -> Instant.EPOCH);
			Table table = builder.build(Belt.defaultContext());
			List<Table> tableList = Arrays.asList(table, table, table);

			final double[] callbackResult = new double[]{-1.0};
			Table result = Appender.append(tableList, p -> callbackResult[0] = p, Belt.defaultContext());
			assertEquals(3 * table.height(), result.height());
			assertArrayEquals(readToArrayObjects(tableList), readToArrayObjects(result));
			assertEquals(1.0, callbackResult[0], EPSILON);
		}

		@Test
		public void testDifferentTables() {
			TableBuilder builder = Builders.newTableBuilder(17);
			builder.addTime("time", i -> LocalTime.of(i, i));
			builder.addNominal("nominal", i -> "value_" + i);
			builder.addDateTime("date-time", i -> Instant.EPOCH);
			Table table = builder.build(Belt.defaultContext());
			List<Table> tableList = Arrays.asList(table, table.columns(new int[]{2, 0, 1}),
					table.rows(new int[]{11, 1}, Belt.defaultContext()));
			Table result = Appender.append(tableList, null, Belt.defaultContext());
			Object[] expecteds = readToArrayObjects(tableList);
			Object[] actuals = readToArrayObjects(result);
			assertArrayEquals(expecteds, actuals);
		}

		@Test
		public void testCallbackMonotonicity() {
			TableBuilder builder = Builders.newTableBuilder(17);
			builder.addTime("time", i -> LocalTime.of(i, i));
			builder.addNominal("nominal", i -> "value_" + i);
			builder.addMetaData("nominal", ColumnRole.ID);
			builder.addDateTime("date-time", i -> Instant.EPOCH);
			Table table = builder.build(Belt.defaultContext());
			Table[] tables = new Table[99];
			Arrays.fill(tables, table);
			List<Table> tableList = Arrays.asList(tables);
			final double[] callbackResult = new double[]{-1.0};
			Appender.append(tableList, p -> {
				if (callbackResult[0] > p) {
					throw new IllegalStateException();
				} else {
					callbackResult[0] = p;
				}
			}, Belt.defaultContext());
			assertEquals(1.0, callbackResult[0], EPSILON);
		}
	}

	public static class TableAppendInput {

		@Test(expected = NullPointerException.class)
		public void testNullList() {
			Appender.append(null, v -> {}, Belt.defaultContext());
		}

		@Test
		public void testEmptyList() {
			Table table = Appender.append(Collections.emptyList(), null, Belt.defaultContext());
			assertEquals(0, table.width());
			assertEquals(0, table.height());
		}

		@Test(expected = NullPointerException.class)
		public void testFirstListEntryNull() {
			Appender.append(Arrays.asList(null, new Table(5)), null, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testListContainsNull() {
			Table table = Builders.newTableBuilder(10).addReal("real", i -> i).addInt53Bit("int", i -> i * i)
					.build(Belt.defaultContext());
			Appender.append(Arrays.asList(table, table, null, table), null, Belt.defaultContext());
		}

		@Test
		public void testNoColumns() {
			Table table = Appender.append(Arrays.asList(new Table(5), new Table(10), new Table(5)), null, Belt.defaultContext());
			assertEquals(0, table.width());
			assertEquals(20, table.height());
		}

		@Test(expected = Appender.TableTooLongException.class)
		public void testTableTooLong() {
			Appender.append(Arrays.asList(new Table(5), new Table(Integer.MAX_VALUE), new Table(5)), null, Belt.defaultContext());
		}

		@Test
		public void testIncompatibleColumnWidths() {
			try {
				Table table = Builders.newTableBuilder(10).addReal("real", i -> i).addInt53Bit("int", i -> i * i)
						.build(Belt.defaultContext());
				Appender.append(
						Arrays.asList(table, table.columns(new int[]{1, 0}), table.columns(new int[]{0}), table), null, Belt.defaultContext());
				fail();
			}catch(Appender.IncompatibleTableWidthException e){
				assertEquals(2, e.getTableIndex());
			}
		}

		@Test
		public void testIncompatibleColumnNames() {
			Table table = Builders.newTableBuilder(10).addReal("real", i -> i).addInt53Bit("int", i -> i * i)
					.build(Belt.defaultContext());
			Table table2 = Builders.newTableBuilder(table).rename("real", "numeric").build(Belt.defaultContext());
			try {
				Appender.append(Arrays.asList(table, table.columns(new int[]{1, 0}), table2, table), null, Belt.defaultContext());
				fail();
			}catch(Appender.IncompatibleColumnsException e){
				assertEquals("real", e.getColumnName());
				assertEquals(2, e.getTableIndex());
			}
		}

		@Test
		public void testIncompatibleColumnType() {
			Table table = Builders.newTableBuilder(10).addReal("real", i -> i).addInt53Bit("int", i -> i * i)
					.build(Belt.defaultContext());
			Table table2 = Builders.newTableBuilder(table).replaceNominal("real", i->"value"+i).build(Belt.defaultContext());
			try {
				Appender.append(Arrays.asList(table, table.columns(new int[]{1, 0}), table2, table), null, Belt.defaultContext());
				fail();
			}catch(Appender.IncompatibleTypesException e){
				assertEquals(2, e.getIndex());
				assertEquals(ColumnType.REAL.toString(), e.getDesiredType());
				assertEquals(ColumnType.NOMINAL.toString(), e.getActualType());
				assertEquals("real", e.getColumnName());
			}
		}

	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	private static double[] randomInteger(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.floor(10 * Math.random()));
		return data;
	}

	private static int[] randomCategories(int n, int numberOfCategories) {
		int[] data = new int[n];
		Arrays.setAll(data, i -> (int) (numberOfCategories * Math.random()));
		return data;
	}

	private static double[] readToArrayNumeric(Column column, int totalSize) {
		NumericReader reader = Readers.numericReader(column);
		double[] result = new double[reader.remaining()];
		Arrays.fill(result, Double.NaN);
		int i = 0;
		while (reader.hasRemaining() && i < totalSize) {
			result[i++] = reader.read();
		}
		return result;
	}

	private static Object[] readToArrayObjects(Column column, int totalSize) {
		ObjectReader<Object> reader = Readers.objectReader(column, Object.class);
		Object[] result = new Object[reader.remaining()];
		int i = 0;
		while (reader.hasRemaining() && i < totalSize) {
			result[i++] = reader.read();
		}
		return result;
	}

	private static Object[] readToArrayObjects(Table table) {
		MixedRowReader reader = Readers.mixedRowReader(table);
		Object[] result = new Object[table.height() * table.width()];
		int i = 0;
		while (reader.hasRemaining()) {
			reader.move();
			for (int j = 0; j < table.width(); j++) {
				result[i++] = reader.getObject(j);
			}
		}
		return result;
	}

	private static Object[] readToArrayObjects(List<Table> tables) {
		int width = tables.get(0).width();
		int totalHeight = 0;
		for (Table table : tables) {
			totalHeight += table.height();
		}
		List<String> firstLabels = tables.get(0).labels();
		Object[] result = new Object[width * totalHeight];
		int i = 0;
		for (Table table : tables) {
			MixedRowReader reader = Readers.mixedRowReader(table.columns(firstLabels));
			while (reader.hasRemaining()) {
				reader.move();
				for (int j = 0; j < table.width(); j++) {
					result[i++] = reader.getObject(j);
				}
			}
		}
		return result;
	}


	private static double[] readToArrayNumeric(List<Column> columns, int totalLength) {
		double[] result = new double[totalLength];
		Arrays.fill(result, Double.NaN);
		int i = 0;
		for (Column column : columns) {
			NumericReader reader = Readers.numericReader(column);
			while (reader.hasRemaining() && i < totalLength) {
				result[i++] = reader.read();
			}
		}
		return result;
	}

	private static Object[] readToArrayObjects(List<Column> columns, int totalLength) {
		Object[] result = new Object[totalLength];
		int i = 0;
		for (Column column : columns) {
			ObjectReader<Object> reader = Readers.objectReader(column, Object.class);
			while (reader.hasRemaining() && i < totalLength) {
				result[i++] = reader.read();
			}
		}
		return result;
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

	private static int[] mapping(int n) {
		int[] indices = permutation(n);
		if (n > 0) {
			int a = (int) (Math.random() * n);
			int b = (int) (Math.random() * n);
			indices[a] = -1;
			indices[b] = 2 * n;
		}
		return indices;
	}

	private static List<String> getMappingList(int length) {
		List<String> list = new ArrayList<>(length);
		list.add(null);
		for (int i = 1; i < length; i++) {
			list.add("value" + i);
		}
		return list;
	}
}
