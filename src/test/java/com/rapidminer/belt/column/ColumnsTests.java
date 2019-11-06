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

package com.rapidminer.belt.column;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.CategoricalBuffer;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.transform.NumericColumnFiltererTests;
import com.rapidminer.belt.table.ColumnAccessor;
import com.rapidminer.belt.util.Belt;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ColumnsTests {

	private static final ColumnType<Integer> INTEGER_FREE_TYPE = ColumnTypes.objectType("test", Integer.class, null);

	private static List<String> getMappingList(int length) {
		List<String> list = new ArrayList<>(length);
		list.add(null);
		for (int i = 0; i < length; i++) {
			list.add("value" + i);
		}
		return list;
	}

	public static class Properties {


		@Test
		public void testBicategorical() {
			Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(2))),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new BooleanDictionary<>(getMappingList(2), 1)),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(3))),
					new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11])};
			Boolean[] test = new Boolean[columns.length];
			Arrays.setAll(test, i -> Columns.isBicategorical(columns[i]));
			assertArrayEquals(new Boolean[]{false, true, true, false, false}, test);
		}

		@Test
		public void testAtMostBicategorical() {
			Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(2))),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new BooleanDictionary<>(getMappingList(1), 1)),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(3))),
					new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11])};
			Boolean[] test = new Boolean[columns.length];
			Arrays.setAll(test, i -> Columns.isAtMostBicategorical(columns[i]));
			assertArrayEquals(new Boolean[]{false, true, true, false, false}, test);
		}

		@Test
		public void testBicategoricalAndBiboolean() {
			Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(2))),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(3))),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new BooleanDictionary<>(getMappingList(2), 1)),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(4))),
					new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11]),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new BooleanDictionary<>(getMappingList(1), 1)),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new BooleanDictionary<>(getMappingList(1), -1))};
			Boolean[] test = new Boolean[columns.length];
			Arrays.setAll(test, i -> Columns.isBicategoricalAndBoolean(columns[i]));
			assertArrayEquals(new Boolean[]{false, false, false, true, false, false, false, false}, test);
		}

	}

	public static class DictionaryChange {

		@Test
		public void testToBoolean() {
			List<String> mappingList = getMappingList(2);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
					new Dictionary<>(mappingList));
			Column column2 = Columns.toBoolean(column, mappingList.get(1));
			assertEquals(mappingList.get(1),
					column2.getDictionary(String.class).get(column2.getDictionary(String.class).getPositiveIndex()));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testToBooleanNotCategorical() {
			Columns.toBoolean(new DoubleArrayColumn(new double[3]), null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testToBooleanMappingTooBig() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
					new Dictionary<>(getMappingList(3)));
			Columns.toBoolean(column, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testToBooleanWrongInstance() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
					new Dictionary<>(getMappingList(2)));
			Columns.toBoolean(column, Integer.valueOf(3));
		}

		@Test(expected = NullPointerException.class)
		public void testChangeDictionaryNullFrom() {
			Columns.changeDictionary(null, new DoubleArrayColumn(new double[3]));
		}

		@Test(expected = NullPointerException.class)
		public void testChangeDictionaryNullTo() {
			Columns.changeDictionary(new DoubleArrayColumn(new double[3]), null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testChangeDictionaryDifferentClass() {
			Columns.changeDictionary(new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(3))),
					new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("bla", Instant.class, null), new int[22],
							new Dictionary<>(Collections.emptyList())));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testChangeDictionaryNotCategoricalFirst() {
			Columns.changeDictionary(new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11]),
					new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("bla", Integer.class, null), new int[22],
							new Dictionary<>(Collections.emptyList())));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testChangeDictionaryNotCategoricalSecond() {
			Columns.changeDictionary(
					new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("bla", Integer.class, null), new int[22],
							new Dictionary<>(Collections.emptyList())), new SimpleObjectColumn<>(INTEGER_FREE_TYPE,
							new Object[11]));
		}

		@Test(expected = NullPointerException.class)
		public void testMergeDictionaryNullFrom() {
			Columns.mergeDictionary(null, new DoubleArrayColumn(new double[3]));
		}

		@Test(expected = NullPointerException.class)
		public void testMergeDictionaryNullTo() {
			Columns.mergeDictionary(new DoubleArrayColumn(new double[3]), null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMergeDictionaryDifferentClass() {
			Columns.mergeDictionary(new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(3))),
					new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("bla", Instant.class, null), new int[22],
							new Dictionary<>(Collections.emptyList())));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMergeDictionaryNotCategoricalFirst() {
			Columns.mergeDictionary(new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11]),
					new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("bla", Integer.class, null), new int[22],
							new Dictionary<>(Collections.emptyList())));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMergeDictionaryNotCategoricalSecond() {
			Columns.mergeDictionary(
					new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("bla", Integer.class, null), new int[22],
							new Dictionary<>(Collections.emptyList())), new SimpleObjectColumn<>(INTEGER_FREE_TYPE,
							new Object[11]));
		}

		@Test
		public void testChangeDictionary() {
			Column column = Columns.changeDictionary(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(getMappingList(3))),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(Arrays.asList(null, "x", "y"))));
			assertEquals(Arrays.asList(null, "x", "y"), column.getDictionary(String.class).getValueList());
		}

		@Test
		public void testMergeDictionary() {
			Column column = Columns.mergeDictionary(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(Arrays.asList(null, "a", "b"))),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22],
							new Dictionary<>(Arrays.asList(null, "x", "b"))));
			assertEquals(Arrays.asList(null, "x", "b", "a"), column.getDictionary(String.class).getValueList());
		}
	}

	public static class RemoveUnused {

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			Columns.removeUnusedDictionaryValues(null, Columns.CleanupOption.COMPACT, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			Columns.removeUnusedDictionaryValues(new SimpleCategoricalColumn<String>(ColumnTypes.NOMINAL, new int[0],
							new Dictionary<>(Collections.singletonList(null))), Columns.CleanupOption.COMPACT,
					null);
		}

		@Test
		public void testNotNominal() {
			Column input = new DoubleArrayColumn(new double[0]);
			Column output = Columns.removeUnusedDictionaryValues(input, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertSame(input, output);
		}

		@Test
		public void testAllUsedContinuous() {
			Column nominal =
					Builders.newTableBuilder(10).addNominal("bla", i -> "value" + i).build(Belt.defaultContext()).column(0);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertSame(nominal, output);
		}

		@Test
		public void testAllUsedRemove() {
			Column nominal =
					Builders.newTableBuilder(10).addNominal("bla", i -> "value" + i).build(Belt.defaultContext()).column(0);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			assertSame(nominal, output);
		}

		@Test
		public void testNoneUsedContinuous() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL).map(new int[0],true);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertNotSame(nominal, output);
			assertEquals(nominal.size(), output.size());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(Collections.singletonList(null), newDic.getValueList());
		}

		@Test
		public void testNoneUsedRemove() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL).map(new int[0],true);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertEquals(nominal.size(), output.size());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(Collections.singletonList(null), newDic.getValueList());
		}


		@Test
		public void testNotAllUsedContinuous() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertArrayEquals(readAllToArray(nominal), readAllToArray(output));
			Set<Integer> usedIndices = new HashSet<>();
			CategoricalReader reader = Readers.categoricalReader(output);
			while(reader.hasRemaining()){
				usedIndices.add(reader.read());
			}
			assertEquals(new HashSet<>(Arrays.asList(1,2,3,4,5,6,7,8,9)), usedIndices);
			assertEquals(9, output.getDictionary(String.class).maximalIndex());
		}

		@Test
		public void testNotAllUsedRemove() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			Set<String> values = new HashSet<>();
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
				values.add("value"+i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			values.remove("value5");
			values.remove("value7");
			values.add("bla");
			values.add(null);
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertArrayEquals(readAllToArray(nominal), readAllToArray(output));
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(2, newDic.maximalIndex() - newDic.size());
			assertEquals(values, new HashSet<>(newDic.getValueList()));
		}

		@Test
		public void testNotAllUsedRemoveBooleanPos() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanPos() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanPosSecond() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanPosSecond() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanNeg() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanNeg() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanNegSecond() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "yes");
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanNegSecond() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "yes");
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanOnlyPos() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanOnlyPos() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanOnlyNeg() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanOnlyNeg() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, null);
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary<String> newDic = output.getDictionary(String.class);
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}
	}

	public static class Compact {

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			Columns.compactDictionary(null);
		}

		@Test
		public void testNotCategorical() {
			Column input = new DoubleArrayColumn(new double[0]);
			Column output = Columns.compactDictionary(input);
			assertSame(input, output);
		}

		@Test
		public void testObjectValues() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL);
			Column withGaps = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Column withoutGaps = Columns.compactDictionary(withGaps);
			assertNotSame(withGaps, withoutGaps);
			assertArrayEquals(readAllToArray(withGaps), readAllToArray(withoutGaps));
		}

		@Test
		public void testIndices() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL);
			Column withGaps = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Column withoutGaps = Columns.compactDictionary(withGaps);
			Column compactified = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertArrayEquals(readAllIndicesToArray(compactified), readAllIndicesToArray(withoutGaps));
		}

		@Test
		public void testWithoutGaps() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn(ColumnTypes.NOMINAL);
			Column withoutGaps = Columns.compactDictionary(nominal);
			assertSame(nominal, withoutGaps);
		}

		@Test
		public void testBoolean() {
			CategoricalBuffer<String> buffer = Buffers.categoricalBuffer(10, 2);
			buffer.set(0, "no");
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn(ColumnTypes.NOMINAL, "yes");
			Column withGaps = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Column withoutGaps = Columns.compactDictionary(withGaps);
			assertArrayEquals(readAllToArray(withGaps), readAllToArray(withoutGaps));
			assertEquals(booleanColumn.getDictionary(Object.class).get(booleanColumn.getDictionary(String.class).getPositiveIndex()),
					withoutGaps.getDictionary(Object.class).get(withoutGaps.getDictionary(String.class).getPositiveIndex()));
		}


		@Test
		public void testOneValueColumn() {
			int size = 42;
			CategoricalColumn column = new InternalColumnsImpl().newSingleValueCategoricalColumn(ColumnTypes.NOMINAL,
					"blabla", size);
			assertTrue(ColumnTestUtils.isSparse(column));
			Object[] values = new Object[size];
			column.fill(values, 0);
			Object[] expected = new Object[size];
			Arrays.fill(expected, "blabla");
			assertArrayEquals(expected, values);
		}
	}

	public static class ReplaceInDictionary {

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			Columns.replaceSingleInDictionary(null, "green", "yellow");
		}

		@Test(expected = NullPointerException.class)
		public void testNullOld() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, null, "yellow");
		}

		@Test(expected = NullPointerException.class)
		public void testNullNew() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, "green", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNotCategorical() {
			Column column = new SimpleObjectColumn<>(ColumnTypes.objectType("bla", String.class, null), new Object[2]);
			Columns.replaceSingleInDictionary(column, "green", "yellow");
		}

		@Test(expected = IllegalArgumentException.class)
		public void testIncompatibleType() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, Integer.MAX_VALUE, Integer.MIN_VALUE);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testIncompatibleSecond() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, "black", Integer.MIN_VALUE);
		}

		@Test
		public void testSubtype() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.categoricalType("blup", Number.class, null),
					new int[0],
					new Dictionary<>(Arrays.asList(12.4, Double.MAX_VALUE, 11)));
			Column replaced = Columns.replaceSingleInDictionary(column, Double.MAX_VALUE, Integer.MIN_VALUE);
			assertEquals(Arrays.asList(12.4, Integer.MIN_VALUE, 11),
					replaced.getDictionary(Number.class).getValueList());
		}

		@Test
		public void testSimpleReplace() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Column replaced = Columns.replaceSingleInDictionary(column, "green", "yellow");
			assertEquals(Arrays.asList(null, "blue", "yellow", "red"),
					replaced.getDictionary(String.class).getValueList());
		}

		@Test
		public void testSimpleReplaceBoolean() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new BooleanDictionary<>(Arrays.asList(null, "red", "green"), 2));
			Column replaced = Columns.replaceSingleInDictionary(column, "green", "yellow");
			assertEquals(Arrays.asList(null, "red", "yellow"), replaced.getDictionary(String.class).getValueList());
			assertEquals(column.getDictionary(Object.class).getPositiveIndex(),
					replaced.getDictionary(Object.class).getPositiveIndex());
		}

		@Test
		public void testReplaceNotPresent() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Column replaced = Columns.replaceSingleInDictionary(column, "pink", "yellow");
			assertEquals(column.getDictionary(String.class).getValueList(),
					replaced.getDictionary(String.class).getValueList());
		}

		@Test(expected = Columns.IllegalReplacementException.class)
		public void testReplaceAlreadyPresent() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, "green", "blue");
		}

		@Test(expected = NullPointerException.class)
		public void testNullOldInMap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = Stream.of(new AbstractMap.SimpleEntry<>("green", "pink"),
					new AbstractMap.SimpleEntry<>((String) null, "yellow"))
					.collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
			Columns.replaceInDictionary(column, oldToNew, String.class);
		}

		@Test(expected = NullPointerException.class)
		public void testNullNewInMap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", null);
			Columns.replaceInDictionary(column, oldToNew, String.class);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnInMap() {
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Columns.replaceInDictionary(null, oldToNew, String.class);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Columns.replaceInDictionary(column, oldToNew, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullMap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceInDictionary(column, null, String.class);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNotCategoricalMap() {
			Column column = new SimpleObjectColumn<>(ColumnTypes.objectType("bla", String.class, null), new Object[2]);
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Columns.replaceInDictionary(column, oldToNew, String.class);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testIncompatibleTypeMap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<Integer, Integer> oldToNew = new HashMap<>();
			oldToNew.put(3, 4);
			oldToNew.put(5, 6);
			Columns.replaceInDictionary(column, oldToNew, Integer.class);
		}

		@Test
		public void testMap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Column replaced = Columns.replaceInDictionary(column, oldToNew, String.class);
			assertEquals(Arrays.asList(null, "yellow", "pink", "red"),
					replaced.getDictionary(String.class).getValueList());
		}

		@Test(expected = Columns.IllegalReplacementException.class)
		public void testMapAlreadyPresent() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "pink");
			Columns.replaceInDictionary(column, oldToNew, String.class);
		}

		@Test(expected = Columns.IllegalReplacementException.class)
		public void testMapAlreadyPresentOld() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "red");
			Columns.replaceInDictionary(column, oldToNew, String.class);
		}


		@Test
		public void testSingleVsMap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Column replaced = Columns.replaceSingleInDictionary(column, "green", "yellow");
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "yellow");
			Column replaced2 = Columns.replaceInDictionary(column, oldToNew, String.class);
			assertEquals(replaced.getDictionary(String.class).getValueList(),
					replaced2.getDictionary(String.class).getValueList());
		}

		@Test
		public void testMapOrderDoesNotMatter() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new LinkedHashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "green");
			Column replaced = Columns.replaceInDictionary(column, oldToNew, String.class);
			assertEquals(Arrays.asList(null, "green", "pink", "red"),
					replaced.getDictionary(String.class).getValueList());
			oldToNew = new LinkedHashMap<>();
			oldToNew.put("blue", "green");
			oldToNew.put("green", "pink");
			replaced = Columns.replaceInDictionary(column, oldToNew, String.class);
			assertEquals(Arrays.asList(null, "green", "pink", "red"),
					replaced.getDictionary(String.class).getValueList());
		}

		@Test
		public void testMapSwap() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new LinkedHashMap<>();
			oldToNew.put("green", "blue");
			oldToNew.put("blue", "green");
			Column replaced = Columns.replaceInDictionary(column, oldToNew, String.class);
			assertEquals(Arrays.asList(null, "green", "blue", "red"),
					replaced.getDictionary(String.class).getValueList());
		}

		@Test
		public void testMapCyclic() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0],
					new Dictionary<>(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new LinkedHashMap<>();
			oldToNew.put("blue", "green");
			oldToNew.put("green", "red");
			oldToNew.put("red", "blue");
			Column replaced = Columns.replaceInDictionary(column, oldToNew, String.class);
			assertEquals(Arrays.asList(null, "green", "red", "blue"),
					replaced.getDictionary(String.class).getValueList());
		}


	}

	private static String[] readAllToArray(Column column) {
		ObjectReader<String> reader = Readers.objectReader(column, String.class);
		String[] result = new String[reader.remaining()];
		int i = 0;
		while (reader.hasRemaining()) {
			result[i++] = reader.read();
		}
		return result;
	}

	private static int[] readAllIndicesToArray(Column column) {
		CategoricalReader reader = Readers.categoricalReader(column);
		int[] result = new int[reader.remaining()];
		int i = 0;
		while (reader.hasRemaining()) {
			result[i++] = reader.read();
		}
		return result;
	}
}
