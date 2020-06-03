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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.util.Belt;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ColumnsTests {

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
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(2))),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new BooleanDictionary(getMappingList(2), 1)),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(3))),
					new SimpleObjectColumn<>(ColumnType.TEXTSET, new Object[11])};
			Boolean[] test = new Boolean[columns.length];
			Arrays.setAll(test, i -> Columns.isBicategorical(columns[i]));
			assertArrayEquals(new Boolean[]{false, true, true, false, false}, test);
		}

		@Test
		public void testAtMostBicategorical() {
			Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(2))),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new BooleanDictionary(getMappingList(1), 1)),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(3))),
					new SimpleObjectColumn<>(ColumnType.TEXTSET, new Object[11])};
			Boolean[] test = new Boolean[columns.length];
			Arrays.setAll(test, i -> Columns.isAtMostBicategorical(columns[i]));
			assertArrayEquals(new Boolean[]{false, true, true, false, false}, test);
		}

		@Test
		public void testBicategoricalAndBiboolean() {
			Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(2))),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(3))),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new BooleanDictionary(getMappingList(2), 1)),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(4))),
					new SimpleObjectColumn<>(ColumnType.TEXTSET, new Object[11]),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new BooleanDictionary(getMappingList(1), 1)),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new BooleanDictionary(getMappingList(1), -1))};
			Boolean[] test = new Boolean[columns.length];
			Arrays.setAll(test, i -> Columns.isBicategoricalAndBoolean(columns[i]));
			assertArrayEquals(new Boolean[]{false, false, false, true, false, false, false, false}, test);
		}

	}

	public static class DictionaryChange {

		@Test
		public void testToBoolean() {
			List<String> mappingList = getMappingList(2);
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
					new Dictionary(mappingList));
			Column column2 = Columns.toBoolean(column, mappingList.get(1));
			assertEquals(mappingList.get(1),
					column2.getDictionary().get(column2.getDictionary().getPositiveIndex()));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testToBooleanNotCategorical() {
			Columns.toBoolean(new DoubleArrayColumn(new double[3]), null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testToBooleanMappingTooBig() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
					new Dictionary(getMappingList(3)));
			Columns.toBoolean(column, null);
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
		public void testChangeDictionaryNotCategoricalFirst() {
			Columns.changeDictionary(new SimpleObjectColumn<>(ColumnType.TEXTSET, new Object[11]),
					new SimpleCategoricalColumn(ColumnTestUtils.categoricalType(String.class, null), new int[22],
							new Dictionary(Collections.emptyList())));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testChangeDictionaryNotCategoricalSecond() {
			Columns.changeDictionary(
					new SimpleCategoricalColumn(ColumnTestUtils.categoricalType(String.class, null), new int[22],
							new Dictionary(Collections.emptyList())), new SimpleObjectColumn<>(ColumnType.TEXTSET,
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
		public void testMergeDictionaryNotCategoricalFirst() {
			Columns.mergeDictionary(new SimpleObjectColumn<>(ColumnType.TEXTSET, new Object[11]),
					new SimpleCategoricalColumn(ColumnTestUtils.categoricalType(String.class, null), new int[22],
							new Dictionary(Collections.emptyList())));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testMergeDictionaryNotCategoricalSecond() {
			Columns.mergeDictionary(
					new SimpleCategoricalColumn(ColumnTestUtils.categoricalType(String.class, null), new int[22],
							new Dictionary(Collections.emptyList())), new SimpleObjectColumn<>(ColumnType.TEXTSET,
							new Object[11]));
		}

		@Test
		public void testChangeDictionary() {
			Column column = Columns.changeDictionary(
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(getMappingList(3))),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(Arrays.asList(null, "x", "y"))));
			assertEquals(Arrays.asList(null, "x", "y"), column.getDictionary().getValueList());
		}

		@Test
		public void testMergeDictionary() {
			Column column = Columns.mergeDictionary(
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(Arrays.asList(null, "a", "b"))),
					new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[22],
							new Dictionary(Arrays.asList(null, "x", "b"))));
			assertEquals(Arrays.asList(null, "x", "b", "a"), column.getDictionary().getValueList());
		}
	}

	public static class RemoveUnused {

		@Test(expected = NullPointerException.class)
		public void testNullColumn() {
			Columns.removeUnusedDictionaryValues(null, Columns.CleanupOption.COMPACT, Belt.defaultContext());
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			Columns.removeUnusedDictionaryValues(new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
							new Dictionary(Collections.singletonList(null))), Columns.CleanupOption.COMPACT,
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
			NominalBuffer buffer = Buffers.nominalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn().map(new int[0],true);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertNotSame(nominal, output);
			assertEquals(nominal.size(), output.size());
			Dictionary newDic = output.getDictionary();
			assertEquals(Collections.singletonList(null), newDic.getValueList());
		}

		@Test
		public void testNoneUsedRemove() {
			NominalBuffer buffer = Buffers.nominalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn().map(new int[0],true);
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertEquals(nominal.size(), output.size());
			Dictionary newDic = output.getDictionary();
			assertEquals(Collections.singletonList(null), newDic.getValueList());
		}


		@Test
		public void testNotAllUsedContinuous() {
			NominalBuffer buffer = Buffers.nominalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn();
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
			assertEquals(9, output.getDictionary().maximalIndex());
		}

		@Test
		public void testNotAllUsedRemove() {
			NominalBuffer buffer = Buffers.nominalBuffer(10);
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
			Column nominal = buffer.toColumn();
			Column output = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			assertNotSame(nominal, output);
			assertArrayEquals(readAllToArray(nominal), readAllToArray(output));
			Dictionary newDic = output.getDictionary();
			assertEquals(2, newDic.maximalIndex() - newDic.size());
			assertEquals(values, new HashSet<>(newDic.getValueList()));
		}

		@Test
		public void testNotAllUsedRemoveBooleanPos() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanPos() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanPosSecond() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanPosSecond() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "no");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertTrue(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanNeg() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanNeg() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanNegSecond() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "yes");
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanNegSecond() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "yes");
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(1, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertTrue(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanOnlyPos() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanOnlyPos() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedRemoveBooleanOnlyNeg() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn(null);
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
			assertEquals(0, newDic.size());
			assertTrue(newDic.isBoolean());
			assertFalse(newDic.hasNegative());
			assertFalse(newDic.hasPositive());
		}

		@Test
		public void testNotAllUsedContinuousBooleanOnlyNeg() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, null);
			}
			Column booleanColumn = buffer.toBooleanColumn(null);
			Column output = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			Dictionary newDic = output.getDictionary();
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
			NominalBuffer buffer = Buffers.nominalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn();
			Column withGaps = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Column withoutGaps = Columns.compactDictionary(withGaps);
			assertNotSame(withGaps, withoutGaps);
			assertArrayEquals(readAllToArray(withGaps), readAllToArray(withoutGaps));
		}

		@Test
		public void testIndices() {
			NominalBuffer buffer = Buffers.nominalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn();
			Column withGaps = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Column withoutGaps = Columns.compactDictionary(withGaps);
			Column compactified = Columns.removeUnusedDictionaryValues(nominal, Columns.CleanupOption.COMPACT,
					Belt.defaultContext());
			assertArrayEquals(readAllIndicesToArray(compactified), readAllIndicesToArray(withoutGaps));
		}

		@Test
		public void testWithoutGaps() {
			NominalBuffer buffer = Buffers.nominalBuffer(10);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "value" + i);
			}
			buffer.set(5, "bla");
			buffer.set(7, "bla");
			Column nominal = buffer.toColumn();
			Column withoutGaps = Columns.compactDictionary(nominal);
			assertSame(nominal, withoutGaps);
		}

		@Test
		public void testBoolean() {
			NominalBuffer buffer = Buffers.nominalBuffer(10, 2);
			buffer.set(0, "no");
			buffer.set(0, "yes");
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, "yes");
			}
			Column booleanColumn = buffer.toBooleanColumn("yes");
			Column withGaps = Columns.removeUnusedDictionaryValues(booleanColumn, Columns.CleanupOption.REMOVE,
					Belt.defaultContext());
			Column withoutGaps = Columns.compactDictionary(withGaps);
			assertArrayEquals(readAllToArray(withGaps), readAllToArray(withoutGaps));
			assertEquals(booleanColumn.getDictionary().get(booleanColumn.getDictionary().getPositiveIndex()),
					withoutGaps.getDictionary().get(withoutGaps.getDictionary().getPositiveIndex()));
		}


		@Test
		public void testOneValueColumn() {
			int size = 42;
			CategoricalColumn column = new InternalColumnsImpl().newSingleValueCategoricalColumn(ColumnType.NOMINAL,
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
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, null, "yellow");
		}

		@Test(expected = NullPointerException.class)
		public void testNullNew() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, "green", null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNotCategorical() {
			Column column = new SimpleObjectColumn<>(ColumnType.TEXT, new Object[2]);
			Columns.replaceSingleInDictionary(column, "green", "yellow");
		}

		@Test
		public void testSimpleReplace() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Column replaced = Columns.replaceSingleInDictionary(column, "green", "yellow");
			assertEquals(Arrays.asList(null, "blue", "yellow", "red"),
					replaced.getDictionary().getValueList());
		}

		@Test
		public void testSimpleReplaceBoolean() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new BooleanDictionary(Arrays.asList(null, "red", "green"), 2));
			Column replaced = Columns.replaceSingleInDictionary(column, "green", "yellow");
			assertEquals(Arrays.asList(null, "red", "yellow"), replaced.getDictionary().getValueList());
			assertEquals(column.getDictionary().getPositiveIndex(),
					replaced.getDictionary().getPositiveIndex());
		}

		@Test
		public void testReplaceNotPresent() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Column replaced = Columns.replaceSingleInDictionary(column, "pink", "yellow");
			assertEquals(column.getDictionary().getValueList(),
					replaced.getDictionary().getValueList());
		}

		@Test(expected = Columns.IllegalReplacementException.class)
		public void testReplaceAlreadyPresent() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceSingleInDictionary(column, "green", "blue");
		}

		@Test(expected = NullPointerException.class)
		public void testNullOldInMap() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = Stream.of(new AbstractMap.SimpleEntry<>("green", "pink"),
					new AbstractMap.SimpleEntry<>((String) null, "yellow"))
					.collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
			Columns.replaceInDictionary(column, oldToNew);
		}

		@Test(expected = NullPointerException.class)
		public void testNullNewInMap() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", null);
			Columns.replaceInDictionary(column, oldToNew);
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnInMap() {
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Columns.replaceInDictionary(null, oldToNew);
		}

		@Test(expected = NullPointerException.class)
		public void testNullMap() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Columns.replaceInDictionary(column, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNotCategoricalMap() {
			Column column = new SimpleObjectColumn<>(ColumnType.TEXT, new Object[2]);
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Columns.replaceInDictionary(column, oldToNew);
		}

		@Test
		public void testMap() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "yellow");
			Column replaced = Columns.replaceInDictionary(column, oldToNew);
			assertEquals(Arrays.asList(null, "yellow", "pink", "red"),
					replaced.getDictionary().getValueList());
		}

		@Test(expected = Columns.IllegalReplacementException.class)
		public void testMapAlreadyPresent() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "pink");
			Columns.replaceInDictionary(column, oldToNew);
		}

		@Test(expected = Columns.IllegalReplacementException.class)
		public void testMapAlreadyPresentOld() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "red");
			Columns.replaceInDictionary(column, oldToNew);
		}


		@Test
		public void testSingleVsMap() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Column replaced = Columns.replaceSingleInDictionary(column, "green", "yellow");
			Map<String, String> oldToNew = new HashMap<>();
			oldToNew.put("green", "yellow");
			Column replaced2 = Columns.replaceInDictionary(column, oldToNew);
			assertEquals(replaced.getDictionary().getValueList(),
					replaced2.getDictionary().getValueList());
		}

		@Test
		public void testMapOrderDoesNotMatter() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new LinkedHashMap<>();
			oldToNew.put("green", "pink");
			oldToNew.put("blue", "green");
			Column replaced = Columns.replaceInDictionary(column, oldToNew);
			assertEquals(Arrays.asList(null, "green", "pink", "red"),
					replaced.getDictionary().getValueList());
			oldToNew = new LinkedHashMap<>();
			oldToNew.put("blue", "green");
			oldToNew.put("green", "pink");
			replaced = Columns.replaceInDictionary(column, oldToNew);
			assertEquals(Arrays.asList(null, "green", "pink", "red"),
					replaced.getDictionary().getValueList());
		}

		@Test
		public void testMapSwap() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new LinkedHashMap<>();
			oldToNew.put("green", "blue");
			oldToNew.put("blue", "green");
			Column replaced = Columns.replaceInDictionary(column, oldToNew);
			assertEquals(Arrays.asList(null, "green", "blue", "red"),
					replaced.getDictionary().getValueList());
		}

		@Test
		public void testMapCyclic() {
			Column column = new SimpleCategoricalColumn(ColumnType.NOMINAL, new int[0],
					new Dictionary(Arrays.asList(null, "blue", "green", "red")));
			Map<String, String> oldToNew = new LinkedHashMap<>();
			oldToNew.put("blue", "green");
			oldToNew.put("green", "red");
			oldToNew.put("red", "blue");
			Column replaced = Columns.replaceInDictionary(column, oldToNew);
			assertEquals(Arrays.asList(null, "green", "red", "blue"),
					replaced.getDictionary().getValueList());
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
