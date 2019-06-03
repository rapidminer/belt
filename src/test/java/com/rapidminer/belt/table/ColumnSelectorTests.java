/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.TimeColumn;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.table.ColumnAccessor;
import com.rapidminer.belt.table.ColumnSelector;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.util.Belt;
import com.rapidminer.belt.util.ColumnAnnotation;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnRole;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ColumnSelectorTests {

	public static class InputValidation {

		ColumnSelector selector = new Table(5).select();

		@Test(expected = NullPointerException.class)
		public void testWithNullTable() {
			new ColumnSelector(null);
		}

		@Test(expected = NullPointerException.class)
		public void testwithTypeId() {
			selector.ofTypeId(null);
		}

		@Test(expected = NullPointerException.class)
		public void testwithoutTypeId() {
			selector.notOfTypeId(null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithCategory() {
			selector.ofCategory(null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithoutCategory() {
			selector.notOfCategory(null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithCapability() {
			selector.withCapability(null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithoutCapability() {
			selector.withoutCapability(null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithMetaData() {
			selector.withMetaData((ColumnMetaData)null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithoutMetaData() {
			selector.withoutMetaData((ColumnMetaData)null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithMetaDataType() {
			selector.withMetaData((Class<ColumnMetaData>)null);
		}

		@Test(expected = NullPointerException.class)
		public void testWithoutMetaDataType() {
			selector.withoutMetaData((Class<ColumnMetaData>)null);
		}

	}


	public static class MetaData{

		static class DummyData implements ColumnMetaData {

			@Override
			public String type() {
				return "moo";
			}
		}

		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Builders.newTableBuilder(100)
					.addReal("zero", i -> 0)
					.addReal("one", i -> 1)
					.addReal("two", i -> 2)
					.addReal("three", i -> 3)
					.addReal("four", i -> 4)
					.addMetaData("zero", ColumnRole.ID)
					.addMetaData("one", ColumnRole.METADATA)
					.addMetaData("one", new ColumnAnnotation("First annotation"))
					.addMetaData("one", new ColumnAnnotation("Second annotation"))
					.addMetaData("three", ColumnRole.METADATA)
					.addMetaData("four", ColumnRole.LABEL)
					.build(Belt.defaultContext());
		}

		@Test
		public void testTypeLookupWithoutMatch() {
			ColumnSelector selector = table.select().withMetaData(DummyData.class);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testTypeLookupWithSingleMatch() {
			ColumnSelector selector = table.select().withMetaData(ColumnAnnotation.class);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("one");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testTypeLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.class);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "one", "three", "four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInstanceLookupWithoutMatch() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.CLUSTER);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testInstanceLookupWithSingleMatch() {
			ColumnSelector selector = table.select().withMetaData(new ColumnAnnotation("First annotation"));
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("one");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInstanceLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.METADATA);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("one", "three");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseTypeLookupWithoutMatch() {
			ColumnSelector selector = table.select().withoutMetaData(DummyData.class);
			List<String> matches = selector.labels();
			assertEquals(table.labels(), matches);
			assertEquals(ColumnSelector.toColumnList(table.labels(), table), selector.columns());
		}

		@Test
		public void testInverseTypeLookupWithSingleMatch() {
			ColumnSelector selector = table.select().withoutMetaData(ColumnAnnotation.class);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "two", "three", "four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseTypeLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().withoutMetaData(ColumnRole.class);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("two");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseInstanceLookupWithoutMatch() {
			ColumnSelector selector = table.select().withoutMetaData(ColumnRole.CLUSTER);
			List<String> matches = selector.labels();
			assertEquals(table.labels(), matches);
			assertEquals(ColumnSelector.toColumnList(table.labels(), table), selector.columns());
		}

		@Test
		public void testInverseInstanceLookupWithSingleMatch() {
			ColumnSelector selector = table.select().withoutMetaData(new ColumnAnnotation("First annotation"));
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "two", "three", "four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseInstanceLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().withoutMetaData(ColumnRole.METADATA);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "two", "four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}
	}

	public static class ColumnInformation{

		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Builders.newTableBuilder(100)
					.addReal("zero", i -> 0)
					.addInt("one", i -> 1)
					.addNominal("two", i -> "bla")
					.addTime("three", i -> LocalTime.NOON)
					.addDateTime("four", i -> Instant.EPOCH)
					.addInt("five", i -> 0)
					.addMetaData("zero", ColumnRole.ID)
					.addMetaData("one", ColumnRole.METADATA)
					.addMetaData("one", new ColumnAnnotation("First annotation"))
					.addMetaData("one", new ColumnAnnotation("Second annotation"))
					.addMetaData("three", ColumnRole.METADATA)
					.addMetaData("four", ColumnRole.LABEL)
					.build(Belt.defaultContext());
		}

		@Test
		public void testTypeLookupWithoutMatch() {
			ColumnSelector selector = table.select().ofTypeId(Column.TypeId.CUSTOM);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testTypeLookupWithSingleMatch() {
			ColumnSelector selector = table.select().ofTypeId(Column.TypeId.REAL);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("zero");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testTypeLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().ofTypeId(Column.TypeId.INTEGER);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("one", "five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testCategoryLookupWithoutMatch() {
			ColumnSelector selector =
					table.columns(new int[]{0, 1, 2, 5}).select().ofCategory(Column.Category.OBJECT);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testCategoryLookupWithSingleMatch() {
			ColumnSelector selector = table.select().ofCategory(Column.Category.CATEGORICAL);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("two");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testCategoryLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().ofCategory(Column.Category.NUMERIC);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "one", "five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testCapabilityLookupWithoutMatch() {
			ColumnSelector selector =
					table.columns(new int[]{0, 1, 5}).select().withCapability(Column.Capability.OBJECT_READABLE);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testCapabilityLookupWithSingleMatch() {
			ColumnSelector selector = table.columns(new int[]{0, 1, 2, 5}).select().withCapability(Column.Capability.OBJECT_READABLE);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("two");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testCapabilityLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().withCapability(Column.Capability.NUMERIC_READABLE);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "one", "two", "three", "five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseTypeLookupWithoutMatch() {
			ColumnSelector selector = table.select().notOfTypeId(Column.TypeId.CUSTOM);
			List<String> matches = selector.labels();
			assertEquals(table.labels(), matches);
			assertEquals(ColumnSelector.toColumnList(table.labels(), table), selector.columns());
		}

		@Test
		public void testInverseTypeLookupWithSingleMatch() {
			ColumnSelector selector = table.select().notOfTypeId(Column.TypeId.REAL);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("one", "two", "three", "four", "five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseTypeLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().notOfTypeId(Column.TypeId.INTEGER);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "two", "three", "four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseCategoryLookupWithoutMatch() {
			Table table = ColumnInformation.table.columns(new int[]{0, 1, 2, 5});
			ColumnSelector selector = table.select().notOfCategory(Column.Category.OBJECT);
			List<String> matches = selector.labels();
			assertEquals(table.labels(), matches);
			assertEquals(ColumnSelector.toColumnList(table.labels(), ColumnInformation.table), selector.columns());
		}

		@Test
		public void testInverseCategoryLookupWithSingleMatch() {
			ColumnSelector selector = table.select().notOfCategory(Column.Category.CATEGORICAL);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "one", "three", "four", "five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseCategoryLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().notOfCategory(Column.Category.NUMERIC);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("two", "three", "four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseCapabilityLookupWithoutMatch() {
			Table table = ColumnInformation.table.columns(new int[]{0, 1, 5});
			ColumnSelector selector = table.select().withoutCapability(Column.Capability.OBJECT_READABLE);
			List<String> matches = selector.labels();
			assertEquals(table.labels(), matches);
			assertEquals(ColumnSelector.toColumnList(table.labels(), table), selector.columns());
		}

		@Test
		public void testInverseCapabilityLookupWithSingleMatch() {
			Table table = ColumnInformation.table.columns(new int[]{0, 1, 2, 5});
			ColumnSelector selector = table.select().withoutCapability(Column.Capability.OBJECT_READABLE);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("zero", "one", "five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testInverseCapabilityLookupWithMultipleMatches() {
			ColumnSelector selector = table.select().withoutCapability(Column.Capability.NUMERIC_READABLE);
			List<String> matches = selector.labels();
			List<String> expected = Arrays.asList("four");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}
	}


	public static class Mixed {

		private static Table table;

		@BeforeClass
		public static void createTable() {
			table = Builders.newTableBuilder(100)
					.addReal("zero", i -> 0)
					.addInt("one", i -> 1)
					.addNominal("two", i -> "bla")
					.addTime("three", i -> LocalTime.NOON)
					.addDateTime("four", i -> Instant.EPOCH)
					.addInt("five", i -> 0)
					.addMetaData("zero", ColumnRole.ID)
					.addMetaData("one", ColumnRole.METADATA)
					.addMetaData("one", new ColumnAnnotation("First annotation"))
					.addMetaData("one", new ColumnAnnotation("Second annotation"))
					.addMetaData("three", ColumnRole.METADATA)
					.addMetaData("four", ColumnRole.LABEL)
					.addMetaData("five", ColumnRole.LABEL)
					.build(Belt.defaultContext());
		}

		@Test
		public void testTwoTypes() {
			ColumnSelector selector = table.select().ofTypeId(Column.TypeId.INTEGER).ofTypeId(Column.TypeId.REAL);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testTwoCategories() {
			ColumnSelector selector = table.select().ofCategory(Column.Category.NUMERIC).ofCategory(Column.Category.CATEGORICAL);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testNotTwoCategories() {
			ColumnSelector selector = table.select().notOfCategory(Column.Category.CATEGORICAL).notOfCategory(Column.Category.NUMERIC);
			List<String> matches = selector.labels();
			List<String> expected =table.select().ofCategory(Column.Category.OBJECT).labels();
			assertEquals(expected, matches);
			List<Column> expected2 =table.select().ofCategory(Column.Category.OBJECT).columns();
			assertEquals(expected2, selector.columns());
		}

		@Test
		public void testTwoRoles() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.LABEL).withMetaData(ColumnRole.ID);
			List<String> matches = selector.labels();
			assertTrue(matches.isEmpty());
			assertTrue(selector.columns().isEmpty());
		}

		@Test
		public void testTwoMetaData() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.METADATA).withMetaData(new ColumnAnnotation("First annotation"));
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("one");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testNumericLabel() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.LABEL).ofCategory(Column.Category.NUMERIC);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("five");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testNominalLabel() {
			ColumnSelector selector = table.select().ofTypeId(Column.TypeId.NOMINAL).withMetaData(ColumnRole.LABEL);
			List<String> matches = selector.labels();
			List<String> expected = Collections.emptyList();
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testRegularNumericReadable() {
			ColumnSelector selector = table.select().withCapability(Column.Capability.NUMERIC_READABLE).withoutMetaData(ColumnRole.class);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("two");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testRegularNominal() {
			ColumnSelector selector = table.select().withoutMetaData(ColumnRole.class).ofTypeId(Column.TypeId.NOMINAL);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("two");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testManyConditions() {
			ColumnSelector selector = table.select().withMetaData(ColumnRole.class).ofTypeId(Column.TypeId.INTEGER)
					.withMetaData(ColumnRole.METADATA).ofCategory(
							Column.Category.NUMERIC).notOfCategory(Column.Category.OBJECT)
					.withCapability(Column.Capability.SORTABLE).withoutCapability(
							Column.Capability.OBJECT_READABLE).notOfTypeId(Column.TypeId.TIME);
			List<String> matches = selector.labels();
			List<String> expected = Collections.singletonList("one");
			assertEquals(expected, matches);
			assertEquals(ColumnSelector.toColumnList(expected, table), selector.columns());
		}

		@Test
		public void testNoSelection() {
			ColumnSelector selector = table.select();
			assertEquals(table.labels(), selector.labels());
			assertEquals(ColumnSelector.toColumnList(table.labels(), table), selector.columns());
		}
	}

	public static class ToColumnList {

		@Test(expected = NullPointerException.class)
		public void testNullList() {
			ColumnSelector.toColumnList(null, new Table(3));
		}

		@Test(expected = NullPointerException.class)
		public void testNullTable() {
			ColumnSelector.toColumnList(Collections.emptyList(), null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullInList() {
			Table table = new Table(
					new Column[]{ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10]),
							ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10])},
					new String[]{"x", "y"});
			ColumnSelector.toColumnList(Arrays.asList("x", null), table);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongInList() {
			Table table = new Table(
					new Column[]{ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10]),
							ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10])},
					new String[]{"x", "y"});
			ColumnSelector.toColumnList(Arrays.asList("x", "a"), table);
		}

		@Test
		public void testEmptyList() {
			Table table = new Table(new Column[]{ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10])},
					new String[]{"x"});
			assertTrue(ColumnSelector.toColumnList(Collections.emptyList(), table).isEmpty());
		}

		@Test
		public void testSingle() {
			Column column = ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10]);
			Table table = new Table(
					new Column[]{ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10]), column},
					new String[]{"x", "y"});
			List<Column> result = ColumnSelector.toColumnList(Collections.singletonList("y"), table);
			assertEquals(Collections.singletonList(column), result);
		}

		@Test
		public void testMore() {
			Column column1 = ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10]);
			TimeColumn column2 = ColumnAccessor.get().newTimeColumn(new long[10]);
			Table table = new Table(
					new Column[]{ColumnAccessor.get().newNumericColumn(TypeId.REAL, new double[10]), column1, column2},
					new String[]{"x", "y", "z"});
			List<Column> result = ColumnSelector.toColumnList(Arrays.asList("z", "y"), table);
			assertEquals(Arrays.asList(column2, column1), result);
		}
	}
}
