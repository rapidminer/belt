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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Columns;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.util.Belt;


/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class TablesTests {

	private static final Context CTX = Belt.defaultContext();

	public static class Adapt {

		@Test(expected = NullPointerException.class)
		public void testNullFromTable() {
			Tables.adapt(null, new Table(1), Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.CHANGE);
		}

		@Test(expected = NullPointerException.class)
		public void testNullToTable() {
			Tables.adapt(new Table(1), null, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.CHANGE);
		}

		@Test
		public void testEmptyFromTable() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(new Table(1), test, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
					Tables.DictionaryHandling.CHANGE);
			assertEquals(0, adjusted.width());
			assertEquals(1, adjusted.height());
		}

		@Test
		public void testEmptyToTableKeepAdditional() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, new Table(1), Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
					Tables.DictionaryHandling.CHANGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testEmptyToTableUnchanged() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, new Table(1), Tables.ColumnHandling.UNCHANGED,
							Tables.DictionaryHandling.UNCHANGED);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testEmptyToTableReorder() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, new Table(1), Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.CHANGE);
			assertEquals(0, adjusted.width());
			assertEquals(test.height(), adjusted.height());
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnHandling() {
			Tables.adapt(new Table(1), new Table(5), null, Tables.DictionaryHandling.CHANGE);
		}

		@Test(expected = NullPointerException.class)
		public void testNullDictionaryHandling() {
			Tables.adapt(new Table(1), new Table(5), Tables.ColumnHandling.REORDER, null);
		}

		@Test
		public void testDifferentToTableUnchanged() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.UNCHANGED);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testDifferentToTableUnchangedChange() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED,
					Tables.DictionaryHandling.CHANGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testDifferentToTableUnchangedMerge() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED,
					Tables.DictionaryHandling.MERGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testDifferentToTableReorder() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER,
					Tables.DictionaryHandling.UNCHANGED);
			assertEquals(0, adjusted.width());
			assertEquals(test.height(), adjusted.height());
		}

		@Test
		public void testDifferentToTableReorderChange() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER,
					Tables.DictionaryHandling.CHANGE);
			assertEquals(0, adjusted.width());
			assertEquals(test.height(), adjusted.height());
		}

		@Test
		public void testDifferentToTableReorderMerge() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.MERGE);
			assertEquals(0, adjusted.width());
			assertEquals(test.height(), adjusted.height());
		}

		@Test
		public void testDifferentToTableKeep() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
					Tables.DictionaryHandling.UNCHANGED);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testDifferentToTableKeepChange() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
					Tables.DictionaryHandling.CHANGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testDifferentToTableKeepMerge() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2",
					i -> "val" + i)
					.addTime("time2", i -> LocalTime.NOON).build(CTX);

			Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
					Tables.DictionaryHandling.MERGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testSimilarToTableUnchanged() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.UNCHANGED);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertArrayEquals(test.getColumns(), adjusted.getColumns());
		}

		@Test
		public void testSimilarToTableUnchangedChange() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.CHANGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertEquals(Arrays.asList(test.getColumns()[0], test.getColumns()[2]),
					Arrays.asList(adjusted.getColumns()[0], adjusted.getColumns()[2]));
			assertEquals(ColumnAccessor.get().getDictionaryList(test2.column(1).getDictionary()),
					ColumnAccessor.get().getDictionaryList(adjusted.column(1).getDictionary()));
		}

		@Test
		public void testSimilarToTableUnchangedMerge() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.MERGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels(), adjusted.labels());
			assertEquals(Arrays.asList(test.getColumns()[0], test.getColumns()[2]),
					Arrays.asList(adjusted.getColumns()[0], adjusted.getColumns()[2]));
			assertEquals(ColumnAccessor.get().getDictionaryList(test.column(1).getDictionary()),
					ColumnAccessor.get().getDictionaryList(adjusted.column(1).getDictionary()));
		}

		@Test
		public void testSimilarToTableReorderUnchanged() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.UNCHANGED);
			assertEquals(test.width() - 1, adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels().subList(1, 3), adjusted.labels());
			assertEquals(Arrays.asList(test.getColumns()).subList(1, 3), Arrays.asList(adjusted.getColumns()));
		}

		@Test
		public void testSimilarToTableReorderChange() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.CHANGE);
			assertEquals(test.width() - 1, adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels().subList(1, 3), adjusted.labels());
			assertEquals(test.getColumns()[2], adjusted.getColumns()[1]);
			assertEquals(test2.getColumns()[1].getDictionary(),
					adjusted.getColumns()[0].getDictionary());
		}

		@Test
		public void testSimilarToTableReorderMerge() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.MERGE);
			assertEquals(test.width() - 1, adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(test.labels().subList(1, 3), adjusted.labels());
			assertEquals(test.getColumns()[2], adjusted.getColumns()[1]);
			assertEquals(ColumnAccessor.get().getDictionaryList(test.getColumns()[1].getDictionary()),
					ColumnAccessor.get().getDictionaryList(adjusted.getColumns()[0].getDictionary()));
		}

		@Test
		public void testSimilarToTableReorderKeepUnchanged() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
							Tables.DictionaryHandling.UNCHANGED);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(Arrays.asList(test.label(1), test.label(2), test.label(0)), adjusted.labels());
			assertEquals(Arrays.asList(test.getColumns()[1], test.getColumns()[2], test.getColumns()[0]),
					Arrays.asList(adjusted.getColumns()));
		}

		@Test
		public void testSimilarToTableReorderKeepChange() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
							Tables.DictionaryHandling.CHANGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(Arrays.asList(test.label(1), test.label(2), test.label(0)), adjusted.labels());
			assertEquals(Arrays.asList(test.getColumns()[2], test.getColumns()[0]),
					Arrays.asList(adjusted.getColumns()).subList(1, 3));
			assertEquals(test2.column(1).getDictionary(), adjusted.column(0).getDictionary());
		}

		@Test
		public void testSimilarToTableReorderKeepMerge() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Table adjusted =
					Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
							Tables.DictionaryHandling.MERGE);
			assertEquals(test.width(), adjusted.width());
			assertEquals(test.height(), adjusted.height());
			assertEquals(Arrays.asList(test.label(1), test.label(2), test.label(0)), adjusted.labels());
			assertEquals(Arrays.asList(test.getColumns()[2], test.getColumns()[0]),
					Arrays.asList(adjusted.getColumns()).subList(1, 3));
			assertEquals(ColumnAccessor.get().getDictionaryList(test.column(1).getDictionary()),
					ColumnAccessor.get().getDictionaryList(adjusted.column(0).getDictionary()));
		}
	}

	public static class GapRemoval {

		@Test
		public void testGapRemoval(){
			final NominalBuffer nominalBuffer = Buffers.nominalBuffer(100);
			nominalBuffer.set(0, "red");
			nominalBuffer.set(1, "green");
			for (int i = 1; i <= 50; i++) {
				nominalBuffer.set(i, "blue");
			}
			for (int i = 51; i < 100; i++) {
				nominalBuffer.set(i, "green");
			}
			Table table =
					Builders.newTableBuilder(100).add("nominal", nominalBuffer.toColumn()).add("nominal2",
							nominalBuffer.toColumn()).add("nominal3", nominalBuffer.toColumn()).build(Belt.defaultContext());
			table = table.rows(0, 50, Belt.defaultContext());
			table =
					Builders.newTableBuilder(50).add("nominal",
							Columns.removeUnusedDictionaryValues(table.column("nominal"), Columns.CleanupOption.REMOVE,
									Belt.defaultContext())).add("nominal2",
							table.column("nominal2")).add("nominal3",
							Columns.removeUnusedDictionaryValues(table.column("nominal3"), Columns.CleanupOption.COMPACT,
									Belt.defaultContext())).build(Belt.defaultContext());
			final Table compacted = Tables.compactDictionaries(table);
			assertNotSame(table, compacted);
			for (Column column : compacted.columnList()) {
				assertEquals(column.getDictionary().maximalIndex(), column.getDictionary().size());
			}
		}

		@Test
		public void testNoGapRemoval(){
			final NominalBuffer nominalBuffer = Buffers.nominalBuffer(100);
			nominalBuffer.set(0, "red");
			nominalBuffer.set(1, "green");
			for (int i = 1; i <= 50; i++) {
				nominalBuffer.set(i, "blue");
			}
			for (int i = 51; i < 100; i++) {
				nominalBuffer.set(i, "green");
			}
			Table table =
					Builders.newTableBuilder(100).add("nominal", nominalBuffer.toColumn()).add("nominal2",
							nominalBuffer.toColumn()).add("nominal3", nominalBuffer.toColumn()).build(Belt.defaultContext());
			table = table.rows(0, 50, Belt.defaultContext());
			final Table compacted = Tables.compactDictionaries(table);
			assertSame(table, compacted);
			for (Column column : compacted.columnList()) {
				assertEquals(column.getDictionary().maximalIndex(), column.getDictionary().size());
			}
		}
	}

	public static class Incompatible {

		@Test(expected = NullPointerException.class)
		public void testNullFromTable() {
			Tables.findIncompatible(null, new Table(1), Tables.ColumnSetRequirement.SUBSET);
		}

		@Test(expected = NullPointerException.class)
		public void testNullToTable() {
			Tables.findIncompatible(new Table(1), null, Tables.ColumnSetRequirement.SUBSET);
		}

		@Test
		public void testEmptySchema() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, new Table(1),
					Tables.ColumnSetRequirement.SUPERSET, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES,
					Tables.TypeRequirement.ALLOW_INT_FOR_REAL);
			assertEquals(0, incompatible.size());
		}

		@Test
		public void testEmptySchemaSubset() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, new Table(1),
					Tables.ColumnSetRequirement.SUBSET, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES,
					Tables.TypeRequirement.ALLOW_INT_FOR_REAL);
			assertEquals(3, incompatible.size());
			assertEquals(new HashSet<>(test.labels()), incompatible.keySet());
			assertEquals(Arrays.asList(Tables.Incompatibility.WRONG_COLUMN_PRESENT,
					Tables.Incompatibility.WRONG_COLUMN_PRESENT, Tables.Incompatibility.WRONG_COLUMN_PRESENT),
					new ArrayList<>(incompatible.values()));
		}

		@Test
		public void testEmptyTable() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(new Table(1), test,
					Tables.ColumnSetRequirement.SUPERSET);
			assertEquals(3, incompatible.size());
			assertEquals(new HashSet<>(test.labels()), incompatible.keySet());
			assertEquals(Arrays.asList(Tables.Incompatibility.MISSING_COLUMN,
					Tables.Incompatibility.MISSING_COLUMN, Tables.Incompatibility.MISSING_COLUMN),
					new ArrayList<>(incompatible.values()));
		}

		@Test
		public void testEmptyTableSubset() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(new Table(1), test,
					Tables.ColumnSetRequirement.SUBSET);
			assertTrue(incompatible.isEmpty());
		}

		@Test(expected = NullPointerException.class)
		public void testNullColumnHandling() {
			Tables.findIncompatible(new Table(1), new Table(5), null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypeHandling() {
			Tables.findIncompatible(new Table(1), new Table(5), Tables.ColumnSetRequirement.SUPERSET,
					(Tables.TypeRequirement[]) null);
		}

		@Test
		public void testMissingAndWrong() {
			Table test = Builders.newTableBuilder(10).addReal("real2", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, test2,
					Tables.ColumnSetRequirement.EQUAL, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES);
			assertEquals(2, incompatible.size());
			Map<String, Tables.Incompatibility> expected = new HashMap<>();
			expected.put("real", Tables.Incompatibility.MISSING_COLUMN);
			expected.put("real2", Tables.Incompatibility.WRONG_COLUMN_PRESENT);
			assertEquals(expected, incompatible);
		}

		@Test
		public void testEqualToMuch() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).addInt53Bit("int", i -> i).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, test2,
					Tables.ColumnSetRequirement.EQUAL, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES);
			assertEquals(1, incompatible.size());
			assertEquals(Collections.singletonMap("int", Tables.Incompatibility.WRONG_COLUMN_PRESENT), incompatible);
		}

		@Test
		public void testEqualMuchMore() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).addInt53Bit("int", i -> i)
					.addInt53Bit("int2", i -> i).addInt53Bit("int3", i -> i)
					.addInt53Bit("int4", i -> i).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, test2,
					Tables.ColumnSetRequirement.EQUAL, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES);
			Map<String, Tables.Incompatibility> expected = new HashMap<>();
			expected.put("int", Tables.Incompatibility.WRONG_COLUMN_PRESENT);
			expected.put("int2", Tables.Incompatibility.WRONG_COLUMN_PRESENT);
			expected.put("int3", Tables.Incompatibility.WRONG_COLUMN_PRESENT);
			expected.put("int4", Tables.Incompatibility.WRONG_COLUMN_PRESENT);
			assertEquals(expected, incompatible);
		}

		@Test
		public void testDifferentType() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addInt53Bit("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test2, test,
					Tables.ColumnSetRequirement.EQUAL, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES);
			assertEquals(1, incompatible.size());
			assertEquals(Collections.singletonMap("real", Tables.Incompatibility.TYPE_MISMATCH), incompatible);
		}

		@Test
		public void testDifferentTypeInt() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(1).addInt53Bit("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test2, test,
					Tables.ColumnSetRequirement.SUBSET, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES,
					Tables.TypeRequirement.ALLOW_INT_FOR_REAL);
			assertEquals(0, incompatible.size());
		}

		@Test
		public void testDifferentDict() {
			Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);
			Table test2 = Builders.newTableBuilder(5).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, test2,
					Tables.ColumnSetRequirement.SUPERSET, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES,
					Tables.TypeRequirement.REQUIRE_SUB_DICTIONARIES);
			assertEquals(1, incompatible.size());
			assertEquals(Collections.singletonMap("nominal", Tables.Incompatibility.NOT_SUB_DICTIONARY), incompatible);
		}

		@Test
		public void testEverything() {
			Table test = Builders.newTableBuilder(10).addInt53Bit("real", i -> i).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).addText("text", i -> null).addTextset("textset", i -> null).build(CTX);
			Table test2 = Builders.newTableBuilder(5).addReal("real", i -> i * 0.3).addNominal("nominal",
					i -> "val" + i)
					.addTime("time", i -> LocalTime.NOON).addTextset("text", i -> null).build(CTX);

			Map<String, Tables.Incompatibility> incompatible = Tables.findIncompatible(test, test2,
					Tables.ColumnSetRequirement.SUBSET, Tables.TypeRequirement.REQUIRE_MATCHING_TYPES,
					Tables.TypeRequirement.REQUIRE_SUB_DICTIONARIES, Tables.TypeRequirement.ALLOW_INT_FOR_REAL);
			Map<String, Tables.Incompatibility> expected = new HashMap<>();
			expected.put("nominal", Tables.Incompatibility.NOT_SUB_DICTIONARY);
			expected.put("text", Tables.Incompatibility.TYPE_MISMATCH);
			expected.put("textset", Tables.Incompatibility.WRONG_COLUMN_PRESENT);
			assertEquals(expected, incompatible);
		}
	}
}
