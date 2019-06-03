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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.time.LocalTime;
import java.util.Arrays;

import org.junit.Test;

import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.table.Builders;
import com.rapidminer.belt.table.ColumnAccessor;
import com.rapidminer.belt.table.Table;
import com.rapidminer.belt.table.Tables;
import com.rapidminer.belt.util.Belt;


/**
 * @author Gisa Meier
 */
public class TablesTests {

	private static final Context CTX = Belt.defaultContext();

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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted = Tables.adapt(new Table(1), test, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
				Tables.DictionaryHandling.CHANGE);
		assertEquals(0, adjusted.width());
		assertEquals(1, adjusted.height());
	}

	@Test
	public void testEmptyToTableKeepAdditional() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
				.addTime("time2", i -> LocalTime.NOON).build(CTX);

		Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.CHANGE);
		assertEquals(test.width(), adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(test.labels(), adjusted.labels());
		assertArrayEquals(test.getColumns(), adjusted.getColumns());
	}

	@Test
	public void testDifferentToTableUnchangedMerge() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
				.addTime("time2", i -> LocalTime.NOON).build(CTX);

		Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.MERGE);
		assertEquals(test.width(), adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(test.labels(), adjusted.labels());
		assertArrayEquals(test.getColumns(), adjusted.getColumns());
	}

	@Test
	public void testDifferentToTableReorder() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
				.addTime("time2", i -> LocalTime.NOON).build(CTX);

		Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER,
				Tables.DictionaryHandling.UNCHANGED);
		assertEquals(0, adjusted.width());
		assertEquals(test.height(), adjusted.height());
	}

	@Test
	public void testDifferentToTableReorderChange() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
				.addTime("time2", i -> LocalTime.NOON).build(CTX);

		Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.CHANGE);
		assertEquals(0, adjusted.width());
		assertEquals(test.height(), adjusted.height());
	}

	@Test
	public void testDifferentToTableReorderMerge() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
				.addTime("time2", i -> LocalTime.NOON).build(CTX);

		Table adjusted = Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.MERGE);
		assertEquals(0, adjusted.width());
		assertEquals(test.height(), adjusted.height());
	}

	@Test
	public void testDifferentToTableKeep() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal2", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted =
				Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.CHANGE);
		assertEquals(test.width(), adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(test.labels(), adjusted.labels());
		assertEquals(Arrays.asList(test.getColumns()[0], test.getColumns()[2]),
				Arrays.asList(adjusted.getColumns()[0], adjusted.getColumns()[2]));
		assertEquals(ColumnAccessor.get().getDictionaryList(test2.column(1).getDictionary(Object.class)),
				ColumnAccessor.get().getDictionaryList(adjusted.column(1).getDictionary(Object.class)));
	}

	@Test
	public void testSimilarToTableUnchangedMerge() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted =
				Tables.adapt(test, test2, Tables.ColumnHandling.UNCHANGED, Tables.DictionaryHandling.MERGE);
		assertEquals(test.width(), adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(test.labels(), adjusted.labels());
		assertEquals(Arrays.asList(test.getColumns()[0], test.getColumns()[2]),
				Arrays.asList(adjusted.getColumns()[0], adjusted.getColumns()[2]));
		assertEquals(ColumnAccessor.get().getDictionaryList(test.column(1).getDictionary(Object.class)),
				ColumnAccessor.get().getDictionaryList(adjusted.column(1).getDictionary(Object.class)));
	}

	@Test
	public void testSimilarToTableReorderUnchanged() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted =
				Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.CHANGE);
		assertEquals(test.width() - 1, adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(test.labels().subList(1, 3), adjusted.labels());
		assertEquals(test.getColumns()[2], adjusted.getColumns()[1]);
		assertEquals(test2.getColumns()[1].getDictionary(Object.class),
				adjusted.getColumns()[0].getDictionary(Object.class));
	}

	@Test
	public void testSimilarToTableReorderMerge() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted =
				Tables.adapt(test, test2, Tables.ColumnHandling.REORDER, Tables.DictionaryHandling.MERGE);
		assertEquals(test.width() - 1, adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(test.labels().subList(1, 3), adjusted.labels());
		assertEquals(test.getColumns()[2], adjusted.getColumns()[1]);
		assertEquals(ColumnAccessor.get().getDictionaryList(test.getColumns()[1].getDictionary(Object.class)),
				ColumnAccessor.get().getDictionaryList(adjusted.getColumns()[0].getDictionary(Object.class)));
	}

	@Test
	public void testSimilarToTableReorderKeepUnchanged() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
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
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted =
				Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
						Tables.DictionaryHandling.CHANGE);
		assertEquals(test.width(), adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(Arrays.asList(test.label(1), test.label(2), test.label(0)), adjusted.labels());
		assertEquals(Arrays.asList(test.getColumns()[2], test.getColumns()[0]),
				Arrays.asList(adjusted.getColumns()).subList(1, 3));
		assertEquals(test2.column(1).getDictionary(Object.class), adjusted.column(0).getDictionary(Object.class));
	}

	@Test
	public void testSimilarToTableReorderKeepMerge() {
		Table test = Builders.newTableBuilder(10).addReal("real", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);
		Table test2 = Builders.newTableBuilder(1).addReal("real2", i -> i * 0.3).addNominal("nominal", i -> "val" + i)
				.addTime("time", i -> LocalTime.NOON).build(CTX);

		Table adjusted =
				Tables.adapt(test, test2, Tables.ColumnHandling.REORDER_KEEP_ADDITIONAL,
						Tables.DictionaryHandling.MERGE);
		assertEquals(test.width(), adjusted.width());
		assertEquals(test.height(), adjusted.height());
		assertEquals(Arrays.asList(test.label(1), test.label(2), test.label(0)), adjusted.labels());
		assertEquals(Arrays.asList(test.getColumns()[2], test.getColumns()[0]),
				Arrays.asList(adjusted.getColumns()).subList(1, 3));
		assertEquals(ColumnAccessor.get().getDictionaryList(test.column(1).getDictionary(Object.class)),
				ColumnAccessor.get().getDictionaryList(adjusted.column(0).getDictionary(Object.class)));
	}
}
