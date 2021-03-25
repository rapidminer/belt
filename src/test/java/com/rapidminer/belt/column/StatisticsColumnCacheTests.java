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
package com.rapidminer.belt.column;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.NominalBuffer;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.column.Statistics.Statistic;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.util.Belt;


/**
 * Tests that the column cache works and is used as expected in {@link Statistics}.
 *
 * @author Kevin Majchrzak
 */
public class StatisticsColumnCacheTests {

	private static final int ROWS = 5;
	public static final Context CTX = Belt.defaultContext();

	@Test
	public void testCacheSingleStat(){
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.COUNT);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MIN);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MAX);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MEAN);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.VAR);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.SD);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.P25);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.P50);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.P75);
		checkForSingleStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MEDIAN);

		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.COUNT);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MIN);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MAX);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MEAN);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.SD);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.P25);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.P50);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.P75);
		checkForSingleStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MEDIAN);

		checkForSingleStat(Buffers.dateTimeBuffer(ROWS, false, false).toColumn(), Statistic.COUNT);
		checkForSingleStat(Buffers.dateTimeBuffer(ROWS, false, false).toColumn(), Statistic.MIN);
		checkForSingleStat(Buffers.dateTimeBuffer(ROWS, false, false).toColumn(), Statistic.MAX);

		NominalBuffer nominalBuffer = Buffers.nominalBuffer(ROWS);
		nominalBuffer.set(0, "one");
		nominalBuffer.set(1, "two");
		nominalBuffer.set(2, "three");
		nominalBuffer.set(3, "four");
		nominalBuffer.set(4, "five");
		checkForSingleStat(nominalBuffer.toColumn(), Statistic.COUNT);
		checkForSingleStat(nominalBuffer.toColumn(), Statistic.LEAST);
		checkForSingleStat(nominalBuffer.toColumn(), Statistic.MODE);

		ObjectBuffer<StringSet> textsetBuffer = Buffers.textsetBuffer(ROWS);
		textsetBuffer.set(0, new StringSet(Collections.singleton("one")));
		textsetBuffer.set(1, new StringSet(Collections.singleton("two")));
		textsetBuffer.set(2, new StringSet(Collections.singleton("three")));
		textsetBuffer.set(3, new StringSet(Collections.singleton("four")));
		textsetBuffer.set(4, new StringSet(Collections.singleton("five")));
		checkForSingleStat(textsetBuffer.toColumn(), Statistic.COUNT);
	}

	@Test
	public void testCacheMultiStat(){
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.COUNT);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MIN);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MAX);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MEAN);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.VAR);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.SD);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.P25);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.P50);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.P75);
		checkForMultiStat(Buffers.realBuffer(ROWS, false).toColumn(), Statistic.MEDIAN);

		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.COUNT);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MIN);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MAX);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MEAN);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.SD);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.P25);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.P50);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.P75);
		checkForMultiStat(Buffers.timeBuffer(ROWS, false).toColumn(), Statistic.MEDIAN);

		checkForMultiStat(Buffers.dateTimeBuffer(ROWS, false, false).toColumn(), Statistic.COUNT);
		checkForMultiStat(Buffers.dateTimeBuffer(ROWS, false, false).toColumn(), Statistic.MIN);
		checkForMultiStat(Buffers.dateTimeBuffer(ROWS, false, false).toColumn(), Statistic.MAX);

		NominalBuffer nominalBuffer = Buffers.nominalBuffer(ROWS);
		nominalBuffer.set(0, "one");
		nominalBuffer.set(1, "two");
		nominalBuffer.set(2, "three");
		nominalBuffer.set(3, "four");
		nominalBuffer.set(4, "five");
		checkForMultiStat(nominalBuffer.toColumn(), Statistic.COUNT);
		checkForMultiStat(nominalBuffer.toColumn(), Statistic.LEAST);
		checkForMultiStat(nominalBuffer.toColumn(), Statistic.MODE);

		ObjectBuffer<StringSet> textsetBuffer = Buffers.textsetBuffer(ROWS);
		textsetBuffer.set(0, new StringSet(Collections.singleton("one")));
		textsetBuffer.set(1, new StringSet(Collections.singleton("two")));
		textsetBuffer.set(2, new StringSet(Collections.singleton("three")));
		textsetBuffer.set(3, new StringSet(Collections.singleton("four")));
		textsetBuffer.set(4, new StringSet(Collections.singleton("five")));
		checkForMultiStat(textsetBuffer.toColumn(), Statistic.COUNT);
	}

	@Test
	public void testParallel() throws InterruptedException {
		final Column column = spy(Buffers.realBuffer(100).toColumn());
		final ExecutorService service = Executors.newFixedThreadPool(10);
		Runnable[] runnables = new Runnable[200];
		Arrays.setAll(runnables, i -> (() -> Statistics.compute(column, Statistic.COUNT, CTX)));
		for (Runnable runnable : runnables) {
			service.execute(runnable);
		}
		service.shutdown();
		service.awaitTermination(1, TimeUnit.MINUTES);
		verify(column, times(1)).cacheStat(eq(Statistic.COUNT), any(Statistics.Result.class));
	}

	private void checkForSingleStat(Column column, Statistic stat) {
		assertTrue(allStatsNull(column));
		Statistics.compute(column, stat, CTX);
		assertNotNull(column.getStat(stat));
	}

	private void checkForMultiStat(Column column, Statistic stat) {
		assertTrue(allStatsNull(column));
		Set<Statistic> stats = new HashSet<>();
		stats.add(stat);
		Statistics.compute(column, stats, CTX);
		assertNotNull(column.getStat(stat));
	}

	private boolean allStatsNull(Column column) {
		for (Statistic stat : Statistic.values()) {
			if (column.getStat(stat) != null) {
				return false;
			}
		}
		return true;
	}

}
