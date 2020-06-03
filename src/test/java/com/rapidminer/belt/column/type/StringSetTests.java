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

package com.rapidminer.belt.column.type;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.junit.Test;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.util.Order;


/**
 * @author Gisa Meier
 */
public class StringSetTests {

	@Test
	public void testConstructors() {
		List<String> list = Arrays.asList("a", null, "cc", "b", null, "d");
		StringSet stringSet1 = new StringSet(list, v -> v != null ? v + "!" : v);
		StringSet stringSet2 = new StringSet(i -> list.get(i) == null ? null : list.get(i) + "!", list.size());

		assertTrue(stringSet1.equals(stringSet2));
		assertTrue(stringSet2.equals(stringSet1));
		assertTrue(stringSet1.containsAll(stringSet2));
		assertEquals(stringSet1.compareTo(stringSet2), 0);
		assertEquals(stringSet1.hashCode(), stringSet2.hashCode());
	}

	@Test
	public void testConstructors2() {
		List<String> list = Arrays.asList("a", null, "cc", "b", null, "d");
		StringSet stringSet1 = new StringSet(list, v -> v != null ? v + "!" : v);
		StringSet stringSet2 =
				new StringSet(list.stream().filter(Objects::nonNull).map(v -> v + "!").collect(Collectors.toList()));

		assertTrue(stringSet1.equals(stringSet2));
		assertTrue(stringSet2.equals(stringSet1));
		assertTrue(stringSet1.containsAll(stringSet2));
		assertEquals(stringSet1.compareTo(stringSet2), 0);
		assertEquals("[a!, cc!, b!, d!]", stringSet1.toString());
	}

	@Test
	public void testDifferentOrder() {
		List<String> list = Arrays.asList("a", "cc", "b", "d");
		StringSet stringSet1 = new StringSet(list, v -> v != null ? v + "!" : v);
		List<String> list2 = Arrays.asList("d", "cc", "a", "b", "d");
		StringSet stringSet2 = new StringSet(list, v -> v != null ? v + "!" : v);

		assertTrue(stringSet1.equals(stringSet2));
		assertTrue(stringSet2.equals(stringSet1));
		assertTrue(stringSet1.containsAll(stringSet2));
		assertEquals(stringSet1.compareTo(stringSet2), 0);
		assertEquals(stringSet2.compareTo(stringSet1), 0);
		assertEquals(stringSet1.hashCode(), stringSet2.hashCode());
	}

	@Test
	public void testConstructorsEmpty() {
		StringSet stringSet1 = new StringSet(null);
		StringSet stringSet2 = new StringSet(null, null);

		assertTrue(stringSet1.equals(stringSet2));
		assertTrue(stringSet2.equals(stringSet1));
		assertEquals(stringSet1.compareTo(stringSet2), 0);
		assertTrue(stringSet1.isEmpty());
	}

	@Test
	public void testNullLimit() {
		StringSet stringSet1 = new StringSet(i -> "val", 0);
		StringSet stringSet2 = new StringSet(Collections.emptyList());

		assertTrue(stringSet1.equals(stringSet2));
		assertTrue(stringSet2.equals(stringSet1));
		assertEquals(stringSet1.compareTo(stringSet2), 0);
		assertTrue(stringSet1.isEmpty());
	}

	@Test
	public void testSort() {
		List<StringSet> sets = Arrays.asList(new StringSet(Arrays.asList("a", "b")), new StringSet(null),
				new StringSet(Arrays.asList("a", "c")), new StringSet(i -> "a", 1));
		ObjectBuffer<StringSet> buffer = Buffers.textsetBuffer(4);
		for (int i = 0; i < 4; i++) {
			buffer.set(i, sets.get(i));
		}
		Column column = buffer.toColumn();
		int[] sort = column.sort(Order.ASCENDING);
		Column mapped = column.rows(sort, true);
		Object[] sortedColumn = new Object[4];
		mapped.fill(sortedColumn, 0);

		Object[] expected = new Object[]{new StringSet(null), new StringSet(i -> "a", 1), new StringSet(Arrays.asList(
				"a", "b")),	new StringSet(Arrays.asList("a", "c"))};

		assertArrayEquals(expected, sortedColumn);

		sets.sort(StringSet::compareTo);
		assertArrayEquals(expected, sets.toArray());
	}

	@Test
	public void testArray() {
		StringSet set = new StringSet(Arrays.asList("a", "b", "c", "d"));
		assertArrayEquals(set.toArray(), set.toArray(new String[0]));
		List<String> newList = new ArrayList<>();
		set.forEach(newList::add);
		assertArrayEquals(set.toArray(new String[0]), newList.toArray(new String[0]));
		assertEquals(set.toString(), Arrays.toString(set.toArray()));
		assertEquals(set.toString(), Arrays.toString(set.toArray(new String[0])));
	}

	@Test
	public void testContains() {
		StringSet set = new StringSet(Arrays.asList("a", "b", "c", "d"), null);
		assertTrue(set.contains("b"));
		assertTrue(set.containsAll(Arrays.asList("c", "b")));
	}

	@Test
	public void testCollectors() {
		StringSet set = new StringSet(Arrays.asList("a", "b", "c", "d"));
		List<String> firstList = new ArrayList<>();
		set.forEach(firstList::add);
		List<String> secondList = new ArrayList<>();
		set.iterator().forEachRemaining(secondList::add);
		assertEquals(firstList, secondList);
		assertEquals(firstList, set.stream().collect(Collectors.toList()));
		assertEquals(firstList, set.parallelStream().collect(Collectors.toList()));
		assertEquals(firstList, new ArrayList<>(set));
	}
}
