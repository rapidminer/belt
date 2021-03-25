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

package com.rapidminer.belt.column.type;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
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
public class StringListTests {

	@Test
	public void testConstructors() {
		List<String> list = Arrays.asList("a", null, "cc", "b", null, "d");
		StringList stringList1 = new StringList(list, v -> v != null ? v + "!" : v);
		StringList stringList2 = new StringList(i -> list.get(i) == null ? null : list.get(i) + "!", list.size());

		assertTrue(stringList1.equals(stringList2));
		assertTrue(stringList2.equals(stringList1));
		assertTrue(stringList1.containsAll(stringList2));
		assertEquals(stringList1.compareTo(stringList2), 0);
		assertEquals(stringList1.hashCode(), stringList2.hashCode());
	}

	@Test
	public void testConstructors2() {
		List<String> list = Arrays.asList("a", null, "cc", "b", null, "d");
		StringList stringList1 = new StringList(list, v -> v != null ? v + "!" : v);
		StringList stringList2 =
				new StringList(list.stream().filter(Objects::nonNull).map(v -> v + "!").collect(Collectors.toList()));

		assertTrue(stringList1.equals(stringList2));
		assertTrue(stringList2.equals(stringList1));
		assertTrue(stringList1.containsAll(stringList2));
		assertEquals(stringList1.compareTo(stringList2), 0);
		assertEquals("[a!, cc!, b!, d!]", stringList1.toString());
	}

	@Test
	public void testDifferentOrder() {
		List<String> list = Arrays.asList("a", "cc", "b", "d");
		StringList stringList1 = new StringList(list, v -> v != null ? v + "!" : v);
		List<String> list2 = Arrays.asList("d", "cc", "a", "b", "d");
		StringList stringList2 = new StringList(list2, v -> v != null ? v + "!" : v);

		assertFalse(stringList1.equals(stringList2));
		assertFalse(stringList2.equals(stringList1));
		assertTrue(stringList1.containsAll(stringList2));
		assertTrue(stringList1.compareTo(stringList2) < 0);
		assertTrue(stringList2.compareTo(stringList1) > 0);
		assertNotEquals(stringList1.hashCode(), stringList2.hashCode());
	}

	@Test
	public void testConstructorsEmpty() {
		StringList stringList1 = new StringList(null);
		StringList stringList2 = new StringList(null, null);

		assertTrue(stringList1.equals(stringList2));
		assertTrue(stringList2.equals(stringList1));
		assertEquals(stringList1.compareTo(stringList2), 0);
		assertTrue(stringList1.isEmpty());
	}

	@Test
	public void testNullLimit() {
		StringList stringList1 = new StringList(i -> "val", 0);
		StringList stringList2 = new StringList(Collections.emptyList());

		assertTrue(stringList1.equals(stringList2));
		assertTrue(stringList2.equals(stringList1));
		assertEquals(stringList1.compareTo(stringList2), 0);
		assertTrue(stringList1.isEmpty());
	}

	@Test
	public void testSort() {
		List<StringList> lists = Arrays.asList(new StringList(Arrays.asList("a", "b")), new StringList(i -> "b", 1), new StringList(null),
				new StringList(Arrays.asList("a", "c")), new StringList(i -> "b", 1), new StringList(i -> "a", 1));
		ObjectBuffer<StringList> buffer = Buffers.textlistBuffer(6);
		for (int i = 0; i < 6; i++) {
			buffer.set(i, lists.get(i));
		}
		Column column = buffer.toColumn();
		int[] sort = column.sort(Order.ASCENDING);
		Column mapped = column.rows(sort, true);
		Object[] sortedColumn = new Object[6];
		mapped.fill(sortedColumn, 0);

		Object[] expected = new Object[]{new StringList(null), new StringList(i -> "a", 1), new StringList(Arrays.asList(
				"a", "b")),	new StringList(Arrays.asList("a", "c")), new StringList(i -> "b", 1), new StringList(i -> "b", 1)};

		assertArrayEquals(expected, sortedColumn);

		lists.sort(StringList::compareTo);
		assertArrayEquals(expected, lists.toArray());
	}

	@Test
	public void testArray() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"));
		assertArrayEquals(list.toArray(), list.toArray(new String[0]));
		List<String> newList = new ArrayList<>();
		list.forEach(newList::add);
		assertArrayEquals(list.toArray(new String[0]), newList.toArray(new String[0]));
		assertEquals(list.toString(), Arrays.toString(list.toArray()));
		assertEquals(list.toString(), Arrays.toString(list.toArray(new String[0])));
	}

	@Test
	public void testContains() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"), null);
		assertTrue(list.contains("b"));
		assertTrue(list.containsAll(Arrays.asList("c", "b")));
		assertEquals(1,list.indexOf("b"));
		assertEquals(1,list.lastIndexOf("b"));
	}

	@Test
	public void testCollectors() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"));
		List<String> firstList = new ArrayList<>();
		list.forEach(firstList::add);
		List<String> secondList = new ArrayList<>();
		list.iterator().forEachRemaining(secondList::add);
		assertEquals(firstList, secondList);
		assertEquals(firstList, list.stream().collect(Collectors.toList()));
		assertEquals(firstList, list.parallelStream().collect(Collectors.toList()));
		assertEquals(firstList, new ArrayList<>(list));
	}

	@Test
	public void testListIterator() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"));
		ListIterator<String> listIterator = list.listIterator();
		assertTrue(listIterator.hasNext());
		assertFalse(listIterator.hasPrevious());
		assertEquals("a", listIterator.next());
		assertEquals(1, listIterator.nextIndex());
		assertEquals("b", listIterator.next());
		assertTrue(listIterator.hasPrevious());
		assertEquals(1, listIterator.previousIndex());
		assertEquals("b", listIterator.previous());
	}

	@Test
	public void testSubList() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"));
		List<String> sameSublist = list.subList(0, list.size());
		assertEquals(Arrays.asList(list.toArray(new String[0])), sameSublist);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testReplaceAll() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"));
		list.replaceAll(x -> x + "!");
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testSortNative() {
		StringList list = new StringList(Arrays.asList("a", "b", "c", "d"));
		list.sort(String::compareTo);
	}
}
