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

package com.rapidminer.belt.buffer;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.type.StringSet;


public class ObjectBufferToColumnTests {

	private static final ColumnType<String> CATEGORICAL_STRING_COLUMN = ColumnTestUtils.categoricalType(
			String.class, String::compareTo);

	private String[] identityStrings(int nValues) {
		String[] data = new String[nValues];
		Arrays.setAll(data, String::valueOf);
		return data;
	}

	private StringSet[] stringSets(int nValues) {
		StringSet[] data = new StringSet[nValues];
		Arrays.setAll(data, i -> new StringSet(Arrays.asList("val"+i, "val-"+i)));
		return data;
	}

	@Test
	public void testStringBuffer() {
		int n = 100;
		String[] data = identityStrings(n);
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		Column column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testStringSetBuffer() {
		int n = 100;
		StringSet[] data = stringSets(n);
		ObjectBuffer<StringSet> buffer = new ObjectBuffer<>(ColumnType.TEXTSET, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		Column column = buffer.toColumn();
		StringSet[] result = new StringSet[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}


	@Test
	public void testTypes() {
		ObjectBuffer<StringSet> buffer = new ObjectBuffer<>(ColumnType.TEXTSET, 100);
		Column column = buffer.toColumn();

	}

	@Test(expected = IllegalArgumentException.class)
	public void testMismatchingCategory() {
		ObjectBuffer<String> buffer = new ObjectBuffer<>(CATEGORICAL_STRING_COLUMN, 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalStateException.class)
	public void testFreeze() {
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, 100);
		buffer.toColumn();
		buffer.set(0, "test");
	}

}
