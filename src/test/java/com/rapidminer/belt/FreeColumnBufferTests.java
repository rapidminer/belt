/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2018 RapidMiner GmbH
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

package com.rapidminer.belt;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;


/**
 * @author Gisa Meier
 */
public class FreeColumnBufferTests {

	private static int[] random(int n) {
		int[] data = new int[n];
		Random random = new Random();
		Arrays.setAll(data, i -> random.nextInt(20));
		return data;
	}

	private static String[] randomStrings(int n) {
		String[] data = new String[n];
		Random random = new Random();
		Arrays.setAll(data, i -> "" + random.nextInt(30));
		return data;
	}

	private FreeColumnBuffer<String> toFreeBuffer(Column column) {
		return new FreeColumnBuffer<>(String.class, column);
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

	@Test
	public void testBufferLength() {
		FreeColumnBuffer buffer = new FreeColumnBuffer<String>(197);
		assertEquals(197, buffer.size());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBufferNegativeLength() {
		new FreeColumnBuffer<String>(-5);
	}


	@Test
	public void testZeroBufferLength() {
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(0);;
		assertEquals(0, buffer.size());
	}

	@Test
	public void testSet() {
		int n = 123;
		int[] testData = random(n);
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(n);
		for (int i = 0; i < n; i++) {
			buffer.set(i, "value" + testData[i]);
		}
		buffer.set(42, null);
		String[] expected = Arrays.stream(testData).mapToObj(i -> "value" + i).toArray(String[]::new);
		expected[42] = null;
		String[] result = new String[n];
		Arrays.setAll(result, buffer::get);
		assertArrayEquals(expected, result);
	}

	@Test
	public void testGetData() {
		int n = 142;
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(n);
		for (int i = 0; i < n; i++) {
			buffer.set(i, "value" + i);
		}
		buffer.set(42, null);
		String[] expectedCategories = new String[n];
		Arrays.setAll(expectedCategories, i -> "value" + i);
		expectedCategories[42] = null;
		assertArrayEquals(expectedCategories, buffer.getData());
	}

	@Test
	public void testFromCategoricalColumn() {
		int n = 142;
		UInt8CategoricalBuffer<String> nominalBuffer = new UInt8CategoricalBuffer<>(n);
		for (int i = 0; i < n; i++) {
			nominalBuffer.set(i, "value" + i);
		}
		nominalBuffer.set(42, null);
		CategoricalColumn<String> column = nominalBuffer.toColumn(ColumnTypes.NOMINAL);

		FreeColumnBuffer<String> buffer = toFreeBuffer(column);

		String[] expectedCategories = new String[n];
		Arrays.setAll(expectedCategories, i -> "value" + i);
		expectedCategories[42] = null;
		assertArrayEquals(expectedCategories, buffer.getData());
	}

	@Test
	public void testFromFreeColumn() {
		int nValues = 165;
		String[] data = randomStrings(nValues);
		FreeColumn<String> column = new SimpleFreeColumn<>(ColumnTypes.freeType("com.rapidminer.belt.column.test.stringcolumn",
				String.class, null), data);
		FreeColumnBuffer buffer = toFreeBuffer(column);
		assertEquals(column.size(), buffer.size());

		for (int i = 0; i < column.size(); i++) {
			String expected = (String) column.getData()[i];
			assertEquals(expected, buffer.get(i));
		}
	}

	@Test
	public void testFromMappedFreeColumn() {
		int nValues = 165;
		String[] data = randomStrings(nValues);
		int[] mapping = permutation(data.length);
		String[] mappedData = new String[data.length];
		for (int i = 0; i < data.length; i++) {
			mappedData[mapping[i]] = data[i];
		}
		MappedFreeColumn<String> column = new MappedFreeColumn<>(ColumnTypes.freeType(
				"com.rapidminer.belt.column.test.stringcolumn", String.class, null),
				mappedData, mapping);
		FreeColumnBuffer buffer = toFreeBuffer(column);
		assertEquals(column.size(), buffer.size());

		for (int i = 0; i < column.size(); i++) {
			String expected = (String) column.getData()[column.getRowMapping()[i]];
			assertEquals(expected, buffer.get(i));
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromColumnWrongType() {
		int n = 142;
		UInt8CategoricalBuffer<String> nominalBuffer = new UInt8CategoricalBuffer<>(n);
		for (int i = 0; i < n; i++) {
			nominalBuffer.set(i, "value" + i);
		}
		nominalBuffer.set(42, null);
		CategoricalColumn<String> column = nominalBuffer.toColumn(ColumnTypes.NOMINAL);

		new FreeColumnBuffer<>(Integer.class, column) ;
	}

	@Test(expected = IllegalStateException.class)
	public void testSetAfterFreeze() {
		int n = 12;
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(n);
		for (int i = 0; i < n; i++) {
			buffer.set(i, "value" + i);
		}
		buffer.freeze();
		buffer.set(5, "no");
	}

	@Test
	public void testToStringSmall() {
		int[] data = {5, 7, 3, 1, 4, 4, 8};
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(8);
		for (int i = 0; i < data.length; i++) {
			buffer.set(i, "value" + data[i]);
		}
		String expected = "Free Buffer (" + (data.length + 1) + ")\n(" + "value5, value7, value3, value1, value4, value4, value8, ?)";

		assertEquals(expected, buffer.toString());
	}

	@Test
	public void testToStringMaxFit() {
		int[] datablock = {5, 7, 3, 1, 4, 4, 8, 10};
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(32);
		for (int i = 0; i < buffer.size(); i++) {
			buffer.set(i, "value" + datablock[i % datablock.length]);
		}

		String expected = "Free Buffer (" + buffer.size() + ")\n("
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10)";

		assertEquals(expected, buffer.toString());
	}

	@Test
	public void testToStringBigger() {
		int[] datablock = {5, 7, 3, 1, 4, 4, 8, 10};
		int length = 33;
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(length);
		for (int i = 0; i < buffer.size() - 1; i++) {
			buffer.set(i, "value" + datablock[i % datablock.length]);
		}
		buffer.set(buffer.size() - 1, "value" + 100);

		String expected = "Free Buffer (" + length + ")\n(" + "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, ..., value100)";

		assertEquals(expected, buffer.toString());
	}

	@Test
	public void testToStringBiggerLastMissing() {
		int[] datablock = {5, 7, 3, 1, 4, 4, 8, 10};
		int length = 33;
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(length);
		for (int i = 0; i < buffer.size() - 1; i++) {
			buffer.set(i, "value" + datablock[i % datablock.length]);
		}
		buffer.set(buffer.size() - 1, null);

		String expected = "Free Buffer (" + length + ")\n(" + "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, ..., ?)";

		assertEquals(expected, buffer.toString());
	}


}
