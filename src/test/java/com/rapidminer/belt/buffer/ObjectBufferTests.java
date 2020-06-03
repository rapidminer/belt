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

package com.rapidminer.belt.buffer;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.ColumnTestUtils;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.reader.ObjectReader;
import com.rapidminer.belt.reader.Readers;


/**
 * @author Gisa Meier
 */
public class ObjectBufferTests {

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

	private ObjectBuffer<String> toFreeBuffer(Column column) {
		return new ObjectBuffer<>(ColumnType.TEXT, column);
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
		ObjectBuffer buffer = new ObjectBuffer<String>(ColumnType.TEXT,197);
		assertEquals(197, buffer.size());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBufferNegativeLength() {
		Buffers.textsetBuffer(-5);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBufferNegativeLengthText() {
		Buffers.textBuffer(-1);
	}

	@Test
	public void testZeroBufferLength() {
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT,0);;
		assertEquals(0, buffer.size());
	}

	@Test
	public void testSet() {
		int n = 123;
		int[] testData = random(n);
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT,n);
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
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, n);
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
		UInt8NominalBuffer nominalBuffer = new UInt8NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < n; i++) {
			nominalBuffer.set(i, "value" + i);
		}
		nominalBuffer.set(42, null);
		CategoricalColumn column = nominalBuffer.toColumn();

		ObjectBuffer<String> buffer = toFreeBuffer(column);

		String[] expectedCategories = new String[n];
		Arrays.setAll(expectedCategories, i -> "value" + i);
		expectedCategories[42] = null;
		assertArrayEquals(expectedCategories, buffer.getData());
	}

	@Test
	public void testFromFreeColumn() {
		int nValues = 165;
		String[] data = randomStrings(nValues);
		Column column = ColumnAccessor.get().newObjectColumn(ColumnType.TEXT, data);
		ObjectBuffer buffer = toFreeBuffer(column);
		assertEquals(column.size(), buffer.size());

		ObjectReader<String> reader = Readers.objectReader(column, String.class);
		for (int i = 0; i < column.size(); i++) {
			String expected = reader.read();
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
		Column column = ColumnTestUtils.getMappedObjectColumn(ColumnType.TEXT, mappedData, mapping);
		ObjectBuffer buffer = toFreeBuffer(column);
		assertEquals(column.size(), buffer.size());

		ObjectReader<String> reader = Readers.objectReader(column, String.class);
		for (int i = 0; i < column.size(); i++) {
			String expected = reader.read();
			assertEquals(expected, buffer.get(i));
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFromColumnWrongType() {
		int n = 142;
		UInt8NominalBuffer nominalBuffer = new UInt8NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < n; i++) {
			nominalBuffer.set(i, "value" + i);
		}
		nominalBuffer.set(42, null);
		CategoricalColumn column = nominalBuffer.toColumn();

		new ObjectBuffer<>(ColumnType.TEXTSET, column) ;
	}

	@Test(expected = IllegalStateException.class)
	public void testSetAfterFreeze() {
		int n = 12;
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, n);
		for (int i = 0; i < n; i++) {
			buffer.set(i, "value" + i);
		}
		buffer.freeze();
		buffer.set(5, "no");
	}

	@Test
	public void testToStringSmall() {
		int[] data = {5, 7, 3, 1, 4, 4, 8};
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT,8);
		for (int i = 0; i < data.length; i++) {
			buffer.set(i, "value" + data[i]);
		}
		String expected = "Object Buffer (" + (data.length + 1) + ")\n(" + "value5, value7, value3, value1, value4, value4, value8, ?)";

		assertEquals(expected, buffer.toString());
	}

	@Test
	public void testToStringMaxFit() {
		int[] datablock = {5, 7, 3, 1, 4, 4, 8, 10};
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, 32);
		for (int i = 0; i < buffer.size(); i++) {
			buffer.set(i, "value" + datablock[i % datablock.length]);
		}

		String expected = "Object Buffer (" + buffer.size() + ")\n("
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
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, length);
		for (int i = 0; i < buffer.size() - 1; i++) {
			buffer.set(i, "value" + datablock[i % datablock.length]);
		}
		buffer.set(buffer.size() - 1, "value" + 100);

		String expected = "Object Buffer (" + length + ")\n(" + "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, ..., value100)";

		assertEquals(expected, buffer.toString());
	}

	@Test
	public void testToStringBiggerLastMissing() {
		int[] datablock = {5, 7, 3, 1, 4, 4, 8, 10};
		int length = 33;
		ObjectBuffer<String> buffer = new ObjectBuffer<>(ColumnType.TEXT, length);
		for (int i = 0; i < buffer.size() - 1; i++) {
			buffer.set(i, "value" + datablock[i % datablock.length]);
		}
		buffer.set(buffer.size() - 1, null);

		String expected = "Object Buffer (" + length + ")\n(" + "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, value8, value10, "
				+ "value5, value7, value3, value1, value4, value4, ..., ?)";

		assertEquals(expected, buffer.toString());
	}


	@Test(expected = NullPointerException.class)
	public void testNullColumn() {
		Buffers.textsetBuffer( null);
	}

	@Test
	public void testFromColumn() {
		Column column = Buffers.textsetBuffer(13).toColumn();
		ObjectBuffer<StringSet> buffer = Buffers.textsetBuffer(column);
		assertEquals(13, buffer.size());
	}

	@Test(expected = NullPointerException.class)
	public void testNullType() {
		new ObjectBuffer<>(null, Buffers.textsetBuffer(3).toColumn());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongCapability() {
		Buffers.textsetBuffer(Buffers.realBuffer(4).toColumn());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongType() {
		Buffers.textsetBuffer(ColumnAccessor.get().newDateTimeColumn(new long[0], null));
	}

	@Test(expected = NullPointerException.class)
	public void testNullColumnText() {
		Buffers.textBuffer( null);
	}

	@Test(expected = NullPointerException.class)
	public void testNullTypeText() {
		new ObjectBuffer<>(null, Buffers.textBuffer(3).toColumn());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongCapabilityText() {
		Buffers.textBuffer(Buffers.realBuffer(4).toColumn());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testWrongTypeText() {
		Buffers.textBuffer(ColumnAccessor.get().newDateTimeColumn(new long[0], null));
	}

	@Test
	public void testFromColumnText() {
		Column column = Buffers.textBuffer(13).toColumn();
		ObjectBuffer<String> buffer = Buffers.textBuffer(column);
		assertEquals(13, buffer.size());
	}

	@Test
	public void testFromNominalColumnText() {
		NominalBuffer nominalBuffer = Buffers.nominalBuffer(17, 3);
		for (int i = 0; i < nominalBuffer.size(); i++) {
			nominalBuffer.set(i, "val"+(i%3));
		}

		ObjectBuffer<String> buffer = Buffers.textBuffer(nominalBuffer.toColumn());
		assertEquals(17, buffer.size());
	}

}
