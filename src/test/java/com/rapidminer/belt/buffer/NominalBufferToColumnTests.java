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

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.util.IntegerFormats.Format;


public class NominalBufferToColumnTests {

	private String[] identityStrings(int nValues, int nCategories) {
		String[] data = new String[nValues];
		Arrays.setAll(data, i -> String.valueOf(i % nCategories));
		return data;
	}

	private BigInteger[] identityBigInts(int nValues, int nCategories) {
		BigInteger[] data = new BigInteger[nValues];
		Arrays.setAll(data, i -> BigInteger.valueOf(i % nCategories));
		return data;
	}

	@Test
	public void testUInt2StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT2.maxValue());
		UInt2NominalBuffer buffer = new UInt2NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt4StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT4.maxValue());
		UInt4NominalBuffer buffer = new UInt4NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt8To2StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT2.maxValue());
		UInt8NominalBuffer buffer = new UInt8NominalBuffer(ColumnType.NOMINAL, n, Format.UNSIGNED_INT2);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt8To4StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT4.maxValue());
		UInt8NominalBuffer buffer = new UInt8NominalBuffer(ColumnType.NOMINAL, n, Format.UNSIGNED_INT4);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt8StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT8.maxValue());
		UInt8NominalBuffer buffer = new UInt8NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt16StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT16.maxValue());
		UInt16NominalBuffer buffer = new UInt16NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testInt32StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.SIGNED_INT32.maxValue());
		Int32NominalBuffer buffer = new Int32NominalBuffer(ColumnType.NOMINAL, n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt2MismatchingCategory() {
		UInt2NominalBuffer buffer = new UInt2NominalBuffer(ColumnType.TEXT,100);
		buffer.toColumn();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt4MismatchingCategory() {
		UInt4NominalBuffer buffer = new UInt4NominalBuffer(ColumnType.TEXT, 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt8MismatchingCategory() {
		UInt8NominalBuffer buffer = new UInt8NominalBuffer(ColumnType.TEXT, 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt16MismatchingCategory() {
		UInt16NominalBuffer buffer = new UInt16NominalBuffer(ColumnType.TEXT, 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInt32MismatchingCategory() {
		Int32NominalBuffer buffer = new Int32NominalBuffer(ColumnType.TEXT, 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt2Freeze() {
		UInt2NominalBuffer buffer = new UInt2NominalBuffer(ColumnType.NOMINAL, 100);
		buffer.toColumn();
		buffer.set(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt4Freeze() {
		UInt4NominalBuffer buffer = new UInt4NominalBuffer(ColumnType.NOMINAL, 100);
		buffer.toColumn();
		buffer.set(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt8Freeze() {
		UInt8NominalBuffer buffer = new UInt8NominalBuffer(ColumnType.NOMINAL, 100);
		buffer.toColumn();
		buffer.set(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt16Freeze() {
		UInt16NominalBuffer buffer = new UInt16NominalBuffer(ColumnType.NOMINAL, 100);
		buffer.toColumn();
		buffer.set(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testInt32Freeze() {
		Int32NominalBuffer buffer = new Int32NominalBuffer(ColumnType.NOMINAL, 100);
		buffer.toColumn();
		buffer.set(0, "test");
	}

}
