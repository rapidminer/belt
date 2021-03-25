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


/**
 * @author Kevin Majchrzak
 */
public class NominalBufferSparseToColumnTests {

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
	public void testUInt8StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT8.maxValue());
		UInt8NominalBufferSparse buffer = new UInt8NominalBufferSparse(ColumnType.NOMINAL, String.valueOf(0), n
		);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
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
		UInt16NominalBufferSparse buffer = new UInt16NominalBufferSparse(ColumnType.NOMINAL, String.valueOf(1), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
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
		Int32NominalBufferSparse buffer = new Int32NominalBufferSparse(ColumnType.NOMINAL, String.valueOf(-1), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn column = buffer.toColumn();
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt8MismatchingCategory() {
		UInt8NominalBufferSparse buffer = new UInt8NominalBufferSparse(ColumnType.TEXT, "defaultValue", 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt16MismatchingCategory() {
		UInt16NominalBufferSparse buffer = new UInt16NominalBufferSparse(ColumnType.TEXT, "defaultValue", 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInt32MismatchingCategory() {
		Int32NominalBufferSparse buffer = new Int32NominalBufferSparse(ColumnType.TEXT,"defaultValue", 100);
		buffer.toColumn();
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt8Freeze() {
		UInt8NominalBufferSparse buffer = new UInt8NominalBufferSparse(ColumnType.NOMINAL, "defaultValue", 100);
		buffer.toColumn();
		buffer.setNext(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt16Freeze() {
		UInt16NominalBufferSparse buffer = new UInt16NominalBufferSparse(ColumnType.NOMINAL, "defaultValue", 100);
		buffer.toColumn();
		buffer.setNext(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testInt32Freeze() {
		Int32NominalBufferSparse buffer = new Int32NominalBufferSparse(ColumnType.NOMINAL,"defaultValue", 100);
		buffer.toColumn();
		buffer.setNext(0, "test");
	}

}
