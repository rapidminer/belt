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

import static org.junit.Assert.assertArrayEquals;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;


public class FreeBufferToColumnTests {

	private static final ColumnType<String> FREE_STRING_COLUMN = ColumnTypes.freeType(
			"com.rapidminer.belt.test.freestringcolumn", String.class, String::compareTo);

	private static final ColumnType<BigInteger> FREE_BIGINT_COLUMN = ColumnTypes.freeType(
			"com.rapidminer.belt.test.freebigintcolumn", BigInteger.class, BigInteger::compareTo);

	private static final ColumnType<String> CATEGORICAL_STRING_COLUMN = ColumnTypes.categoricalType(
			"com.rapidminer.belt.test.categoricalstringcolumn", String.class, String::compareTo);

	private String[] identityStrings(int nValues) {
		String[] data = new String[nValues];
		Arrays.setAll(data, String::valueOf);
		return data;
	}

	private BigInteger[] identityBigInts(int nValues) {
		BigInteger[] data = new BigInteger[nValues];
		Arrays.setAll(data, BigInteger::valueOf);
		return data;
	}

	@Test
	public void testStringBuffer() {
		int n = 100;
		String[] data = identityStrings(n);
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		FreeColumn<String> column = buffer.toColumn(FREE_STRING_COLUMN);
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testBigIntBuffer() {
		int n = 100;
		BigInteger[] data = identityBigInts(n);
		FreeColumnBuffer<BigInteger> buffer = new FreeColumnBuffer<>(n);
		for (int i = 0; i < 100; i++) {
			buffer.set(i, data[i]);
		}
		FreeColumn<BigInteger> column = buffer.toColumn(FREE_BIGINT_COLUMN);
		BigInteger[] result = new BigInteger[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}


	@Test
	public void testTypes() {
		FreeColumnBuffer<BigInteger> buffer = new FreeColumnBuffer<>(100);
		FreeColumn<BigInteger> column = buffer.toColumn(FREE_BIGINT_COLUMN);

	}

	@Test(expected = IllegalArgumentException.class)
	public void testMismatchingCategory() {
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(100);
		buffer.toColumn(CATEGORICAL_STRING_COLUMN);
	}

	@Test(expected = IllegalStateException.class)
	public void testFreeze() {
		FreeColumnBuffer<String> buffer = new FreeColumnBuffer<>(100);
		buffer.toColumn(FREE_STRING_COLUMN);
		buffer.set(0, "test");
	}

}
