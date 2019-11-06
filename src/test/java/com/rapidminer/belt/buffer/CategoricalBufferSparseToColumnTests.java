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

package com.rapidminer.belt.buffer;


import static org.junit.Assert.assertArrayEquals;

import java.math.BigInteger;
import java.util.Arrays;

import org.junit.Test;

import com.rapidminer.belt.column.CategoricalColumn;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.ColumnTypes;
import com.rapidminer.belt.util.IntegerFormats.Format;


/**
 * @author Kevin Majchrzak
 */
public class CategoricalBufferSparseToColumnTests {

	private static final ColumnType<String> CATEGORICAL_STRING_COLUMN = ColumnTypes.categoricalType(
			"com.rapidminer.belt.test.categoricalstringcolumn", String.class, String::compareTo);

	private static final ColumnType<BigInteger> CATEGORICAL_BIGINT_COLUMN = ColumnTypes.categoricalType(
			"com.rapidminer.belt.test.categoricalbigintcolumn", BigInteger.class, BigInteger::compareTo);

	private static final ColumnType<String> STRING_COLUMN = ColumnTypes.objectType(
			"com.rapidminer.belt.test.stringcolumn", String.class, String::compareTo);

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
		UInt8CategoricalBufferSparse<String> buffer = new UInt8CategoricalBufferSparse<>(String.valueOf(0), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn<String> column = buffer.toColumn(CATEGORICAL_STRING_COLUMN);
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt16StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.UNSIGNED_INT16.maxValue());
		UInt16CategoricalBufferSparse<String> buffer = new UInt16CategoricalBufferSparse<>(String.valueOf(1), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn<String> column = buffer.toColumn(CATEGORICAL_STRING_COLUMN);
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testInt32StringBuffer() {
		int n = 100;
		String[] data = identityStrings(n, Format.SIGNED_INT32.maxValue());
		Int32CategoricalBufferSparse<String> buffer = new Int32CategoricalBufferSparse<>(String.valueOf(-1), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn<String> column = buffer.toColumn(CATEGORICAL_STRING_COLUMN);
		String[] result = new String[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt8BigIntBuffer() {
		int n = 100;
		BigInteger[] data = identityBigInts(n, Format.UNSIGNED_INT8.maxValue());
		UInt8CategoricalBufferSparse<BigInteger> buffer = new UInt8CategoricalBufferSparse<>(BigInteger.valueOf(0), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn<BigInteger> column = buffer.toColumn(CATEGORICAL_BIGINT_COLUMN);
		BigInteger[] result = new BigInteger[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testUInt16BigIntBuffer() {
		int n = 100;
		BigInteger[] data = identityBigInts(n, Format.UNSIGNED_INT16.maxValue());
		UInt16CategoricalBufferSparse<BigInteger> buffer = new UInt16CategoricalBufferSparse<>(BigInteger.valueOf(0), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn<BigInteger> column = buffer.toColumn(CATEGORICAL_BIGINT_COLUMN);
		BigInteger[] result = new BigInteger[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test
	public void testInt32BigIntBuffer() {
		int n = 100;
		BigInteger[] data = identityBigInts(n, Format.SIGNED_INT32.maxValue());
		Int32CategoricalBufferSparse<BigInteger> buffer = new Int32CategoricalBufferSparse<>(BigInteger.valueOf(0), n);
		for (int i = 0; i < 100; i++) {
			buffer.setNext(i, data[i]);
		}
		CategoricalColumn<BigInteger> column = buffer.toColumn(CATEGORICAL_BIGINT_COLUMN);
		BigInteger[] result = new BigInteger[n];
		column.fill(result, 0);
		assertArrayEquals(data, result);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt8MismatchingCategory() {
		UInt8CategoricalBufferSparse<String> buffer = new UInt8CategoricalBufferSparse<>("defaultValue", 100);
		buffer.toColumn(STRING_COLUMN);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testUInt16MismatchingCategory() {
		UInt16CategoricalBufferSparse<String> buffer = new UInt16CategoricalBufferSparse<>("defaultValue", 100);
		buffer.toColumn(STRING_COLUMN);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testInt32MismatchingCategory() {
		Int32CategoricalBufferSparse<String> buffer = new Int32CategoricalBufferSparse<>("defaultValue", 100);
		buffer.toColumn(STRING_COLUMN);
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt8Freeze() {
		UInt8CategoricalBufferSparse<String> buffer = new UInt8CategoricalBufferSparse<>("defaultValue", 100);
		buffer.toColumn(CATEGORICAL_STRING_COLUMN);
		buffer.setNext(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testUInt16Freeze() {
		UInt16CategoricalBufferSparse<String> buffer = new UInt16CategoricalBufferSparse<>("defaultValue", 100);
		buffer.toColumn(CATEGORICAL_STRING_COLUMN);
		buffer.setNext(0, "test");
	}

	@Test(expected = IllegalStateException.class)
	public void testInt32Freeze() {
		Int32CategoricalBufferSparse<String> buffer = new Int32CategoricalBufferSparse<>("defaultValue", 100);
		buffer.toColumn(CATEGORICAL_STRING_COLUMN);
		buffer.setNext(0, "test");
	}

}
