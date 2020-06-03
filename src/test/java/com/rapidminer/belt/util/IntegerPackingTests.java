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

package com.rapidminer.belt.util;

import static org.junit.Assert.assertArrayEquals;

import java.util.Random;

import org.junit.Test;


/**
 * @author Michael Knopf
 */
public class IntegerPackingTests {

	@Test
	public void test2BitPacking() {
		int nValues = 128;

		// Generate integer values in range [0, 3]
		Random rng = new Random(5456782423784294L);
		int[] data = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			data[i] = rng.nextInt(4);
		}

		// Store values in packed byte array
		byte[] packed = new byte[nValues / 4];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(packed, i, data[i]);
		}

		// Unpack values
		int[] unpacked = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			unpacked[i] = IntegerFormats.readUInt2(packed, i);
		}

		assertArrayEquals(data, unpacked);
	}

	@Test
	public void test4BitPacking() {
		int nValues = 128;

		// Generate integer values in range [0, 15]
		Random rng = new Random(6723457693446529L);
		int[] data = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			data[i] = rng.nextInt(16);
		}

		// Store values in packed byte array
		byte[] packed = new byte[nValues / 2];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(packed, i, data[i]);
		}

		// Unpack values
		int[] unpacked = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			unpacked[i] = IntegerFormats.readUInt4(packed, i);
		}

		assertArrayEquals(data, unpacked);
	}

	@Test
	public void test2BitOverflow() {
		int nValues = 128;

		// Generate integer values in range [0, 2^31-1]
		Random rng = new Random( 4524356737345834L);
		int[] data = new int[nValues];
		int[] expected = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			int value = rng.nextInt(Integer.MAX_VALUE);
			data[i] = value;
			expected[i] = value % 4; // overflow of values larger than 3
		}

		// Store values in packed byte array
		byte[] packed = new byte[nValues / 4];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt2(packed, i, data[i]);
		}

		// Unpack values
		int[] unpacked = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			unpacked[i] = IntegerFormats.readUInt2(packed, i);
		}

		assertArrayEquals(expected, unpacked);
	}

	@Test
	public void test4BitOverflow() {
		int nValues = 128;

		// Generate integer values in range [0, 2^31-1]
		Random rng = new Random(8923456725348672L);
		int[] data = new int[nValues];
		int[] expected = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			int value = rng.nextInt(Integer.MAX_VALUE);
			data[i] = value;
			expected[i] = value % 16; // overflow of values larger than 15
		}

		// Store values in packed byte array
		byte[] packed = new byte[nValues / 2];
		for (int i = 0; i < data.length; i++) {
			IntegerFormats.writeUInt4(packed, i, data[i]);
		}

		// Unpack values
		int[] unpacked = new int[nValues];
		for (int i = 0; i < nValues; i++) {
			unpacked[i] = IntegerFormats.readUInt4(packed, i);
		}

		assertArrayEquals(expected, unpacked);
	}

}
