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

package com.rapidminer.belt.util;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;


/**
 * Utility functions for the different integer formats used by Belt. Includes support for 2, 4, 8, and 16Bit unsigned
 * integers as well as 32Bit signed integers. The 8, 16, and 32Bit formats correspond to Java {@code byte},
 * {@code short}, and {@code int} primitives respectively. 2 and 4Bit integer support is implemented by packing multiple
 * values in a single {@code byte}.
 *
 * @author Michael Knopf
 */
public final class IntegerFormats {

	/**
	 * Integer formats used by Belt. Includes 2, 4, 8, and 16Bit unsigned integers as well as 32Bit signed integers.
	 */
	public enum Format {

		/** 2Bit unsigned integer (max. value 3, four values are stored in a single {@code byte}). **/
		UNSIGNED_INT2(3),

		/** 4Bit unsigned integer (max. value 15, two values are stored in a single {@code byte}). **/
		UNSIGNED_INT4(15),

		/** 8Bit unsigned integer (max. value 255, stored as {@code byte}). **/
		UNSIGNED_INT8(255),

		/** 16Bit unsigned integer (max. value 65535, stored as {@code short}). **/
		UNSIGNED_INT16(65535),

		/** 32Bit signed integer (max. value {@link Integer#MAX_VALUE}, stored as {@code int}). **/
		SIGNED_INT32(Integer.MAX_VALUE);

		private final int maxValue;

		Format(int maxValue) {
			this.maxValue = maxValue;
		}

		/**
		 * Returns the maximum value supported by this format.
		 *
		 * @return the maximum value
		 */
		public int maxValue() {
			return maxValue;
		}

		/**
		 * Finds the smallest {@link Format} with maximal supported value greater or equal the parameter.
		 *
		 * @param maxValue
		 * 		the necessary maximal supported value
		 * @return the smallest format supporting the value
		 */
		public static Format findMinimal(int maxValue){
			for (Format format : values()) {
				if (maxValue <= format.maxValue()) {
					return format;
				}
			}
			return Format.SIGNED_INT32;
		}

	}

	/** Formats packed into {@code byte} primitives. */
	public static final Set<Format> BYTE_BACKED_FORMATS = Collections.unmodifiableSet(EnumSet.of(Format.UNSIGNED_INT2,
			Format.UNSIGNED_INT4, Format.UNSIGNED_INT8));

	/**
	 * Container for byte arrays containing packed integer values.
	 */
	public static final class PackedIntegers {

		private final byte[] data;
		private final Format format;
		private final int size;

		public PackedIntegers(byte[] data, Format format, int size) {
			this.data = data;
			this.format = format;
			this.size = size;
		}

		/**
		 * Returns the array holding the (packed) data.
		 *
		 * @return the data array
		 */
		public byte[] data() {
			return data;
		}

		/**
		 * Return the format of the (packed) data.
		 *
		 * @return the data format
		 */
		public Format format() {
			return format;
		}

		/**
		 * Return the number of element stored in the wrapped array.
		 *
		 * @return the number of elements
		 */
		public int size() {
			return size;
		}

	}

	/**
	 * Reads the ith unsigned 2Bit integer from the given byte array. The method assumes that {@code byte} values are
	 * packed starting with the least significant bits. For example, it is assumed that the 5th unsigned 2Bit integer is
	 * stored in the two least significant bits of the second value of the given array.
	 *
	 * @param source
	 * 		the array of 2Bit integers
	 * @param i
	 * 		the index to look up
	 * @return the ith unsigned 2Bit integer (range {@code [0, 3]})
	 */
	public static int readUInt2(byte[] source, int i) {
		int enclosingByte = source[i >>> 2];
		int byteIndex = (i & 0b11) << 1;
		return (enclosingByte & (0b11 << byteIndex)) >>> byteIndex;
	}

	/**
	 * Overwrites the ith unsigned 2Bit integer in the given byte array with the given value. The method assumes that
	 * {@code byte} values are packed starting with the least significant bits. For example, it is assumed that the 5th
	 * unsigned 2Bit integer is stored in the two least significant bits of the second value of the given array.
	 *
	 * @param target
	 * 		the array of 2Bit integers
	 * @param index
	 * 		the index to write
	 * @param value
	 * 		the new value in range {@code [0, 3]}
	 */
	public static void writeUInt2(byte[] target, int index, int value) {
		int arrayIndex = index >>> 2;
		int byteIndex = (index & 0b11) << 1;
		// zero out target bits
		byte enclosingByte = target[arrayIndex];
		enclosingByte &= ~(0b11 << byteIndex);
		// set value
		enclosingByte |= (value & 0b11) << byteIndex;
		target[arrayIndex] = enclosingByte;
	}

	/**
	 * Reads the ith unsigned 4Bit integer from the given byte array. The method assumes that {@code byte} values are
	 * filled starting with the least significant bits. For example, it is assumed that the 3th unsigned 4Bit integer is
	 * stored in the four least significant bits of the second value of the given array.
	 *
	 * @param source
	 * 		the array of 4Bit integers
	 * @param i
	 * 		the index to look up
	 * @return the ith unsigned 4Bit integer (range {@code [0, 15]})
	 */
	public static int readUInt4(byte[] source, int i) {
		int enclosingByte = source[i >>> 1];
		int byteIndex = (i & 0b1) << 2;
		return (enclosingByte & (0b1111 << byteIndex)) >>> byteIndex;
	}

	/**
	 * Overwrites the ith unsigned 4Bit integer in the given byte array with the given value. The method assumes that
	 * {@code byte} values are filled starting with the least significant bits. For example, it is assumed that the 3th
	 * unsigned 4Bit integer is stored in the four least significant bits of the second value of the given array.
	 *
	 * @param target
	 * 		the array of 4Bit integers
	 * @param index
	 * 		the index to write
	 * @param value
	 * 		the new value in range {@code [0, 15]}
	 */
	public static void writeUInt4(byte[] target, int index, int value) {
		int arrayIndex = index >>> 1;
		int byteIndex = (index & 0b1) << 2;
		// zero out target bits
		byte enclosingByte = target[arrayIndex];
		enclosingByte &= ~(0b1111 << byteIndex);
		// set value
		enclosingByte |= (value & 0b1111) << byteIndex;
		target[arrayIndex] = enclosingByte;
	}

	private IntegerFormats() {
		// Prevent instantiation of utility class
		throw new AssertionError();
	}

}
