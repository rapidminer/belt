/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2019 RapidMiner GmbH
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

import static com.rapidminer.belt.util.IntegerFormats.*;

/**
 * Utility class for mapping methods.
 *
 * @author Gisa Schaefer
 */
public final class Mapping {

	/** Value to use when merging mappings with index out of bounds */
	private static final int MISSING = -1;

	// Suppress default constructor for noninstantiability
	private Mapping() {
		throw new AssertionError();
	}

	/**
	 * Applies the given mapping to the given data set creating a deep copy. When the mapping is out of range, the new
	 * value will be {@link Double#NaN}.
	 *
	 * @param data
	 * 		the source data
	 * @param mapping
	 * 		the index mapping to apply
	 * @return the remapped deep copy
	 */
	public static double[] apply(double[] data, int[] mapping) {
		double[] copy = new double[mapping.length];
		for (int i = 0; i < copy.length; i++) {
			int position = mapping[i];
			if (position < 0 || position >= data.length) {
				copy[i] = Double.NaN;
			} else {
				copy[i] = data[position];
			}
		}
		return copy;
	}

	/**
	 * Applies the given mapping to the given data set creating a deep copy.
	 *
	 * @param data
	 * 		the source data
	 * @param mapping
	 * 		the index mapping to apply
	 * @param outOfRange
	 * 		the value to use when the mapping is out of range of the data
	 * @return the remapped deep copy
	 */
	public static long[] apply(long[] data, int[] mapping, long outOfRange) {
		long[] copy = new long[mapping.length];
		for (int i = 0; i < copy.length; i++) {
			int position = mapping[i];
			if (position < 0 || position >= data.length) {
				copy[i] = outOfRange;
			} else {
				copy[i] = data[position];
			}
		}
		return copy;
	}

	/**
	 * Applies the given mapping to the given data set creating a deep copy. When the mapping is out of range, the new
	 * value will be 0.
	 *
	 * @param data
	 * 		the source data
	 * @param mapping
	 * 		the index mapping to apply
	 * @return the remapped deep copy
	 */
	public static int[] apply(int[] data, int[] mapping) {
		int[] copy = new int[mapping.length];
		for (int i = 0; i < copy.length; i++) {
			int position = mapping[i];
			if (position < 0 || position >= data.length) {
				copy[i] = 0;
			} else {
				copy[i] = data[position];
			}
		}
		return copy;
	}

	/**
	 * Applies the given mapping to the given data set creating a deep copy.  When the mapping is out of range, the
	 * new value will be 0.
	 *
	 * @param data
	 * 		the source data
	 * @param mapping
	 * 		the index mapping to apply
	 * @return the remapped deep copy
	 */
	public static short[] apply(short[] data, int[] mapping) {
		short[] copy = new short[mapping.length];
		for (int i = 0; i < copy.length; i++) {
			int position = mapping[i];
			if (position < 0 || position >= data.length) {
				copy[i] = 0;
			} else {
				copy[i] = data[position];
			}
		}
		return copy;
	}

	/**
	 * Applies the given mapping to the given data set creating a deep copy.  When the mapping is out of range, the
	 * new value will be 0.
	 *
	 * @param data
	 * 		the source data
	 * @param mapping
	 * 		the index mapping to apply
	 * @return the remapped deep copy
	 */
	public static PackedIntegers apply(PackedIntegers data, int[] mapping) {
		switch (data.format()) {
			case UNSIGNED_INT2:
				return applyUInt2(data, mapping);
			case UNSIGNED_INT4:
				return applyUInt4(data, mapping);
			case UNSIGNED_INT8:
				return applyUInt8(data, mapping);
			case UNSIGNED_INT16:
			case SIGNED_INT32:
			default:
				throw new IllegalArgumentException("Given format is not byte backed");
		}
	}

	private static PackedIntegers applyUInt2(PackedIntegers data, int[] mapping) {
		byte[] source = data.data();
		byte[] target = new byte[mapping.length % 4 == 0 ? mapping.length / 4 : mapping.length / 4 + 1];
		for (int i = 0; i < mapping.length; i++) {
			int position = mapping[i];
			if (position >= 0 && position < data.size()) {
				writeUInt2(target, i, readUInt2(source, position));
			}
		}
		return new PackedIntegers(target, Format.UNSIGNED_INT2, mapping.length);
	}

	private static PackedIntegers applyUInt4(PackedIntegers data, int[] mapping) {
		byte[] source = data.data();
		byte[] target = new byte[mapping.length % 2 == 0 ? mapping.length / 2 : mapping.length / 2 + 1];
		for (int i = 0; i < mapping.length; i++) {
			int position = mapping[i];
			if (position >= 0 && position < data.size()) {
				writeUInt4(target, i, readUInt4(source, position));
			}
		}
		return new PackedIntegers(target, Format.UNSIGNED_INT4, mapping.length);
	}

	private static PackedIntegers applyUInt8(PackedIntegers data, int[] mapping) {
		byte[] source = data.data();
		byte[] target = new byte[mapping.length];
		for (int i = 0; i < mapping.length; i++) {
			int position = mapping[i];
			if (position >= 0 && position < data.size()) {
				target[i] = source[position];
			}
		}
		return new PackedIntegers(target, Format.UNSIGNED_INT8, mapping.length);
	}

	/**
	 * Applies the given mapping to the given data set creating a deep copy.  When the mapping is out of range, the
	 * new value will be {@code null}.
	 *
	 * @param data
	 * 		the source data
	 * @param mapping
	 * 		the index mapping to apply
	 * @return the remapped deep copy
	 */
	public static Object[] apply(Object[] data, int[] mapping){
		Object[] copy = new Object[mapping.length];
		for (int i = 0; i < copy.length; i++) {
			int position = mapping[i];
			if (position < 0 || position >= data.length) {
				copy[i] = null;
			} else {
				copy[i] = data[position];
			}
		}
		return copy;
	}

	/**
	 * Merges (composes) the given mappings {@code f: X -> Y} and {@code g: Y -> Z} into a single mapping
	 * {@code h: X -> Z}.  When the mapping is out of range, the new value will be {@link #MISSING}.
	 *
	 * @param f
	 * 		the first mapping
	 * @param g
	 * 		the second mapping
	 * @return the merged mapping
	 */
	public static int[] merge(int[] f, int[] g) {
		int[] merged = new int[f.length];
		for (int i = 0; i < merged.length; i++) {
			int position = f[i];
			if (position < 0 || position >= g.length) {
				merged[i] = MISSING;
			} else {
				merged[i] = g[position];
			}
		}
		return merged;
	}

}
