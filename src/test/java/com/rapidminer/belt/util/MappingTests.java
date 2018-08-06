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

package com.rapidminer.belt.util;

import static org.junit.Assert.assertArrayEquals;

import java.util.Arrays;

import org.junit.Test;


/**
 * @author Gisa Schaefer
 */
public class MappingTests {

	private static final double EPSILON = 1e-10;

	@Test
	public void testMaterializeEmpty() {
		int nValues = 15;
		double[] data = new double[nValues];
		Arrays.setAll(data, i -> i);

		assertArrayEquals(new double[0], Mapping.apply(data, new int[0]), EPSILON);
	}

	@Test
	public void testMaterialize() {
		int nValues = 15;
		double[] data = new double[nValues];
		Arrays.setAll(data, i -> i);
		int[] mapping = {2, 5, 8, 3, 1, 7, 11, 0, 5};
		double[] expected = {2, 5, 8, 3, 1, 7, 11, 0, 5};

		assertArrayEquals(expected, Mapping.apply(data, mapping), EPSILON);
	}

	@Test
	public void testMaterializeOutOfBounds() {
		int nValues = 15;
		double[] data = new double[nValues];
		Arrays.setAll(data, i -> i);
		int[] mapping = {2, 5, 8, 3, -1, 7, 11, 0, 15};
		double[] expected = {2, 5, 8, 3, Double.NaN, 7, 11, 0, Double.NaN};

		assertArrayEquals(expected, Mapping.apply(data, mapping), EPSILON);
	}

	@Test
	public void testMerge() {
		int[] first = {0, 2, 4, 6};
		int[] second = {0, 2, 4, 6, 8, 10, 12, 14};
		int[] expected = {0, 4, 8, 12};

		assertArrayEquals(expected, Mapping.merge(first, second));
	}

	@Test
	public void testMergeOutOfBounds() {
		int[] first = {0, 4, 8, -2};
		int[] second = {0, 2, 4, 6, 8, 10, 12, 14};
		int[] expected = {0, 8, -1, -1};

		assertArrayEquals(expected, Mapping.merge(first, second));
	}

	@Test
	public void testMergeSecondOutOfBounds() {
		int[] first = {0, 2, 4, 6};
		int[] second = {0, 2, -4, 6, 8, 10, 122, 14};
		int[] expected = {0, -4, 8, 122};

		assertArrayEquals(expected, Mapping.merge(first, second));
	}


	@Test
	public void testMergeOfEmptyMapping() {
		int[] first = {};
		int[] second = {0, 2, 4, 6, 8, 10, 12, 14};
		int[] expected = {};

		assertArrayEquals(expected, Mapping.merge(first, second));
	}

	@Test
	public void testMergeOfSuperset() {
		int[] first = {0, 2, 0, 3, 5, 5, 5, 7, 8, 8, 8, 4, 6, 9, 10, 9, 10};
		int[] second = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
		int[] expected = {0, 2, 0, 3, 5, 5, 5, 7, 8, 8, 8, 4, 6, 9, 10, 9, 10};

		assertArrayEquals(expected, Mapping.merge(first, second));
	}

}
