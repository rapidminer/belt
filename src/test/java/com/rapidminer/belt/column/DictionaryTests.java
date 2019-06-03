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

package com.rapidminer.belt.column;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.Test;

import com.rapidminer.belt.column.BooleanDictionary;
import com.rapidminer.belt.column.Dictionary;


/**
 * @author Gisa Meier
 */
public class DictionaryTests {

	@Test
	public void testToString() {
		Dictionary<String> dic = new Dictionary<>(Arrays.asList(null, "bla", "blub", "blubblub"));
		assertEquals("Dictionary [null, bla, blub, blubblub]", dic.toString());
	}

	@Test
	public void testBooleanToString() {
		Dictionary<String> dic = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		assertEquals("Dictionary [null, bla, blub], positive: bla", dic.toString());
	}

	@Test
	public void testBooleanToStringNoPositive() {
		Dictionary<String> dic = new BooleanDictionary<>(Arrays.asList(null, "bla"), -1);
		assertEquals("Dictionary [null, bla], no positive", dic.toString());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBooleanTooBig() {
		new BooleanDictionary<>(Arrays.asList(null, "bla", "blub", "blubblub"), 1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBooleanTwoNoPositive() {
		new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), -1);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBooleanNullIndex() {
		new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 0);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBooleanOutOfBoundsIndex() {
		new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testBooleanDictWithNullWrongIndex() {
		new BooleanDictionary<>(Arrays.asList(null, null, "bla"), 1);
	}

	@Test
	public void testBooleanDictWithNull() {
		Dictionary<String> dict = new BooleanDictionary<>(Arrays.asList(null, null, "bla"), 2);
		assertTrue(dict.isBoolean());
		assertTrue(dict.hasPositive());
		assertFalse(dict.hasNegative());
		assertEquals(2, dict.getPositiveIndex());
		assertEquals(1, dict.size());
		assertEquals(2, dict.maximalIndex());
		assertEquals(-1, dict.getNegativeIndex());
	}

	@Test
	public void testBooleanDictWithNullNegative() {
		Dictionary<String> dict = new BooleanDictionary<>(Arrays.asList(null, null, "bla"), -1);
		assertTrue(dict.isBoolean());
		assertFalse(dict.hasPositive());
		assertTrue(dict.hasNegative());
		assertEquals(2, dict.getNegativeIndex());
		assertEquals(1, dict.size());
		assertEquals(2, dict.maximalIndex());
	}

	@Test
	public void testBooleanDictNegative() {
		Dictionary<String> dict = new BooleanDictionary<>(Arrays.asList(null, "bla", "blup"), 1);
		assertEquals(2, dict.getNegativeIndex());

		dict = new BooleanDictionary<>(Arrays.asList(null, "bla", "blup"), 2);
		assertEquals(1, dict.getNegativeIndex());

		dict = new BooleanDictionary<>(Arrays.asList(null, "bla"), 1);
		assertEquals(-1, dict.getNegativeIndex());

		dict = new BooleanDictionary<>(Arrays.asList(null, "bla"), -1);
		assertEquals(1, dict.getNegativeIndex());

		dict = new BooleanDictionary<>(Collections.singletonList(null), -1);
		assertEquals(-1, dict.getNegativeIndex());
	}

	@Test
	public void testValueList() {
		assertEquals(Arrays.asList(null, "bla", "blub"), new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"),
				1).getValueList());
	}

	@Test
	public void testGet() {
		Dictionary<String> dict = new Dictionary<>(Arrays.asList(null, "bla", null, null, "blup"), 2);
		String[] result = new String[7];
		for (int i = 0; i < 7; i++) {
			result[i] = dict.get(i - 1);
		}
		assertArrayEquals(new String[]{null, null, "bla", null, null, "blup", null}, result);
	}

	@Test
	public void testUnused() {
		Dictionary<String> dict = new Dictionary<>(Arrays.asList(null, "bla", null, null, "blup"), 2);
		assertEquals(4, dict.maximalIndex());
		assertEquals(2, dict.size());
	}

	@Test
	public void testIterator() {
		Dictionary<Integer> dic = new Dictionary<>(Arrays.asList(null, 1, 2, 3, 4, 5));
		int index = 1;
		for (Dictionary.Entry e : dic) {
			assertEquals(index, e.getIndex());
			assertEquals(index++, e.getValue());
		}
	}

	@Test
	public void testIteratorUnused() {
		Dictionary<Integer> dic = new Dictionary<>(Arrays.asList(null, 1, 2, null, null, 5, 6, 7, null, 9));
		int count = 0;
		for (Dictionary.Entry e : dic) {
			assertEquals(e.getValue(), e.getIndex());
			count++;
		}
		assertEquals(6, count);
	}

	@Test
	public void testInverse() {
		Dictionary<String> dic = new Dictionary<>(Arrays.asList(null, "a", "b", "c", "d", "e"));
		Map<String, Integer> inverse = dic.createInverse();
		assertEquals(inverse.get(null).intValue(), 0);
		for(Dictionary.Entry e: dic){
			assertEquals(inverse.get(e.getValue()).intValue(), e.getIndex());
		}
	}

	@Test
	public void testBooleanEquals() {
		BooleanDictionary<String> dic1 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		BooleanDictionary<String> dic2 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		assertTrue(dic1.equals(dic2));
	}

	@Test
	public void testBooleanNotEqualIndex() {
		BooleanDictionary<String> dic1 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		BooleanDictionary<String> dic2 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 2);
		assertFalse(dic1.equals(dic2));
	}

	@Test
	public void testBooleanNotEqualDic() {
		BooleanDictionary<String> dic1 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		BooleanDictionary<String> dic2 = new BooleanDictionary<>(Arrays.asList(null, "bla"), 1);
		assertFalse(dic1.equals(dic2));
	}

	@Test
	public void testBooleanNotEqualNonBooleanDic() {
		Dictionary<String> dic1 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		Dictionary<String> dic2 = new Dictionary<>(Arrays.asList(null, "bla", "blub"));
		assertFalse(dic1.equals(dic2));
		assertFalse(dic2.equals(dic1));
	}

	@Test
	public void testBooleanEqualsSame() {
		BooleanDictionary<String> dic1 = new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1);
		BooleanDictionary<String> dic2 = dic1;
		assertTrue(dic1.equals(dic2));
	}

	@Test
	public void testEquals() {
		Dictionary<String> dic1 = new Dictionary<>(Arrays.asList(null, "bla", "blub"));
		Dictionary<String> dic2 = new Dictionary<>(Arrays.asList(null, "bla", "blub"));
		assertTrue(dic1.equals(dic2));
	}

	@Test
	public void testNotEquals() {
		Dictionary<String> dic1 = new Dictionary<>(Arrays.asList(null, "bla", "blub"));
		Dictionary<String> dic2 = new Dictionary<>(Arrays.asList(null, "bla", "blub", "blip"));
		assertFalse(dic1.equals(dic2));
	}

	@Test
	public void testToBoolean() {
		Dictionary<String> dic = new Dictionary<>(Arrays.asList(null, "bla", "blub"));
		assertFalse(dic.isBoolean());
		assertEquals(new BooleanDictionary<>(Arrays.asList(null, "bla", "blub"), 1), dic.toBoolean("bla"));
	}

	@Test
	public void testToBooleanNull() {
		Dictionary<String> dic = new Dictionary<>(Arrays.asList(null, "bla"));
		assertEquals(new BooleanDictionary<>(Arrays.asList(null, "bla"), -1), dic.toBoolean(null));
	}
}
