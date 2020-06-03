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

package com.rapidminer.belt.table;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnRole;


/**
 * @author Kevin Majchrzak
 */
public class ColumnRenamerTests {

	/**
	 * Tests that the renaming operations work as expected and independent of their order. Also tests that label names
	 * can be swapped (e.g. renaming "A" to "B" and "B" to "A").
	 */
	@Test
	public void testIndependentOrder() {
		String[] labelsOne = new String[]{"A", "B", "C", "D", "E"};
		String[] labelsTwo = Arrays.copyOf(labelsOne, labelsOne.length);

		Map<String, Integer> labelMapOne = new LinkedHashMap<>();
		labelMapOne.put("A", 0);
		labelMapOne.put("B", 1);
		labelMapOne.put("C", 2);
		labelMapOne.put("D", 3);
		labelMapOne.put("E", 4);

		Map<String, Integer> labelMapTwo = new LinkedHashMap<>();
		labelMapTwo.put("C", 2);
		labelMapTwo.put("A", 0);
		labelMapTwo.put("D", 3);
		labelMapTwo.put("E", 4);
		labelMapTwo.put("B", 1);

		Map<String, List<ColumnMetaData>> metaDataMapOne = new LinkedHashMap<>();
		metaDataMapOne.put("A", Collections.singletonList(ColumnRole.ID));
		metaDataMapOne.put("B", Collections.singletonList(ColumnRole.LABEL));
		metaDataMapOne.put("C", Collections.singletonList(ColumnRole.PREDICTION));
		metaDataMapOne.put("D", Collections.singletonList(ColumnRole.WEIGHT));

		Map<String, List<ColumnMetaData>> metaDataMapTwo = new LinkedHashMap<>();
		metaDataMapTwo.put("B", Collections.singletonList(ColumnRole.LABEL));
		metaDataMapTwo.put("C", Collections.singletonList(ColumnRole.PREDICTION));
		metaDataMapTwo.put("A", Collections.singletonList(ColumnRole.ID));
		metaDataMapTwo.put("D", Collections.singletonList(ColumnRole.WEIGHT));

		Map<String, String> renamingMapOne = new LinkedHashMap<>();
		renamingMapOne.put("A", "B");
		renamingMapOne.put("B", "C");
		renamingMapOne.put("C", "Y");
		renamingMapOne.put("D", "A");

		Map<String, String> renamingMapTwo = new LinkedHashMap<>();
		renamingMapTwo.put("B", "C");
		renamingMapTwo.put("A", "B");
		renamingMapTwo.put("D", "A");
		renamingMapTwo.put("C", "Y");

		ColumnRenamer renamerOne = new ColumnRenamer(labelsOne, labelMapOne, metaDataMapOne, renamingMapOne);
		ColumnRenamer renamerTwo = new ColumnRenamer(labelsTwo, labelMapTwo, metaDataMapTwo, renamingMapTwo);

		assertArrayEquals(renamerOne.getLabels(), renamerTwo.getLabels());
		assertEquals(renamerOne.getLabelMap(), renamerTwo.getLabelMap());
		assertEquals(renamerOne.getMetaDataMap(), renamerTwo.getMetaDataMap());
	}

	/**
	 * Tests that attempting to add duplicate labels will lead to an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testDuplicateLabels() {
		String[] labels = new String[]{"A", "B", "C", "D", "E"};

		Map<String, Integer> labelMap = new LinkedHashMap<>();
		labelMap.put("A", 0);
		labelMap.put("B", 1);
		labelMap.put("C", 2);
		labelMap.put("D", 3);
		labelMap.put("E", 4);

		Map<String, List<ColumnMetaData>> metaDataMap = new LinkedHashMap<>();
		metaDataMap.put("A", Collections.singletonList(ColumnRole.ID));
		metaDataMap.put("B", Collections.singletonList(ColumnRole.LABEL));
		metaDataMap.put("C", Collections.singletonList(ColumnRole.PREDICTION));
		metaDataMap.put("D", Collections.singletonList(ColumnRole.WEIGHT));

		// try to add X twice - should lead to IllegalArgumentException
		Map<String, String> renamingMap = new LinkedHashMap<>();
		renamingMap.put("D", "X");
		renamingMap.put("C", "B");
		renamingMap.put("X", "Y");
		renamingMap.put("B", "X");

		ColumnRenamer renamer = new ColumnRenamer(labels, labelMap, metaDataMap, renamingMap);
	}

	/**
	 * Tests that unknown labels (old labels that are not in the table) are ignored.
	 */
	@Test
	public void testIgnoreUnknownLabels() {
		String[] labelsOne = new String[]{"A", "B", "C", "D", "E"};
		String[] labelsTwo = Arrays.copyOf(labelsOne, labelsOne.length);

		Map<String, Integer> labelMapOne = new LinkedHashMap<>();
		labelMapOne.put("A", 0);
		labelMapOne.put("B", 1);
		labelMapOne.put("C", 2);
		labelMapOne.put("D", 3);
		labelMapOne.put("E", 4);

		Map<String, Integer> labelMapTwo = new LinkedHashMap<>();
		labelMapTwo.put("C", 2);
		labelMapTwo.put("A", 0);
		labelMapTwo.put("D", 3);
		labelMapTwo.put("E", 4);
		labelMapTwo.put("B", 1);

		Map<String, List<ColumnMetaData>> metaDataMapOne = new LinkedHashMap<>();
		metaDataMapOne.put("A", Arrays.asList(ColumnRole.ID));
		metaDataMapOne.put("B", Arrays.asList(ColumnRole.LABEL));
		metaDataMapOne.put("C", Arrays.asList(ColumnRole.PREDICTION));
		metaDataMapOne.put("D", Arrays.asList(ColumnRole.WEIGHT));

		Map<String, List<ColumnMetaData>> metaDataMapTwo = new LinkedHashMap<>();
		metaDataMapTwo.put("B", Arrays.asList(ColumnRole.LABEL));
		metaDataMapTwo.put("C", Arrays.asList(ColumnRole.PREDICTION));
		metaDataMapTwo.put("A", Arrays.asList(ColumnRole.ID));
		metaDataMapTwo.put("D", Arrays.asList(ColumnRole.WEIGHT));

		Map<String, String> renamingMapOne = new LinkedHashMap<>();
		renamingMapOne.put("A", "B");
		renamingMapOne.put("B", "C");
		renamingMapOne.put("C", "Y");
		renamingMapOne.put("D", "A");

		Map<String, String> renamingMapTwo = new LinkedHashMap<>();
		renamingMapTwo.put("A", "B");
		renamingMapTwo.put("W", "A"); // should be ignored
		renamingMapTwo.put("B", "C");
		renamingMapTwo.put("C", "Y");
		renamingMapTwo.put("Y", "Z"); // should be ignored
		renamingMapTwo.put("D", "A");

		ColumnRenamer renamerOne = new ColumnRenamer(labelsOne, labelMapOne, metaDataMapOne, renamingMapOne);
		ColumnRenamer renamerTwo = new ColumnRenamer(labelsTwo, labelMapTwo, metaDataMapTwo, renamingMapTwo);

		assertArrayEquals(renamerOne.getLabels(), renamerTwo.getLabels());
		assertEquals(renamerOne.getLabelMap(), renamerTwo.getLabelMap());
		assertEquals(renamerOne.getMetaDataMap(), renamerTwo.getMetaDataMap());
	}
}
