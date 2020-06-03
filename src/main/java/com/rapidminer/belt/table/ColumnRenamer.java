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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnReference;


/**
 * Helper class to mass-rename the labels of a {@link Table}.
 *
 * @author Gisa Meier, Kevin Majchrzak
 */
final class ColumnRenamer {

	private final String[] labels;
	private final Map<String, Integer> labelMap;
	private Map<String, List<ColumnMetaData>> metaDataMap;
	private boolean copiedMetaData = false;

	/**
	 * Creates a new renamer by copying the input except for the metaDataMap. The metaDataMap will only be copied if
	 * changes are required. It renames the labels and updates the label map and meta data map according to the given
	 * renaming map. The {@link ColumnReference}s are also renamed.
	 *
	 * @param labels
	 * 		the old labels
	 * @param labelMap
	 * 		the old label map
	 * @param metaDataMap
	 * 		the old meta data map
	 * @param renamingMap
	 * 		The renaming map. It holds the mapping from old labels to new labels. Please be aware that duplicate labels in
	 * 		the resulting table will lead to an {@link IllegalArgumentException}.
	 * @throws IllegalArgumentException
	 * 		if a new name is invalid or already in use
	 */
	ColumnRenamer(String[] labels, Map<String, Integer> labelMap,
				  Map<String, List<ColumnMetaData>> metaDataMap, Map<String, String> renamingMap) {
		this.labels = Arrays.copyOf(labels, labels.length);
		this.labelMap = new HashMap<>(labelMap);
		this.metaDataMap = metaDataMap;
		rename(labelMap, metaDataMap, renamingMap);
	}

	/**
	 * @return the labels
	 */
	String[] getLabels() {
		return labels;
	}

	/**
	 * @return the label map
	 */
	Map<String, Integer> getLabelMap() {
		return labelMap;
	}

	/**
	 * @return the map from label to column meta data
	 */
	Map<String, List<ColumnMetaData>> getMetaDataMap() {
		return metaDataMap;
	}

	/**
	 * Applies the renaming to its fields.
	 *
	 * @throws IllegalArgumentException
	 * 		if a new name is invalid or already in use
	 */
	private void rename(Map<String, Integer> oldLabelMap, Map<String, List<ColumnMetaData>> oldMetaDataMap,
						Map<String, String> renamingMap) {
		// used to make sure there are no duplicate labels and to clean up the labelMap at the end
		Set<String> renamedAndNotAddedAgain = new HashSet<>();
		for (String oldLabel : renamingMap.keySet()) {
			if (oldLabelMap.containsKey(oldLabel)) {
				renamedAndNotAddedAgain.add(oldLabel);
			}
		}
		for (Map.Entry<String, String> renaming : renamingMap.entrySet()) {
			String oldLabel = renaming.getKey();
			Integer index = oldLabelMap.get(oldLabel);
			// old labels that were not part of the table are ignored
			if (index != null) {
				String newLabel = renaming.getValue();
				Table.requireValidLabel(newLabel);
				if (labelMap.containsKey(newLabel) && !renamedAndNotAddedAgain.remove(newLabel)) {
					// throw if the table already contains the label (and it has / will not be renamed)
					throw new IllegalArgumentException("Table already contains label " + newLabel);
				}
				replaceLabel(oldMetaDataMap, oldLabel, newLabel, index);
			}
		}
		// clean up labelMap and metaDataMap (we remove the labels / meta data data for labels
		// that were deleted but not added again)
		for(String s : renamedAndNotAddedAgain){
			labelMap.remove(s);
			metaDataMap.remove(s);
		}
		renameReferences(renamingMap);
	}

	/**
	 * Helper method that should never be used outside of {@link #rename(Map, Map, Map)}. It replaces a label but does
	 * not remove the old one. Therefore, the labels, labelMap and metaDataMap remain in a possibly inconsistent state
	 * when this method finishes. The cleanup happens at the end of the rename method.
	 */
	private void replaceLabel(Map<String, List<ColumnMetaData>> oldMetaDataMap, String oldLabel, String newLabel,
							  Integer index) {
		labels[index] = newLabel;
		labelMap.put(newLabel, index);
		if (oldMetaDataMap.containsKey(oldLabel)) {
			if (!copiedMetaData) {
				metaDataMap = new HashMap<>(metaDataMap);
				copiedMetaData = true;
			}
			metaDataMap.put(newLabel, oldMetaDataMap.get(oldLabel));
		} else {
			if (!copiedMetaData) {
				metaDataMap = new HashMap<>(metaDataMap);
				copiedMetaData = true;
			}
			// this is the equivalent to the put from above
			// for the case that the old meta data map has no entry
			// for oldLabel
			metaDataMap.remove(newLabel);
		}
	}

	/**
	 * Runs once over all meta data and checks for {@link ColumnReference}s that refer to a column that is renamed.
	 *
	 * @param renamingMap
	 * 		the map from old to new column name
	 */
	private void renameReferences(Map<String, String> renamingMap) {
		for (Map.Entry<String, List<ColumnMetaData>> entry : metaDataMap.entrySet()) {
			int index = 0;
			for (ColumnMetaData columnMetaData : entry.getValue()) {
				if (columnMetaData instanceof ColumnReference) {
					String newName = renamingMap.get(((ColumnReference) columnMetaData).getColumn());
					if (newName != null) {
						renameReference(entry.getKey(), entry.getValue(), index, newName,
								((ColumnReference) columnMetaData).getValue());
					}
					break;
				}
				index++;
			}
		}
	}

	/**
	 * Renames the column reference at the index position in the oldMetaData by replacing it by a new reference.
	 */
	private void renameReference(String key, List<ColumnMetaData> oldMetaData, int index, String newName,
								 String value) {
		if (!copiedMetaData) {
			metaDataMap = new HashMap<>(metaDataMap);
			copiedMetaData = true;
		}
		List<ColumnMetaData> newMetaData = new ArrayList<>(oldMetaData);
		newMetaData.set(index, new ColumnReference(newName, value));
		metaDataMap.put(key, newMetaData);
	}

}