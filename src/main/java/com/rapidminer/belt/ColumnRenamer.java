/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2018 RapidMiner GmbH
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


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnReference;


/**
 * Helper class to mass-rename the labels of a {@link Table}.
 *
 * @author Gisa Meier
 */
final class ColumnRenamer {

	private final String[] labels;
	private final Map<String, Integer> labelMap;
	private Map<String, List<ColumnMetaData>> metaDataMap;
	private boolean copiedMetaData = false;

	/**
	 * Creates a new renamer by copying the input except for the metaDataMap. The metaDataMap will only be copied if
	 * changes are required.
	 */
	ColumnRenamer(String[] labels, Map<String, Integer> labelMap,
				  Map<String, List<ColumnMetaData>> metaDataMap) {
		this.labels = Arrays.copyOf(labels, labels.length);
		this.labelMap = new HashMap<>(labelMap);
		this.metaDataMap = metaDataMap;
	}

	/**
	 * Applies the renaming to its fields.
	 *
	 * @throws IllegalArgumentException
	 * 		if a new name is invalid or already in use
	 */
	void rename(Map<String, String> renamingMap) {
		for (Map.Entry<String, String> renaming : renamingMap.entrySet()) {
			String oldLabel = renaming.getKey();
			String newLabel = renaming.getValue();

			Table.requireValidLabel(newLabel);
			Integer index = labelMap.remove(oldLabel);
			if (labelMap.containsKey(newLabel)) {
				throw new IllegalArgumentException("Table already contains label " + newLabel);
			}

			if (index != null) {
				replaceLabel(oldLabel, newLabel, index);
			}
			updateColumnReferences(oldLabel, newLabel);
		}
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

	private void replaceLabel(String oldLabel, String newLabel, Integer index) {
		labels[index] = newLabel;
		labelMap.put(newLabel, index);
		if (metaDataMap.containsKey(oldLabel)) {
			if (!copiedMetaData) {
				metaDataMap = new HashMap<>(metaDataMap);
				copiedMetaData = true;
			}
			metaDataMap.put(newLabel, metaDataMap.remove(oldLabel));
		}
	}

	/**
	 * Updates the {@link ColumnReference}s in the meta data map by renaming them from the old to the new label.
	 */
	private void updateColumnReferences(String oldLabel, String newLabel) {
		for (Map.Entry<String, List<ColumnMetaData>> entry : metaDataMap.entrySet()) {
			ColumnReference invalidReference = findInvalidReference(oldLabel, entry.getValue());
			if (invalidReference != null) {
				if (!copiedMetaData) {
					metaDataMap = new HashMap<>(metaDataMap);
					copiedMetaData = true;
				}
				replaceInvalidReference(invalidReference, newLabel, entry.getKey(), metaDataMap);
			}
		}
	}

	/**
	 * Finds the {@link ColumnReference} in the meta data list that has the referenced label as reference column.
	 *
	 * @param referencedLabel
	 * 		the label to find
	 * @param metaData
	 * 		the list to search
	 * @return the found column reference or {@code null}
	 */
	static ColumnReference findInvalidReference(String referencedLabel, List<ColumnMetaData> metaData) {
		for (ColumnMetaData data : metaData) {
			if (data instanceof ColumnReference) {
				//there is only one entry of this type because ColumnReference is column unique
				ColumnReference reference = (ColumnReference) data;
				if (reference.getColumn().equals(referencedLabel)) {
					return reference;
				}
				return null;
			}
		}
		return null;
	}

	/**
	 * Removes the invalid reference from the map and replaces it by a new column reference if the replacement label is
	 * not {@code null}.
	 *
	 * @param invalidReference
	 * 		the invalid reference to remove
	 * @param replacementLabel
	 * 		the replacement column label for a new column reference. If this is {@code null} no new reference is added.
	 * @param key
	 * 		the key for the map entry to consider
	 * @param columnMetaData
	 * 		the map containing the key
	 */
	static void replaceInvalidReference(ColumnReference invalidReference, String replacementLabel,
										String key, Map<String, List<ColumnMetaData>> columnMetaData) {
		List<ColumnMetaData> list = columnMetaData.get(key);
		list.remove(invalidReference);
		if (replacementLabel != null) {
			list.add(new ColumnReference(replacementLabel, invalidReference.getValue()));
		} else if (list.isEmpty()) {
			columnMetaData.remove(key);
		}
	}


}