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

import java.util.Arrays;
import java.util.List;

import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;


/**
 * Utility class for sorting a table by multiple columns.
 *
 * @author Gisa Meier, Michael Knopf
 */
class TableSorter {

	private final Column[] columnSet;
	private final int[] columns;

	private final boolean varyingOrders;
	private final List<Order> orders;
	private final Order order;

	/**
	 * Creates new table sorter with varying sorting orders.
	 *
	 * @param table
	 * 		the table to sort
	 * @param orders
	 * 		the list of sorting orders
	 * @param columns
	 * 		the columns to sort
	 */
	TableSorter(Table table, List<Order> orders, int[] columns) {
		this.columnSet = table.getColumns();
		this.columns = columns;
		this.varyingOrders = true;
		this.orders = orders;
		this.order = null;
	}

	/**
	 * Creates new table sorter with a constant sorting order.
	 *
	 * @param table
	 * 		the table to sort
	 * @param order
	 * 		the sorting order
	 * @param columns
	 * 		the columns to sort
	 */
	TableSorter(Table table, Order order, int[] columns) {
		this.columnSet = table.getColumns();
		this.columns = columns;
		this.varyingOrders = false;
		this.orders = null;
		this.order = order;
	}

	/**
	 * Indirect stable sort of the wrapped table by the given columns.
	 *
	 * @return the index mapping resulting in a sorted sequence
	 */
	int[] sort() {
		Column first = columnSet[columns[0]];
		Order sortingOrder = varyingOrders ? orders.get(0) : order;
		int[] sortedMapping = first.sort(sortingOrder);
		if (columns.length > 1 && first.size() > 1) {
			sortUniformIntervals(0, sortedMapping);
		}
		return sortedMapping;
	}

	/**
	 * Sorts the sub-interval {@code [start, end)} of the given mapping by the column of the given index first, followed
	 * by the the column with the next higher index and so on.
	 *
	 * @param index
	 * 		the index of the first column to sort by
	 * @param mapping
	 * 		the mapping to sort partially
	 * @param start
	 * 		the start of the sub-interval to sort (inclusive)
	 * @param end
	 * 		the end of the sub-interval to sort (exclusive)
	 */
	private void sort(int index, int[] mapping, int start, int end) {
		Column column = columnSet[columns[index]];
		Order sortingOrder = varyingOrders ? orders.get(index) : order;

		int[] mappingSubset = Arrays.copyOfRange(mapping, start, end);
		column = column.map(mappingSubset, true);

		int[] sorting = column.sort(sortingOrder);
		int[] sortedMappingSubset = Mapping.merge(sorting, mappingSubset);

		if (index < columns.length - 1) {
			// There are more columns to sort by
			sortUniformIntervals(index, sortedMappingSubset);
		}

		// Copy the sorted sub interval into the original mapping
		System.arraycopy(sortedMappingSubset, 0, mapping, start, sortedMappingSubset.length);
	}

	/**
	 * Searches the column defined by the given index and mapping for uniform intervals. Found intervals are sorted by
	 * the next column.
	 *
	 * @param index
	 * 		the index of the current column
	 * @param mapping
	 * 		the mapping (subset) to search
	 */
	private void sortUniformIntervals(int index, final int[] mapping) {
		Column column = columnSet[columns[index]];
		column = column.map(mapping, true);

		ColumnReader reader = new ColumnReader(column, column.size());

		double lastValue = reader.read();
		int marker = 0;
		int position = 1;
		while (reader.hasRemaining()) {
			double value = reader.read();
			if (Double.compare(lastValue, value) != 0) {
				if (position - marker > 1) {
					// The last two or more elements had the same value
					sort(index + 1, mapping, marker, position);
				}
				lastValue = value;
				marker = position;
			}
			position++;
		}

		// Check whether last value was part of uniform interval
		if (position - marker > 1) {
			sort(index + 1, mapping, marker, position);
		}
	}

}
