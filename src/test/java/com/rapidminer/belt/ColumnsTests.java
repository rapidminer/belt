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

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;


/**
 * @author Gisa Meier
 */
public class ColumnsTests {

	private static final ColumnType<Integer> INTEGER_FREE_TYPE = ColumnTypes.freeType("test", Integer.class, null);

	private static List<String> getMappingList(int length) {
		List<String> list = new ArrayList<>(length);
		list.add(null);
		for (int i = 1; i < length; i++) {
			list.add("value" + i);
		}
		return list;
	}

	@Test
	public void testBicategorical() {
		Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(3), 1),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(2), 1),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(4)),
				new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11])};
		Boolean[] test = new Boolean[columns.length];
		Arrays.setAll(test, i -> Columns.isBicategorical(columns[i]));
		assertArrayEquals(new Boolean[]{false, true, false, false, false}, test);
	}

	@Test
	public void testAtMostBicategorical() {
		Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(3), 1),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(2), 1),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(4)),
				new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11])};
		Boolean[] test = new Boolean[columns.length];
		Arrays.setAll(test, i -> Columns.isAtMostBicategorical(columns[i]));
		assertArrayEquals(new Boolean[]{false, true, true, false, false}, test);
	}

	@Test
	public void testBicategoricalAndBiboolean() {
		Column[] columns = new Column[]{new DoubleArrayColumn(new double[43]),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(3), 1),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(3)),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(2), 1),
				new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[22], getMappingList(4)),
				new SimpleObjectColumn<>(INTEGER_FREE_TYPE, new Object[11]),
				new SimpleCategoricalColumn<String>(ColumnTypes.NOMINAL, new int[22], getMappingList(3), 1) {
					@Override
					public boolean toBoolean(int categoryIndex) {
						return false;
					}
				},
				new SimpleCategoricalColumn<String>(ColumnTypes.NOMINAL, new int[22], getMappingList(3), 1) {
					@Override
					public boolean toBoolean(int categoryIndex) {
						return true;
					}
				}};
		Boolean[] test = new Boolean[columns.length];
		Arrays.setAll(test, i -> Columns.isBicategoricalAndBiboolean(columns[i]));
		assertArrayEquals(new Boolean[]{false, true, false, false, false, false, false, false}, test);
	}


}
