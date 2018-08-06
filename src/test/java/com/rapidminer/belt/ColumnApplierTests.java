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
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.DoubleBinaryOperator;
import java.util.function.IntToDoubleFunction;
import java.util.function.ToDoubleFunction;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Tests {@link ApplierNumericToNumeric} and {@link ApplierNumericToNumericMulti}.
 *
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class ColumnApplierTests {

	private static final double EPSILON = 1e-10;

	private static final int NUMBER_OF_ROWS = 75;

	private static final Context CTX = Belt.defaultContext();

	/**
	 * Use anonymous classes instead of lambdas here: lambdas in parallel before outer class is initialized cause
	 * deadlocks!
	 */
	@SuppressWarnings("Convert2Lambda")
	private static final Table table = Table.newTable(NUMBER_OF_ROWS)
			.addReal("a", new IntToDoubleFunction() {
						@Override
						public double applyAsDouble(int value) {
							return value;
						}
					}
			)
			.addReal("b", new IntToDoubleFunction() {
						@Override
						public double applyAsDouble(int value) {
							return 2 * value;
						}
					}
			)
			.build(CTX);

	private static final int MAX_VALUES = 30;

	private static final ColumnType<String> STRING_FREE_TYPE = ColumnTypes.freeType("test", String.class, null);

	private static final Column OBJECT_NOT_READABLE_COLUMN = new Column(EnumSet.of(Column.Capability.SORTABLE), 3) {
		@Override
		public ColumnType<?> type() {
			return ColumnTypes.NOMINAL;
		}

		@Override
		Column map(int[] mapping, boolean preferView) {
			return null;
		}

		@Override
		long writeToChannel(FileChannel channel, long startPosition) throws IOException {
			return 0;
		}
	};


	private static double[] readBufferToArray(ColumnBuffer buffer) {
		double[] data = new double[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(CategoricalColumnBuffer<?> buffer) {
		AbstractCategoricalColumnBuffer<?> readBuffer = (AbstractCategoricalColumnBuffer<?>) buffer;
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = readBuffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(FreeColumnBuffer<?> buffer) {
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(TimeColumnBuffer buffer) {
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(AbstractDateTimeBuffer buffer) {
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	private static int[] randomInts(int n) {
		int[] data = new int[n];
		Random random = new Random();
		int bound = Math.min(Math.max(2, n / 20), MAX_VALUES - 1);
		Arrays.setAll(data, i -> random.nextInt(bound) + 1);
		return data;
	}

	private static List<String> getMappingList() {
		List<String> list = new ArrayList<>(MAX_VALUES);
		list.add(null);
		for (int i = 1; i < 30; i++) {
			list.add("value" + i);
		}
		return list;
	}

	public static class InputValidationOneColumnNumericToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform("a").applyNumericToReal(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a").applyNumericToReal(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform("b").applyNumericToReal(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToReal(i -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationOneColumnNumericToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyNumericToInteger(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToInteger(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToInteger(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToInteger(i -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnCategoricalToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyCategoricalToReal(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToReal(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToReal(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToReal(i -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationOneColumnCategoricalToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyCategoricalToInteger(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToInteger(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToInteger(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToInteger(i -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationOneColumnObjectToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyObjectToReal(String.class, i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOpeator() {
			table.transform(0).applyObjectToReal(String.class, null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToReal(null, i -> 0, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToReal(String.class, i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToReal(String.class, i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToReal(String.class, i -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationOneColumnObjectToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyObjectToInteger(String.class, i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToInteger(String.class, null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToInteger(null, i -> 0, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToInteger(String.class, i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToInteger(String.class, i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN)
					.applyObjectToInteger(String.class, i -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyNumericToCategorical(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToCategorical(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToCategorical(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToCategorical(i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(0).applyNumericToCategorical(i -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(0).applyNumericToCategorical(i -> 0, 1, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperatorFormat() {
			table.transform(0).applyNumericToCategorical(null, 1, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToCategorical(i -> 0, Integer.MAX_VALUE, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToFree {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyNumericToFree(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToFree(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToFree(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToFree(i -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyNumericToTime(i -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToTime(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToTime(i -> null, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToTime(i -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyNumericToDateTime(i -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToDateTime(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToDateTime(i -> null, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToDateTime(i -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnCategoricalToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyCategoricalToCategorical(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToCategorical(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToCategorical(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToCategorical(i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(0).applyCategoricalToCategorical(i -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(0).applyCategoricalToCategorical(i -> 0, 1, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperatorFormat() {
			table.transform(0).applyCategoricalToCategorical(null, 1, Workload.LARGE, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToCategorical(i -> 0, Integer.MAX_VALUE, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationOneColumnCategoricalToFree {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyCategoricalToFree(i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToFree(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToFree(i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToFree(i -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnCategoricalToTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyCategoricalToTime(i -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToTime(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToTime(i -> null, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToTime(i -> null, Workload.DEFAULT, CTX);
		}
	}


	public static class InputValidationOneColumnCategoricalToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyCategoricalToDateTime(i -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToDateTime(null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToDateTime(i -> null, Workload.DEFAULT, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleFreeColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToDateTime(i -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToCategorical(null, i -> 0, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToCategorical(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN)
					.applyObjectToCategorical(String.class, i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypeFormat() {
			table.transform(0).applyObjectToCategorical(null, i -> 0, 1, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, 1, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperatorFormat() {
			table.transform(0).applyObjectToCategorical(String.class, null, 1, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeFormat() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, 2, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN)
					.applyObjectToCategorical(String.class, i -> 0, 3, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToFree {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyObjectToFree(String.class, i -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToFree(String.class, null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToFree(null, i -> 0, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToFree(String.class, i -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToFree(String.class, i -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToFree(String.class, i -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyObjectToTime(String.class, i -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToTime(String.class, null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToTime(null, i -> null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToTime(String.class, i -> null, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToTime(String.class, i -> null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN)
					.applyObjectToTime(String.class, i -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(0).applyObjectToDateTime(String.class, i -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToDateTime(String.class, null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToDateTime(null, i -> null, Workload.LARGE, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToDateTime(String.class, i -> null, Workload.DEFAULT, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToDateTime(String.class, i -> null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN)
					.applyObjectToDateTime(String.class, i -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform("a", "b").applyNumericToReal(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").applyNumericToReal(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform("a", "b").applyNumericToReal((ToDoubleFunction<Row>) null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Arrays.asList(table.column("a"), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToReal(row -> 0, Workload.DEFAULT, CTX);
		}


	}

	public static class InputValidationMoreColumnsNumericToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyNumericToInteger(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToInteger(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToInteger(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Arrays.asList(table.column("a"), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToInteger(row -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyCategoricalToReal(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToReal(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToReal(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyCategoricalToReal(row -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyCategoricalToInteger(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToInteger(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToInteger(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyCategoricalToInteger(row -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneralToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyGeneralToReal(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyGeneralToReal(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyGeneralToReal(null, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneralToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyGeneralToInteger(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyGeneralToInteger(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyGeneralToInteger(null, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToReal {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform().applyObjectToReal(String.class, row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToReal(String.class, row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToReal(null, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToReal(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToReal(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(new int[]{0, 1}).applyObjectToReal(String.class, row -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsObjectToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToInteger(null, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform().applyObjectToInteger(String.class, row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(Arrays.asList("a", "b"))
					.applyObjectToInteger(String.class, row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToInteger(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToInteger(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(new int[]{0, 1}).applyObjectToInteger(String.class, row -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsObjectToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(null, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(Arrays.asList("a", "b"))
					.applyObjectToCategorical(String.class, row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToCategorical(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypeFormat() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(null, row -> 0, 1, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, row -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(Arrays.asList("a", "b"))
					.applyObjectToCategorical(String.class, row -> 0, 1, Workload.DEFAULT,
							null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, null, 1, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToCategorical(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsCategoricalToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(null, Workload.DEFAULT, CTX);
		}


		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(row -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(row -> 0, 1, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(null, 1, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyCategoricalToCategorical(row -> 0, Workload.DEFAULT, CTX);
		}

	}


	public static class InputValidationMoreColumnsNumericToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(row -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(row -> 0, 1, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(null, 1, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new TransformerMulti(Arrays.asList(table.column(0), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToCategorical(row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Arrays.asList(table.column(0), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToCategorical(row -> 0, 1, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsGeneralToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyGeneralToCategorical(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyGeneralToCategorical(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyGeneralToCategorical(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkloadFormat() {
			table.transform(new int[]{0, 1}).applyGeneralToCategorical(row -> 0, 1, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(new int[]{0, 1}).applyGeneralToCategorical(row -> 0, 1, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyGeneralToCategorical(null, 1, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToFree {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToFree(null, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyObjectToFree(String.class, row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToFree(String.class, row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToFree(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToFree(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToFree(String.class, row -> 0, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToTime {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToTime(null, row -> null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToTime(String.class, row -> null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, row -> null, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(null, row -> null, Workload.DEFAULT, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToDateTime(String.class, row -> null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, row -> null, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsCategoricalToFree {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyCategoricalToFree(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToFree(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToFree(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToFree(row -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(row -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(row -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToFree {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyNumericToFree(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToFree(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToFree(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToFree(row -> 0, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyNumericToTime(row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToTime(row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToTime(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToTime(row -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyNumericToDateTime(row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToDateTime(row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToDateTime(null, Workload.DEFAULT, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new TransformerMulti(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToDateTime(row -> null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsGeneralToFree {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyGeneralToFree(row -> 0, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyGeneralToFree(row -> 0, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyGeneralToFree(null, Workload.DEFAULT, CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneralToTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyGeneralToTime(row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyGeneralToTime(row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyGeneralToTime(null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationMoreColumnsGeneralToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform(new int[]{0, 1}).applyGeneralToDateTime(row -> null, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyGeneralToDateTime(row -> null, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyGeneralToDateTime(null, Workload.DEFAULT, CTX);
		}
	}

	public static class InputValidationTwoColumns {

		@Test(expected = NullPointerException.class)
		public void testNullWorkload() {
			table.transform("a", "b").applyNumericToReal((a, b) -> a + b, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").applyNumericToReal((a, b) -> a + b, Workload.DEFAULT, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform().applyNumericToReal((DoubleBinaryOperator) null, Workload.DEFAULT, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongNumberOfColumns() {
			new TransformerMulti(new Column[]{table.column("a")})
					.applyNumericToReal((a, b) -> a + b, Workload.DEFAULT, CTX);
		}

	}

	public static class OneColumnCalculatorNumericToReal {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToNumeric calculator =
					new ApplierNumericToNumeric(new DoubleArrayColumn(data), i -> i / 2, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToNumeric calculator =
					new ApplierNumericToNumeric(new DoubleArrayColumn(data), i -> 2.5 * i, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = 2.5 * data[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(0).applyNumericToReal(v -> 2.5 * v, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2.5 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

	}

	public static class OneColumnCalculatorNumericToInteger {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToNumeric calculator = new ApplierNumericToNumeric(new DoubleArrayColumn(data), i -> i,
					true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToNumeric calculator =
					new ApplierNumericToNumeric(new DoubleArrayColumn(data), i -> 2 * i, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = Math.round(2 * data[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(0).applyNumericToInteger(v -> 2 * v, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

	}

	public static class OneColumnCalculatorNumericToCategorical {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToCategorical<String> calculator = new ApplierNumericToCategorical<>(
					new DoubleArrayColumn(data), i -> i + "", IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToCategorical<String> calculator = new ApplierNumericToCategorical<>(
					new DoubleArrayColumn(data), i -> i + "", IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			Int32CategoricalBuffer<String> buffer = table.transform(0).applyNumericToCategorical(v -> v + "", Workload
					.DEFAULT, CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> (double) i + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	@RunWith(Parameterized.class)
	public static class OneColumnCalculatorNumericToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToCategorical<String> calculator = new ApplierNumericToCategorical<>(
					new DoubleArrayColumn(data), i -> i > 0.5 ? "True" : "False", format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToCategorical<String> calculator = new ApplierNumericToCategorical<>(
					new DoubleArrayColumn(data), i -> i > 0.5 ? "True" : "False", format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] > 0.5 ? "True" : "False";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int size = Math.min(format.maxValue(), IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 1);
			double[] data = random(size);
			Table table = new Table(new Column[]{new DoubleArrayColumn(data)}, new String[]{"one"});
			CategoricalColumnBuffer<String> buffer = table.transform(0).applyNumericToCategorical(v -> v > 0.5 ? "Big"
							: "Small",
					format.maxValue(), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> data[i] > 0.5 ? "Big" : "Small");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorNumericToFree {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToFree<String> calculator = new ApplierNumericToFree<>(
					new DoubleArrayColumn(data), i -> i + "");
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToFree<String> calculator = new ApplierNumericToFree<>(
					new DoubleArrayColumn(data), i -> i + "");
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			FreeColumnBuffer<String> buffer = table.transform(0).applyNumericToFree(v -> v + "", Workload.DEFAULT,
					CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> (double) i + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorNumericToTime {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToTime calculator = new ApplierNumericToTime(
					new DoubleArrayColumn(data), i -> LocalTime.ofNanoOfDay(Math.round(i)));
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToTime calculator = new ApplierNumericToTime(
					new DoubleArrayColumn(data), i -> LocalTime.ofNanoOfDay(Math.round(1_000_000_000 * i)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.ofNanoOfDay(Math.round(1_000_000_000 * data[i]));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			TimeColumnBuffer buffer = table.transform(0)
					.applyNumericToTime(v -> LocalTime.ofNanoOfDay(Math.round(10_000_000_000L * v)), Workload.DEFAULT,
							CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> LocalTime.ofNanoOfDay(Math.round(10_000_000_000L * (double) i)));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorNumericToDateTime {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToDateTime calculator = new ApplierNumericToDateTime(
					new DoubleArrayColumn(data), i -> Instant.ofEpochMilli(Math.round(i)));
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToDateTime calculator = new ApplierNumericToDateTime(
					new DoubleArrayColumn(data), i -> Instant.ofEpochMilli(Math.round(1_000_000_000 * i)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(Math.round(1_000_000_000 * data[i]));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			HighPrecisionDateTimeBuffer buffer = table.transform(0)
					.applyNumericToDateTime(v -> Instant.ofEpochMilli(Math.round(10_000_000_000L * v)),
							Workload.DEFAULT,
							CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> Instant.ofEpochMilli(Math.round(10_000_000_000L * (double) i)));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	public static class OneColumnCalculatorCategoricalToReal {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToNumeric calculator = new ApplierCategoricalToNumeric(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i / 2, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToNumeric calculator = new ApplierCategoricalToNumeric(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = 2.5 * data[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int[] data = new int[123];
			Arrays.setAll(data, i -> i);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>
							())},
							new String[]{"one"});
			ColumnBuffer buffer = table.transform(0).applyCategoricalToReal(v -> 2.5 * v, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> 2.5 * i);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}
	}

	public static class OneColumnCalculatorCategoricalToInteger {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToNumeric calculator = new ApplierCategoricalToNumeric(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i, true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToNumeric calculator = new ApplierCategoricalToNumeric(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = 2 * data[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int[] data = new int[123];
			Arrays.setAll(data, i -> i);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>
							())},
							new String[]{"one"});
			ColumnBuffer buffer = table.transform(0).applyCategoricalToInteger(v -> 2 * v, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}
	}

	public static class OneColumnCalculatorCategoricalToCategorical {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToCategorical<String> calculator = new ApplierCategoricalToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i + "",
					IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToCategorical<String> calculator = new ApplierCategoricalToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					i -> i + "", IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(111);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>())}
							, new String[]{"one"});
			Int32CategoricalBuffer<String> buffer = table.transform(0).applyCategoricalToCategorical(v -> v + "",
					Workload.DEFAULT, CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> data[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	@RunWith(Parameterized.class)
	public static class OneColumnCalculatorCategoricalToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToCategorical<String> calculator = new ApplierCategoricalToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					i -> i == 1 ? "True" : "False", format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToCategorical<String> calculator = new ApplierCategoricalToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					i -> i == 1 ? "True" : "False", format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] == 1 ? "True" : "False";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int size = Math.min(format.maxValue(), IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 1);
			int[] data = randomInts(size);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>())}
							, new String[]{"one"});
			CategoricalColumnBuffer<String> buffer = table.transform(0).applyCategoricalToCategorical(v -> v == 0 ?
							"Missing" : "Data",
					format.maxValue(), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> data[i] == 0 ? "Missing" : "Data");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorCategoricalToFree {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToFree<String> calculator = new ApplierCategoricalToFree<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i + "");
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToFree<String> calculator = new ApplierCategoricalToFree<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i + "");
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(142);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>())}
							, new String[]{"one"});
			FreeColumnBuffer<String> buffer = table.transform(0).applyCategoricalToFree(v -> v + "", Workload.DEFAULT,
					CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> data[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorCategoricalToTime {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToTime calculator = new ApplierCategoricalToTime(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					i -> LocalTime.of(i / 24, i % 60));
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToTime calculator = new ApplierCategoricalToTime(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					i -> LocalTime.of(i / 24, i % 60));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.of(data[i] / 24, data[i] % 60);
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(142);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>())}
							, new String[]{"one"});
			TimeColumnBuffer buffer =
					table.transform(0).applyCategoricalToTime(i -> LocalTime.of(i / 24, i % 60), Workload.DEFAULT,
							CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> LocalTime.of(data[i] / 24, data[i] % 60));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	public static class OneColumnCalculatorCategoricalToDateTime {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierCategoricalToDateTime calculator = new ApplierCategoricalToDateTime(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					Instant::ofEpochMilli);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToDateTime calculator = new ApplierCategoricalToDateTime(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()),
					i -> Instant.ofEpochMilli(i * 100_000));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(data[i] * 100_000);
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(142);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>())}
							, new String[]{"one"});
			HighPrecisionDateTimeBuffer buffer =
					table.transform(0)
							.applyCategoricalToDateTime(i -> Instant.ofEpochMilli(i * 1_000_000), Workload.DEFAULT,
									CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> Instant.ofEpochMilli(data[i] * 1_000_000));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorObjectToReal {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			SimpleCategoricalColumn<String> source =
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToNumeric<String> calculator = new ApplierObjectToNumeric<>(
					source, String.class, v -> v.length() / 2.0, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			SimpleCategoricalColumn<String> source =
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList);
			ApplierObjectToNumeric<String> calculator = new ApplierObjectToNumeric<>(
					source, String.class, v -> v.length() / 2.0, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(data[i]).length() / 2.0;
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(123);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data,
					mappingList)},
					new String[]{"one"});
			ColumnBuffer buffer = table.transform(0).applyObjectToReal(String.class, v -> v.equals("value1") ? 0.5 :
					-0.5, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1") ? 0.5 : -0.5);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}
	}

	public static class OneColumnCalculatorObjectToInteger {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			SimpleCategoricalColumn<String> source =
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToNumeric<String> calculator = new ApplierObjectToNumeric<>(
					source, String.class, String::length, true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			SimpleCategoricalColumn<String> source =
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList);
			ApplierObjectToNumeric<String> calculator = new ApplierObjectToNumeric<>(
					source, String.class, String::length, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(data[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(122);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data,
					mappingList)},
					new String[]{"one"});
			ColumnBuffer buffer = table.transform(0).applyObjectToInteger(String.class, v -> v.equals("value1") ? 1 :
					0, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1") ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}
	}

	public static class OneColumnCalculatorObjectToCategorical {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierObjectToCategorical<String, Integer> calculator = new ApplierObjectToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList()), String.class,
					String::length, IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToCategorical<String, Integer> calculator = new ApplierObjectToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList), String.class,
					String::length,
					IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(data[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(111);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList)}
					, new String[]{"one"});
			Int32CategoricalBuffer<Boolean> buffer = table.transform(0).applyObjectToCategorical(String.class,
					v -> v.equals("value1"), Workload.DEFAULT, CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1"));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	@RunWith(Parameterized.class)
	public static class OneColumnCalculatorObjectToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierObjectToCategorical<String, Boolean> calculator = new ApplierObjectToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList()), String.class,
					v -> v.length() == 6, format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToCategorical<String, Boolean> calculator = new ApplierObjectToCategorical<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList()), String.class,
					v -> v.length() == 6, format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(data[i]).length() == 6;
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int size = Math.min(format.maxValue(), IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 1);
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data,
					mappingList)},
					new String[]{"one"});
			CategoricalColumnBuffer<Integer> buffer = table.transform(0).applyObjectToCategorical(String.class,
					v -> v.equals("value1") ? 1 : 0, format.maxValue(), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1") ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorObjectToFree {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierObjectToFree<String, Integer> calculator = new ApplierObjectToFree<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList()), String.class,
					String::length);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToFree<String, Integer> calculator = new ApplierObjectToFree<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList), String.class,
					String::length);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(data[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(142);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList)}
					, new String[]{"one"});
			FreeColumnBuffer<Boolean> buffer = table.transform(0).applyObjectToFree(String.class, "value1"::equals,
					Workload.DEFAULT, CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1"));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	public static class OneColumnCalculatorObjectToTime {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierObjectToTime<String> calculator = new ApplierObjectToTime<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList()), String.class,
					v -> LocalTime.of(v.length(), 1));
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToTime<String> calculator = new ApplierObjectToTime<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList), String.class,
					v -> LocalTime.of(v.length(), 1));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.of(mappingList.get(data[i]).length(), 1);
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(142);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList)}
					, new String[]{"one"});
			TimeColumnBuffer buffer =
					table.transform(0).applyObjectToTime(String.class, v -> LocalTime.of(12, v.length()),
							Workload.DEFAULT, CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> LocalTime.of(12, mappingList.get(data[i]).length()));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class OneColumnCalculatorObjectToDateTime {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = new int[size];
			ApplierObjectToDateTime<String> calculator = new ApplierObjectToDateTime<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList()), String.class,
					v -> Instant.ofEpochMilli(v.length()));
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToDateTime<String> calculator = new ApplierObjectToDateTime<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList), String.class,
					v -> Instant.ofEpochMilli(v.length() * 100_000));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(mappingList.get(data[i]).length() * 100_000);
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			int[] data = randomInts(142);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList)}
					, new String[]{"one"});
			HighPrecisionDateTimeBuffer buffer =
					table.transform(0)
							.applyObjectToDateTime(String.class, v -> Instant.ofEpochMilli(v.length() * 1_000_000),
									Workload.DEFAULT, CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> Instant.ofEpochMilli(mappingList.get(data[i]).length() * 1_000_000));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorNumericToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToNumericMulti calculator =
					new ApplierNumericToNumericMulti(new Column[]{new DoubleArrayColumn(data),
							new DoubleArrayColumn(data), new DoubleArrayColumn(data)}, row -> 0, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToNumericMulti calculator =
					new ApplierNumericToNumericMulti(new Column[]{new DoubleArrayColumn(first),
							new DoubleArrayColumn(second), new DoubleArrayColumn(third)},
							row -> row.get(2) + row.get(0) * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2.5 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToReal(row -> row.get(0) + row.get(1) /
					2.0, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

	}

	public static class MoreColumnsCalculatorNumericToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNumericToNumericMulti calculator =
					new ApplierNumericToNumericMulti(new Column[]{new DoubleArrayColumn(data),
							new DoubleArrayColumn(data), new DoubleArrayColumn(data)}, row -> 0, true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToNumericMulti calculator =
					new ApplierNumericToNumericMulti(new Column[]{new DoubleArrayColumn(first),
							new DoubleArrayColumn(second), new DoubleArrayColumn(third)},
							row -> row.get(2) + row.get(0) * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = Math.round(third[i] + 2 * first[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToInteger(row -> row.get(0) + row.get
					(1) / 2 + 0.1, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

	}


	public static class MoreColumnsCalculatorCategoricalToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierCategoricalToNumericMulti calculator = new ApplierCategoricalToNumericMulti(new Column[]{column,
					column, column}, row -> 0, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			ApplierCategoricalToNumericMulti calculator = new ApplierCategoricalToNumericMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> row.get(2) + row.get(0) * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2.5 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>
							()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>())}
							, new String[]{"one", "two"});
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToReal(row -> row.get(0) + row.get
					(1) * 2 + 0.1, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> first[i] + 2 * second[i] + 0.1);
			assertArrayEquals(expected, result, EPSILON);
		}

	}

	public static class MoreColumnsCalculatorCategoricalToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierCategoricalToNumericMulti calculator = new ApplierCategoricalToNumericMulti(new Column[]{column,
					column, column}, row -> 0, true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			ApplierCategoricalToNumericMulti calculator = new ApplierCategoricalToNumericMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> row.get(2) + row.get(0) * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>
							()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>())}
							, new String[]{"one", "two"});
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToInteger(row -> row.get(0) + row
					.get(1) * 2 + 0.1, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> first[i] + 2 * second[i]);
			assertArrayEquals(expected, result, EPSILON);
		}

	}

	public static class MoreColumnsCalculatorObjectToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToNumericMulti<Object> calculator = new ApplierObjectToNumericMulti<>(new Column[]{column,
					column, column}, Object.class, row -> 0, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}

		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToNumericMulti<String> calculator = new ApplierObjectToNumericMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> row.get(2).length() + row.get(0).length() * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(third[i]).length() + 2.5 * mappingList.get(first[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToReal(String.class,
					row -> row.get(0).length() + row.get(1).length() * 2.5, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[table.height()];
			Arrays.setAll(expected,
					i -> mappingList.get(first[i]).length() + 2.5 * mappingList.get(second[i]).length());
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

	}

	public static class MoreColumnsCalculatorObjectToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToNumericMulti<Object> calculator = new ApplierObjectToNumericMulti<>(new Column[]{column,
					column, column}, Object.class, row -> 0, true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToNumericMulti<String> calculator = new ApplierObjectToNumericMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> row.get(2).length() + row.get(0).length() * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(third[i]).length() + 2 * mappingList.get(first[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			double[] result = new double[table.height()];
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToInteger(String.class,
					row -> row.get(0).length() + row.get(1).length() * 2 + 0.1, Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).length() + 2 * mappingList.get(second[i]).length());
			assertArrayEquals(expected, result, EPSILON);
		}

	}


	public static class MoreColumnsCalculatorGeneralToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToNumericMulti calculator = new ApplierGeneralToNumericMulti(new Column[]{column,
					column, column}, row -> 0, false);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}

		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			List<String> mappingList = getMappingList();
			ApplierGeneralToNumericMulti calculator = new ApplierGeneralToNumericMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + row.getIndex(0) * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2.5 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			double[] second = random(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList), new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyGeneralToReal(
					row -> Objects.toString(row.getObject(0), "").length() * 2.5 + row.getNumeric(1),
					Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[table.height()];
			Arrays.setAll(expected,
					i -> Objects.toString(mappingList.get(first[i]), "").length() * 2.5 + second[i]);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

	}

	public static class MoreColumnsCalculatorGeneralToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToNumericMulti calculator = new ApplierGeneralToNumericMulti(new Column[]{column,
					column, column}, row -> 0, true);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			List<String> mappingList = getMappingList();
			ApplierGeneralToNumericMulti calculator = new ApplierGeneralToNumericMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + row.getIndex(0) * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ColumnBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = Math.round(third[i] + 2 * first[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			double[] second = random(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList), new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			ColumnBuffer buffer = table.transform(new int[]{0, 1}).applyGeneralToInteger(
					row -> Objects.toString(row.getObject(0), "").length() * 2 + (int) (row.getNumeric(1) * 5),
					Workload.DEFAULT, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			double[] expected = new double[table.height()];
			Arrays.setAll(expected,
					i -> Objects.toString(mappingList.get(first[i]), "").length() * 2 + (int) (5 * second[i]));
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

	}

	public static class MoreColumnsCalculatorObjectToCategorical {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToCategoricalMulti<Object, Object> calculator =
					new ApplierObjectToCategoricalMulti<>(new Column[]{column,
							column, column}, Object.class, row -> new Object(), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToCategoricalMulti<String, Integer> calculator =
					new ApplierObjectToCategoricalMulti<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
							row -> row.get(2).length() + row.get(0).length() * 2, IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Integer> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(third[i]).length() + 2 * mappingList.get(first[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			Int32CategoricalBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyObjectToCategorical(String
							.class,
					v -> v.get(0).equals(v.get(1)), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	@RunWith(Parameterized.class)
	public static class MoreColumnsCalculatorObjectToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToCategoricalMulti<Object, Integer> calculator =
					new ApplierObjectToCategoricalMulti<>(new Column[]{column,
							column, column}, Object.class, row -> 0, format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToCategoricalMulti<String, Boolean> calculator =
					new ApplierObjectToCategoricalMulti<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
							row -> row.get(2).length() == row.get(0).length(), format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Boolean> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(third[i]).length() == mappingList.get(first[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			CategoricalColumnBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyObjectToCategorical(String
							.class,
					v -> v.get(0).equals(v.get(1)) ? 1 : 0, format.maxValue(), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}
	}


	public static class MoreColumnsCalculatorCategoricalToCategorical {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierCategoricalToCategoricalMulti<Object> calculator =
					new ApplierCategoricalToCategoricalMulti<>(new Column[]{column,
							column, column}, row -> new Object(), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			ApplierCategoricalToCategoricalMulti<Integer> calculator =
					new ApplierCategoricalToCategoricalMulti<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
							row -> row.get(2) + row.get(0) * 2, IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Integer> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>
							()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
							, new String[]{"one", "two"});
			Int32CategoricalBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyCategoricalToCategorical(
					v -> v.get(0) == v.get(1), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i]);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	@RunWith(Parameterized.class)
	public static class MoreColumnsCalculatorCategoricalToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierCategoricalToCategoricalMulti<Object> calculator =
					new ApplierCategoricalToCategoricalMulti<>(new Column[]{column,
							column, column}, row -> 0, format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierCategoricalToCategoricalMulti<Boolean> calculator =
					new ApplierCategoricalToCategoricalMulti<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)},
							row -> row.get(2) + 2 == row.get(0), format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Boolean> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2 == first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			CategoricalColumnBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyCategoricalToCategorical(
					v -> v.get(0) == v.get(1) ? 1 : 0, format.maxValue(), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}
	}

	public static class MoreColumnsCalculatorGeneralToCategorical {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToCategoricalMulti<Object> calculator =
					new ApplierGeneralToCategoricalMulti<>(new Column[]{column,
							column, column}, row -> new Object(), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			ApplierGeneralToCategoricalMulti<Integer> calculator = new ApplierGeneralToCategoricalMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, getMappingList()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, getMappingList()),
					new DoubleArrayColumn(third)},
					row -> (int) (5 * row.getNumeric(2)) + row.getIndex(0), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Integer> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = (int) (5 * third[i]) + first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			double[] second = random(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList), new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			Int32CategoricalBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyGeneralToCategorical(
					v -> v.getNumeric(1) < getLengthOrNull(v.getObject(0)) / 10.0, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> second[i] < getLengthOrNull(mappingList.get(first[i])) / 10.0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		private static int getLengthOrNull(Object object) {
			if (object == null) {
				return 0;
			}
			return ((String) object).length();
		}

	}


	@RunWith(Parameterized.class)
	public static class MoreColumnsCalculatorGeneralToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToCategoricalMulti<Object> calculator =
					new ApplierGeneralToCategoricalMulti<>(new Column[]{column,
							column, column}, row -> 0, format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			List<String> mappingList = getMappingList();
			ApplierGeneralToCategoricalMulti<Boolean> calculator = new ApplierGeneralToCategoricalMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + 2 < row.getIndex(0), format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Boolean> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2 < first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			double[] second = random(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList), new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			CategoricalColumnBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyGeneralToCategorical(
					v -> getLengthOrNull(v.getObject(0)) <= v.getNumeric(1) * 10 ? 1 : 0, format.maxValue(),
					Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> getLengthOrNull(mappingList.get(first[i])) <= 10 * second[i] ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		private static int getLengthOrNull(Object object) {
			if (object == null) {
				return 0;
			}
			return ((String) object).length();
		}
	}

	public static class MoreColumnsCalculatorNumericToCategorical {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNumericToCategoricalMulti<Object> calculator =
					new ApplierNumericToCategoricalMulti<>(new Column[]{column,
							column, column}, row -> new Object(), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToCategoricalMulti<String> calculator = new ApplierNumericToCategoricalMulti<>(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> Objects.toString(row.get(2) + row.get(0)), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<String> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + first[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			double[] second = random(size);
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			Int32CategoricalBuffer<String> buffer = table.transform(new int[]{0, 1}).applyNumericToCategorical(
					v -> v.get(0) * v.get(1) + "", Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] * second[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


	@RunWith(Parameterized.class)
	public static class MoreColumnsCalculatorNumericToCategoricalFormats {


		@Parameter
		public IntegerFormats.Format format;

		@Parameters(name = "{0}")
		public static Iterable<IntegerFormats.Format> formats() {
			return Arrays.asList(IntegerFormats.Format.values());
		}

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNumericToCategoricalMulti<Object> calculator =
					new ApplierNumericToCategoricalMulti<>(new Column[]{column,
							column, column}, row -> 0, format);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToCategoricalMulti<Boolean> calculator = new ApplierNumericToCategoricalMulti<>(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> row.get(2) * row.get(0) > 0.5, format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalColumnBuffer<Boolean> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] * first[i] > 0.5;
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			double[] second = random(size);
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			CategoricalColumnBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyNumericToCategorical(
					v -> v.get(0) + v.get(1) / 2 > 0.5 ? 1 : 0, format.maxValue(), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] + second[i] / 2 > 0.5 ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}
	}

	public static class MoreColumnsCalculatorNumericToFree {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNumericToFreeMulti<Object> calculator = new ApplierNumericToFreeMulti<>(new Column[]{column,
					column, column}, row -> new Object());
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToFreeMulti<String> calculator = new ApplierNumericToFreeMulti<>(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> Objects.toString(row.get(2) + row.get(0)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer<String> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + first[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			double[] second = random(size);
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			FreeColumnBuffer<String> buffer = table.transform(new int[]{0, 1}).applyNumericToFree(
					v -> v.get(0) * v.get(1) + "", Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] * second[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorNumericToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNumericToTimeMulti calculator = new ApplierNumericToTimeMulti(new Column[]{column,
					column, column}, row -> LocalTime.NOON);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToTimeMulti calculator = new ApplierNumericToTimeMulti(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> LocalTime.of((int) Math.floor(row.get(2) * 24), (int) Math.floor(row.get(0) * 60)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.of((int) Math.floor(third[i] * 24), (int) Math.floor(first[i] * 60));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			double[] second = random(size);
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			TimeColumnBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToTime(
					row -> LocalTime.of((int) Math.floor(row.get(0) * 24), (int) Math.floor(row.get(1) * 60)),
					Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected,
					i -> LocalTime.of((int) Math.floor(first[i] * 24), (int) Math.floor(second[i] * 60)));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorNumericToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNumericToDateTimeMulti calculator = new ApplierNumericToDateTimeMulti(new Column[]{column,
					column, column}, row -> Instant.MAX);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] first = random(size);
			double[] second = random(size);
			double[] third = random(size);
			ApplierNumericToDateTimeMulti calculator = new ApplierNumericToDateTimeMulti(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> Instant.ofEpochMilli(Math.round(Math.pow(10 * row.get(2), 30 * row.get(0)))));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(Math.round(Math.pow(10 * third[i], 30 * first[i])));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			double[] second = random(size);
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new DoubleArrayColumn(second)}
					, new String[]{"one", "two"});
			HighPrecisionDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToDateTime(
					row -> Instant.ofEpochMilli(Math.round(Math.pow(10 * row.get(0), 30 * row.get(1)))),
					Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected,
					i -> Instant.ofEpochMilli(Math.round(Math.pow(10 * first[i], 30 * second[i]))));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorCategoricalToFree {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierCategoricalToFreeMulti<Object> calculator = new ApplierCategoricalToFreeMulti<>(new Column[]{column,
					column, column}, row -> new Object());
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			ApplierCategoricalToFreeMulti<Integer> calculator = new ApplierCategoricalToFreeMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> row.get(2) + row.get(0) * 2);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer<Integer> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>
							()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
							, new String[]{"one", "two"});
			FreeColumnBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyCategoricalToFree(
					v -> v.get(0) == v.get(1), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i]);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorCategoricalToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierCategoricalToTimeMulti calculator = new ApplierCategoricalToTimeMulti(new Column[]{column,
					column, column}, row -> LocalTime.NOON);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			ApplierCategoricalToTimeMulti calculator = new ApplierCategoricalToTimeMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> LocalTime.of(row.get(2) % 24, row.get(0)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.of(third[i] % 24, first[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>
							()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
							, new String[]{"one", "two"});
			TimeColumnBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToTime(
					v -> v.get(0) == v.get(1) ? LocalTime.NOON : LocalTime.MIDNIGHT, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i] ? LocalTime.NOON : LocalTime.MIDNIGHT);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorCategoricalToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierCategoricalToDateTimeMulti calculator = new ApplierCategoricalToDateTimeMulti(new Column[]{column,
					column, column}, row -> Instant.MIN);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			ApplierCategoricalToDateTimeMulti calculator = new ApplierCategoricalToDateTimeMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> Instant.ofEpochMilli(Math.round(Math.pow(row.get(2), row.get(0)))));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(Math.round(Math.pow(third[i], first[i])));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table =
					new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>
							()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
							, new String[]{"one", "two"});
			HighPrecisionDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToDateTime(
					v -> v.get(0) == v.get(1) ? Instant.MIN : Instant.MAX, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i] ? Instant.MIN : Instant.MAX);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorObjectToFree {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToFreeMulti<Object, Object> calculator = new ApplierObjectToFreeMulti<>(new Column[]{column,
					column, column}, Object.class, row -> new Object());
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToFreeMulti<String, Integer> calculator = new ApplierObjectToFreeMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> row.get(2).length() + row.get(0).length() * 2);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer<Integer> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = mappingList.get(third[i]).length() + 2 * mappingList.get(first[i]).length();
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			FreeColumnBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyObjectToFree(String.class,
					v -> v.get(0).equals(v.get(1)), Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorObjectToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToTimeMulti<Object> calculator = new ApplierObjectToTimeMulti<>(new Column[]{column,
					column, column}, Object.class, row -> LocalTime.NOON);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToTimeMulti<String> calculator = new ApplierObjectToTimeMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> LocalTime.of(row.get(2).length(), row.get(0).length() * 2));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.of(mappingList.get(third[i]).length(), 2 * mappingList.get(first[i]).length());
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			TimeColumnBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToTime(String.class,
					v -> v.get(0).equals(v.get(1)) ? LocalTime.NOON : LocalTime.MIDNIGHT, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? LocalTime
					.NOON :
					LocalTime.MIDNIGHT);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorObjectToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierObjectToDateTimeMulti<Object> calculator = new ApplierObjectToDateTimeMulti<>(new Column[]{column,
					column, column}, Object.class, row -> Instant.MAX);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			int[] third = randomInts(size);
			List<String> mappingList = getMappingList();
			ApplierObjectToDateTimeMulti<String> calculator = new ApplierObjectToDateTimeMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> Instant.ofEpochMilli(row.get(2).length() * row.get(0).length() * 1_999_999));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(
						mappingList.get(third[i]).length() * 1_999_999 * mappingList.get(first[i]).length());
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first,
					mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			HighPrecisionDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class,
					v -> v.get(0).equals(v.get(1)) ? Instant.MAX : Instant.MIN, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected,
					i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? Instant.MAX : Instant.MIN);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorGeneralToFree {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToFreeMulti<Object> calculator = new ApplierGeneralToFreeMulti<>(new Column[]{column,
					column, column}, row -> new Object());
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			List<String> mappingList = getMappingList();
			ApplierGeneralToFreeMulti<Double> calculator = new ApplierGeneralToFreeMulti<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + row.getIndex(0) * 2);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			FreeColumnBuffer<Double> buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			FreeColumnBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyGeneralToFree(
					v -> v.getNumeric(0) < ((String) v.getObject(1)).length() / 10.0, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] < mappingList.get(second[i]).length() / 10.0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorGeneralToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToTimeMulti calculator = new ApplierGeneralToTimeMulti(new Column[]{column,
					column, column}, row -> LocalTime.NOON);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			List<String> mappingList = getMappingList();
			ApplierGeneralToTimeMulti calculator = new ApplierGeneralToTimeMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> LocalTime.of((int) Math.round(12 * row.getNumeric(2)), row.getIndex(0) * 2));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeColumnBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.of((int) Math.round(12 * third[i]), 2 * first[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			TimeColumnBuffer buffer = table.transform(new int[]{0, 1}).applyGeneralToTime(
					v -> v.getNumeric(0) < ((String) v.getObject(1)).length() / 10.0 ?
							LocalTime.NOON : LocalTime.MIDNIGHT, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] < mappingList.get(second[i]).length() / 10.0 ?
					LocalTime.NOON : LocalTime.MIDNIGHT);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}

	public static class MoreColumnsCalculatorGeneralToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierGeneralToDateTimeMulti calculator = new ApplierGeneralToDateTimeMulti(new Column[]{column,
					column, column}, row -> Instant.MAX);
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] first = randomInts(size);
			int[] second = randomInts(size);
			double[] third = random(size);
			List<String> mappingList = getMappingList();
			ApplierGeneralToDateTimeMulti calculator = new ApplierGeneralToDateTimeMulti(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> Instant.ofEpochMilli(Math.round(row.getIndex(0) * row.getNumeric(2) * 100_000)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			HighPrecisionDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(Math.round(100_000 * third[i] * first[i]));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			int size = 123;
			double[] first = random(size);
			int[] second = randomInts(size);
			List<String> mappingList = getMappingList();
			Table table = new Table(new Column[]{new DoubleArrayColumn(first),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList)}
					, new String[]{"one", "two"});
			HighPrecisionDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyGeneralToDateTime(
					v -> v.getNumeric(0) < ((String) v.getObject(1)).length() / 10.0 ?
							Instant.MAX : Instant.MIN, Workload.DEFAULT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] < mappingList.get(second[i]).length() / 10.0 ?
					Instant.MAX : Instant.MIN);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

	}


}

