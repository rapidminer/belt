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
 * Tests {@link ApplierNumericToNumeric} and {@link ApplierNNumericToNumeric}.
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
	private static final Table table = Builders.newTableBuilder(NUMBER_OF_ROWS)
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


	private static double[] readBufferToArray(NumericBuffer buffer) {
		double[] data = new double[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(CategoricalBuffer<?> buffer) {
		CategoricalBuffer<?> readBuffer = (CategoricalBuffer<?>) buffer;
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = readBuffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(ObjectBuffer<?> buffer) {
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(TimeBuffer buffer) {
		Object[] data = new Object[buffer.size()];
		for (int j = 0; j < buffer.size(); j++) {
			data[j] = buffer.get(j);
		}
		return data;
	}

	private static Object[] readBufferToArray(DateTimeBuffer buffer) {
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
		public void testNullContext() {
			table.transform("a").applyNumericToReal(i -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform("b").applyNumericToReal(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10])).applyNumericToReal(i -> 0, CTX);
		}

	}

	public static class InputValidationOneColumnNumericToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToInteger(i -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToInteger(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10])).applyNumericToInteger(i -> 0,
					CTX);
		}
	}

	public static class InputValidationOneColumnCategoricalToReal {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToReal(i -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToReal(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToReal(i -> 0, CTX);
		}

	}

	public static class InputValidationOneColumnCategoricalToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToInteger(i -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToInteger(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToInteger(i -> 0, CTX);
		}

	}

	public static class InputValidationOneColumnObjectToReal {

		@Test(expected = NullPointerException.class)
		public void testNullOpeator() {
			table.transform(0).applyObjectToReal(String.class, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToReal(null, i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToReal(String.class, i -> 0, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToReal(String.class, i -> 0, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToReal(String.class, i -> 0, CTX);
		}

	}

	public static class InputValidationOneColumnObjectToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToInteger(String.class, null,CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToInteger(null, i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToInteger(String.class, i -> 0, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToInteger(String.class, i -> 0,  CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN)
					.applyObjectToInteger(String.class, i -> 0, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToCategorical(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToCategorical(i -> 0, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToCategorical(i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(0).applyNumericToCategorical(i -> 0, 1, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperatorFormat() {
			table.transform(0).applyNumericToCategorical(null, 1,  CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToCategorical(i -> 0, Integer.MAX_VALUE, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToFree {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToObject(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToObject(i -> 0, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToObject(i -> 0, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToTime {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToTime(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToTime(i -> null, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToTime(i -> null, CTX);
		}
	}

	public static class InputValidationOneColumnNumericToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyNumericToDateTime(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyNumericToDateTime(i -> null, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyNumericToDateTime(i -> null, CTX);
		}
	}

	public static class InputValidationOneColumnCategoricalToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToCategorical(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToCategorical(i -> 0, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToCategorical(i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(0).applyCategoricalToCategorical(i -> 0, 1, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperatorFormat() {
			table.transform(0).applyCategoricalToCategorical(null, 1, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToCategorical(i -> 0, Integer.MAX_VALUE, CTX);
		}

	}

	public static class InputValidationOneColumnCategoricalToFree {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToObject(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToObject(i -> 0, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToObject(i -> 0, CTX);
		}
	}

	public static class InputValidationOneColumnCategoricalToTime {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToTime(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToTime(i -> null, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToTime(i -> null, CTX);
		}
	}


	public static class InputValidationOneColumnCategoricalToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyCategoricalToDateTime(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyCategoricalToDateTime(i -> null, null);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(new SimpleObjectColumn<>(STRING_FREE_TYPE, new Object[10]))
					.applyCategoricalToDateTime(i -> null,CTX);
		}
	}

	public static class InputValidationOneColumnObjectToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToCategorical(null, i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToCategorical(String.class, null, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToCategorical(String.class, i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypeFormat() {
			table.transform(0).applyObjectToCategorical(null, i -> 0, 1, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, 1, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullOperatorFormat() {
			table.transform(0).applyObjectToCategorical(String.class, null, 1, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeFormat() {
			table.transform(0).applyObjectToCategorical(String.class, i -> 0, 2, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToCategorical(String.class, i -> 0, 3, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToFree {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToObject(String.class, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToObject(null, i -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToObject(String.class, i -> 0, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToObject(String.class, i -> 0, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToObject(String.class, i -> 0, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToTime {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToTime(String.class, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToTime(null, i -> null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToTime(String.class, i -> null, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToTime(String.class, i -> null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToTime(String.class, i -> null, CTX);
		}
	}

	public static class InputValidationOneColumnObjectToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullOperator() {
			table.transform(0).applyObjectToDateTime(String.class, null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(0).applyObjectToDateTime(null, i -> null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(0).applyObjectToDateTime(String.class, i -> null, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(0).applyObjectToDateTime(String.class, i -> null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new Transformer(OBJECT_NOT_READABLE_COLUMN).applyObjectToDateTime(String.class, i -> null, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToReal {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").applyNumericToReal(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform("a", "b").applyNumericToReal((ToDoubleFunction<NumericRow>) null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Arrays.asList(table.column("a"), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToReal(row -> 0, CTX);
		}


	}

	public static class InputValidationMoreColumnsNumericToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToInteger(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToInteger(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Arrays.asList(table.column("a"), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToInteger(row -> 0, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToReal {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToReal(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToReal(null,CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyCategoricalToReal(row -> 0, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToInteger(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToInteger(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyCategoricalToInteger(row -> 0,CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneralToReal {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyMixedToReal(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyMixedToReal(null, CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneralToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyMixedToInteger(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyMixedToInteger(null, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToReal {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToReal(String.class, row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToReal(null, row -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToReal(String.class, null,CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToReal(String.class, row -> 0, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(new int[]{0, 1}).applyObjectToReal(String.class, row -> 0, CTX);
		}
	}

	public static class InputValidationMoreColumnsObjectToInteger {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToInteger(null, row -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(Arrays.asList("a", "b")).applyObjectToInteger(String.class, row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToInteger(String.class, null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToInteger(String.class, row -> 0, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(new int[]{0, 1}).applyObjectToInteger(String.class, row -> 0, CTX);
		}
	}

	public static class InputValidationMoreColumnsObjectToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(null, row -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(Arrays.asList("a", "b"))
					.applyObjectToCategorical(String.class, row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToCategorical(String.class, row -> 0, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongType() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, row -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullTypeFormat() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(null, row -> 0, 1, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(Arrays.asList("a", "b")).applyObjectToCategorical(String.class, row -> 0, 1, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, null, 1, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToCategorical(String.class, row -> 0, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToCategorical(String.class, row -> 0, CTX);
		}

	}

	public static class InputValidationMoreColumnsCategoricalToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(row -> 0, 1, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToCategorical(null, 1, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyCategoricalToCategorical(row -> 0, CTX);
		}

	}


	public static class InputValidationMoreColumnsNumericToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(row -> 0, 1,null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyNumericToCategorical(null, 1, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapability() {
			new RowTransformer(Arrays.asList(table.column(0), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToCategorical(row -> 0, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Arrays.asList(table.column(0), OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToCategorical(row -> 0, 1, CTX);
		}
	}

	public static class InputValidationMoreColumnsGeneralToCategorical {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyMixedToCategorical(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyMixedToCategorical(null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContextFormat() {
			table.transform(new int[]{0, 1}).applyMixedToCategorical(row -> 0, 1, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunctionFormat() {
			table.transform(new int[]{0, 1}).applyMixedToCategorical(null, 1, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToFree {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToObject(null, row -> 0, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToObject(String.class, row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToObject(String.class, null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToObject(String.class, row -> 0, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToObject(String.class, row -> 0, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToTime {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToTime(null, row -> null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToTime(String.class, row -> null, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToTime(String.class, row -> null, CTX);
		}

	}

	public static class InputValidationMoreColumnsObjectToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullType() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(null, row -> null, CTX);
		}

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyObjectToDateTime(String.class, row -> null, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongTypeForamt() {
			table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class, row -> null, CTX);
		}

	}

	public static class InputValidationMoreColumnsCategoricalToFree {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToObject(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToObject(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToObject(row -> 0, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToTime {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToTime(row -> null, CTX);
		}
	}

	public static class InputValidationMoreColumnsCategoricalToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			table.transform(new int[]{0, 1}).applyCategoricalToDateTime(row -> null, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToFree {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToObject(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToObject(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToObject(row -> 0, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToTime {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToTime(row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToTime(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToTime(row -> null, CTX);
		}
	}

	public static class InputValidationMoreColumnsNumericToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyNumericToDateTime(row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyNumericToDateTime(null, CTX);
		}

		@Test(expected = UnsupportedOperationException.class)
		public void testWrongCapabilityFormat() {
			new RowTransformer(Collections.singletonList(OBJECT_NOT_READABLE_COLUMN))
					.applyNumericToDateTime(row -> null, CTX);
		}
	}

	public static class InputValidationMoreColumnsGeneralToFree {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyMixedToObject(row -> 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyMixedToObject(null, CTX);
		}

	}

	public static class InputValidationMoreColumnsGeneralToTime {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyMixedToTime(row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyMixedToTime(null, CTX);
		}
	}

	public static class InputValidationMoreColumnsGeneralToDateTime {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform(new int[]{0, 1}).applyMixedToDateTime(row -> null, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform(new int[]{0, 1}).applyMixedToDateTime(null, CTX);
		}
	}

	public static class InputValidationTwoColumns {

		@Test(expected = NullPointerException.class)
		public void testNullContext() {
			table.transform("a", "b").applyNumericToReal((a, b) -> a + b, null);
		}

		@Test(expected = NullPointerException.class)
		public void testNullFunction() {
			table.transform().applyNumericToReal((DoubleBinaryOperator) null, CTX);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongNumberOfColumns() {
			new RowTransformer(new Column[]{table.column("a")}).applyNumericToReal((a, b) -> a + b, CTX);
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
			NumericBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = 2.5 * data[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			NumericBuffer buffer = table.transform(0).applyNumericToReal(v -> 2.5 * v, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2.5 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			NumericBuffer buffer =
					new Transformer(new DoubleArrayColumn(new double[0])).applyNumericToReal(v -> 2.5 * v, CTX);
			assertEquals(0, buffer.size());
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
			NumericBuffer buffer = calculator.getResult();

			double[] expected = new double[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = Math.round(2 * data[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			NumericBuffer buffer = table.transform(0).applyNumericToInteger(v -> 2 * v, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			NumericBuffer buffer =
					new Transformer(new DoubleArrayColumn(new double[0])).applyNumericToInteger(v -> 2 * v, CTX);
			assertEquals(0, buffer.size());
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
			CategoricalBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testWhole() {
			Int32CategoricalBuffer<String> buffer = table.transform(0).applyNumericToCategorical(v -> v + "", CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> (double) i + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			CategoricalBuffer<String> buffer =
					new Transformer(new DoubleArrayColumn(new double[0])).applyNumericToCategorical(v -> v + "", CTX);
			assertEquals(0, buffer.size());
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
			CategoricalBuffer buffer = calculator.getResult();

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
			CategoricalBuffer<String> buffer = table.transform(0).applyNumericToCategorical(v -> v > 0.5 ? "Big"
							: "Small",
					format.maxValue(), CTX);
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
			ApplierNumericToObject<String> calculator = new ApplierNumericToObject<>(
					new DoubleArrayColumn(data), i -> i + "");
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			double[] data = random(size);
			ApplierNumericToObject<String> calculator = new ApplierNumericToObject<>(
					new DoubleArrayColumn(data), i -> i + "");
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			for (int i = start; i < end; i++) {
				expected[i] = data[i] + "";
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			ObjectBuffer<String> buffer = table.transform(0).applyNumericToObject(v -> v + "", CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> (double) i + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			ObjectBuffer<String> buffer =
					new Transformer(new DoubleArrayColumn(new double[0])).applyNumericToObject(v -> v + "", CTX);
			assertEquals(0, buffer.size());
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
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
			for (int i = start; i < end; i++) {
				expected[i] = LocalTime.ofNanoOfDay(Math.round(1_000_000_000 * data[i]));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			TimeBuffer buffer = table.transform(0)
					.applyNumericToTime(v -> LocalTime.ofNanoOfDay(Math.round(10_000_000_000L * v)), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> LocalTime.ofNanoOfDay(Math.round(10_000_000_000L * (double) i)));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			TimeBuffer buffer =
					new Transformer(new DoubleArrayColumn(new double[0])).applyNumericToTime(v -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
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
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
			for (int i = start; i < end; i++) {
				expected[i] = Instant.ofEpochMilli(Math.round(1_000_000_000 * data[i]));
			}
			assertArrayEquals(expected, readBufferToArray(buffer));
		}


		@Test
		public void testWhole() {
			NanosecondDateTimeBuffer buffer = table.transform(0)
					.applyNumericToDateTime(v -> Instant.ofEpochMilli(Math.round(10_000_000_000L * v)), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> Instant.ofEpochMilli(Math.round(10_000_000_000L * (double) i)));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			DateTimeBuffer buffer =
					new Transformer(new DoubleArrayColumn(new double[0]))
							.applyNumericToDateTime(v -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
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
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(0).applyCategoricalToReal(v -> 2.5 * v, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> 2.5 * i);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			NumericBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyCategoricalToReal(v -> v * 0.5, CTX);
			assertEquals(0, buffer.size());
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
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(0).applyCategoricalToInteger(v -> 2 * v, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			NumericBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyCategoricalToInteger(v -> v * 0.5, CTX);
			assertEquals(0, buffer.size());
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
			CategoricalBuffer buffer = calculator.getResult();

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
			Int32CategoricalBuffer<String> buffer = table.transform(0).applyCategoricalToCategorical(v -> v + "", CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> data[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			CategoricalBuffer<String> buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyCategoricalToCategorical(v -> v + "", CTX);
			assertEquals(0, buffer.size());
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
			CategoricalBuffer buffer = calculator.getResult();

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
			CategoricalBuffer<String> buffer = table.transform(0).applyCategoricalToCategorical(v -> v == 0 ?
							"Missing" : "Data",
					format.maxValue(), CTX);
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
			ApplierCategoricalToObject<String> calculator = new ApplierCategoricalToObject<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i + "");
			calculator.init(1);
			assertEquals(size, calculator.getResult().size());
		}


		@Test
		public void testMappingPart() {
			int size = 75;
			int[] data = randomInts(size);
			ApplierCategoricalToObject<String> calculator = new ApplierCategoricalToObject<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>()), i -> i + "");
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer buffer = calculator.getResult();

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
			ObjectBuffer<String> buffer = table.transform(0).applyCategoricalToObject(v -> v + "", CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> data[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			ObjectBuffer<String> buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyCategoricalToObject(v -> v + "", CTX);
			assertEquals(0, buffer.size());
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
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
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
			TimeBuffer buffer =
					table.transform(0).applyCategoricalToTime(i -> LocalTime.of(i / 24, i % 60), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> LocalTime.of(data[i] / 24, data[i] % 60));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			TimeBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyCategoricalToTime(v -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
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
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
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
			NanosecondDateTimeBuffer buffer =
					table.transform(0)
							.applyCategoricalToDateTime(i -> Instant.ofEpochMilli(i * 1_000_000), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> Instant.ofEpochMilli(data[i] * 1_000_000));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			DateTimeBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyCategoricalToDateTime(v -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
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
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(0).applyObjectToReal(String.class, v -> v.equals("value1") ? 0.5 :
					-0.5, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1") ? 0.5 : -0.5);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			NumericBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyObjectToReal(String.class, v -> v.length(), CTX);
			assertEquals(0, buffer.size());
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
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(0).applyObjectToInteger(String.class, v -> v.equals("value1") ? 1 :
					0, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			double[] expected = new double[data.length];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1") ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			NumericBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyObjectToInteger(String.class, String::length, CTX);
			assertEquals(0, buffer.size());
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
			CategoricalBuffer buffer = calculator.getResult();

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
					v -> v.equals("value1"), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1"));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			CategoricalBuffer<Object> buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyObjectToCategorical(String.class, v -> v, CTX);
			assertEquals(0, buffer.size());
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
			CategoricalBuffer buffer = calculator.getResult();

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
			CategoricalBuffer<Integer> buffer = table.transform(0).applyObjectToCategorical(String.class,
					v -> v.equals("value1") ? 1 : 0, format.maxValue(), CTX);
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
			ApplierObjectToObject<String, Integer> calculator = new ApplierObjectToObject<>(
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
			ApplierObjectToObject<String, Integer> calculator = new ApplierObjectToObject<>(
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, mappingList), String.class,
					String::length);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer buffer = calculator.getResult();

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
			ObjectBuffer<Boolean> buffer = table.transform(0).applyObjectToObject(String.class, "value1"::equals,
					CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> mappingList.get(data[i]).equals("value1"));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			ObjectBuffer<Integer> buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyObjectToObject(String.class, String::length, CTX);
			assertEquals(0, buffer.size());
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
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
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
			TimeBuffer buffer =
					table.transform(0).applyObjectToTime(String.class, v -> LocalTime.of(12, v.length()), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> LocalTime.of(12, mappingList.get(data[i]).length()));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			TimeBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyObjectToTime(Object.class, v -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
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
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[data.length];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
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
			NanosecondDateTimeBuffer buffer =
					table.transform(0)
							.applyObjectToDateTime(String.class, v -> Instant.ofEpochMilli(v.length() * 1_000_000), CTX);
			Object[] expected = new Object[buffer.size()];
			Arrays.setAll(expected, i -> Instant.ofEpochMilli(mappingList.get(data[i]).length() * 1_000_000));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			DateTimeBuffer buffer =
					new Transformer(
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], Arrays.asList(null, "bla")))
							.applyObjectToDateTime(Object.class, v -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorNumericToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNNumericToNumeric calculator =
					new ApplierNNumericToNumeric(new Column[]{new DoubleArrayColumn(data),
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
			ApplierNNumericToNumeric calculator =
					new ApplierNNumericToNumeric(new Column[]{new DoubleArrayColumn(first),
							new DoubleArrayColumn(second), new DoubleArrayColumn(third)},
							row -> row.get(2) + row.get(0) * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = third[i] + 2.5 * first[i];
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}


		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToReal(row -> row.get(0) + row.get(1) /
					2.0, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			int size = 0;
			double[] data = new double[size];
			NumericBuffer buffer = new RowTransformer(new Column[]{new DoubleArrayColumn(data),
					new DoubleArrayColumn(data), new DoubleArrayColumn(data)})
					.applyNumericToReal(row -> row.get(0) + row.get(1) /
							2.0, CTX);
			assertEquals(0, buffer.size());
		}

	}

	public static class MoreColumnsCalculatorNumericToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = new double[size];
			ApplierNNumericToNumeric calculator =
					new ApplierNNumericToNumeric(new Column[]{new DoubleArrayColumn(data),
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
			ApplierNNumericToNumeric calculator =
					new ApplierNNumericToNumeric(new Column[]{new DoubleArrayColumn(first),
							new DoubleArrayColumn(second), new DoubleArrayColumn(third)},
							row -> row.get(2) + row.get(0) * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

			double[] expected = new double[first.length];
			for (int i = start; i < end; i++) {
				expected[i] = Math.round(third[i] + 2 * first[i]);
			}
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testWhole() {
			double[] result = new double[table.height()];
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToInteger(row -> row.get(0) + row.get
					(1) / 2 + 0.1, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> 2 * i);
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			int size = 0;
			double[] data = new double[size];
			NumericBuffer buffer = new RowTransformer(new Column[]{new DoubleArrayColumn(data),
					new DoubleArrayColumn(data), new DoubleArrayColumn(data)})
					.applyNumericToInteger(row -> row.get(0) + row.get(1) /
							2.0, CTX);
			assertEquals(0, buffer.size());
		}

	}


	public static class MoreColumnsCalculatorCategoricalToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNCategoricalToNumeric calculator = new ApplierNCategoricalToNumeric(new Column[]{column,
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
			ApplierNCategoricalToNumeric calculator = new ApplierNCategoricalToNumeric(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> row.get(2) + row.get(0) * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToReal(row -> row.get(0) + row.get
					(1) * 2 + 0.1, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> first[i] + 2 * second[i] + 0.1);
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			NumericBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyCategoricalToReal(row -> row.get(0) + row.get
							(1) * 2 + 0.1, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorCategoricalToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNCategoricalToNumeric calculator = new ApplierNCategoricalToNumeric(new Column[]{column,
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
			ApplierNCategoricalToNumeric calculator = new ApplierNCategoricalToNumeric(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> row.get(2) + row.get(0) * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToInteger(row -> row.get(0) + row
					.get(1) * 2 + 0.1, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> first[i] + 2 * second[i]);
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			NumericBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyCategoricalToInteger(row -> row.get(0) + row.get
							(1) * 2, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorObjectToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNObjectToNumeric<Object> calculator = new ApplierNObjectToNumeric<>(new Column[]{column,
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
			ApplierNObjectToNumeric<String> calculator = new ApplierNObjectToNumeric<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> row.get(2).length() + row.get(0).length() * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToReal(String.class,
					row -> row.get(0).length() + row.get(1).length() * 2.5, CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[table.height()];
			Arrays.setAll(expected,
					i -> mappingList.get(first[i]).length() + 2.5 * mappingList.get(second[i]).length());
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			NumericBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToReal(Object.class,row -> row.get(0).hashCode()*row.get(2).hashCode(), CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorObjectToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNObjectToNumeric<Object> calculator = new ApplierNObjectToNumeric<>(new Column[]{column,
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
			ApplierNObjectToNumeric<String> calculator = new ApplierNObjectToNumeric<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> row.get(2).length() + row.get(0).length() * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToInteger(String.class,
					row -> row.get(0).length() + row.get(1).length() * 2 + 0.1, CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			for (int i = 0; i < buffer.size(); i++) {
				result[i] = buffer.get(i);
			}
			double[] expected = new double[result.length];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).length() + 2 * mappingList.get(second[i]).length());
			assertArrayEquals(expected, result, EPSILON);
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			NumericBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToInteger(Object.class,row -> row.get(0).hashCode()*row.get(2).hashCode(), CTX);
			assertEquals(0, buffer.size());
		}
	}


	public static class MoreColumnsCalculatorGeneralToReal {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierMixedToNumeric calculator = new ApplierMixedToNumeric(new Column[]{column,
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
			ApplierMixedToNumeric calculator = new ApplierMixedToNumeric(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + row.getIndex(0) * 2.5, false);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyMixedToReal(
					row -> Objects.toString(row.getObject(0), "").length() * 2.5 + row.getNumeric(1), CTX);
			assertEquals(Column.TypeId.REAL, buffer.type());
			double[] expected = new double[table.height()];
			Arrays.setAll(expected,
					i -> Objects.toString(mappingList.get(first[i]), "").length() * 2.5 + second[i]);
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			NumericBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyMixedToReal(row -> row.getObject(0).hashCode() * row.getNumeric(2), CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorGeneralToInteger {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierMixedToNumeric calculator = new ApplierMixedToNumeric(new Column[]{column,
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
			ApplierMixedToNumeric calculator = new ApplierMixedToNumeric(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + row.getIndex(0) * 2, true);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NumericBuffer buffer = calculator.getResult();

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
			NumericBuffer buffer = table.transform(new int[]{0, 1}).applyMixedToInteger(
					row -> Objects.toString(row.getObject(0), "").length() * 2 + (int) (row.getNumeric(1) * 5), CTX);
			assertEquals(Column.TypeId.INTEGER, buffer.type());
			double[] expected = new double[table.height()];
			Arrays.setAll(expected,
					i -> Objects.toString(mappingList.get(first[i]), "").length() * 2 + (int) (5 * second[i]));
			assertArrayEquals(expected, readBufferToArray(buffer), EPSILON);
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			NumericBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyMixedToInteger(row -> row.getObject(0).hashCode() * row.getNumeric(2), CTX);
			assertEquals(0, buffer.size());
		}

	}

	public static class MoreColumnsCalculatorObjectToCategorical {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNObjectToCategorical<Object, Object> calculator =
					new ApplierNObjectToCategorical<>(new Column[]{column,
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
			ApplierNObjectToCategorical<String, Integer> calculator =
					new ApplierNObjectToCategorical<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
							row -> row.get(2).length() + row.get(0).length() * 2, IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Integer> buffer = calculator.getResult();

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
					v -> v.get(0).equals(v.get(1)), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			CategoricalBuffer<Boolean> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToCategorical(Object.class, row -> row.get(0)== row.get(2), CTX);
			assertEquals(0, buffer.size());
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
			ApplierNObjectToCategorical<Object, Integer> calculator =
					new ApplierNObjectToCategorical<>(new Column[]{column,
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
			ApplierNObjectToCategorical<String, Boolean> calculator =
					new ApplierNObjectToCategorical<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
							row -> row.get(2).length() == row.get(0).length(), format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Boolean> buffer = calculator.getResult();

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
			CategoricalBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyObjectToCategorical(String
							.class,
					v -> v.get(0).equals(v.get(1)) ? 1 : 0, format.maxValue(), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? 1 : 0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			CategoricalBuffer<Boolean> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToCategorical(Object.class, row -> row.get(0) == row.get(2), CTX);
			assertEquals(0, buffer.size());
		}
	}


	public static class MoreColumnsCalculatorCategoricalToCategorical {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierNCategoricalToCategorical<Object> calculator =
					new ApplierNCategoricalToCategorical<>(new Column[]{column,
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
			ApplierNCategoricalToCategorical<Integer> calculator =
					new ApplierNCategoricalToCategorical<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
							row -> row.get(2) + row.get(0) * 2, IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Integer> buffer = calculator.getResult();

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
					v -> v.get(0) == v.get(1), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i]);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			CategoricalBuffer<Boolean> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyCategoricalToCategorical(row -> row.get(0) == row.get(1) * 2, CTX);
			assertEquals(0, buffer.size());
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
			ApplierNCategoricalToCategorical<Object> calculator =
					new ApplierNCategoricalToCategorical<>(new Column[]{column,
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
			ApplierNCategoricalToCategorical<Boolean> calculator =
					new ApplierNCategoricalToCategorical<>(new Column[]{
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
							new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)},
							row -> row.get(2) + 2 == row.get(0), format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Boolean> buffer = calculator.getResult();

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
			CategoricalBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyCategoricalToCategorical(
					v -> v.get(0) == v.get(1) ? 1 : 0, format.maxValue(), CTX);
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
			ApplierMixedToCategorical<Object> calculator =
					new ApplierMixedToCategorical<>(new Column[]{column,
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
			ApplierMixedToCategorical<Integer> calculator = new ApplierMixedToCategorical<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, getMappingList()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, getMappingList()),
					new DoubleArrayColumn(third)},
					row -> (int) (5 * row.getNumeric(2)) + row.getIndex(0), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Integer> buffer = calculator.getResult();

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
			Int32CategoricalBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyMixedToCategorical(
					v -> v.getNumeric(1) < getLengthOrNull(v.getObject(0)) / 10.0, CTX);
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

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			CategoricalBuffer<String> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyMixedToCategorical(row -> row.getObject(0, String.class)+ row.getIndex(2), CTX);
			assertEquals(0, buffer.size());
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
			ApplierMixedToCategorical<Object> calculator =
					new ApplierMixedToCategorical<>(new Column[]{column,
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
			ApplierMixedToCategorical<Boolean> calculator = new ApplierMixedToCategorical<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + 2 < row.getIndex(0), format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Boolean> buffer = calculator.getResult();

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
			CategoricalBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyMixedToCategorical(
					v -> getLengthOrNull(v.getObject(0)) <= v.getNumeric(1) * 10 ? 1 : 0, format.maxValue(), CTX);
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
			ApplierNNumericToCategorical<Object> calculator =
					new ApplierNNumericToCategorical<>(new Column[]{column,
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
			ApplierNNumericToCategorical<String> calculator = new ApplierNNumericToCategorical<>(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> Objects.toString(row.get(2) + row.get(0)), IntegerFormats.Format.SIGNED_INT32);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<String> buffer = calculator.getResult();

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
					v -> v.get(0) * v.get(1) + "", CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] * second[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			int size = 0;
			double[] data = new double[size];
			CategoricalBuffer<String> buffer = new RowTransformer(new Column[]{new DoubleArrayColumn(data),
					new DoubleArrayColumn(data), new DoubleArrayColumn(data)})
					.applyNumericToCategorical(row -> row.get(0) + "," + row.get(1), CTX);
			assertEquals(0, buffer.size());
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
			ApplierNNumericToCategorical<Object> calculator =
					new ApplierNNumericToCategorical<>(new Column[]{column,
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
			ApplierNNumericToCategorical<Boolean> calculator = new ApplierNNumericToCategorical<>(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> row.get(2) * row.get(0) > 0.5, format);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			CategoricalBuffer<Boolean> buffer = calculator.getResult();

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
			CategoricalBuffer<Integer> buffer = table.transform(new int[]{0, 1}).applyNumericToCategorical(
					v -> v.get(0) + v.get(1) / 2 > 0.5 ? 1 : 0, format.maxValue(), CTX);
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
			ApplierNNumericToObject<Object> calculator = new ApplierNNumericToObject<>(new Column[]{column,
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
			ApplierNNumericToObject<String> calculator = new ApplierNNumericToObject<>(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> Objects.toString(row.get(2) + row.get(0)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer<String> buffer = calculator.getResult();

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
			ObjectBuffer<String> buffer = table.transform(new int[]{0, 1}).applyNumericToObject(
					v -> v.get(0) * v.get(1) + "", CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] * second[i] + "");
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			int size = 0;
			double[] data = new double[size];
			ObjectBuffer<String> buffer = new RowTransformer(new Column[]{new DoubleArrayColumn(data),
					new DoubleArrayColumn(data), new DoubleArrayColumn(data)})
					.applyNumericToObject(row -> row.get(0) + "," + row.get(1), CTX);
			assertEquals(0, buffer.size());
		}

	}

	public static class MoreColumnsCalculatorNumericToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNNumericToTime calculator = new ApplierNNumericToTime(new Column[]{column,
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
			ApplierNNumericToTime calculator = new ApplierNNumericToTime(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> LocalTime.of((int) Math.floor(row.get(2) * 24), (int) Math.floor(row.get(0) * 60)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
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
			TimeBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToTime(
					row -> LocalTime.of((int) Math.floor(row.get(0) * 24), (int) Math.floor(row.get(1) * 60)), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected,
					i -> LocalTime.of((int) Math.floor(first[i] * 24), (int) Math.floor(second[i] * 60)));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			int size = 0;
			double[] data = new double[size];
			TimeBuffer buffer = new RowTransformer(new Column[]{new DoubleArrayColumn(data),
					new DoubleArrayColumn(data), new DoubleArrayColumn(data)})
					.applyNumericToTime(row -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
		}

	}

	public static class MoreColumnsCalculatorNumericToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			double[] data = random(size);
			Column column = new DoubleArrayColumn(data);
			ApplierNNumericToDateTime calculator = new ApplierNNumericToDateTime(new Column[]{column,
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
			ApplierNNumericToDateTime calculator = new ApplierNNumericToDateTime(new Column[]{
					new DoubleArrayColumn(first),
					new DoubleArrayColumn(second),
					new DoubleArrayColumn(third)},
					row -> Instant.ofEpochMilli(Math.round(Math.pow(10 * row.get(2), 30 * row.get(0)))));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
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
			NanosecondDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyNumericToDateTime(
					row -> Instant.ofEpochMilli(Math.round(Math.pow(10 * row.get(0), 30 * row.get(1)))), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected,
					i -> Instant.ofEpochMilli(Math.round(Math.pow(10 * first[i], 30 * second[i]))));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			int size = 0;
			double[] data = new double[size];
			DateTimeBuffer buffer = new RowTransformer(new Column[]{new DoubleArrayColumn(data),
					new DoubleArrayColumn(data), new DoubleArrayColumn(data)})
					.applyNumericToDateTime(row -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorCategoricalToFree {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierNCategoricalToObject<Object> calculator = new ApplierNCategoricalToObject<>(new Column[]{column,
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
			ApplierNCategoricalToObject<Integer> calculator = new ApplierNCategoricalToObject<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> row.get(2) + row.get(0) * 2);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer<Integer> buffer = calculator.getResult();

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
			ObjectBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyCategoricalToObject(
					v -> v.get(0) == v.get(1), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i]);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			ObjectBuffer<Boolean> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyCategoricalToObject(row -> row.get(0) == row.get(1), CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorCategoricalToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierNCategoricalToTime calculator = new ApplierNCategoricalToTime(new Column[]{column,
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
			ApplierNCategoricalToTime calculator = new ApplierNCategoricalToTime(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> LocalTime.of(row.get(2) % 24, row.get(0)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
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
			TimeBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToTime(
					v -> v.get(0) == v.get(1) ? LocalTime.NOON : LocalTime.MIDNIGHT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i] ? LocalTime.NOON : LocalTime.MIDNIGHT);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			TimeBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyCategoricalToTime(row -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
		}

	}

	public static class MoreColumnsCalculatorCategoricalToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, new ArrayList<>());
			ApplierNCategoricalToDateTime calculator = new ApplierNCategoricalToDateTime(new Column[]{column,
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
			ApplierNCategoricalToDateTime calculator = new ApplierNCategoricalToDateTime(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, new ArrayList<>()),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, new ArrayList<>())},
					row -> Instant.ofEpochMilli(Math.round(Math.pow(row.get(2), row.get(0)))));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
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
			NanosecondDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyCategoricalToDateTime(
					v -> v.get(0) == v.get(1) ? Instant.MIN : Instant.MAX, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] == second[i] ? Instant.MIN : Instant.MAX);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			DateTimeBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyCategoricalToDateTime(row -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorObjectToFree {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNObjectToObject<Object, Object> calculator = new ApplierNObjectToObject<>(new Column[]{column,
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
			ApplierNObjectToObject<String, Integer> calculator = new ApplierNObjectToObject<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> row.get(2).length() + row.get(0).length() * 2);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer<Integer> buffer = calculator.getResult();

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
			ObjectBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyObjectToObject(String.class,
					v -> v.get(0).equals(v.get(1)), CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])));
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			ObjectBuffer<Boolean> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToObject(Object.class, row -> row.get(0)== row.get(2), CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorObjectToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNObjectToTime<Object> calculator = new ApplierNObjectToTime<>(new Column[]{column,
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
			ApplierNObjectToTime<String> calculator = new ApplierNObjectToTime<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> LocalTime.of(row.get(2).length(), row.get(0).length() * 2));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
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
			TimeBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToTime(String.class,
					v -> v.get(0).equals(v.get(1)) ? LocalTime.NOON : LocalTime.MIDNIGHT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? LocalTime
					.NOON :
					LocalTime.MIDNIGHT);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			TimeBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToTime(String.class, row -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorObjectToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierNObjectToDateTime<Object> calculator = new ApplierNObjectToDateTime<>(new Column[]{column,
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
			ApplierNObjectToDateTime<String> calculator = new ApplierNObjectToDateTime<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, third, mappingList)}, String.class,
					row -> Instant.ofEpochMilli(row.get(2).length() * row.get(0).length() * 1_999_999));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
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
			NanosecondDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyObjectToDateTime(String.class,
					v -> v.get(0).equals(v.get(1)) ? Instant.MAX : Instant.MIN, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected,
					i -> mappingList.get(first[i]).equals(mappingList.get(second[i])) ? Instant.MAX : Instant.MIN);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			DateTimeBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyObjectToDateTime(String.class, row -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorGeneralToFree {

		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierMixedToObject<Object> calculator = new ApplierMixedToObject<>(new Column[]{column,
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
			ApplierMixedToObject<Double> calculator = new ApplierMixedToObject<>(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> row.getNumeric(2) + row.getIndex(0) * 2);
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			ObjectBuffer<Double> buffer = calculator.getResult();

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
			ObjectBuffer<Boolean> buffer = table.transform(new int[]{0, 1}).applyMixedToObject(
					v -> v.getNumeric(0) < ((String) v.getObject(1)).length() / 10.0, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] < mappingList.get(second[i]).length() / 10.0);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			ObjectBuffer<String> buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyMixedToObject(row -> row.getObject(0, String.class)+row.getNumeric(1), CTX);
			assertEquals(0, buffer.size());
		}

	}

	public static class MoreColumnsCalculatorGeneralToTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierMixedToTime calculator = new ApplierMixedToTime(new Column[]{column,
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
			ApplierMixedToTime calculator = new ApplierMixedToTime(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> LocalTime.of((int) Math.round(12 * row.getNumeric(2)), row.getIndex(0) * 2));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			TimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 00:00
			Arrays.fill(expected, LocalTime.MIN);
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
			TimeBuffer buffer = table.transform(new int[]{0, 1}).applyMixedToTime(
					v -> v.getNumeric(0) < ((String) v.getObject(1)).length() / 10.0 ?
							LocalTime.NOON : LocalTime.MIDNIGHT, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] < mappingList.get(second[i]).length() / 10.0 ?
					LocalTime.NOON : LocalTime.MIDNIGHT);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			TimeBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyMixedToTime( row -> LocalTime.NOON, CTX);
			assertEquals(0, buffer.size());
		}
	}

	public static class MoreColumnsCalculatorGeneralToDateTime {


		@Test
		public void testResultSize() {
			Random random = new Random();
			int size = random.nextInt(100);
			int[] data = randomInts(size);
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, data, getMappingList());
			ApplierMixedToDateTime calculator = new ApplierMixedToDateTime(new Column[]{column,
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
			ApplierMixedToDateTime calculator = new ApplierMixedToDateTime(new Column[]{
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, first, mappingList),
					new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, second, mappingList),
					new DoubleArrayColumn(third)},
					row -> Instant.ofEpochMilli(Math.round(row.getIndex(0) * row.getNumeric(2) * 100_000)));
			calculator.init(1);
			int start = 10;
			int end = 30;
			calculator.doPart(start, end, 0);
			NanosecondDateTimeBuffer buffer = calculator.getResult();

			Object[] expected = new Object[size];
			//values outside of start-end are initially 1.1.1970
			Arrays.fill(expected, Instant.EPOCH);
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
			NanosecondDateTimeBuffer buffer = table.transform(new int[]{0, 1}).applyMixedToDateTime(
					v -> v.getNumeric(0) < ((String) v.getObject(1)).length() / 10.0 ?
							Instant.MAX : Instant.MIN, CTX);
			Object[] expected = new Object[size];
			Arrays.setAll(expected, i -> first[i] < mappingList.get(second[i]).length() / 10.0 ?
					Instant.MAX : Instant.MIN);
			assertArrayEquals(expected, readBufferToArray(buffer));
		}

		@Test
		public void testNullSize() {
			Column column = new SimpleCategoricalColumn<>(ColumnTypes.NOMINAL, new int[0], getMappingList());
			DateTimeBuffer buffer = new RowTransformer(Arrays.asList(column, column, column))
					.applyMixedToDateTime(row -> Instant.EPOCH, CTX);
			assertEquals(0, buffer.size());
		}

	}


}

