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

import java.time.Instant;
import java.time.LocalTime;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.IntBinaryOperator;
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;
import java.util.function.ObjDoubleConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Supplies transformation methods (different kinds of apply and reduce methods) starting from the given {@link
 * #transformationColumn}. Depending on how the transformation column should be read (numeric, categorical or objects)
 * the right method (e.g. applyNumericTo or reduceCategorical) must be selected.
 *
 * @author Gisa Meier
 */
public final class Transformer {

	/**
	 * Message for {@code null} contexts
	 */
	private static final String MESSAGE_CONTEXT_NULL = "Context must not be null";

	/**
	 * Message for {@code null} reducers
	 */
	private static final String MESSAGE_REDUCER_NULL = "Reducer function must not be null";

	/**
	 * Message for {@code null} workloads
	 */
	private static final String MESSAGE_WORKLOAD_NULL = "Workload must not be null";

	/**
	 * Message for {@code null} suppliers
	 */
	private static final String MESSAGE_SUPPLIER_NULL = "Supplier function must not be null";

	/**
	 * Message for {@code null} operators
	 */
	private static final String MESSAGE_MAPPING_OPERATOR_NULL = "Mapping operator must not be null";

	/**
	 * Message for {@code null} types
	 */
	private static final String MESSAGE_TYPE_NULL = "Type must not be null";

	/**
	 * Message for {@code null} types
	 */
	private static final String MESSAGE_COMBINER_NULL = "Combiner function must not be null";

	private final Column transformationColumn;

	/**
	 * Creates a new transformer for transformations of the given column.
	 *
	 * @param transformationColumn
	 * 		the column to transform to something
	 * @throws NullPointerException
	 * 		if the transformation column is {@code null}
	 */
	public Transformer(Column transformationColumn) {
		this.transformationColumn =
				Objects.requireNonNull(transformationColumn, "Transformation column must not be null");
	}

	/**
	 * Creates a task that applies the given unary operator to the numeric-readable transformation column in
	 * order to create a real column buffer. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel. If the transformation column is not numeric-readable, an
	 * {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public ColumnTask applyNumericToReal(DoubleUnaryOperator operator, Workload workload) {
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ColumnTask(new ParallelExecutor<>(
				new ApplierNumericToNumeric(transformationColumn, operator, false), workload).create(),
				transformationColumn.size());
	}


	/**
	 * Creates a task that applies the given unary operator to the numeric-readable transformation column to
	 * create an integer buffer. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel. If the transformation column is not numeric-readable, an
	 * {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public ColumnTask applyNumericToInteger(DoubleUnaryOperator operator, Workload workload) {
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ColumnTask(new ParallelExecutor<>(
				new ApplierNumericToNumeric(transformationColumn, operator, true), workload).create(),
				transformationColumn.size());
	}


	/**
	 * Applies the given unary operator to the numeric-readable transformation column returning the result in a new
	 * real {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public ColumnBuffer applyNumericToReal(DoubleUnaryOperator operator, Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToReal(operator, workload).run(context);
	}

	/**
	 * Applies the given unary operator to the numeric readable column returning the result in a new {@link
	 * ColumnBuffer} of type {@link Column.TypeId#INTEGER}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public ColumnBuffer applyNumericToInteger(DoubleUnaryOperator operator, Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToInteger(operator, workload).run(context);
	}


	/**
	 * Creates a task that applies the given operator to the categorical transformation column. Depending on the input
	 * size and the specified workload per data-point, the computation might be performed in parallel. If the
	 * transformation column is not categorical, an {@link UnsupportedOperationException} will happen on execution of
	 * the task.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public ColumnTask applyCategoricalToReal(IntToDoubleFunction operator, Workload workload) {
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ColumnTask(new ParallelExecutor<>(
				new ApplierCategoricalToNumeric(transformationColumn, operator, false), workload).create(),
				transformationColumn.size());
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new real {@link
	 * ColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 */
	public ColumnBuffer applyCategoricalToReal(IntToDoubleFunction operator, Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyCategoricalToReal(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator to the categorical transformation column creating an integer
	 * buffer. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel. If the transformation column is not categorical, an {@link UnsupportedOperationException}
	 * will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public ColumnTask applyCategoricalToInteger(IntToDoubleFunction operator, Workload workload) {
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ColumnTask(new ParallelExecutor<>(
				new ApplierCategoricalToNumeric(transformationColumn, operator, true), workload).create(),
				transformationColumn.size());
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new integer {@link
	 * ColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 */
	public ColumnBuffer applyCategoricalToInteger(IntToDoubleFunction operator, Workload workload,
												  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyCategoricalToInteger(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator to the object-readable transformation column. Depending on the
	 * input size and the specified workload per data-point, the computation might be performed in parallel. If the
	 * transformation column is not object-readable or not of the given type, an {@link UnsupportedOperationException}
	 * or {@link IllegalArgumentException} will happen on execution of the task.
	 *
	 * @param type
	 * 		the type of the objects in the transformation column
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of objects in the column
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public <T> ColumnTask applyObjectToReal(Class<T> type, ToDoubleFunction<T> operator, Workload
			workload) {
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		return new ColumnTask(new ParallelExecutor<>(
				new ApplierObjectToNumeric<T>(transformationColumn, type, operator, false), workload).create(),
				transformationColumn.size());
	}

	/**
	 * Applies the given operator to the object-readable column returning the result in a new {@link ColumnBuffer}.
	 * Depending on the input size and the specified workload per data-point, the computation might be performed in
	 * parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type
	 */
	public <T> ColumnBuffer applyObjectToReal(Class<T> type, ToDoubleFunction<T> operator, Workload workload,
											  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyObjectToReal(type, operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator to the object-readable transformation column to create an integer
	 * column buffer. If the transformation column is not object-readable or not of the given type, an {@link
	 * UnsupportedOperationException} or {@link IllegalArgumentException} will happen on execution of the task.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of objects in the column
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public <T> ColumnTask applyObjectToInteger(Class<T> type, ToDoubleFunction<T> operator, Workload workload) {
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		return new ColumnTask(new ParallelExecutor<>(
				new ApplierObjectToNumeric<T>(transformationColumn, type, operator, true), workload).create(),
				transformationColumn.size());
	}

	/**
	 * Applies the given operator to the object-readable transformation column returning the result in a new integer
	 * {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type
	 */
	public <T> ColumnBuffer applyObjectToInteger(Class<T> type, ToDoubleFunction<T> operator,
												 Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyObjectToInteger(type, operator, workload).run(context);
	}


	/**
	 * Applies the given operator to the object-readable transformation column returning the result in a new {@link
	 * CategoricalColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type
	 */
	public <T, R> Int32CategoricalBuffer<T> applyObjectToCategorical(Class<R> type, Function<R, T> operator,
																	 Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(new ApplierObjectToCategorical<>
				(transformationColumn,
						type, operator, IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator to the object-readable transformation column returning the result in a new {@link
	 * CategoricalColumnBuffer}. The format of the target buffer is derived from given maxNumberOfValues parameter.
	 * Depending on the input size and the specified workload per data-point, the computation might be performed in
	 * parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type or if more different values
	 * 		are	supplied than supported by the buffer format calculated from the maxNumberOfValues
	 */
	public <T, R> CategoricalColumnBuffer<T> applyObjectToCategorical(Class<R> type, Function<R, T> operator,
																	  int maxNumberOfValues, Workload workload,
																	  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(Math.min(transformationColumn.size(),
				maxNumberOfValues));
		return new ParallelExecutor<>(new ApplierObjectToCategorical<>(transformationColumn,
				type, operator, format), workload).create().run(context);
	}

	/**
	 * Applies the given operator to the object-readable transformation column returning the result in a new {@link
	 * FreeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type
	 */
	public <T, R> FreeColumnBuffer<T> applyObjectToFree(Class<R> type, Function<R, T> operator,
														Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		return new ParallelExecutor<>(new ApplierObjectToFree<>(transformationColumn, type, operator), workload)
				.create().run(context);
	}

	/**
	 * Applies the given operator to the object-readable transformation column returning the result in a new {@link
	 * TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <R>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type
	 */
	public <R> TimeColumnBuffer applyObjectToTime(Class<R> type, Function<R, LocalTime> operator,
												  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		return new ParallelExecutor<>(new ApplierObjectToTime<>(transformationColumn, type, operator), workload)
				.create().run(context);
	}

	/**
	 * Applies the given operator to the object-readable transformation column returning the result in a new {@link
	 * HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param type
	 * 		the type of the objects in the column to work on
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <R>
	 * 		type of objects in the column
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation column cannot be read as the given type
	 */
	public <R> HighPrecisionDateTimeBuffer applyObjectToDateTime(Class<R> type, Function<R, Instant> operator,
																 Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		return new ParallelExecutor<>(new ApplierObjectToDateTime<>(transformationColumn, type, operator), workload)
				.create().run(context);
	}

	/**
	 * Applies the given operator to the numeric-readable transformation column returning the result in a new {@link
	 * CategoricalColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public <T> Int32CategoricalBuffer<T> applyNumericToCategorical(DoubleFunction<T> operator,
																   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(new ApplierNumericToCategorical<>
				(transformationColumn, operator, IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator to the numeric-readable transformation column returning the result in a new {@link
	 * CategoricalColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 * @throws IllegalArgumentException
	 * 		if more different values are supplied than supported by the buffer format calculated from the
	 * 		maxNumberOfValues
	 */
	public <T> CategoricalColumnBuffer<T> applyNumericToCategorical(DoubleFunction<T> operator,
																	int maxNumberOfValues, Workload workload,
																	Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(Math.min(transformationColumn.size(),
				maxNumberOfValues));
		return new ParallelExecutor<>(new ApplierNumericToCategorical<>(transformationColumn, operator, format),
				workload).create().run(context);
	}

	/**
	 * Applies the given operator to the numeric-readable transformation column returning the result in a new {@link
	 * FreeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public <T> FreeColumnBuffer<T> applyNumericToFree(DoubleFunction<T> operator, Workload workload,
													  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ApplierNumericToFree<>(transformationColumn, operator), workload).create()
				.run(context);
	}

	/**
	 * Applies the given operator to the numeric-readable transformation column returning the result in a new {@link
	 * TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public TimeColumnBuffer applyNumericToTime(DoubleFunction<LocalTime> operator, Workload workload,
											   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ApplierNumericToTime(transformationColumn, operator), workload).create()
				.run(context);
	}

	/**
	 * Applies the given operator to the numeric-readable transformation column returning the result in a new {@link
	 * HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public HighPrecisionDateTimeBuffer applyNumericToDateTime(DoubleFunction<Instant> operator, Workload workload,
															  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ApplierNumericToDateTime(transformationColumn, operator), workload).create()
				.run(context);
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new {@link
	 * CategoricalColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 */
	public <T> Int32CategoricalBuffer<T> applyCategoricalToCategorical(IntFunction<T> operator,
																	   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(new ApplierCategoricalToCategorical<T>
				(transformationColumn, operator, IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new {@link
	 * CategoricalColumnBuffer}. The format of the target buffer is derived from given maxNumberOfValues parameter.
	 * Depending on the input size and the specified workload per data-point, the computation might be performed in
	 * parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 * @throws IllegalArgumentException
	 * 		if more different values are supplied than supported by the buffer format calculated from the
	 * 		maxNumberOfValues
	 */
	public <T> CategoricalColumnBuffer<T> applyCategoricalToCategorical(IntFunction<T> operator,
																		int maxNumberOfValues, Workload workload,
																		Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(Math.min(transformationColumn.size(),
				maxNumberOfValues));
		return new ParallelExecutor<>(new ApplierCategoricalToCategorical<>(transformationColumn,
				operator, format), workload).create().run(context);
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new {@link
	 * FreeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 */
	public <T> FreeColumnBuffer<T> applyCategoricalToFree(IntFunction<T> operator, Workload workload,
														  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ApplierCategoricalToFree<>(transformationColumn,
				operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new {@link
	 * TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 */
	public TimeColumnBuffer applyCategoricalToTime(IntFunction<LocalTime> operator, Workload workload,
												   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ApplierCategoricalToTime(transformationColumn, operator), workload).create()
				.run(context);
	}

	/**
	 * Applies the given operator to the categorical transformation column returning the result in a new {@link
	 * HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each value
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 */
	public HighPrecisionDateTimeBuffer applyCategoricalToDateTime(IntFunction<Instant> operator, Workload workload,
																  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_MAPPING_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ApplierCategoricalToDateTime(transformationColumn, operator), workload)
				.create().run(context);
	}

	/**
	 * Performs a reduction on the elements of the transformation column, using the provided identity value and an
	 * associative reduction function, and returns the reduced value. This is equivalent to:
	 *
	 * <pre>{@code
	 *     double result = identity;
	 *     ColumnReader reader = new ColumnReader(transformationColumn);
	 *     while (reader.hasRemaining){
	 *         result = reducer.applyAsDouble(result, reader.read())
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The {@code identity} value must be an identity for the reducer function. This means that for all {@code x},
	 * {@code reducer.apply(identity, x)} is equal to {@code x}. The {@code reducer} function must be an associative
	 * function.
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param identity
	 * 		the identity value for the reduction function
	 * @param reducer
	 * 		an associative, stateless function for combining two values
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if one of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 * @see java.util.stream.DoubleStream#reduce(double, DoubleBinaryOperator)
	 */
	public double reduceNumeric(double identity, DoubleBinaryOperator reducer, Workload workload, Context context) {
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ColumnReducerDouble(transformationColumn, identity, reducer), workload)
				.create().run(context);
	}


	/**
	 * Performs a reduction on the elements of the transformation column, using the provided identity, reduction and
	 * combining functions. This is equivalent to:
	 *
	 * <pre>{@code
	 *     double result = identity;
	 *     ColumnReader reader = new ColumnReader(transformationColumn);
	 *     while (reader.hasRemaining){
	 *         result = reducer.applyAsDouble(result, reader.read())
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The {@code identity} value must be an identity for the combiner function.  This means that for all {@code u},
	 * {@code combiner(identity, u)} is equal to {@code u}.  Additionally, the {@code combiner} function must be
	 * compatible with the {@code accumulator} function; for all {@code u} and {@code t}, the following must hold:
	 * <pre>{@code
	 *     combiner.apply(u, reducer.apply(identity, t)) == reducer.apply(u, t)
	 * }</pre>
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param identity
	 * 		the identity value for the combiner function
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the reducer
	 * 		function
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if one of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 * @see java.util.stream.Stream#reduce(Object, BiFunction, BinaryOperator)
	 */
	public double reduceNumeric(double identity, DoubleBinaryOperator reducer, DoubleBinaryOperator combiner,
								Workload workload, Context context) {
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ColumnReducerDouble(transformationColumn, identity, reducer, combiner),
				workload).create().run(context);
	}


	/**
	 * Performs a reduction on the category indices of the {@link Column.Category#CATEGORICAL} transformation column,
	 * using the provided identity value and an associative reduction function, and returns the reduced value. This is
	 * equivalent to:
	 *
	 * <pre>{@code
	 *     int result = identity;
	 *     CategoricalColumnReader reader = new CategoricalColumnReader(transformationColumn);
	 *     while (reader.hasRemaining){
	 *         result = reducer.applyAsInt(result, reader.read())
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The {@code identity} value must be an identity for the reducer function. This means that for all {@code x},
	 * {@code reducer.apply(identity, x)} is equal to {@code x}. The {@code reducer} function must be an associative
	 * function.
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param identity
	 * 		the identity value for the reduction function
	 * @param reducer
	 * 		an associative, stateless function for combining two values
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if one of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 * @see java.util.stream.IntStream#reduce(int, IntBinaryOperator)
	 */
	public int reduceCategorical(int identity, IntBinaryOperator reducer, Workload workload, Context
			context) {
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new CategoricalColumnReducerInt(transformationColumn, identity, reducer),
				workload).create().run(context);
	}


	/**
	 * Performs a reduction on the elements of the{@link Column.Category#CATEGORICAL} transformation column, using the
	 * provided identity, reduction and combining functions.  This is equivalent to:
	 *
	 * <pre>{@code
	 *     int result = identity;
	 *     CategoricalColumnReader reader = new CategoricalColumnReader(transformationColumn);
	 *     while (reader.hasRemaining){
	 *         result = reducer.applyAsInt(result, reader.read())
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The {@code identity} value must be an identity for the combiner function.  This means that for all {@code u},
	 * {@code combiner(identity, u)} is equal to {@code u}.  Additionally, the {@code combiner} function must be
	 * compatible with the {@code accumulator} function; for all {@code u} and {@code t}, the following must hold:
	 * <pre>{@code
	 *     combiner.apply(u, reducer.apply(identity, t)) == reducer.apply(u, t)
	 * }</pre>
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param identity
	 * 		the identity value for the combiner function
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the reducer
	 * 		function
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if one of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 * @see java.util.stream.Stream#reduce(Object, BiFunction, BinaryOperator)
	 */
	public int reduceCategorical(int identity, IntBinaryOperator reducer, IntBinaryOperator combiner,
								 Workload workload, Context context) {
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new CategoricalColumnReducerInt(transformationColumn, identity, reducer,
				combiner), workload).create().run(context);
	}

	/**
	 * Performs a mutable reduction operation on the elements of the transformation column. A mutable reduction is one
	 * in which the
	 * reduced value is a mutable result container, such as an {@code ArrayList}, and elements are incorporated by
	 * updating the state of the result rather than by replacing the result. This produces a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     ColumnReader reader = new ColumnReader(transformationColumn);
	 *     while (reader.hasRemaining){
	 *         reducer.accept(result, reader.read());
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the reducer
	 * 		function
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#NUMERIC_READABLE}
	 * @see java.util.stream.DoubleStream#collect(Supplier, ObjDoubleConsumer, BiConsumer)
	 */
	public <T> T reduceNumeric(Supplier<T> supplier, ObjDoubleConsumer<T> reducer,
							   BiConsumer<T, T> combiner, Workload workload, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ColumnReducer<>(transformationColumn, supplier, reducer, combiner), workload)
				.create().run(context);
	}


	/**
	 * Performs a mutable reduction operation on the category indices of the {@link Column.Category#CATEGORICAL}
	 * transformation column. A mutable reduction is one in which the reduced value is a mutable result container,
	 * such as an {@code
	 * ArrayList}, and elements are incorporated by updating the state of the result rather than by replacing the
	 * result. This produces a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     CategoricalColumnReader reader = new CategoricalColumnReader(transformationColumn);
	 *     while (reader.hasRemaining){
	 *         reducer.accept(result, reader.read());
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called
	 * 		multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the reducer
	 * 		function
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Category#CATEGORICAL}
	 * @see java.util.stream.IntStream#collect(Supplier, ObjIntConsumer, BiConsumer)
	 */
	public <T> T reduceCategorical(Supplier<T> supplier, ObjIntConsumer<T> reducer,
								   BiConsumer<T, T> combiner, Workload workload, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new CategoricalColumnReducer<>(transformationColumn, supplier, reducer,
				combiner), workload).create().run(context);
	}

	/**
	 * Performs a mutable reduction operation on the type objects of the transformation column. The transformation
	 * column must be {@link Column.Capability#OBJECT_READABLE} with element type a subtype of the given type
	 * parameter.
	 * A mutable reduction is one in which the reduced value is a mutable result container, such as an {@code
	 * ArrayList}, and elements are incorporated by updating the state of the result rather than by replacing the
	 * result. This produces a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     ObjectColumnReader<R> reader = new ObjectColumnReader(transformationColumn, type);
	 *     while (reader.hasRemaining){
	 *         reducer.accept(result, reader.read());
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the context and automatically parallelized depending on the input size and the
	 * specified workload per data-point.
	 *
	 * @param type
	 * 		the type of the objects in the column
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called
	 * 		multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the reducer
	 * 		function
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the column
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @throws IllegalArgumentException
	 * 		if type is not a super type of the transformation column element type
	 * @throws UnsupportedOperationException
	 * 		if the transformation column is not {@link Column.Capability#OBJECT_READABLE}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T, R> T reduceObjects(Class<R> type, Supplier<T> supplier, BiConsumer<T, R> reducer,
								  BiConsumer<T, T> combiner, Workload workload, Context context) {
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ObjectColumnReducer<>(transformationColumn, type, supplier, reducer,
				combiner), workload).create().run(context);
	}

}