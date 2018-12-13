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
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

import com.rapidminer.belt.util.IntegerFormats;


/**
 * Supplies transformation methods (different kinds of apply and reduce methods) starting from the given {@link
 * #transformationColumns}. Depending on how the transformation columns should be read (numeric, categorical or
 * objects) the right method (e.g. applyNumericTo or reduceCategorical) must be selected.
 *
 * @author Gisa Meier
 */
public final class RowTransformer {

	/**
	 * Message for {@code null} contexts
	 */
	private static final String MESSAGE_CONTEXT_NULL = "Context must not be null";

	/**
	 * Message for {@code null} reducers
	 */
	private static final String MESSAGE_REDUCER_NULL = "Reducer function must not be null";

	/**
	 * Message for {@code null} operators
	 */
	private static final String MESSAGE_OPERATOR_NULL = "Mapping function must not be null";

	/**
	 * Message for {@code null} operators
	 */
	private static final String MESSAGE_SUPPLIER_NULL = "Supplier function must not be null";

	/**
	 * Message for {@code null} operators
	 */
	private static final String MESSAGE_COMBINER_NULL = "Combiner function must not be null";

	/**
	 * Default progress callback which does nothing at all.
	 */
	private static final DoubleConsumer NOOP_CALLBACK = i -> {};

	private final Column[] transformationColumns;

	private Workload workload = Workload.DEFAULT;

	private DoubleConsumer callback = NOOP_CALLBACK;

	/**
	 * Creates a new starting point for a transformation based on the given columns
	 *
	 * @param transformationColumns
	 * 		the columns to start the transformation from, must be all of the same length
	 * @throws NullPointerException
	 * 		if the list is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the list is empty
	 */
	public RowTransformer(List<Column> transformationColumns) {
		checkColumns(transformationColumns);
		this.transformationColumns = transformationColumns.toArray(new Column[0]);
	}

	/**
	 * Ensures that the columns list is not {@code null} or empty and contains no {@code null} entries.
	 */
	private void checkColumns(List<Column> columns) {
		Objects.requireNonNull(columns, "Transformation columns list must not be null.");
		if (columns.isEmpty()) {
			throw new IllegalArgumentException("Transformation columns list must not be empty.");
		}
		for (Column column : columns) {
			Objects.requireNonNull(column, "Transformation columns list must not contain null.");
		}
	}

	/**
	 * Creates a new transformer from columns of a table.
	 *
	 * @param columns
	 * 		columns from a table (i.e. not {@code null} and contains no {@code null})
	 */
	RowTransformer(Column[] columns) {
		if (columns.length == 0) {
			throw new IllegalArgumentException("Transformation columns array must not be empty.");
		}
		this.transformationColumns = columns;
	}

	/**
	 * Specifies the expected {@link Workload} per data point.
	 *
	 * @param workload
	 * 		the workload
	 * @return this transformer
	 */
	public RowTransformer workload(Workload workload) {
		this.workload = Objects.requireNonNull(workload, "Workload must not be null");
		return this;
	}

	/**
	 * Specifies a progress callback function. The progress is reported as single {@code double} value where zero and
	 * one indicate 0% and 100% progress respectively. The value {@link Double#NaN} is used to indicate indeterminate
	 * states.
	 *
	 * @param callback
	 * 		the progress callback
	 * @return this transformer
	 */
	public RowTransformer callback(DoubleConsumer callback) {
		this.callback = Objects.requireNonNull(callback, "Callback must not be null");
		return this;
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the numeric-readable transformation columns
	 * returning the result in a new real {@link NumericBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * numeric-readable, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public ColumnTask applyNumericToReal(ToDoubleFunction<NumericRow> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNNumericToNumeric(transformationColumns, operator, false),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric readable transformation columns returning the
	 * result in a new real {@link NumericBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public NumericBuffer applyNumericToReal(ToDoubleFunction<NumericRow> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToReal(operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the numeric-readable transformation columns
	 * returning the result in a new integer {@link NumericBuffer}. Depending on the input size and the specified
	 * workload per data-point, the computation might be performed in parallel. If any of the transformation columns is
	 * not numeric-readable, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyNumericToInteger(ToDoubleFunction<NumericRow> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNNumericToNumeric(transformationColumns, operator, true),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new integer {@link NumericBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public NumericBuffer applyNumericToInteger(ToDoubleFunction<NumericRow> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToInteger(operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the categorical transformation columns
	 * returning the result in a new real {@link NumericBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * categorical, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyCategoricalToReal(ToDoubleFunction<CategoricalRow> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNCategoricalToNumeric(transformationColumns, operator,
						false),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new real {@link NumericBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public NumericBuffer applyCategoricalToReal(ToDoubleFunction<CategoricalRow> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyCategoricalToReal(operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the categorical transformation columns
	 * returning the result in a new integer {@link NumericBuffer}. Depending on the input size and the specified
	 * workload per data-point, the computation might be performed in parallel. If any of the transformation columns is
	 * not categorical, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyCategoricalToInteger(ToDoubleFunction<CategoricalRow> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNCategoricalToNumeric(transformationColumns, operator, true),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new integer {@link NumericBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public NumericBuffer applyCategoricalToInteger(ToDoubleFunction<CategoricalRow> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyCategoricalToInteger(operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the object-readable transformation columns
	 * returning the result in a new real {@link NumericBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * object-readable or not readable as the given type, an {@link UnsupportedOperationException} or {@link
	 * IllegalArgumentException} will happen on execution of the task.
	 *
	 * @param type
	 * 		the type of the objects in the columns
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param <T>
	 * 		type of objects in the columns
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> ColumnTask applyObjectToReal(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNObjectToNumeric<>(transformationColumns, type, operator, false),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new real {@link NumericBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <T> NumericBuffer applyObjectToReal(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyObjectToReal(type, operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the object-readable transformation columns
	 * returning the result in a new integer {@link NumericBuffer}. Depending on the input size and the specified
	 * workload per data-point, the computation might be performed in parallel. If any of the transformation columns is
	 * not object-readable or not readable as the given type, an {@link UnsupportedOperationException} or {@link
	 * IllegalArgumentException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param <T>
	 * 		type of objects in the columns
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> ColumnTask applyObjectToInteger(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNObjectToNumeric<>(transformationColumns, type, operator, true),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new integer {@link NumericBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <T> NumericBuffer applyObjectToInteger(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator,
												  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyObjectToInteger(type, operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the transformation columns returning the
	 * result in a new real {@link NumericBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToReal}, {@link #applyCategoricalToReal} or {@link
	 * #applyObjectToReal} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyMixedToReal(ToDoubleFunction<MixedRow> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierMixedToNumeric(transformationColumns, operator, false),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new real
	 * {@link NumericBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToReal}, {@link #applyCategoricalToReal} or {@link
	 * #applyObjectToReal} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public NumericBuffer applyMixedToReal(ToDoubleFunction<MixedRow> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyMixedToReal(operator).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the transformation columns returning the
	 * result in a new integer {@link NumericBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToInteger}, {@link #applyCategoricalToInteger} or {@link
	 * #applyObjectToInteger} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyMixedToInteger(ToDoubleFunction<MixedRow> operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierMixedToNumeric(transformationColumns, operator, true),
						workload, callback).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new
	 * integer {@link NumericBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToInteger}, {@link #applyCategoricalToInteger} or {@link
	 * #applyObjectToInteger} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public NumericBuffer applyMixedToInteger(ToDoubleFunction<MixedRow> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyMixedToInteger(operator).run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link Int32CategoricalBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <R, T> Int32CategoricalBuffer<T> applyObjectToCategorical(Class<R> type, Function<ObjectRow<R>, T> operator,
																	 Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierNObjectToCategorical<>(transformationColumns, type, operator,
						IntegerFormats.Format.SIGNED_INT32), workload, callback).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link CategoricalBuffer}. The format of the target buffer is derived from given
	 * maxNumberOfValues parameter. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param type
	 * 		the type of the objects in the columns
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type or if more different values
	 * 		are	supplied than supported by the buffer format calculated from the maxNumberOfValues
	 */
	public <R, T> CategoricalBuffer<T> applyObjectToCategorical(Class<R> type,
																Function<ObjectRow<R>, T> operator,
																int maxNumberOfValues,
																Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierNObjectToCategorical<>(transformationColumns, type, operator, format),
				workload, callback).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new {@link Int32CategoricalBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <T> Int32CategoricalBuffer<T> applyCategoricalToCategorical(Function<CategoricalRow, T> operator,
																	   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierNCategoricalToCategorical<>(transformationColumns, operator,
						IntegerFormats.Format.SIGNED_INT32), workload, callback).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new {@link CategoricalBuffer}. The format of the target buffer is derived from given maxNumberOfValues
	 * parameter. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 * @throws IllegalArgumentException
	 * 		if more different values are supplied than supported by the buffer format calculated from the
	 * 		maxNumberOfValues
	 */
	public <T> CategoricalBuffer<T> applyCategoricalToCategorical(Function<CategoricalRow, T> operator,
																  int maxNumberOfValues,
																  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierNCategoricalToCategorical<>(transformationColumns, operator, format),
				workload, callback).create().run(context);
	}


	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new {@link Int32CategoricalBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public <T> Int32CategoricalBuffer<T> applyNumericToCategorical(Function<NumericRow, T> operator,
																   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierNNumericToCategorical<>(transformationColumns, operator,
						IntegerFormats.Format.SIGNED_INT32), workload, callback).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new {@link CategoricalBuffer}. The format of the target buffer is derived from given
	 * maxNumberOfValues parameter. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 * @throws IllegalArgumentException
	 * 		if more different values are supplied than supported by the buffer format calculated from the
	 * 		maxNumberOfValues
	 */
	public <T> CategoricalBuffer<T> applyNumericToCategorical(Function<NumericRow, T> operator,
															  int maxNumberOfValues,
															  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierNNumericToCategorical<>(transformationColumns, operator, format),
				workload, callback).create().run(context);
	}


	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * Int32CategoricalBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToCategorical}, {@link #applyCategoricalToCategorical} or
	 * {@link #applyObjectToCategorical} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> Int32CategoricalBuffer<T> applyMixedToCategorical(Function<MixedRow, T> operator,
																 Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierMixedToCategorical<>(transformationColumns, operator,
						IntegerFormats.Format.SIGNED_INT32), workload, callback).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * CategoricalBuffer}. The format of the target buffer is derived from given maxNumberOfValues parameter.
	 * Depending on the input size and the specified workload per data-point, the computation might be performed in
	 * parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToCategorical}, {@link #applyCategoricalToCategorical} or
	 * {@link #applyObjectToCategorical} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if more different values are supplied than supported by the buffer format calculated from the
	 * 		maxNumberOfValues
	 */
	public <T> CategoricalBuffer<T> applyMixedToCategorical(Function<MixedRow, T> operator,
															int maxNumberOfValues,
															Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierMixedToCategorical<>(transformationColumns, operator, format),
				workload, callback).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link ObjectBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <R, T> ObjectBuffer<T> applyObjectToObject(Class<R> type, Function<ObjectRow<R>, T> operator,
													  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNObjectToObject<>(transformationColumns, type, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link TimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param <R>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <R> TimeBuffer applyObjectToTime(Class<R> type, Function<ObjectRow<R>, LocalTime> operator,
											Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNObjectToTime<>(transformationColumns, type, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link NanosecondDateTimeBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param <R>
	 * 		type of objects in the columns
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <R> NanosecondDateTimeBuffer applyObjectToDateTime(Class<R> type,
															  Function<ObjectRow<R>, Instant> operator,
															  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNObjectToDateTime<>(transformationColumns, type, operator), workload, callback).create()
				.run(context);
	}


	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new {@link ObjectBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public <T> ObjectBuffer<T> applyCategoricalToObject(Function<CategoricalRow, T> operator,
														Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNCategoricalToObject<>(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a
	 * new {@link TimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public TimeBuffer applyCategoricalToTime(Function<CategoricalRow, LocalTime> operator,
											 Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNCategoricalToTime(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a
	 * new {@link NanosecondDateTimeBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public NanosecondDateTimeBuffer applyCategoricalToDateTime(Function<CategoricalRow, Instant> operator,
															   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNCategoricalToDateTime(transformationColumns, operator), workload, callback).create()
				.run(context);

	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new {@link ObjectBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public <T> ObjectBuffer<T> applyNumericToObject(Function<NumericRow, T> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNNumericToObject<>(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result
	 * in a new {@link TimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public TimeBuffer applyNumericToTime(Function<NumericRow, LocalTime> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNNumericToTime(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result
	 * in a new {@link NanosecondDateTimeBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link NumericRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public NanosecondDateTimeBuffer applyNumericToDateTime(Function<NumericRow, Instant> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNNumericToDateTime(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * ObjectBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToObject}, {@link #applyCategoricalToObject} or {@link
	 * #applyObjectToObject} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param context
	 * 		the execution context to use
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> ObjectBuffer<T> applyMixedToObject(Function<MixedRow, T> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierMixedToObject<>(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * TimeBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToTime}, {@link #applyCategoricalToTime} or {@link
	 * #applyObjectToTime} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public TimeBuffer applyMixedToTime(Function<MixedRow, LocalTime> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierMixedToTime(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * NanosecondDateTimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToDateTime}, {@link #applyCategoricalToDateTime} or {@link
	 * #applyObjectToDateTime} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link MixedRow})
	 * @param context
	 * 		the execution context to use
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public NanosecondDateTimeBuffer applyMixedToDateTime(Function<MixedRow, Instant> operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierMixedToDateTime(transformationColumns, operator), workload, callback).create()
				.run(context);
	}

	/**
	 * Creates a task that applies the given binary operator to the numeric-readable transformation column pair
	 * returning the result in a new real {@link NumericBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * numeric-readable, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each pair
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there are not exactly two transformation columns
	 */
	public ColumnTask applyNumericToReal(DoubleBinaryOperator operator) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		if (transformationColumns.length != 2) {
			throw new IllegalArgumentException("There must be exactly two transformation columns.");
		}
		return applyNumericToReal(row -> operator.applyAsDouble(row.get(0), row.get(1)));
	}


	/**
	 * Applies the given binary operator to the numeric-readable transformation column pair returning the result in a
	 * new {@link NumericBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each pair
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 * @throws IllegalArgumentException
	 * 		if there are not exactly two transformation columns
	 */
	public NumericBuffer applyNumericToReal(DoubleBinaryOperator operator, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToReal(operator).run(context);
	}


	/**
	 * Performs a mutable reduction operation on the rows of the numeric-readable transformation columns. A mutable
	 * reduction is one in which the reduced value is a mutable result container, such as an {@code ArrayList}, and
	 * elements are incorporated by updating the state of the result rather than by replacing the result. This produces
	 * a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     NumericRowReader reader = new NumericRowReader(transformationColumns);
	 *     while (reader.hasRemaining){
	 *     	   reader.move();
	 *         reducer.accept(result, reader);
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the given context and automatically parallelized depending on the input size
	 * and the specified workload per data-point.
	 *
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the	accumulator
	 * 		function
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T> T reduceNumeric(Supplier<T> supplier, BiConsumer<T, NumericRow> reducer, BiConsumer<T, T> combiner,
							   Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new ColumnsReducer<>(transformationColumns, supplier, reducer, combiner),
				workload, callback).create().run(context);
	}

	/**
	 * Performs a mutable reduction operation on the rows of the {@link Column.Category#CATEGORICAL} transformation
	 * columns. A mutable reduction is one in which the reduced value is a mutable result container, such as an {@code
	 * ArrayList}, and elements are incorporated by updating the state of the result rather than by replacing the
	 * result. This produces a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     CategoricalRowReader reader = new CategoricalRowReader(transformationColumns);
	 *     while (reader.hasRemaining){
	 *     	   reader.move();
	 *         reducer.accept(result, reader);
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the given context and automatically parallelized depending on the input size
	 * and the specified workload per data-point.
	 *
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the	accumulator
	 * 		function
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T> T reduceCategorical(Supplier<T> supplier, BiConsumer<T, CategoricalRow> reducer,
								   BiConsumer<T, T> combiner, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new CategoricalColumnsReducer<>(transformationColumns, supplier, reducer,
				combiner), workload, callback).create().run(context);
	}

	/**
	 * Performs a mutable reduction operation on the rows of the object-readable transformation columns. A mutable
	 * reduction is one in which the reduced value is a mutable result container, such as an {@code ArrayList}, and
	 * elements are incorporated by updating the state of the result rather than by replacing the result. This produces
	 * a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     ObjectRowReader<R> reader = new ObjectRowReader<>(transformationColumns, type);
	 *     while (reader.hasRemaining){
	 *     	   reader.move();
	 *         reducer.accept(result, reader);
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the given context and automatically parallelized depending on the input size
	 * and the specified workload per data-point.
	 *
	 * @param type
	 * 		the type of the objects in the columns
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the	accumulator
	 * 		function
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @param <R>
	 * 		type of objects in the columns
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @throws IllegalArgumentException
	 * 		if type is not a super type of the element type of all of the columns
	 * @throws UnsupportedOperationException
	 * 		if the one of the columns is not {@link Column.Capability#OBJECT_READABLE}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T, R> T reduceObjects(Class<R> type, Supplier<T> supplier, BiConsumer<T, ObjectRow<R>> reducer,
								  BiConsumer<T, T> combiner, Context context) {
		Objects.requireNonNull(type, "Type must not be null");
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(
				new ObjectColumnsReducer<>(transformationColumns, type, supplier, reducer, combiner), workload,
				callback).create().run(context);
	}

	/**
	 * Performs a mutable reduction operation on the rows of the transformation columns. A mutable reduction is one in
	 * which the reduced value is a mutable result container, such as an {@code ArrayList}, and elements are
	 * incorporated by updating the state of the result rather than by replacing the result. This produces a result
	 * equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     MixedRowReader reader = new MixedRowReader(transformationColumns);
	 *     while (reader.hasRemaining){
	 *     	   reader.move();
	 *         reducer.accept(result, reader);
	 *     }
	 *     return result;
	 * }</pre>
	 *
	 * but is not constrained to execute sequentially.
	 *
	 * <p>The reduction is executed via the given context and automatically parallelized depending on the input size
	 * and the specified workload per data-point.
	 *
	 * <p>This method uses a reducer working with a {@link MixedRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #reduceCategorical}, {@link #reduceNumeric} or {@link #reduceObjects}
	 * instead.
	 *
	 * @param supplier
	 * 		a function that creates a new result container. In case of a parallel execution, this function may be
	 * 		called multiple times and must return a new instance each time.
	 * @param reducer
	 * 		a stateless function for incorporating an additional element into a result
	 * @param combiner
	 * 		an associative, stateless function for combining two values, which must be compatible with the	accumulator
	 * 		function
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T> T reduceMixed(Supplier<T> supplier, BiConsumer<T, MixedRow> reducer,
							 BiConsumer<T, T> combiner, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new MixedColumnsReducer<>(transformationColumns, supplier, reducer,
				combiner), workload, callback).create().run(context);
	}

}