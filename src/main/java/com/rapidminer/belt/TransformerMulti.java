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
public final class TransformerMulti {

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

	private final Column[] transformationColumns;

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
	public TransformerMulti(List<Column> transformationColumns) {
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
	TransformerMulti(Column[] columns) {
		if (columns.length == 0) {
			throw new IllegalArgumentException("Transformation columns array must not be empty.");
		}
		this.transformationColumns = columns;
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the numeric-readable transformation columns
	 * returning the result in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * numeric-readable, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 */
	public ColumnTask applyNumericToReal(ToDoubleFunction<Row> operator, Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNumericToNumericMulti(transformationColumns, operator, false),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric readable transformation columns returning the
	 * result in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public ColumnBuffer applyNumericToReal(ToDoubleFunction<Row> operator, Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToReal(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the numeric-readable transformation columns
	 * returning the result in a new integer {@link ColumnBuffer}. Depending on the input size and the specified
	 * workload per data-point, the computation might be performed in parallel. If any of the transformation columns is
	 * not numeric-readable, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyNumericToInteger(ToDoubleFunction<Row> operator, Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierNumericToNumericMulti(transformationColumns, operator, true),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new integer {@link ColumnBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public ColumnBuffer applyNumericToInteger(ToDoubleFunction<Row> operator, Workload workload,
											  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToInteger(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the categorical transformation columns
	 * returning the result in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * categorical, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyCategoricalToReal(ToDoubleFunction<CategoricalRow> operator, Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierCategoricalToNumericMulti(transformationColumns, operator,
						false),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public ColumnBuffer applyCategoricalToReal(ToDoubleFunction<CategoricalRow> operator, Workload workload,
											   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyCategoricalToReal(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the categorical transformation columns
	 * returning the result in a new integer {@link ColumnBuffer}. Depending on the input size and the specified
	 * workload per data-point, the computation might be performed in parallel. If any of the transformation columns is
	 * not categorical, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyCategoricalToInteger(ToDoubleFunction<CategoricalRow> operator, Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierCategoricalToNumericMulti(transformationColumns, operator, true),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new integer {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public ColumnBuffer applyCategoricalToInteger(ToDoubleFunction<CategoricalRow> operator, Workload
			workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyCategoricalToInteger(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the object-readable transformation columns
	 * returning the result in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * object-readable or not readable as the given type, an {@link UnsupportedOperationException} or {@link
	 * IllegalArgumentException} will happen on execution of the task.
	 *
	 * @param type
	 * 		the type of the objects in the columns
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of objects in the columns
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> ColumnTask applyObjectToReal(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator,
											Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierObjectToNumericMulti<>(transformationColumns, type, operator, false),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <T> ColumnBuffer applyObjectToReal(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator,
											  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyObjectToReal(type, operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the object-readable transformation columns
	 * returning the result in a new integer {@link ColumnBuffer}. Depending on the input size and the specified
	 * workload per data-point, the computation might be performed in parallel. If any of the transformation columns is
	 * not object-readable or not readable as the given type, an {@link UnsupportedOperationException} or {@link
	 * IllegalArgumentException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of objects in the columns
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> ColumnTask applyObjectToInteger(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator,
											   Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierObjectToNumericMulti<>(transformationColumns, type, operator,
						true),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new integer {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <T> ColumnBuffer applyObjectToInteger(Class<T> type, ToDoubleFunction<ObjectRow<T>> operator,
												 Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyObjectToInteger(type, operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the transformation columns returning the
	 * result in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToReal}, {@link #applyCategoricalToReal} or {@link
	 * #applyObjectToReal} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyGeneralToReal(ToDoubleFunction<GeneralRow> operator, Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierGeneralToNumericMulti(transformationColumns, operator, false),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new real
	 * {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToReal}, {@link #applyCategoricalToReal} or {@link
	 * #applyObjectToReal} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnBuffer applyGeneralToReal(ToDoubleFunction<GeneralRow> operator, Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyGeneralToReal(operator, workload).run(context);
	}

	/**
	 * Creates a task that applies the given operator of arbitrary arity to the transformation columns returning the
	 * result in a new integer {@link ColumnBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToInteger}, {@link #applyCategoricalToInteger} or {@link
	 * #applyObjectToInteger} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnTask applyGeneralToInteger(ToDoubleFunction<GeneralRow> operator, Workload workload) {
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ColumnTask(
				new ParallelExecutor<>(new ApplierGeneralToNumericMulti(transformationColumns, operator, true),
						workload).create(), transformationColumns[0].size());
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new
	 * integer {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToInteger}, {@link #applyCategoricalToInteger} or {@link
	 * #applyObjectToInteger} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public ColumnBuffer applyGeneralToInteger(ToDoubleFunction<GeneralRow> operator, Workload workload,
											  Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyGeneralToInteger(operator, workload).run(context);
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
	 * @param workload
	 * 		the expected workload per data point
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
																	 Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierObjectToCategoricalMulti<>(transformationColumns, type, operator,
						IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link CategoricalColumnBuffer}. The format of the target buffer is derived from given
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
	 * @param workload
	 * 		the expected workload per data point
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
	public <R, T> CategoricalColumnBuffer<T> applyObjectToCategorical(Class<R> type,
																	  Function<ObjectRow<R>, T> operator,
																	  int maxNumberOfValues,
																	  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierObjectToCategoricalMulti<>(transformationColumns, type, operator, format),
				workload).create().run(context);
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
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 * @throws IllegalArgumentException
	 * 		if the objects in the transformation columns cannot be read as the given type
	 */
	public <T> Int32CategoricalBuffer<T> applyCategoricalToCategorical(Function<CategoricalRow, T> operator,
																	   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierCategoricalToCategoricalMulti<>(transformationColumns, operator,
						IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new {@link CategoricalColumnBuffer}. The format of the target buffer is derived from given maxNumberOfValues
	 * parameter. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <T> CategoricalColumnBuffer<T> applyCategoricalToCategorical(Function<CategoricalRow, T> operator,
																		int maxNumberOfValues,
																		Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierCategoricalToCategoricalMulti<>(transformationColumns, operator, format),
				workload).create().run(context);
	}


	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new {@link Int32CategoricalBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public <T> Int32CategoricalBuffer<T> applyNumericToCategorical(Function<Row, T> operator,
																   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierNumericToCategoricalMulti<>(transformationColumns, operator,
						IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new {@link CategoricalColumnBuffer}. The format of the target buffer is derived from given
	 * maxNumberOfValues parameter. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <T> CategoricalColumnBuffer<T> applyNumericToCategorical(Function<Row, T> operator,
																	int maxNumberOfValues,
																	Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierNumericToCategoricalMulti<>(transformationColumns, operator, format),
				workload).create().run(context);
	}


	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * Int32CategoricalBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToCategorical}, {@link #applyCategoricalToCategorical} or
	 * {@link #applyObjectToCategorical} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> Int32CategoricalBuffer<T> applyGeneralToCategorical(Function<GeneralRow, T> operator,
																   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return (Int32CategoricalBuffer<T>) new ParallelExecutor<>(
				new ApplierGeneralToCategoricalMulti<>(transformationColumns, operator,
						IntegerFormats.Format.SIGNED_INT32), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * CategoricalColumnBuffer}. The format of the target buffer is derived from given maxNumberOfValues parameter.
	 * Depending on the input size and the specified workload per data-point, the computation might be performed in
	 * parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToCategorical}, {@link #applyCategoricalToCategorical} or
	 * {@link #applyObjectToCategorical} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param maxNumberOfValues
	 * 		the maximal number of different values generated by the operator. Decides the format of the buffer.
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if more different values are supplied than supported by the buffer format calculated from the
	 * 		maxNumberOfValues
	 */
	public <T> CategoricalColumnBuffer<T> applyGeneralToCategorical(Function<GeneralRow, T> operator,
																	int maxNumberOfValues,
																	Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		IntegerFormats.Format format = IntegerFormats.Format.findMinimal(maxNumberOfValues);
		return new ParallelExecutor<>(
				new ApplierGeneralToCategoricalMulti<>(transformationColumns, operator, format),
				workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link FreeColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <R, T> FreeColumnBuffer<T> applyObjectToFree(Class<R> type, Function<ObjectRow<R>, T> operator,
														Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierObjectToFreeMulti<>(transformationColumns, type, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <R> TimeColumnBuffer applyObjectToTime(Class<R> type, Function<ObjectRow<R>, LocalTime> operator,
												  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierObjectToTimeMulti<>(transformationColumns, type, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the object-readable transformation columns returning the result
	 * in a new {@link HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link ObjectRow})
	 * @param type
	 * 		the type of the objects in the columns
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
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
	public <R> HighPrecisionDateTimeBuffer applyObjectToDateTime(Class<R> type,
																 Function<ObjectRow<R>, Instant> operator,
																 Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierObjectToDateTimeMulti<>(transformationColumns, type, operator), workload).create()
				.run(context);
	}


	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a new {@link FreeColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public <T> FreeColumnBuffer<T> applyCategoricalToFree(Function<CategoricalRow, T> operator,
														  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierCategoricalToFreeMulti<>(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a
	 * new {@link TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public TimeColumnBuffer applyCategoricalToTime(Function<CategoricalRow, LocalTime> operator,
												   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierCategoricalToTimeMulti(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the categorical transformation columns returning the result
	 * in a
	 * new {@link HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link CategoricalRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 */
	public HighPrecisionDateTimeBuffer applyCategoricalToDateTime(Function<CategoricalRow, Instant> operator,
																  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierCategoricalToDateTimeMulti(transformationColumns, operator), workload).create().run(context);

	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result in a new {@link FreeColumnBuffer}. Depending on the input size and the specified workload per data-point,
	 * the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public <T> FreeColumnBuffer<T> applyNumericToFree(Function<Row, T> operator,
													  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNumericToFreeMulti<>(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result
	 * in a new {@link TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public TimeColumnBuffer applyNumericToTime(Function<Row, LocalTime> operator,
											   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNumericToTimeMulti(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the numeric-readable transformation columns returning the
	 * result
	 * in a new {@link HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link Row})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public HighPrecisionDateTimeBuffer applyNumericToDateTime(Function<Row, Instant> operator,
															  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierNumericToDateTimeMulti(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * FreeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToFree}, {@link #applyCategoricalToFree} or {@link
	 * #applyObjectToFree} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @param <T>
	 * 		type of the result
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public <T> FreeColumnBuffer<T> applyGeneralToFree(Function<GeneralRow, T> operator,
													  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierGeneralToFreeMulti<>(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * TimeColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation might
	 * be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToTime}, {@link #applyCategoricalToTime} or {@link
	 * #applyObjectToTime} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public TimeColumnBuffer applyGeneralToTime(Function<GeneralRow, LocalTime> operator,
											   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierGeneralToTimeMulti(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Applies the given operator of arbitrary arity to the transformation columns returning the result in a new {@link
	 * HighPrecisionDateTimeBuffer}. Depending on the input size and the specified workload per data-point, the
	 * computation might be performed in parallel.
	 *
	 * <p>This method uses an operator starting from a {@link GeneralRow} which allows to read the row in different
	 * ways. This is not as performant as reading the row only in one specific way. So whenever possible it is
	 * recommended to use the methods {@link #applyNumericToDateTime}, {@link #applyCategoricalToDateTime} or {@link
	 * #applyObjectToDateTime} instead.
	 *
	 * @param operator
	 * 		the operator to apply to each tuple (see {@link GeneralRow})
	 * @param context
	 * 		the execution context to use
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the mapping
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 */
	public HighPrecisionDateTimeBuffer applyGeneralToDateTime(Function<GeneralRow, Instant> operator,
															  Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		return new ParallelExecutor<>(
				new ApplierGeneralToDateTimeMulti(transformationColumns, operator), workload).create().run(context);
	}

	/**
	 * Creates a task that applies the given binary operator to the numeric-readable transformation column pair
	 * returning the result in a new real {@link ColumnBuffer}. Depending on the input size and the specified workload
	 * per data-point, the computation might be performed in parallel. If any of the transformation columns is not
	 * numeric-readable, an {@link UnsupportedOperationException} will happen on execution of the task.
	 *
	 * @param operator
	 * 		the operator to apply to each pair
	 * @param workload
	 * 		the expected workload per data point
	 * @return a task that applies the given operator
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there are not exactly two transformation columns
	 */
	public ColumnTask applyNumericToReal(DoubleBinaryOperator operator, Workload workload) {
		Objects.requireNonNull(operator, MESSAGE_OPERATOR_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		if (transformationColumns.length != 2) {
			throw new IllegalArgumentException("There must be exactly two transformation columns.");
		}
		return applyNumericToReal(row -> operator.applyAsDouble(row.get(0), row.get(1)),
				workload);
	}


	/**
	 * Applies the given binary operator to the numeric-readable transformation column pair returning the result in a
	 * new {@link ColumnBuffer}. Depending on the input size and the specified workload per data-point, the computation
	 * might be performed in parallel.
	 *
	 * @param operator
	 * 		the operator to apply to each pair
	 * @param workload
	 * 		the expected workload per data point
	 * @return a buffer containing the result of the operation
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 * @throws IllegalArgumentException
	 * 		if there are not exactly two transformation columns
	 */
	public ColumnBuffer applyNumericToReal(DoubleBinaryOperator operator, Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return applyNumericToReal(operator, workload).run(context);
	}


	/**
	 * Performs a mutable reduction operation on the rows of the numeric-readable transformation columns. A mutable
	 * reduction is one in which the reduced value is a mutable result container, such as an {@code ArrayList}, and
	 * elements are incorporated by updating the state of the result rather than by replacing the result. This produces
	 * a result equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     RowReader reader = new RowReader(transformationColumns);
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
	 * 		if any of the transformation columns is not {@link Column.Capability#NUMERIC_READABLE}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T> T reduceNumeric(Supplier<T> supplier, BiConsumer<T, Row> reducer, BiConsumer<T, T> combiner,
							   Workload workload, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new ColumnsReducer<>(transformationColumns, supplier, reducer, combiner),
				workload).create().run(context);
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
	 * 		if any of the transformation columns is not {@link Column.Category#CATEGORICAL}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T> T reduceCategorical(Supplier<T> supplier, BiConsumer<T, CategoricalRow> reducer,
								   BiConsumer<T, T> combiner, Workload workload, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new CategoricalColumnsReducer<>(transformationColumns, supplier, reducer,
				combiner), workload).create().run(context);
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
	 * @param workload
	 * 		the expected workload per data point
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
								  BiConsumer<T, T> combiner, Workload workload, Context context) {
		Objects.requireNonNull(type, "Type must not be null");
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(
				new ObjectColumnsReducer<>(transformationColumns, type, supplier, reducer, combiner), workload)
				.create().run(context);
	}

	/**
	 * Performs a mutable reduction operation on the rows of the transformation columns. A mutable reduction is one in
	 * which the reduced value is a mutable result container, such as an {@code ArrayList}, and elements are
	 * incorporated by updating the state of the result rather than by replacing the result. This produces a result
	 * equivalent to:
	 *
	 * <pre>{@code
	 *     T result = supplier.get();
	 *     GeneralRowReader reader = new GeneralRowReader(transformationColumns);
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
	 * <p>This method uses a reducer working with a {@link GeneralRow} which allows to read the row in different
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
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @param <T>
	 * 		type of the result
	 * @return the result of the reduction
	 * @throws NullPointerException
	 * 		if any of the arguments is or the supplier returns {@code null}
	 * @see java.util.stream.Stream#collect(Supplier, BiConsumer, BiConsumer)
	 */
	public <T> T reduceGeneral(Supplier<T> supplier, BiConsumer<T, GeneralRow> reducer,
							   BiConsumer<T, T> combiner, Workload workload, Context context) {
		Objects.requireNonNull(supplier, MESSAGE_SUPPLIER_NULL);
		Objects.requireNonNull(reducer, MESSAGE_REDUCER_NULL);
		Objects.requireNonNull(combiner, MESSAGE_COMBINER_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Objects.requireNonNull(workload, MESSAGE_WORKLOAD_NULL);
		return new ParallelExecutor<>(new GeneralColumnsReducer<>(transformationColumns, supplier, reducer,
				combiner), workload).create().run(context);
	}

}