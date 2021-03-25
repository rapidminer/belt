/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2021 RapidMiner GmbH
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

package com.rapidminer.belt.transform;

import java.util.Objects;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.Workload;


/**
 * Supplies filter methods starting from the given {@link #filterColumn}. Depending on how the filter column should
 * be read (numeric, categorical or objects) the right method (e.g. filterNumericTo) must be selected.
 *
 * @author Gisa Meier
 */
public final class Filterer {

	/**
	 * Message for {@code null} filter function.
	 */
	private static final String MESSAGE_FILTER_FUNCTION_NULL = "Filter function must not be null";

	/**
	 * Message for {@code null} contexts
	 */
	private static final String MESSAGE_CONTEXT_NULL = "Context must not be null";

	/**
	 * Default progress callback which does nothing at all.
	 */
	private static final DoubleConsumer NOOP_CALLBACK = i -> {};

	private final Column filterColumn;

	private Workload workload = Workload.DEFAULT;

	/**
	 * Creates a new transform for transformations of the given column.
	 *
	 * @param filterColumn
	 * 		the column to transform to something
	 * @throws NullPointerException
	 * 		if the transformation column is {@code null}
	 */
	public Filterer(Column filterColumn) {
		this.filterColumn =
				Objects.requireNonNull(filterColumn, "Filter column must not be null");
	}

	/**
	 * Specifies the expected {@link Workload} per data point.
	 *
	 * @param workload
	 * 		the workload
	 * @return this transform
	 * @throws NullPointerException
	 * 		if the workload is {@code null}
	 */
	public Filterer workload(Workload workload) {
		this.workload = Objects.requireNonNull(workload, "Workload must not be null");
		return this;
	}

	/**
	 * Filters the numeric-readable filter column with respect to the predicate returning as result the accepted rows
	 * indices. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param predicate
	 * 		the predicate to apply
	 * @param context
	 * 		the execution context to use
	 * @return an array containing the accepted rows
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the filter column is not {@link Column.Capability#NUMERIC_READABLE}
	 */
	public int[] filterNumeric(DoublePredicate predicate, Context context) {
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new NumericColumnFilterer(filterColumn, predicate), workload, NOOP_CALLBACK)
				.execute(context);
	}

	/**
	 * Filters the categorical filter column with respect to the predicate returning as result the accepted rows
	 * rows indices. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param predicate
	 * 		the predicate to apply
	 * @param context
	 * 		the execution context to use
	 * @return an array containing the accepted rows
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the filter column is not {@link Column.Category#CATEGORICAL}
	 */
	public int[] filterCategorical(IntPredicate predicate, Context context) {
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new CategoricalColumnFilterer(filterColumn, predicate), workload,
				NOOP_CALLBACK).execute(context);
	}

	/**
	 * Filters the object-readable filter column with respect to the predicate returning as result the accepted rows
	 * indices. Depending on the input size and the specified workload per data-point, the computation might be
	 * performed in parallel.
	 *
	 * @param type
	 * 		the type as which the columns should be read
	 * @param predicate
	 * 		the predicate to apply
	 * @param context
	 * 		the execution context to use
	 * @return an array containing the accepted rows
	 * @throws NullPointerException
	 * 		if any of the parameters is {@code null}
	 * @throws UnsupportedOperationException
	 * 		if the filter column is not {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if the type is not compatible with the filter column
	 */
	public <T> int[] filterObject(Class<T> type, Predicate<T> predicate, Context context) {
		Objects.requireNonNull(type, "Type must not be null");
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new ObjectColumnFilterer<>(filterColumn, type, predicate), workload,
				NOOP_CALLBACK).execute(context);
	}

}