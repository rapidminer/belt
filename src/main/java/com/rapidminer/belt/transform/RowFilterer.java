/**
 * This file is part of the RapidMiner Belt project. Copyright (C) 2017-2019 RapidMiner GmbH
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

import java.util.List;
import java.util.Objects;
import java.util.function.DoubleConsumer;
import java.util.function.Predicate;

import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.Workload;
import com.rapidminer.belt.reader.CategoricalRow;
import com.rapidminer.belt.reader.MixedRow;
import com.rapidminer.belt.reader.NumericRow;
import com.rapidminer.belt.reader.ObjectRow;


/**
 * Supplies filter methods starting from the given {@link #filterColumns}. Depending on how the filter columns should
 * be read (numeric, categorical or objects) the right method (e.g. filterNumeric) must be selected.
 *
 * @author Gisa Meier
 */
public final class RowFilterer {

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
	private static final DoubleConsumer NOOP_CALLBACK = i -> {
	};

	private final List<Column> filterColumns;

	private Workload workload = Workload.DEFAULT;

	/**
	 * Creates a new starting point for a filter based on the given columns
	 *
	 * @param filterColumns
	 * 		the columns to start the filtering from, must be all of the same length
	 * @param validate
	 * 		whether the input columns require validation
	 * @throws NullPointerException
	 * 		if the list is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the list is empty
	 */
	public RowFilterer(List<Column> filterColumns, boolean validate) {
		if (validate) {
			checkColumns(filterColumns);
		}
		this.filterColumns = filterColumns;
	}

	/**
	 * Creates a new starting point for a filter based on the given columns
	 *
	 * @param filterColumns
	 * 		the columns to start the filtering from, must be all of the same length
	 * @throws NullPointerException
	 * 		if the list is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the list is empty
	 */
	public RowFilterer(List<Column> filterColumns) {
		// Public method, always validate the input columns!
		this(filterColumns, true);
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
	 * Specifies the expected {@link Workload} per data point.
	 *
	 * @param workload
	 * 		the workload
	 * @return this transform
	 * @throws NullPointerException
	 * 		if the workload is {@code null}
	 */
	public RowFilterer workload(Workload workload) {
		this.workload = Objects.requireNonNull(workload, "Workload must not be null");
		return this;
	}


	/**
	 * Filters the numeric-readable filter columns with respect to the predicate returning as result the accepted rows
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
	 * 		if the filter columns are not all {@link Column.Capability#NUMERIC_READABLE}
	 */
	public int[] filterNumeric(Predicate<NumericRow> predicate, Context context) {
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new NumericColumnsFilterer(filterColumns, predicate), workload, NOOP_CALLBACK)
				.execute(context);
	}

	/**
	 * Filters the categorical filter columns with respect to the predicate returning as result the accepted rows
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
	 * 		if the filter columns are not all {@link Column.Category#CATEGORICAL}
	 */
	public int[] filterCategorical(Predicate<CategoricalRow> predicate, Context context) {
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new CategoricalColumnsFilterer(filterColumns, predicate), workload,
				NOOP_CALLBACK).execute(context);
	}

	/**
	 * Filters the object-readable filter columns with respect to the predicate returning as result the accepted rows
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
	 * 		if the filter columns are not all {@link Column.Capability#OBJECT_READABLE}
	 * @throws IllegalArgumentException
	 * 		if not all columns are compatible with the given type
	 */
	public <T> int[] filterObjects(Class<T> type, Predicate<ObjectRow<T>> predicate, Context context) {
		Objects.requireNonNull(type, "Type must not be null");
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new ObjectColumnsFilterer<>(filterColumns, type, predicate), workload,
				NOOP_CALLBACK).execute(context);
	}

	/**
	 * Filters the filter columns with respect to the predicate returning as result the accepted rows
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
	 */
	public int[] filterMixed(Predicate<MixedRow> predicate, Context context) {
		Objects.requireNonNull(predicate, MESSAGE_FILTER_FUNCTION_NULL);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return new ParallelExecutor<>(new MixedColumnsFilterer(filterColumns, predicate), workload,
				NOOP_CALLBACK).execute(context);
	}
}