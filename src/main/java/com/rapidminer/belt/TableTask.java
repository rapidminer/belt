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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.SimpleTaskRunner;
import com.rapidminer.belt.util.Task;
import com.rapidminer.belt.util.TaskAbortedException;


/**
 * Task to compute a new {@link Table}. Requires an active {@link Context} to run.
 *
 * @author Michael Knopf
 */
public class TableTask {

	/**
	 * Message for cancellation of tasks by the invoker.
	 */
	private static final String MESSAGE_ABORTED_BY_INVOKER = "Task aborted by invoker";

	/**
	 * Message template for wrong column index
	 */
	private static final String INVALID_COLUMN_MESSAGE = "Column index: %d, columns: %d";

	private final Task<Table> task;
	private final int width;
	private final int height;
	private final String[] labels;
	private final Map<String, List<ColumnMetaData>> metaDataMap;

	TableTask(String[] labels, Map<String, List<ColumnMetaData>> metaDataMap, int height, Task<Table> task) {
		this.labels = labels;
		this.metaDataMap = metaDataMap;
		this.width = labels.length;
		this.height = height;
		this.task = task;
	}

	TableTask(String[] labels, int height, Task<Table> task) {
		this(labels, Collections.emptyMap(), height, task);
	}

	/**
	 * Runs the table task with the given execution {@link Context}. This operation blocks until the computation of the
	 * table has finished and returns the final table.
	 *
	 * @param ctx
	 * 		the execution context
	 * @return the final table
	 * @throws TaskAbortedException
	 * 		if the computation was aborted
	 */
	public Table run(Context ctx) {
		return new SimpleTaskRunner<>(task).run(ctx);
	}

	/**
	 * Returns the width of the table to be computed.
	 *
	 * @return the width of the table
	 */
	public int width() {
		return width;
	}

	/**
	 * Returns the height of the table to be computed.
	 *
	 * @return the height of the table
	 */
	public int height() {
		return height;
	}

	/**
	 * @return the column labels as unmodifiable list
	 */
	public List<String> labels() {
		return Collections.unmodifiableList(Arrays.asList(labels));
	}

	/**
	 * Creates a new task that creates a table consisting of the given columns. The column index array must contain
	 * valid indices only (i.e., values in the range {@code [0, width())}) and must not contain duplicates.
	 *
	 * @param columns
	 * 		the column indices
	 * @return the table task
	 * @throws NullPointerException
	 * 		if the index array is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the index array contains invalid indices
	 * @throws IllegalArgumentException
	 * 		if the index array contains duplicates
	 */
	public TableTask columns(int[] columns) {
		Objects.requireNonNull(columns, "Column index array must not be null");
		String[] newLabels = new String[columns.length];
		for (int i = 0; i < columns.length; i++) {
			int index = columns[i];
			if (index < 0 || index >= this.labels.length) {
				throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, index, this.labels.length));
			}
			newLabels[i] = labels[columns[i]];
		}
		Map<String, List<ColumnMetaData>> newMetaData = new HashMap<>();
		for (String label: labels) {
			List<ColumnMetaData> list = metaDataMap.get(label);
			if (list != null) {
				newMetaData.put(label, list);
			}
		}
		return new TableTask(newLabels, newMetaData, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.columns(columns);
		}));
	}

	/**
	 * Creates a new task that creates a table consisting of the given columns. The column label list must contain valid
	 * labels only and must not contain duplicates.
	 *
	 * @param columns
	 * 		the column labels
	 * @return the new table
	 * @throws NullPointerException
	 * 		if the given label list is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the label list contains duplicates of is empty
	 */
	public TableTask columns(List<String> columns) {
		Objects.requireNonNull(columns, "Column list must not be null");
		Map<String, List<ColumnMetaData>> newMetaData = new HashMap<>();
		for (String label: columns) {
			List<ColumnMetaData> list = metaDataMap.get(label);
			if (list != null) {
				newMetaData.put(label, list);
			}
		}
		return new TableTask(columns.toArray(new String[0]), newMetaData, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.columns(columns);
		}));
	}

	/**
	 * Creates a new table task to reorder the rows according to the given row set. The row set must contain valid
	 * indices only (i.e., values in the range {@code [0, size())}), but is otherwise without restrictions. In
	 * particular, sub- and supersets, as well as duplicate indices are supported.
	 *
	 * @param rows
	 * 		the rows to select
	 * @return the table task
	 * @throws IndexOutOfBoundsException
	 * 		if the rows contain invalid indices
	 * @throws NullPointerException
	 * 		if the row index array is {@code null}
	 */
	public TableTask rows(int[] rows) {
		Objects.requireNonNull(rows, "Row index array must not be null");
		return new TableTask(this.labels, metaDataMap, rows.length, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.rows(rows).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to select the rows from the given range {@code [from, to)}.
	 *
	 * @param from
	 * 		the start of the range (inclusive)
	 * @param to
	 * 		the end of the range (exclusive)
	 * @return the table task
	 * @throws IndexOutOfBoundsException
	 * 		if start is not a valid row index
	 * @throws IllegalArgumentException
	 * 		if the start is bigger than the end or the end bigger than the height
	 */
	public TableTask rows(int from, int to) {
		if (to < from) {
			throw new IllegalArgumentException("Start row " + from + " must not be bigger than end row " + to);
		}
		return new TableTask(this.labels, metaDataMap,to - from, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.rows(from, to).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to sort by the given column using the given sorting order.
	 *
	 * @param column
	 * 		the column to sort by
	 * @param order
	 * 		the sorting order
	 * @return the task to sort the table
	 * @throws NullPointerException
	 * 		if the order is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index is invalid
	 */
	public TableTask sort(int column, Order order) {
		return new TableTask(this.labels, metaDataMap, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.sort(column, order).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to sort the table by the given columns using the given sorting order. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param order
	 * 		the sorting order
	 * @return the task so sort the table
	 * @throws NullPointerException
	 * 		if the order or the column index array is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index array contains an invalid index
	 */
	public TableTask sort(int[] columns, Order order) {
		return new TableTask(this.labels, metaDataMap, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.sort(columns, order).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to sort the table by the given columns using the given sorting orders. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * <p>This method assumes that a sorting order is specified for each of the given column indices, i.e., that the
	 * column index array and the order list are of the same size.
	 *
	 * @param columns
	 * 		the column to sort
	 * @param orders
	 * 		the sorting orders
	 * @return the sorted table
	 * @throws NullPointerException
	 * 		if the orders or the column index array is {@code null} or the orders contain {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index array contains an invalid index
	 * @throws IllegalArgumentException
	 * 		if the sizes of the orders and the column index array differ
	 */
	public TableTask sort(int[] columns, List<Order> orders) {
		return new TableTask(this.labels, metaDataMap, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.sort(columns, orders).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to sort the table by the given column using the given sorting order.
	 *
	 * @param label
	 * 		the column to sort by
	 * @param order
	 * 		the sorting order
	 * @return the task so sort the table
	 * @throws NullPointerException
	 * 		if the label or order is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the label is invalid
	 */
	public TableTask sort(String label, Order order) {
		return new TableTask(this.labels, metaDataMap, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.sort(label, order).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to sort the table by the given columns using the given sorting order. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param order
	 * 		the sorting order
	 * @return the task so sort the table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the label list contains an invalid label or is empty
	 */
	public TableTask sort(List<String> columns, Order order) {
		return new TableTask(this.labels, metaDataMap, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.sort(columns, order).task.call(ctx, sentinel);
		}));
	}

	/**
	 * Creates a new table task to sort the table by the given columns using the given sorting orders. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * <p>This method assumes that a sorting order is specified for each of the given column labels, i.e., that the
	 * label list and the order list are of the same size.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param orders
	 * 		the sorting orders
	 * @return the sorted table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column labels contain an invalid label, the label list is empty,  or the label and sorting lists differ
	 * 		in size
	 */
	public TableTask sort(List<String> columns, List<Order> orders) {
		return new TableTask(this.labels, metaDataMap, this.height, ((ctx, sentinel) -> {
			Table table = this.task.call(ctx, sentinel);
			if (!ctx.isActive() || !sentinel.get()) {
				throw new TaskAbortedException(MESSAGE_ABORTED_BY_INVOKER);
			}
			return table.sort(columns, orders).task.call(ctx, sentinel);
		}));
	}

	/**
	 * @return the column labels of the table
	 */
	String[] labelArray() {
		return labels;
	}

	/**
	 * @return the column meta data of the table
	 */
	Map<String, List<ColumnMetaData>> metaDataMap() {
		return metaDataMap;
	}

	/**
	 * Returns the wrapped task.
	 *
	 * @return the wrapped task
	 */
	Task<Table> getTask() {
		return task;
	}

	@Override
	public String toString() {
		return "Table task (" + width + "x" + height + ")\n" + PrettyPrinter.printLabels(labels);
	}

}
