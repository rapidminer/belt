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


import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.DoublePredicate;
import java.util.function.IntPredicate;
import java.util.function.Predicate;

import com.rapidminer.belt.Column.TypeId;
import com.rapidminer.belt.function.DoubleBinaryPredicate;
import com.rapidminer.belt.function.IntBinaryPredicate;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.TaskAbortedException;


/**
 * Immutable table using a column-oriented data layout. Values can be read either column-wise view {@link ColumnReader}
 * (recommended) or row-wise via {@link RowReader}.
 *
 * @author Michael Knopf
 */
public final class Table {

	/** Message for {@code null} column indices */
	private static final String MESSAGE_COLUMN_INDICES_NULL = "Column index array must not be null";

	/** Message template for wrong column index */
	private static final String INVALID_COLUMN_MESSAGE = "Column index: %d, columns: %d";

	/** Message for {@code null} contexts */
	private static final String MESSAGE_CONTEXT_NULL = "Context must not be null";

	/**
	 * Message for aborted table tasks.
	 */
	private static final String TASK_ABORTED_BY_INVOKER = "Task aborted by invoker";

	private final Column[] columns;

	private final int width;
	private final int height;

	private final String[] labels;
	private final Map<String, Integer> labelMap;
	private final Map<String, List<ColumnMetaData>> metaDataMap;

	/**
	 * Creates a new table with the given srcColumns.
	 *
	 * @param srcColumns
	 * 		the columns for the table
	 * @param srcLabels
	 * 		the column labels for the table
	 * @param srcMeta
	 * 		the column meta data for the table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column and label arrays are of different size or any of the labels is invalid
	 */
	Table(Column[] srcColumns, String[] srcLabels, Map<String, List<ColumnMetaData>> srcMeta) {
		columns = Objects.requireNonNull(srcColumns, "Column array must not be null");
		labels = requireValidLabels(srcLabels, srcColumns.length);
		labelMap = requireValidLabelMapping(srcLabels);
		if (columns.length > 0) {
			Objects.requireNonNull(columns[0], "Columns must not be null");
			int heightOfFirstColumn = srcColumns[0].size();
			for (Column column : columns) {
				Objects.requireNonNull(column, "Columns must not be null");
				if (column.size() != heightOfFirstColumn) {
					throw new IllegalArgumentException("Columns must be of equal size");
				}
			}
			width = columns.length;
			height = heightOfFirstColumn;
		} else {
			width = 0;
			height = 0;
		}
		if (srcMeta != null) {
			for (String label: srcMeta.keySet()) {
				if (!labelMap.containsKey(label)) {
					throw new IllegalArgumentException("Unknown meta data label: " + label);
				}
			}
			this.metaDataMap = srcMeta;
		} else {
			this.metaDataMap = Collections.emptyMap();
		}
	}

	/**
	 * Creates a new table with the given srcColumns.
	 *
	 * @param srcColumns
	 * 		the columns for the table
	 * @param srcLabels
	 * 		the column labels for the table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column and label arrays are of different size or any of the labels is invalid
	 */
	Table(Column[] srcColumns, String[] srcLabels) {
		this(srcColumns, srcLabels, null);
	}

	/**
	 * @return the width of the table (number of columns)
	 */
	public int width() {
		return width;
	}

	/**
	 * @return the height of the table (number of rows)
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
	 * Looks up the label of the column with the given index.
	 *
	 * @param index
	 * 		the column index
	 * @return the column label
	 * @throws IndexOutOfBoundsException
	 * 		if the given index is out of bounds
	 */
	public String label(int index) {
		if (index < 0 || index >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, index, width));
		}
		return labels[index];
	}

	/**
	 * Checks whether the table contains a column using the given label.
	 *
	 * @param label
	 * 		the label to check
	 * @return {@code true} iff the table contains a column with the given label
	 */
	public boolean contains(String label) {
		return labelMap.containsKey(label);
	}

	/**
	 * Returns the index of the column with the given label.
	 *
	 * @param label
	 * 		the label of the column to look up
	 * @return the index of the column with the given label
	 * @throws NullPointerException
	 * 		if the label is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @see #contains(String)
	 */
	public int index(String label) {
		requireExistingLabel(label);
		return labelMap.get(label);
	}

	/**
	 * Returns the column specified by the index.
	 *
	 * @param index
	 * 		the index of the column
	 * @return the column with the given index
	 * @throws IndexOutOfBoundsException
	 * 		if the given index is out of bounds
	 */
	public Column column(int index){
		if (index < 0 || index >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, index, width));
		}
		return columns[index];
	}


	/**
	 * Returns the column column specified by the label.
	 *
	 * @param label
	 * 		the label of the column to look up
	 * @return the column with the given label
	 * @throws NullPointerException
	 * 		if the label is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public Column column(String label){
		requireExistingLabel(label);
		return columns[labelMap.get(label)];
	}

	/**
	 * Creates a new table consisting of the given columns. The column index array must contain valid indices only
	 * (i.e., values in the range {@code [0, width())}) and must not contain duplicates.
	 *
	 * @param columns
	 * 		the column indices
	 * @return the new table
	 * @throws NullPointerException
	 * 		if the index array is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the index array contains invalid indices
	 * @throws IllegalArgumentException
	 * 		if the index array contains duplicates
	 */
	public Table columns(int[] columns) {
		Objects.requireNonNull(columns, "Column array must not be null");
		Column[] newColumns = new Column[columns.length];
		String[] newLabels = new String[columns.length];
		Map<String, List<ColumnMetaData>> labelMetaMap = new HashMap<>();
		int position = 0;
		for (int index : columns) {
			if (index < 0 || index >= this.columns.length) {
				throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, index, this.columns.length));
			}
			newColumns[position] = this.columns[index];
			String label = labels[index];
			newLabels[position] = label;
			List<ColumnMetaData> metaList = metaDataMap.get(label);
			if (metaList != null) {
				labelMetaMap.put(label, metaList);
			}
			position++;
		}
		return new Table(newColumns, newLabels, labelMetaMap);
	}

	/**
	 * Creates a new table consisting of the given columns. The column label list must contain valid labels only and
	 * must not contain duplicates.
	 *
	 * @param columns
	 * 		the column labels
	 * @return the new table
	 * @throws NullPointerException
	 * 		if the given label list is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the label list contains duplicates of is empty
	 */
	public Table columns(List<String> columns) {
		requireExistingLabels(columns);
		return columns(lookupLabels(columns));
	}

	/**
	 * Creates a new table with rows reordered according to the given row set using the given execution context. If the
	 * mapping contains invalid indices (i.e., values outside of the range {@code [0, size())}), the new table will
	 * contain missing value at that place. In particular, sub- and supersets, as well as duplicate indices are
	 * supported.
	 *
	 * @param rows
	 * 		the rows to select
	 * @param ctx
	 * 		the execution context to use
	 * @return the new table
	 * @throws NullPointerException
	 * 		if the context or row index array is {@code null}
	 */
	public Table rows(int[] rows, Context ctx) {
		Objects.requireNonNull(ctx, MESSAGE_CONTEXT_NULL);
		return rows(rows).run(ctx);
	}

	/**
	 * Creates a new table task to reorder the rows according to the given row set. If the mapping contains invalid
	 * indices (i.e., values outside of the range {@code [0, size())}), the new table will contain missing value at that
	 * place. In particular, sub- and supersets, as well as duplicate indices are supported.
	 *
	 * @param rows
	 * 		the rows to select
	 * @return the table task
	 * @throws NullPointerException
	 * 		if the row index array is {@code null}
	 */
	public TableTask rows(int[] rows) {
		Objects.requireNonNull(rows, "Row index array must not be null");
		return new TableTask(labels, rows.length, (ctx, sentinel) -> map(rows, false));
	}

	/**
	 * Selects the rows from the given range {@code [from, to)}.
	 *
	 * @param from
	 * 		the start of the range (inclusive)
	 * @param to
	 * 		the end of the range (exclusive)
	 * @param ctx
	 * 		the execution context to use
	 * @return the new table with rows from the given range
	 * @throws NullPointerException
	 * 		if the context is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if start is not a valid row index
	 * @throws IllegalArgumentException
	 * 		if the start is bigger than the end or the end bigger than the height
	 */
	public Table rows(int from, int to, Context ctx) {
		Objects.requireNonNull(ctx, MESSAGE_CONTEXT_NULL);
		return rows(from, to).run(ctx);
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
		if (from < 0 || from >= height) {
			throw new IndexOutOfBoundsException("Row index: " + from + ", rows: " + height);
		}
		if (to < from) {
			throw new IllegalArgumentException("Start row " + from + " must not be bigger than end row " + to);
		}
		if (to > height) {
			throw new IllegalArgumentException("End row: " + to + " must not be bigger than height: " + height);
		}
		return new TableTask(labels, to - from, (ctx, sentinel) -> {
			int[] rows = new int[to - from];
			for (int i = from; i < to; i++) {
				rows[i - from] = i;
			}
			return map(rows, false);
		});
	}


	/**
	 * Creates a new table task to sort the table by the given column using the given sorting order.
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
		Objects.requireNonNull(order, "Order must not be null");
		if (column < 0 || column >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, column, width));
		}
		return new TableTask(labels, height, (ctx, sentinel) -> {
			int[] mapping = columns[column].sort(order);
			boolean continueWork = ctx.isActive() && sentinel.get();
			if (!continueWork) {
				throw new TaskAbortedException(TASK_ABORTED_BY_INVOKER);
			}
			return map(mapping, false);
		});
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
		Objects.requireNonNull(order, "Order must not be null");
		Objects.requireNonNull(columns, MESSAGE_COLUMN_INDICES_NULL);

		requireValidColumnIndices(columns);

		return new TableTask(labels, height, (ctx, sentinel) -> {
			if (columns.length == 0) {
				// Nothing to do...
				return this;
			}

			int[] mapping;
			if (columns.length > 1) {
				TableSorter sorter = new TableSorter(this, order, columns);
				mapping = sorter.sort();
			} else {
				Column single = getColumns()[columns[0]];
				mapping = single.sort(order);
			}

			boolean continueWork = ctx.isActive() && sentinel.get();
			if (!continueWork) {
				throw new TaskAbortedException(TASK_ABORTED_BY_INVOKER);
			}

			return map(mapping, false);
		});
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
		Objects.requireNonNull(orders, "Order list must not be null");
		Objects.requireNonNull(columns, MESSAGE_COLUMN_INDICES_NULL);

		requireValidColumnIndices(columns);

		if (orders.size() != columns.length) {
			throw new IllegalArgumentException("Order list and index array must be of same length");
		}

		for (Order order : orders) {
			if (order == null) {
				throw new NullPointerException("Order list must not contain null items");
			}
		}

		return new TableTask(labels, height, (ctx, sentinel) -> {
			if (columns.length == 0) {
				// Nothing to do...
				return this;
			}

			int[] mapping;
			if (columns.length > 1) {
				TableSorter sorter = new TableSorter(this, orders, columns);
				mapping = sorter.sort();
			} else {
				Column single = getColumns()[columns[0]];
				mapping = single.sort(orders.get(0));
			}

			boolean continueWork = ctx.isActive() && sentinel.get();
			if (!continueWork) {
				throw new TaskAbortedException(TASK_ABORTED_BY_INVOKER);
			}

			return map(mapping, false);
		});
	}

	/**
	 * Creates a new table task to sort the table by the given column using the given sorting order and execution
	 * context.
	 *
	 * @param column
	 * 		the column to sort by
	 * @param order
	 * 		the sorting order
	 * @param context
	 * 		the execution context
	 * @return the task to sort the table
	 * @throws NullPointerException
	 * 		if the order or context is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index is invalid
	 */
	public Table sort(int column, Order order, Context context) {
		return sort(column, order).run(context);
	}

	/**
	 * Creates a new table sorted by the given columns using the given sorting order and execution context. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param order
	 * 		the sorting order
	 * @param context
	 * 		the execution context to use
	 * @return the sorted table
	 * @throws NullPointerException
	 * 		if the column index array, order, or context is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index array contains an invalid index
	 */
	public Table sort(int[] columns, Order order, Context context) {
		return sort(columns, order).run(context);
	}

	/**
	 * Creates a new table sorted by the given columns using the given sorting orders and execution context. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * <p>This method assumes that a sorting order is specified for each of the given column indices, i.e., that the
	 * column index array and the order list are of the same size.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param orders
	 * 		the sorting orders
	 * @param context
	 * 		the execution context to use
	 * @return the sorted table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index array contains an invalid index
	 * @throws IllegalArgumentException
	 * 		if the sizes of the orders and the column index array differ
	 */
	public Table sort(int[] columns, List<Order> orders, Context context) {
		return sort(columns, orders).run(context);
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
		requireExistingLabel(label);
		return sort(new int[]{labelMap.get(label)}, order);
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
		requireExistingLabels(columns);
		return sort(lookupLabels(columns), order);
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
		requireExistingLabels(columns);
		return sort(lookupLabels(columns), orders);
	}

	/**
	 * Creates a new table sorted by the given column using the given sorting order.
	 *
	 * @param label
	 *            the column to sort by
	 * @param order
	 *            the sorting order
	 * @param context
	 * 		the execution context to use
	 * @return the sorted table
	 * @throws NullPointerException
	 *             if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 *             if the label is invalid
	 */
	public Table sort(String label, Order order, Context context) {
		return sort(label, order).run(context);
	}

	/**
	 * Creates a new table sorted by the given columns using the given sorting order and execution context. If multiple
	 * columns are specified, the output is equivalent to sorting the entire table by the first column, then subsets of
	 * the same value by the second column, etc.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param order
	 * 		the sorting order
	 * @param context
	 * 		the execution context to use
	 * @return the sorted table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column labels contain an invalid label or is empty
	 */
	public Table sort(List<String> columns, Order order, Context context) {
		return sort(columns, order).run(context);
	}

	/**
	 * Creates a new table sorted by the given columns using the given sorting orders. If multiple columns are
	 * specified, the output is equivalent to sorting the entire table by the first column, then subsets of the same
	 * value by the second column, etc.
	 *
	 * <p>This method assumes that a sorting order is specified for each of the given column indices, i.e., that the
	 * column index array and the order list are of the same size.
	 *
	 * @param columns
	 * 		the columns to sort by
	 * @param orders
	 * 		the sorting orders
	 * @param context
	 * 		the execution context to use
	 * @return the sorted table
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column labels contain an invalid label, the label list is empty,  or the label and sorting lists differ
	 * 		in size
	 */
	public Table sort(List<String> columns, List<Order> orders, Context context) {
		return sort(columns, orders).run(context);
	}

	/**
	 * Creates a transformer for the given column as a starting point for different reduce and apply functions.
	 *
	 * @param column
	 * 		the index of the column to start the transformation from
	 * @return a new transformer
	 * @throws IndexOutOfBoundsException
	 * 		if the column index is invalid
	 * @see Transformer
	 */
	public Transformer transform(int column) {
		if (column < 0 || column >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, column, width));
		}
		return new Transformer(columns[column]);
	}

	/**
	 * Creates a transformer for the given column as a starting point for different reduce and apply functions.
	 *
	 * @param column
	 * 		the label of the column to start the transformation from
	 * @return a new transformer
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 * @throws NullPointerException
	 * 		if the column label is {@code null}
	 * @see Transformer
	 */
	public Transformer transform(String column) {
		requireExistingLabel(column);
		return new Transformer(columns[labelMap.get(column)]);
	}


	/**
	 * Creates a transformer for the given columns as a starting point for different reduce and apply functions.
	 *
	 * @param columns
	 * 		the index array of the columns to start the transformation from
	 * @return a new transformer
	 * @throws IndexOutOfBoundsException
	 * 		if one of the indices is invalid
	 * @throws IllegalArgumentException
	 * 		if the column index array is empty
	 * @throws NullPointerException
	 * 		if the column index array is {@code null}
	 * @see TransformerMulti
	 */
	public TransformerMulti transform(int[] columns) {
		Objects.requireNonNull(columns, MESSAGE_COLUMN_INDICES_NULL);
		requireValidColumnIndices(columns);
		return new TransformerMulti(getColumns(columns));
	}

	/**
	 * Creates a transformer for the given columns as a starting point for different reduce and apply functions.
	 *
	 * @param columns
	 * 		the label list of the columns to start the transformation from
	 * @return a new transformer
	 * @throws IllegalArgumentException
	 * 		if the column label list is empty or contains invalid labels
	 * @throws NullPointerException
	 * 		if the column label list is or contains {@code null}
	 * @see TransformerMulti
	 */
	public TransformerMulti transform(List<String> columns) {
		requireExistingLabels(columns);
		return new TransformerMulti(getColumns(lookupLabels(columns)));
	}

	/**
	 * Creates a transformer for the given columns as a starting point for different reduce and apply functions.
	 *
	 * @param columns
	 * 		the labels of the columns to start the transformation from
	 * @return a new transformer
	 * @throws IllegalArgumentException
	 * 		if the one column labels is invalid or there are no column labels
	 * @throws NullPointerException
	 * 		if the column labels are or contain {@code null}
	 * @see TransformerMulti
	 */
	public TransformerMulti transform(String... columns) {
		Objects.requireNonNull(columns, "Column labels must not be null.");
		List<String> labelsList = Arrays.asList(columns);
		requireExistingLabels(labelsList);
		return new TransformerMulti(getColumns(lookupLabels(labelsList)));
	}

	/**
	 * Creates a transformer for all columns in this table as a starting point for different reduce and apply functions.
	 *
	 * @see TransformerMulti
	 */
	public TransformerMulti transform() {
		return new TransformerMulti(columns);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param column
	 * 		the index of the column on which to evaluate the predicate, must be
	 * 		{@link Column.Capability#NUMERIC_READABLE}
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if the predicate or context is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index is invalid
	 */
	public Table filterNumeric(int column, DoublePredicate predicate, Workload workload, Context context) {
		Objects.requireNonNull(predicate, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		if (column < 0 || column >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, column, width));
		}
		int[] mapping = new ParallelExecutor<>(new ColumnFilterer(columns[column], predicate), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the indices of the columns to filter by, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if any of the column indices is invalid
	 */
	public Table filterNumeric(int[] columns, Predicate<Row> filter, Workload workload, Context context) {
		Objects.requireNonNull(filter, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Column[] filterColumns = getColumns(columns);
		int[] mapping = new ParallelExecutor<>(new ColumnsFilterer(filterColumns, filter), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param column
	 * 		the label of the column on which to evaluate the predicate, must be
	 * 		{@link Column.Capability#NUMERIC_READABLE}
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 */
	public Table filterNumeric(String column, DoublePredicate predicate, Workload workload, Context context) {
		requireExistingLabel(column);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return filterNumeric(labelMap.get(column), predicate, workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param first
	 * 		the label of the first column on which to evaluate the predicate, must be {@link
	 * 		Column.Capability#NUMERIC_READABLE}
	 * @param second
	 * 		the label of the second column on which to evaluate the predicate, must be {@link
	 * 		Column.Capability#NUMERIC_READABLE}
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 */
	public Table filterNumeric(String first, String second, DoubleBinaryPredicate predicate, Workload workload,
							   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return filterNumeric(Arrays.asList(first, second), row -> predicate.test(row.get(0), row.get(1)), workload,
				context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the label of the column on which to evaluate the predicate, must be
	 * 		{@link Column.Capability#NUMERIC_READABLE}
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if any of the column labels is invalid or the label list is empty
	 */
	public Table filterNumeric(List<String> columns, Predicate<Row> filter, Workload workload, Context context) {
		requireExistingLabels(columns);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		int[] indices = lookupLabels(columns);
		return filterNumeric(indices, filter, workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param column
	 * 		the index of the column on which to evaluate the predicate, must be {@link Column.Category#CATEGORICAL}
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if the predicate or context is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index is invalid
	 */
	public Table filterCategorical(int column, IntPredicate predicate, Workload workload, Context context) {
		Objects.requireNonNull(predicate, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		if (column < 0 || column >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, column, width));
		}
		int[] mapping = new ParallelExecutor<>(new CategoricalColumnFilterer(columns[column], predicate), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the indices of the columns to filter by, must be {@link Column.Capability#NUMERIC_READABLE}
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if any of the column indices is invalid
	 */
	public Table filterCategorical(int[] columns, Predicate<CategoricalRow> filter, Workload workload,
								   Context context) {
		Objects.requireNonNull(filter, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Column[] filterColumns = getColumns(columns);
		int[] mapping = new ParallelExecutor<>(new CategoricalColumnsFilterer(filterColumns, filter), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param column
	 * 		the label of the column on which to evaluate the predicate, must be {@link Column.Category#CATEGORICAL}
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 */
	public Table filterCategorical(String column, IntPredicate predicate, Workload workload, Context context) {
		requireExistingLabel(column);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return filterCategorical(labelMap.get(column), predicate, workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param first
	 * 		the label of the first column on which to evaluate the predicate, must be
	 * 		{@link Column.Category#CATEGORICAL}
	 * @param second
	 * 		the label of the second column on which to evaluate the predicate, must be
	 * 		{@link Column.Category#CATEGORICAL}
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 */
	public Table filterCategorical(String first, String second, IntBinaryPredicate predicate, Workload workload,
								   Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return filterCategorical(Arrays.asList(first, second), row -> predicate.test(row.get(0), row.get(1)), workload,
				context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the label of the column on which to evaluate the predicate, must be {@link Column.Category#CATEGORICAL}
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if any of the column labels is invalid or the label list is empty
	 */
	public Table filterCategorical(List<String> columns, Predicate<CategoricalRow> filter, Workload workload,
								   Context context) {
		requireExistingLabels(columns);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		int[] indices = lookupLabels(columns);
		return filterCategorical(indices, filter, workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param column
	 * 		the index of the column on which to evaluate the predicate, must be
	 * 		{@link Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the object type of the column
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if the predicate or context is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if the column index is invalid
	 */
	public <T> Table filterObjects(int column, Class<T> type, Predicate<T> predicate, Workload workload,
								   Context context) {
		Objects.requireNonNull(type, "Type must not be null");
		Objects.requireNonNull(predicate, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		if (column < 0 || column >= width) {
			throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, column, width));
		}
		int[] mapping = new ParallelExecutor<>(new ObjectColumnFilterer<>(columns[column], type, predicate), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the indices of the columns to filter by, must be {@link Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the object type of the columns
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if any of the column indices is invalid
	 */
	public <T> Table filterObjects(int[] columns, Class<T> type, Predicate<ObjectRow<T>> filter, Workload workload,
								   Context context) {
		Objects.requireNonNull(filter, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Column[] filterColumns = getColumns(columns);
		int[] mapping = new ParallelExecutor<>(new ObjectColumnsFilterer<>(filterColumns, type, filter), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param column
	 * 		the label of the column on which to evaluate the predicate, must be
	 * 		{@link Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the object type of the column
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 */
	public <T> Table filterObjects(String column, Class<T> type, Predicate<T> predicate, Workload workload,
								   Context context) {
		requireExistingLabel(column);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return filterObjects(labelMap.get(column), type, predicate, workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param first
	 * 		the label of the first column on which to evaluate the predicate, must be {@link
	 * 		Column.Capability#OBJECT_READABLE} of the given type
	 * @param second
	 * 		the label of the second column on which to evaluate the predicate, must be {@link
	 * 		Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the object type of the columns
	 * @param predicate
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IllegalArgumentException
	 * 		if the column label is invalid
	 */
	public <T> Table filterObjects(String first, String second, Class<T> type, BiPredicate<T, T> predicate,
								   Workload workload, Context context) {
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		return filterObjects(Arrays.asList(first, second), type, row -> predicate.test(row.get(0), row.get(1)),
				workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the label of the column on which to evaluate the predicate, must be
	 * 		{@link Column.Capability#OBJECT_READABLE} of the given type
	 * @param type
	 * 		the object type of the columns
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if any of the column labels is invalid or the label list is empty
	 */
	public <T> Table filterObjects(List<String> columns, Class<T> type, Predicate<ObjectRow<T>> filter,
								   Workload workload, Context context) {
		requireExistingLabels(columns);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		int[] indices = lookupLabels(columns);
		return filterObjects(indices, type, filter, workload, context);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the indices of the columns to filter by
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is {@code null}
	 * @throws IndexOutOfBoundsException
	 * 		if any of the column indices is invalid
	 */
	public Table filterGeneral(int[] columns, Predicate<GeneralRow> filter, Workload workload, Context context) {
		Objects.requireNonNull(filter, "Filter function must not be null");
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		Column[] filterColumns = getColumns(columns);
		int[] mapping = new ParallelExecutor<>(new GeneralColumnsFilterer(filterColumns, filter), workload)
				.create().run(context);
		return map(mapping, false);
	}

	/**
	 * Filters the table by the given filter predicate. Depending on the input size and the specified workload per
	 * data-point, the computation might be performed in parallel.
	 *
	 * @param columns
	 * 		the label of the column on which to evaluate the predicate
	 * @param filter
	 * 		the predicate to apply
	 * @param workload
	 * 		the expected workload per data point
	 * @param context
	 * 		the execution context
	 * @return a table with only those rows where the predicate returns {@code true}
	 * @throws NullPointerException
	 * 		if any of the arguments is or contains {@code null}
	 * @throws IllegalArgumentException
	 * 		if any of the column labels is invalid or the label list is empty
	 */
	public Table filterGeneral(List<String> columns, Predicate<GeneralRow> filter, Workload workload, Context context) {
		requireExistingLabels(columns);
		Objects.requireNonNull(context, MESSAGE_CONTEXT_NULL);
		int[] indices = lookupLabels(columns);
		return filterGeneral(indices, filter, workload, context);
	}

	Map<String, List<ColumnMetaData>> getMetaData() {
		return metaDataMap;
	}

	/**
	 * Returns the meta data attached to the column with the given label (if any).
	 *
	 * @param label
	 * 		the column label
	 * @return unmodifiable list of meta data
	 * @throws NullPointerException
	 * 		if the given label is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public List<ColumnMetaData> getMetaData(String label) {
		requireExistingLabel(label);
		List<ColumnMetaData> meta = metaDataMap.get(label);
		if (meta == null) {
			return Collections.emptyList();
		} else {
			return Collections.unmodifiableList(meta);
		}
	}

	/**
	 * Returns the meta data with the given type attached to the column with the given label (if any).
	 *
	 * @param label
	 * 		the column label
	 * @param type
	 * 		the meta data type
	 * @return the attached meta data
	 * @throws NullPointerException
	 * 		if the given label or type is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @see #getFirstMetaData(String, Class)
	 */
	public <T extends ColumnMetaData> List<T> getMetaData(String label, Class<T> type) {
		requireExistingLabel(label);
		List<ColumnMetaData> meta = metaDataMap.get(label);
		if (meta == null) {
			return Collections.emptyList();
		} else {
			List<T> subset = new ArrayList<>(meta.size());
			for (ColumnMetaData data : meta) {
				if (type.isInstance(data)) {
					subset.add(type.cast(data));
				}
			}
			return subset;
		}
	}

	/**
	 * Returns the first meta datum with the given type attached to the column with the given label (if any). Use of
	 * this method instead of {@link #getMetaData(String, Class)}  is recommended if it is known that there can be at
	 * most one match, e.g., if the meta data type has uniqueness level {@link
	 * com.rapidminer.belt.util.ColumnMetaData.Uniqueness#COLUMN}.
	 *
	 * @param label
	 * 		the column label
	 * @param type
	 * 		the meta data type
	 * @return the first attached meta datum or {@code null}
	 * @throws NullPointerException
	 * 		if the given label or type is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public <T extends ColumnMetaData> T getFirstMetaData(String label, Class<T> type) {
		requireExistingLabel(label);
		List<ColumnMetaData> metaData = metaDataMap.get(label);
		if (metaData == null) {
			return null;
		} else {
			for (ColumnMetaData data : metaData) {
				if (type.isInstance(data)) {
					return type.cast(data);
				}
			}
			return null;
		}
	}

	/**
	 * Returns the labels of all columns that have meta data of the given type attached to them.
	 *
	 * @param type
	 * 		the meta data type to look up
	 * @return the labels of matching columns
	 * @see #withMetaData(ColumnMetaData)
	 */
	public final <T extends ColumnMetaData> List<String> withMetaData(Class<T> type) {
		List<String> matches = new ArrayList<>();
		for (String label : labels) {
			List<ColumnMetaData> list = metaDataMap.get(label);
			if (list != null) {
				for (ColumnMetaData data : list) {
					if (type.isInstance(data)) {
						matches.add(label);
						break;
					}
				}
			}
		}
		return matches;
	}

	/**
	 * Returns the labels of all columns that have the given meta data instance attached to them.
	 *
	 * <p>A match is given if the {@link Object#equals(Object)} returns {@code true}. In particular, it is not
	 * sufficient that the meta data type ids match.
	 *
	 * @param metaData
	 * 		the meta data instance to look up
	 * @return the labels of matching columns
	 * @see #withMetaData(Class)
	 */
	public List<String> withMetaData(ColumnMetaData metaData) {
		List<String> matches = new ArrayList<>();
		for (String label : labels) {
			List<ColumnMetaData> list = metaDataMap.get(label);
			if (list != null) {
				for (ColumnMetaData data : list) {
					if (metaData.equals(data)) {
						matches.add(label);
						break;
					}
				}
			}
		}
		return matches;
	}

	/**
	 * Returns the labels of all columns that do not have meta data of the given types attached to them.
	 *
	 * @param type
	 * 		the meta data types to look up
	 * @return the labels of columns without a match
	 * @see #withoutMetaData(ColumnMetaData)
	 */
	public <T extends ColumnMetaData> List<String> withoutMetaData(Class<T> type) {
		List<String> matches = new ArrayList<>();
		for (String label : labels) {
			List<ColumnMetaData> list = metaDataMap.get(label);
			boolean found = false;
			if (list != null) {
				for (ColumnMetaData data : list) {
					if (type.isInstance(data)) {
						found = true;
						break;
					}
				}
			}
			if (!found) {
				matches.add(label);
			}
		}
		return matches;
	}

	/**
	 * Returns the labels of all columns that do not have the given meta data instances attached to them.
	 *
	 * <p>A match is given if the {@link Object#equals(Object)} returns {@code true}. In particular, it is not
	 * sufficient that the meta data type ids match.
	 *
	 * @param metaData
	 * 		the meta data instances to look up
	 * @return the labels of columns without a match
	 * @see #withoutMetaData(Class)
	 */
	public List<String> withoutMetaData(ColumnMetaData metaData) {
		List<String> matches = new ArrayList<>();
		for (String label : labels) {
			List<ColumnMetaData> list = metaDataMap.get(label);
			boolean found = false;
			if (list != null) {
				for (ColumnMetaData data : list) {
					if (metaData.equals(data)) {
						found = true;
						break;
					}
				}
			}
			if (!found) {
				matches.add(label);
			}
		}
		return matches;
	}

	/**
	 * @return the columns of the table
	 */
	Column[] getColumns() {
		return columns;
	}

	/**
	 * @return the column labels of the table
	 */
	String[] labelArray() {
		return labels;
	}

	/**
	 * Creates a new tables with rows reordered according to the given index mapping. If the mapping contains invalid
	 * indices (i.e., values outside of the range {@code [0, size())}), the new table will contain missing value at that
	 * place. In particular, sub- and supersets, as well as duplicate indices are supported.
	 *
	 * <p>The second parameter allows to indicate whether a view (shallow copy) is preferred over a deep copy of the
	 * underlying data. This might be the case when deriving multiple tables from one and the same source (e.g., when
	 * creating a partition).
	 *
	 * @param mapping
	 * 		the index mapping
	 * @param preferView
	 * 		whether a view is preferred over a deep copy
	 * @return a mapped table
	 */
	Table map(int[] mapping, boolean preferView) {
		Column[] mappedColumns = new Column[columns.length];
		int index = 0;

		// If the table already contains mapped columns, cache merged mappings to prevent duplicates.
		Map<int[], int[]> cache = null;

		for (Column column : columns) {
			if (column instanceof CacheMappedColumn) {
				if (cache == null) {
					cache = new ConcurrentHashMap<>();
				}
				mappedColumns[index] = ((CacheMappedColumn) column).map(mapping, preferView, cache);
			} else {
				mappedColumns[index] = column.map(mapping, preferView);
			}
			index++;
		}

		return new Table(mappedColumns, labels, metaDataMap);
	}

	/**
	 * Checks whether the given indices are in range {@code [0, size)).
	 *
	 * @param indices
	 * 		the indices to check
	 * @throws IndexOutOfBoundsException
	 * 		if an index is out of bounds
	 */
	private void requireValidColumnIndices(int[] indices) {
		for (int index : indices) {
			if (index < 0 || index >= width) {
				throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, index, width));
			}
		}
	}

	/**
	 * Checks and sanitizes the given label array.
	 *
	 * @param labels
	 * 		the labels to sanitize
	 * @param n
	 * 		teh expected number of labels
	 * @return the sanitized labels
	 * @throws NullPointerException
	 * 		if the array of a label is null
	 * @throws IllegalArgumentException
	 * 		if the length of the label array does not match the columns or contains empty labels
	 */
	private String[] requireValidLabels(String[] labels, int n) {
		if (labels == null) {
			throw new NullPointerException("Label array must not be null");
		}
		if (labels.length != n) {
			throw new IllegalArgumentException("Label array length does not match column array");
		}
		for (String label : labels) {
			requireValidLabel(label);
		}
		return labels;
	}

	static void requireValidLabel(String label) {
		if (label == null) {
			throw new NullPointerException("Labels must not be null");
		}
		if (label.isEmpty()) {
			throw new IllegalArgumentException("Labels must not be empty");
		}
	}

	/**
	 * Checks and build the label mapping for the table.
	 *
	 * @param labels
	 * 		the labels to build the mapping from
	 * @return the label to index mapping
	 * @throws IllegalArgumentException
	 * 		if the input contain duplicates
	 */
	private Map<String, Integer> requireValidLabelMapping(String[] labels) {
		Map<String, Integer> map = new HashMap<>(labels.length);
		for (int i = 0; i < labels.length; i++) {
			String label = labels[i];
			if (map.containsKey(label)) {
				throw new IllegalArgumentException("Label array must not contain duplicates");
			}
			map.put(label, i);
		}
		return map;
	}

	/**
	 * Checks whether the given labels is used by the table.
	 *
	 * @param labels
	 * 		the labels to check
	 * @throws NullPointerException
	 * 		if the given list or one of the labels is null
	 * @throws IllegalArgumentException
	 * 		if the label array is empty
	 */
	private void requireExistingLabels(List<String> labels) {
		if (labels == null) {
			throw new NullPointerException("Label list must not be null");
		}
		if (labels.isEmpty()) {
			throw new IllegalArgumentException("Label list must not be empty");
		}
		for (String label : labels) {
			requireExistingLabel(label);
		}
	}

	/**
	 * Checks whether the given label is used by the table.
	 *
	 * @param label
	 * 		the label to check
	 * @throws NullPointerException
	 * 		if the label is null
	 * @throws IllegalArgumentException
	 * 		if the label is not used by the table
	 */
	private void requireExistingLabel(String label) {
		if (label == null) {
			throw new NullPointerException("Labels must not be null");
		}
		if (!labelMap.containsKey(label)) {
			throw new IllegalArgumentException("Unknown column label: " + label);
		}
	}

	/**
	 * Creates an index array from the given list of labels.
	 *
	 * @param labels
	 * 		the labels to look up
	 * @return the index array
	 */
	private int[] lookupLabels(List<String> labels) {
		int[] indices = new int[labels.size()];
		int i = 0;
		for (String label : labels) {
			indices[i++] = labelMap.get(label);
		}
		return indices;
	}

	/**
	 * @return new {@link Column} array with the columns associate to the given column indices.
	 * @throws IndexOutOfBoundsException
	 * 		if one of the column indices is invalid
	 * @throws NullPointerException
	 * 		if the column index array is {@code null}
	 */
	private Column[] getColumns(int[] columnIndices) {
		Objects.requireNonNull(columnIndices, MESSAGE_COLUMN_INDICES_NULL);
		Column[] desiredColumns = new Column[columnIndices.length];
		for (int i = 0; i < desiredColumns.length; i++) {
			int columnIndex = columnIndices[i];
			if (columnIndex < 0 || columnIndex >= width) {
				throw new IndexOutOfBoundsException(String.format(INVALID_COLUMN_MESSAGE, columnIndex, width));
			}
			desiredColumns[i] = columns[columnIndex];
		}
		return desiredColumns;
	}

	/**
	 * Creates a builder for a new {@link Table} with the given number of rows.
	 *
	 * @param rows
	 * 		the number of rows
	 * @return a table builder
	 */
	public static TableBuilder newTable(int rows) {
		return new TableBuilder(rows);
	}

	/**
	 * Creates a builder for a new {@link Table} derived from the given table.
	 *
	 * @param table
	 * 		the source table
	 * @return a table builder
	 */
	public static TableBuilder from(Table table) {
		return new TableBuilder(table);
	}

	/**
	 * Creates a builder for a new {@link Table} derived from the given task.
	 *
	 * @param task
	 * 		the source task
	 * @return a table builder
	 */
	public static TableBuilder from(TableTask task) {
		return new TableBuilder(task);
	}

	/**
	 * Writer for a new {@link Table} with the given column labels and an unknown number of rows. If the number of rows
	 * can be estimated, use {@link #writeTable(List, int)} instead to prevent unnecessary resizing of the data
	 * containers.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label list is {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty
	 */
	public static RowWriter writeTable(List<String> columnLabels) {
		Objects.requireNonNull(columnLabels, "column labels must not be null");
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException("column labels must not be empty");
		}
		return new RowWriter(columnLabels);
	}

	/**
	 * Writer for a new {@link Table} with the given column labels and the given expected number of rows.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param expectedRows
	 * 		an estimate for the number of rows in the final table. A good estimation prevents unnecessary resizing of the
	 * 		data containers.
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if columnLabels is {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or expected rows are negative
	 */
	public static RowWriter writeTable(List<String> columnLabels, int expectedRows) {
		Objects.requireNonNull(columnLabels, "column labels must not be null");
		if (columnLabels.isEmpty()) {
			throw new IllegalArgumentException("column labels must not be empty");
		}
		if (expectedRows < 0) {
			throw new IllegalArgumentException("expected rows must not be negative");
		}
		return new RowWriter(columnLabels, expectedRows);
	}


	/**
	 * Writer for a new {@link Table} with the given column labels and types and an unknown number of rows. If the
	 * number of rows can be estimated, use {@link #writeTable(List, List, int)} instead to prevent unnecessary resizing
	 * of the data containers.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param types
	 * 		the types of the columns
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if column label or type list is {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or not the same size as types list
	 */
	public static RowWriter writeTable(List<String> columnLabels, List<TypeId> types) {
		Objects.requireNonNull(columnLabels, "column labels must not be null");
		Objects.requireNonNull(types, "column types must not be null");
		if(columnLabels.isEmpty()){
			throw new IllegalArgumentException("column labels must not be empty");
		}
		if (columnLabels.size() != types.size()) {
			throw new IllegalArgumentException("column labels and types must be of the same length");
		}
		return new RowWriter(columnLabels, types);
	}

	/**
	 * Writer for a new {@link Table} with the given column labels and the given expected number of rows.
	 *
	 * @param columnLabels
	 * 		the labels of the columns
	 * @param types
	 * 		the types of the columns
	 * @param expectedRows
	 * 		an estimate for the number of rows in the final table. A good estimation prevents unnecessary resizing of the
	 * 		data containers.
	 * @return a writer to write a new table row by row
	 * @throws NullPointerException
	 * 		if columnLabels is {@code null}
	 * @throws IllegalArgumentException
	 * 		if column label list is empty or not the same size as types list or expected rows are negative
	 */
	public static RowWriter writeTable(List<String> columnLabels, List<TypeId> types, int expectedRows) {
		Objects.requireNonNull(columnLabels, "column labels must not be null");
		Objects.requireNonNull(types, "column types must not be null");
		if(columnLabels.isEmpty()){
			throw new IllegalArgumentException("column labels must not be empty");
		}
		if (columnLabels.size() != types.size()) {
			throw new IllegalArgumentException("column labels and types must be of the same length");
		}
		if(expectedRows < 0){
			throw new IllegalArgumentException("expected rows must not be negative");
		}
		return new RowWriter(columnLabels, types, expectedRows);
	}


	/**
	 * Loads a {@link Table}the given path. This method is experimental and might change in the
	 * future.
	 *
	 * @param path
	 * 		path to the table file
	 * @return a new table
	 * @throws IOException
	 * 		if reading the file fails
	 * @throws IllegalArgumentException
	 * 		if the file does not contain a table
	 */
	public static Table load(Path path) throws IOException {
		Objects.requireNonNull(path, "path name must not be null");
		return TableFiler.load(path);
	}

	/**
	 * Stores the given table in a file with the given name. This method is experimental and might change in the future.
	 *
	 * @param table
	 * 		the table to store
	 * @param path
	 * 		where to store the table
	 * @throws IOException
	 * 		if writing to the file fails
	 * @throws IllegalArgumentException
	 * 		if the table or the file name is {@code null}
	 */
	public static void store(Table table, Path path) throws IOException {
		Objects.requireNonNull(path, "path name must not be null");
		Objects.requireNonNull(table, "table must not be null");
		TableFiler.store(table, path);
	}

	@Override
	public String toString() {
		return PrettyPrinter.print(this);
	}

}