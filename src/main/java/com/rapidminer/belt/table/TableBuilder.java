/**
 * This file is part of the RapidMiner Belt project.
 * Copyright (C) 2017-2020 RapidMiner GmbH
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

package com.rapidminer.belt.table;

import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.function.IntToDoubleFunction;
import java.util.function.Supplier;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.buffer.DateTimeBuffer;
import com.rapidminer.belt.buffer.Int32NominalBuffer;
import com.rapidminer.belt.buffer.ObjectBuffer;
import com.rapidminer.belt.buffer.TimeBuffer;
import com.rapidminer.belt.buffer.UInt16NominalBuffer;
import com.rapidminer.belt.buffer.UInt2NominalBuffer;
import com.rapidminer.belt.buffer.UInt4NominalBuffer;
import com.rapidminer.belt.buffer.UInt8NominalBuffer;
import com.rapidminer.belt.column.Column;
import com.rapidminer.belt.column.Column.TypeId;
import com.rapidminer.belt.column.ColumnType;
import com.rapidminer.belt.column.type.StringList;
import com.rapidminer.belt.column.type.StringSet;
import com.rapidminer.belt.execution.Context;
import com.rapidminer.belt.execution.ExecutionUtils;
import com.rapidminer.belt.util.ColumnMetaData;
import com.rapidminer.belt.util.ColumnReference;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * A builder for a {@link Table}.
 *
 * @author Gisa Schaefer
 */
public final class TableBuilder {

	/** Message when table parameter is {@code null} */
	private static final String TABLE_NULL_MESSAGE = "Table must not be null";

	/**
	 * Message for mismatching column sizes.
	 */
	private static final String MISMATCHING_COLUMN_MESSAGE = "Size of the given column does not match previously defined number of rows";

	/**
	 * Message for unknown column label.
	 */
	private static final String UNKNOWN_LABEL_MESSAGE = "Unknown label: ";

	/**
	 * Message for null generator function.
	 */
	private static final String MESSAGE_GENERATOR_NULL = "Generator must not be null";

	/**
	 * Message for null column type.
	 */
	private static final String MESSAGE_TYPE_NULL = "Column type must not be null.";

	/**
	 * Message for non categorical column type.
	 */
	private static final String MESSAGE_TYPE_CATEGORICAL = "Type must be categorical.";

	/**
	 * Container class for columns sources.
	 */
	private static final class ColumnSource {

		private IntToDoubleFunction generator;
		private Column column;
		private Supplier<Column> supplier;
		private Column.TypeId type;

		private ColumnSource(IntToDoubleFunction generator, TypeId type) {
			this.generator = generator;
			this.type = type;
		}

		private ColumnSource(Column column) {
			this.column = column;
			this.type = column.type().id();
		}

		private ColumnSource(Supplier<Column> supplier, TypeId type) {
			this.supplier = supplier;
			this.type = type;
		}

	}

	private final int numberOfRows;
	private final LinkedHashMap<String, ColumnSource> columnSources;
	private Map<String, List<ColumnMetaData>> columnMetaData;
	private boolean ownsMetaData;

	/**
	 * Creates a builder for a {@link Table}.
	 */
	TableBuilder(int rows) {
		if (rows < 0) {
			throw new IllegalArgumentException("Number of rows must not be negative");
		}
		this.columnSources = new LinkedHashMap<>();
		this.columnMetaData = new HashMap<>();
		this.ownsMetaData = true;
		this.numberOfRows = rows;
	}

	/**
	 * Creates a builder for a new {@link Table} based on the given table.
	 *
	 * @param table
	 * 		the table to extend
	 * @throws NullPointerException
	 * 		if the table is {@code null}
	 */
	TableBuilder(Table table) {
		Objects.requireNonNull(table, TABLE_NULL_MESSAGE);
		this.columnSources = new LinkedHashMap<>(table.width());
		this.numberOfRows = table.height();
		Column[] columns = table.getColumns();
		String[] labels = table.labelArray();
		this.columnMetaData = table.getMetaData();
		this.ownsMetaData = false;
		for (int i = 0; i < columns.length; i++) {
			this.columnSources.put(labels[i], new ColumnSource(columns[i]));
		}
	}

	/**
	 * Adds a real column that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public synchronized TableBuilder addReal(String label, IntToDoubleFunction generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		columnSources.put(label, new ColumnSource(generator, TypeId.REAL));
		return this;
	}

	/**
	 * Adds an {@link TypeId#INTEGER_53_BIT} column that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public synchronized TableBuilder addInt53Bit(String label, IntToDoubleFunction generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		columnSources.put(label, new ColumnSource(generator, TypeId.INTEGER_53_BIT));
		return this;
	}

	/**
	 * Adds a nominal column that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public TableBuilder addNominal(String label, IntFunction<String> generator) {
		return addCategorical(label, generator, ColumnType.NOMINAL);
	}

	/**
	 * Adds a nominal column that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @param maxNumberOfValues
	 * 		the maximal number of different values that the generator generates, will be used to determine the data
	 * 		storage format
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public TableBuilder addNominal(String label, IntFunction<String> generator, int maxNumberOfValues) {
		return addCategorical(label, generator, maxNumberOfValues, ColumnType.NOMINAL);
	}

	/**
	 * Adds an object column with type {@link TypeId#TEXT} that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or the generator is {@code null}
	 */
	public synchronized TableBuilder addText(String label, IntFunction<String> generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getObjectColumnSupplier(generator, ColumnType.TEXT);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TEXT));
		return this;
	}

	/**
	 * Adds an object column with type {@link TypeId#TEXT_SET} that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or the generator is {@code null}
	 */
	public synchronized TableBuilder addTextset(String label, IntFunction<StringSet> generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getObjectColumnSupplier(generator, ColumnType.TEXTSET);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TEXT_SET));
		return this;
	}

	/**
	 * Adds an object column with type {@link TypeId#TEXT_LIST} that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or the generator is {@code null}
	 */
	public synchronized TableBuilder addTextlist(String label, IntFunction<StringList> generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getObjectColumnSupplier(generator, ColumnType.TEXTLIST);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TEXT_LIST));
		return this;
	}

	/**
	 * Adds a time column that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public synchronized TableBuilder addTime(String label, IntFunction<LocalTime> generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);

		Supplier<Column> supplier = getTimeColumnSupplier(generator);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TIME));
		return this;
	}

	/**
	 * Adds a date-time column that is filled as specified by the generator. The generated date-time column will be of
	 * nanosecond precision. If you only want second precision, please create a column with
	 * {@link Buffers#dateTimeBuffer(int, boolean)} and use {@link #add(String, Column)}.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public synchronized TableBuilder addDateTime(String label, IntFunction<Instant> generator) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);

		Supplier<Column> supplier = getDateTimeColumnSupplier(generator);
		columnSources.put(label, new ColumnSource(supplier, TypeId.DATE_TIME));
		return this;
	}

	/**
	 * Adds the given meta data to the column with the given label.
	 *
	 * @param label
	 * 		the column label
	 * @param metaData
	 * 		the meta data
	 * @return the builder
	 * @throws NullPointerException
	 * 		if the given label or meta data is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws IllegalStateException
	 * 		if the given meta data violates constraints such as uniqueness levels
	 */
	public synchronized TableBuilder addMetaData(String label, ColumnMetaData metaData) {
		requireUsedLabel(label);
		Objects.requireNonNull(metaData, "Column meta data must not be null");
		ensureMetaDataOwnership();
		addMetaDataUnchecked(label, metaData);
		return this;
	}

	/**
	 * Adds the given meta data to the column with the given label.
	 *
	 * @param label
	 * 		the column label
	 * @param metaData
	 * 		the list of meta data
	 * @return the builder
	 * @throws NullPointerException
	 * 		if the given label, meta data list, or an item of the list is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws IllegalStateException
	 * 		if the given meta data violates constraints such as uniqueness levels
	 */
	public synchronized TableBuilder addMetaData(String label, List<ColumnMetaData> metaData) {
		requireUsedLabel(label);
		Objects.requireNonNull(metaData, "List of column meta data must not be null");
		ensureMetaDataOwnership();
		for (ColumnMetaData item : metaData) {
			Objects.requireNonNull(item, "List of column meta data must not contain null values");
			addMetaDataUnchecked(label, item);
		}
		return this;
	}

	/**
	 * Adds a nominal boolean column with the given positive value and type that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled. It must return exactly two different values (not
	 * 		counting {@code null}) where one is the positive value.
	 * @param positiveValue
	 * 		the positive value of the boolean column
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or generator is {@code null}
	 */
	public TableBuilder addBoolean(String label, IntFunction<String> generator, String positiveValue) {
		return addBoolean(label, generator, positiveValue, ColumnType.NOMINAL);
	}

	/**
	 * Adds the given column. The column size must match the number of rows specified by {@link #TableBuilder(int)}.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param column
	 * 		the column to add
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use
	 * @throws NullPointerException
	 * 		if the label or column is {@code null}
	 * @throws IllegalStateException
	 * 		if the column size does not match the previously defined number of rows
	 */
	public synchronized TableBuilder add(String label, Column column) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(column, "Column must not be null");
		if (numberOfRows != column.size()) {
			throw new IllegalStateException(MISMATCHING_COLUMN_MESSAGE);
		}
		columnSources.put(label, new ColumnSource(column));
		return this;
	}

	/**
	 * Removes the given column from the table. {@link ColumnReference}s to the column are not removed.
	 *
	 * @param label
	 * 		the column to remove
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public synchronized TableBuilder remove(String label) {
		requireUsedLabel(label);
		columnSources.remove(label);
		if (columnMetaData.containsKey(label)) {
			ensureMetaDataOwnership();
			columnMetaData.remove(label);
		}
		return this;
	}

	/**
	 * Removes all meta data of the given type from the column with the given label. Has no effect if no such meta data
	 * is attached to the column
	 *
	 * @param label
	 * 		the column label
	 * @param type
	 * 		the meta data type
	 * @return this builder
	 * @throws NullPointerException
	 * 		if the given label or type is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public synchronized <T extends ColumnMetaData> TableBuilder removeMetaData(String label, Class<T> type) {
		requireUsedLabel(label);
		Objects.requireNonNull(type, "Meta data type must not be null");
		ensureMetaDataOwnership();
		List<ColumnMetaData> list = columnMetaData.get(label);
		if (list != null) {
			list.removeIf(type::isInstance);
			if (list.isEmpty()) {
				columnMetaData.remove(label);
			}
		}
		return this;
	}

	/**
	 * Removes the given meta data from the column with the given label. Has no effect if no such meta data is attached
	 * to the column.
	 *
	 * @param label
	 * 		the column label
	 * @param metaData
	 * 		the meta data
	 * @return this builder
	 * @throws NullPointerException
	 * 		if the given label or meta data is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public synchronized TableBuilder removeMetaData(String label, ColumnMetaData metaData) {
		requireUsedLabel(label);
		Objects.requireNonNull(metaData, "Meta data must not be null");
		ensureMetaDataOwnership();
		List<ColumnMetaData> list = columnMetaData.get(label);
		if (list != null) {
			list.remove(metaData);
			if (list.isEmpty()) {
				columnMetaData.remove(label);
			}
		}
		return this;
	}

	/**
	 * Removes all meta data from the column with the given label. Has no effect if no meta data is attached.
	 *
	 * @param label
	 * 		the column label
	 * @return this builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the given label is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public synchronized TableBuilder clearMetaData(String label) {
		requireUsedLabel(label);
		ensureMetaDataOwnership();
		columnMetaData.remove(label);
		return this;
	}

	/**
	 * Replaces the column with the given label by a real column filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceReal(String label, IntToDoubleFunction generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		columnSources.put(label, new ColumnSource(generator, TypeId.REAL));
		return this;
	}

	/**
	 * Replaces the column with the given label by an {@link TypeId#INTEGER_53_BIT} column filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceInt53Bit(String label, IntToDoubleFunction generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		columnSources.put(label, new ColumnSource(generator, TypeId.INTEGER_53_BIT));
		return this;
	}

	/**
	 * Replaces the column with the given label by a nominal column filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public TableBuilder replaceNominal(String label, IntFunction<String> generator) {
		return replaceCategorical(label, generator, ColumnType.NOMINAL);
	}

	/**
	 * Replaces the column with the given label by a nominal column filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @param maxNumberOfValues
	 * 		the maximal number of different values that the generator generates, will be used to determine the data
	 * 		storage format
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public TableBuilder replaceNominal(String label, IntFunction<String> generator, int maxNumberOfValues) {
		return replaceCategorical(label, generator, maxNumberOfValues, ColumnType.NOMINAL);
	}

	/**
	 * Replaces the column with the given label by an object column of type {@link TypeId#TEXT} filled by the given
	 * generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceText(String label, IntFunction<String> generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getObjectColumnSupplier(generator, ColumnType.TEXT);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TEXT));
		return this;
	}

	/**
	 * Replaces the column with the given label by an object column of type {@link TypeId#TEXT_SET} filled by the given
	 * generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceTextset(String label, IntFunction<StringSet> generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getObjectColumnSupplier(generator, ColumnType.TEXTSET);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TEXT_SET));
		return this;
	}

	/**
	 * Replaces the column with the given label by an object column of type {@link TypeId#TEXT_LIST} filled by the given
	 * generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceTextlist(String label, IntFunction<StringList> generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getObjectColumnSupplier(generator, ColumnType.TEXTLIST);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TEXT_LIST));
		return this;
	}

	/**
	 * Replaces the column with the given label by a time column filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceTime(String label, IntFunction<LocalTime> generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getTimeColumnSupplier(generator);
		columnSources.put(label, new ColumnSource(supplier, TypeId.TIME));
		return this;
	}

	/**
	 * Replaces the column with the given label by a date-time column filled by the given generator. The generated
	 * date-time column will be of nanosecond precision. If you only want second precision, please create a column
	 * with {@link Buffers#dateTimeBuffer(int, boolean)} and use {@link #add(String, Column)}.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator is {@code null}
	 */
	public synchronized TableBuilder replaceDateTime(String label, IntFunction<Instant> generator) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Supplier<Column> supplier = getDateTimeColumnSupplier(generator);
		columnSources.put(label, new ColumnSource(supplier, TypeId.DATE_TIME));
		return this;
	}

	/**
	 * Replaces the column with the given label by a nominal boolean column with the given positive value of the given
	 * type filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled. It must return exactly two different values (not
	 * 		counting {@code null}) where one is the positive value.
	 * @param positiveValue
	 * 		the positive value of the boolean column
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the generator or the label is {@code null}
	 */
	public TableBuilder replaceBoolean(String label, IntFunction<String> generator, String positiveValue) {
		return replaceBoolean(label, generator, positiveValue, ColumnType.NOMINAL);
	}

	/**
	 * Replaces the column with the given label by the given column. The column size must match the size of the
	 * original column.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param column
	 * 		the replacement column
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 * @throws NullPointerException
	 * 		if the column is {@code null}
	 * @throws IllegalStateException
	 * 		if the column size does not match the previously defined number of rows
	 */
	public synchronized TableBuilder replace(String label, Column column) {
		requireUsedLabel(label);
		Objects.requireNonNull(column, "Column must not be null");
		if (numberOfRows != column.size()) {
			throw new IllegalStateException(MISMATCHING_COLUMN_MESSAGE);
		}
		columnSources.put(label, new ColumnSource(column));
		return this;
	}

	/**
	 * Renames the column with the given label to the new label without changing the order of the columns. This also
	 * changes any {@link ColumnReference}s pointing to the old label to the new label.
	 *
	 * For bulk renaming use the {@link Table#rename(Map)} method instead as it is faster.
	 *
	 * @param label
	 * 		the label of the column to be renamed
	 * @param newLabel
	 * 		the new label
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the new label is already in use or invalid or there is not column with the given label
	 * @throws NullPointerException
	 * 		if the new label is {@code null}
	 */
	public synchronized TableBuilder rename(String label, String newLabel) {
		requireUsedLabel(label);
		if (label.equals(newLabel)) {
			//labels are the same, nothing to do
			return this;
		}
		requireValidUnusedLabel(newLabel);

		LinkedHashMap<String, ColumnSource> temp = new LinkedHashMap<>(columnSources);
		columnSources.clear();
		for (Map.Entry<String, ColumnSource> e : temp.entrySet()) {
			if (e.getKey().equals(label)) {
				columnSources.put(newLabel, e.getValue());
			} else {
				columnSources.put(e.getKey(), e.getValue());
			}
		}

		if (columnMetaData.containsKey(label)) {
			ensureMetaDataOwnership();
			columnMetaData.put(newLabel, columnMetaData.remove(label));
		}
		updateReferences(label, newLabel);
		return this;
	}

	/**
	 * Builds the table with the given execution context.
	 *
	 * @param ctx
	 * 		the execution context to use
	 * @return the {@link Table} build from the specified data
	 */
	public synchronized Table build(Context ctx) {
		String[] labels = new String[columnSources.size()];
		ColumnSource[] sources = new ColumnSource[columnSources.size()];
		int idx = 0;
		for (Map.Entry<String, ColumnSource> source : columnSources.entrySet()) {
			labels[idx] = source.getKey();
			sources[idx] = source.getValue();
			idx++;
		}

		// Transfer the ownership to the table tasks. This ensure that further calls to builder methods modifying the
		// column meta data do not corrupt the task (and table generated from it).
		ownsMetaData = false;

		return createTable(labels, columnMetaData, sources, ctx);

	}


	@Override
	public String toString() {
		return "Table builder (" + columnSources.size() + "x" + numberOfRows + ")\n" +
				TablePrinter.printLabels(columnSources.keySet().toArray(new String[0]));
	}

	/**
	 * Returns the currently existing column labels as unmodifiable set.
	 *
	 * @return the column labels as unmodifiable set
	 */
	public synchronized Set<String> labels() {
		return Collections.unmodifiableSet(columnSources.keySet());
	}

	/**
	 * Checks whether the builder currently contains a column using the given label.
	 *
	 * @param label
	 * 		the label to check
	 * @return {@code true} iff the builder contains a column with the given label
	 */
	public synchronized boolean contains(String label){
		return columnSources.containsKey(label);
	}

	/**
	 * Returns the {@link TypeId} of the column specified by the given label.
	 *
	 * @param label
	 * 		the label of the column to look up
	 * @return the {@link TypeId} of the column specified by the label
	 * @throws NullPointerException
	 * 		if the label is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public synchronized TypeId columnTypeId(String label) {
		requireUsedLabel(label);
		return columnSources.get(label).type;
	}

	/**
	 * Returns the meta data attached to the column with the given label (if any).
	 *
	 * @param label
	 * 		the column label
	 * @return an unmodifiable list of meta data
	 * @throws NullPointerException
	 * 		if the given label is {@code null}
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label
	 */
	public synchronized List<ColumnMetaData> getMetaData(String label){
		requireUsedLabel(label);
		return Table.getMetaData(label, columnMetaData);
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
	public synchronized  <T extends ColumnMetaData> List<T> getMetaData(String label, Class<T> type) {
		requireUsedLabel(label);
		return Table.getMetaData(label, type, columnMetaData);
	}

	/**
	 * Returns the first meta datum with the given type attached to the column with the given label (if any). Use of
	 * this method instead of {@link #getMetaData(String, Class)} is recommended if it is known that there can be at
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
	public synchronized <T extends ColumnMetaData> T getFirstMetaData(String label, Class<T> type) {
		requireUsedLabel(label);
		return Table.getFirstMetaData(label, type, columnMetaData);
	}

	/**
	 * Adds a categorical column with the given type that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @param type
	 * 		the column type of the column to create, must be categorical
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use or if the type not categorical
	 * @throws NullPointerException
	 * 		if the label, generator or type is {@code null}
	 */
	private synchronized TableBuilder addCategorical(String label, IntFunction<String> generator, ColumnType<String> type) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Type must be categorical");
		}
		Supplier<Column> supplier = getCategoricalColumnSupplier(generator, type);
		columnSources.put(label, new ColumnSource(supplier, type.id()));
		return this;
	}


	/**
	 * Adds a categorical column with the given type that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @param maxNumberOfValues
	 * 		the maximal number of different values that the generator generates, will be used to determine the data
	 * 		storage format
	 * @param type
	 * 		the column type of the column to create, must be categorical
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use or if the type not categorical
	 * @throws NullPointerException
	 * 		if the label, generator or type is {@code null}
	 */
	private synchronized TableBuilder addCategorical(String label, IntFunction<String> generator, int maxNumberOfValues,
														 ColumnType<String> type) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Type must be categorical");
		}
		Supplier<Column> supplier = getCategoricalColumnSupplier(generator, type, maxNumberOfValues);
		columnSources.put(label, new ColumnSource(supplier, type.id()));
		return this;
	}


	private void ensureMetaDataOwnership() {
		if (!ownsMetaData) {
			Map<String, List<ColumnMetaData>> copy = new HashMap<>(columnMetaData.size());
			for (Map.Entry<String, List<ColumnMetaData>> entry: columnMetaData.entrySet()) {
				copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
			}
			columnMetaData = copy;
			ownsMetaData = true;
		}
	}

	private boolean containsMetaData(String label, ColumnMetaData metaData) {
		if (columnMetaData.containsKey(label)) {
			List<ColumnMetaData> list = columnMetaData.get(label);
			return list.contains(metaData);
		}
		return false;
	}

	private boolean containsMetaDataType(String label, ColumnMetaData metaData) {
		if (columnMetaData.containsKey(label)) {
			String type = metaData.type();
			for (ColumnMetaData meta : columnMetaData.get(label)) {
				if (type.equals(meta.type())) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Adds the meta data to the given column. Validates the uniqueness properties but nothing else.
	 *
	 * @param label
	 * 		the column label
	 * @param metaData
	 * 		the meta data to add
	 */
	private void addMetaDataUnchecked(String label, ColumnMetaData metaData) {
		boolean duplicate = false;
		switch (metaData.uniqueness()) {
			case NONE:
				// Column meta data type need not be unique, but we still need to prevent duplicates.
				if (containsMetaData(label, metaData)) {
					duplicate = true;
				}
				break;
			case COLUMN:
				if (containsMetaDataType(label, metaData)) {
					throw new IllegalStateException("Column meta data must be unique per column");
				}
				break;
			case TABLE:
				for (String columnLabel : columnSources.keySet()) {
					if (containsMetaDataType(columnLabel, metaData)) {
						throw new IllegalStateException("Column meta data must be unique per table");
					}
				}
				break;
		}
		if (!duplicate) {
			List<ColumnMetaData> list = columnMetaData.computeIfAbsent(label, s -> new ArrayList<>(1));
			list.add(metaData);
		}
	}

	/**
	 * Updates all references that point to the old label to the new label.
	 */
	private void updateReferences(String oldLabel, String newLabel) {
		for (Map.Entry<String, List<ColumnMetaData>> entry : columnMetaData.entrySet()) {
			int index = 0;
			for (ColumnMetaData metaData : entry.getValue()) {
				if (metaData instanceof ColumnReference) {
					if (oldLabel.equals(((ColumnReference) metaData).getColumn())) {
						ensureMetaDataOwnership();
						columnMetaData.get(entry.getKey()).set(index, new ColumnReference(newLabel,
								((ColumnReference) metaData).getValue()));
					}
					break; //only one reference per label
				}
				index++;
			}
		}
	}

	/**
	 * Creates a supplier for a time column from the given generator.
	 */
	private Supplier<Column> getTimeColumnSupplier(IntFunction<LocalTime> generator) {
		return () -> {
			TimeBuffer buffer = Buffers.timeBuffer(numberOfRows, false);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for a date-time column from the given generator.
	 */
	private Supplier<Column> getDateTimeColumnSupplier(IntFunction<Instant> generator) {
		return () -> {
			DateTimeBuffer buffer = Buffers.dateTimeBuffer(numberOfRows, true, false);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for a categorical column from the given parameters.
	 */
	private Supplier<Column> getCategoricalColumnSupplier(IntFunction<String> generator, ColumnType<String> type) {
		return () -> {
			Int32NominalBuffer buffer = BufferAccessor.get().newInt32Buffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for a categorical column from the given parameters. Decides on a format depending on
	 * maxValues.
	 */
	private Supplier<Column> getCategoricalColumnSupplier(IntFunction<String> generator, ColumnType<String> type,
															  int maxValues) {
		maxValues = Math.min(maxValues, numberOfRows);
		if (maxValues <= IntegerFormats.Format.UNSIGNED_INT2.maxValue()) {
			return getCategoricalUInt2Supplier(generator, type);
		} else if (maxValues <= IntegerFormats.Format.UNSIGNED_INT4.maxValue()) {
			return getCategoricalUInt4Supplier(generator, type);
		} else if (maxValues <= IntegerFormats.Format.UNSIGNED_INT8.maxValue()) {
			return getCategoricalUInt8Supplier(generator, type);
		} else if (maxValues <= IntegerFormats.Format.UNSIGNED_INT16.maxValue()) {
			return getCategoricalUInt16Supplier(generator, type);
		} else {
			return getCategoricalColumnSupplier(generator, type);
		}
	}

	/**
	 * Creates a supplier for a UInt2 categorical column from the given parameters.
	 */
	private Supplier<Column> getCategoricalUInt2Supplier(IntFunction<String> generator, ColumnType<String> type) {
		return () -> {
			UInt2NominalBuffer buffer = BufferAccessor.get().newUInt2Buffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for a UInt4 categorical column from the given parameters.
	 */
	private Supplier<Column> getCategoricalUInt4Supplier(IntFunction<String> generator, ColumnType<String> type) {
		return () -> {
			UInt4NominalBuffer buffer = BufferAccessor.get().newUInt4Buffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for a UInt8 categorical column from the given parameters.
	 */
	private Supplier<Column> getCategoricalUInt8Supplier(IntFunction<String> generator, ColumnType<String> type) {
		return () -> {
			UInt8NominalBuffer buffer = BufferAccessor.get().newUInt8Buffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for a UInt16 categorical column from the given parameters.
	 */
	private Supplier<Column> getCategoricalUInt16Supplier(IntFunction<String> generator, ColumnType<String> type) {
		return () -> {
			UInt16NominalBuffer buffer = BufferAccessor.get().newUInt16Buffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Creates a supplier for an Object column from the given parameters.
	 */
	private <T> Supplier<Column> getObjectColumnSupplier(IntFunction<T> generator, ColumnType<T> type) {
		return () -> {
			ObjectBuffer<T> buffer = BufferAccessor.get().newObjectBuffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toColumn();
		};
	}

	/**
	 * Adds a boolean column with the given positive value and type that is filled as specified by the generator.
	 *
	 * @param label
	 * 		the label for the new column
	 * @param generator
	 * 		the function specifying how the column should be filled. It must return exactly two different values (not
	 * 		counting {@code null}) where one is the positive value.
	 * @param positiveValue
	 * 		the positive value of the boolean column
	 * @param type
	 * 		the column type of the column to create, must be categorical
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if the label is invalid or already in use or if the type is not categorical
	 * @throws NullPointerException
	 * 		if the label, generator or type is {@code null}
	 */
	private synchronized TableBuilder addBoolean(String label, IntFunction<String> generator, String positiveValue,
													ColumnType<String> type) {
		requireValidUnusedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException("Type must have categorical as category.");
		}
		columnSources.put(label, new ColumnSource(getBooleanColumnSupplier(generator, positiveValue, type), type.id()));
		return this;
	}

	/**
	 * Creates a supplier for a boolean column from the given parameters.
	 */
	private Supplier<Column> getBooleanColumnSupplier(IntFunction<String> generator, String positiveValue,
														  ColumnType<String> type) {
		return () -> {
			UInt2NominalBuffer buffer = BufferAccessor.get().newUInt2Buffer(type, numberOfRows);
			for (int i = 0; i < buffer.size(); i++) {
				buffer.set(i, generator.apply(i));
			}
			return buffer.toBooleanColumn(positiveValue);
		};
	}

	/**
	 * Replaces the column with the given label by a categorical column of the given type filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @param type
	 * 		the column type of the column created by the generator, must be categorical
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label or if the type is not categorical
	 * @throws NullPointerException
	 * 		if the generator or the type is {@code null}
	 */
	private synchronized TableBuilder replaceCategorical(String label, IntFunction<String> generator,
															ColumnType<String> type) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException(MESSAGE_TYPE_CATEGORICAL);
		}
		Supplier<Column> supplier = getCategoricalColumnSupplier(generator, type);
		columnSources.put(label, new ColumnSource(supplier, type.id()));
		return this;
	}

	/**
	 * Replaces the column with the given label by a categorical column of the given type filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled
	 * @param maxNumberOfValues
	 * 		the maximal number of different values that the generator generates, will be used to determine the data
	 * 		storage format
	 * @param type
	 * 		the column type of the column created by the generator, must be categorical
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label or if the type is not categorical
	 * @throws NullPointerException
	 * 		if the generator or the type is {@code null}
	 */
	private synchronized TableBuilder replaceCategorical(String label, IntFunction<String> generator,
															int maxNumberOfValues, ColumnType<String> type) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException(MESSAGE_TYPE_CATEGORICAL);
		}
		Supplier<Column> supplier = getCategoricalColumnSupplier(generator, type, maxNumberOfValues);
		columnSources.put(label, new ColumnSource(supplier, type.id()));
		return this;
	}

	/**
	 * Replaces the column with the given label by a boolean column with the given positive value of the given type
	 * filled by the given generator.
	 *
	 * @param label
	 * 		the label of the column to replace
	 * @param generator
	 * 		the function specifying how the column should be filled. It must return exactly two different values (not
	 * 		counting {@code null}) where one is the positive value.
	 * @param positiveValue
	 * 		the positive value of the boolean column
	 * @param type
	 * 		the categorical column type of the column created by the generator
	 * @return the builder
	 * @throws IllegalArgumentException
	 * 		if there is no column with the given label or if the type is not categorical
	 * @throws NullPointerException
	 * 		if the generator or the type is {@code null}
	 */
	private synchronized TableBuilder replaceBoolean(String label, IntFunction<String> generator, String positiveValue,
														ColumnType<String> type) {
		requireUsedLabel(label);
		Objects.requireNonNull(generator, MESSAGE_GENERATOR_NULL);
		Objects.requireNonNull(type, MESSAGE_TYPE_NULL);
		if (type.category() != Column.Category.CATEGORICAL) {
			throw new IllegalArgumentException(MESSAGE_TYPE_CATEGORICAL);
		}
		columnSources.put(label, new ColumnSource(getBooleanColumnSupplier(generator, positiveValue, type), type.id()));
		return this;
	}

	/**
	 * Builds a new table from the given labels and sources and meta data inside the given context.
	 */
	private Table createTable(String[] labels, Map<String, List<ColumnMetaData>> columnMetaData,
							  ColumnSource[] sources, Context context) {
		return ExecutionUtils.run(() -> {
			Column[] columns = new Column[sources.length];
			for (int i = 0; i < sources.length; i++) {
				context.requireActive();
				columns[i] = getColumn(sources[i]);
			}
			if (columns.length == 0) {
				return new Table(numberOfRows);
			}
			return new Table(columns, labels, columnMetaData);
		}, context);
	}

	/**
	 * @return the column as specified by the information
	 */
	private Column getColumn(ColumnSource information) {
		if (information.column != null) {
			return information.column;
		}
		if (information.supplier != null) {
			return information.supplier.get();
		}
		double[] data = null;
		Column.TypeId type = TypeId.REAL;
		if (information.generator != null) {
			data = new double[numberOfRows];
			if (information.type == TypeId.INTEGER_53_BIT) {
				type = TypeId.INTEGER_53_BIT;
			}
			fillData(information, data);
		}
		return ColumnAccessor.get().newNumericColumn(type, data);
	}

	/**
	 * Fills the data array with the generator from information, rounding when the type of the information is {@link
	 * TypeId#INTEGER_53_BIT}.
	 *
	 * @param information
	 * 		the information for filling the data via generator
	 * @param data
	 * 		the data array to fill
	 */
	private void fillData(ColumnSource information, double[] data) {
		if (information.type == TypeId.INTEGER_53_BIT) {
			for (int j = 0; j < numberOfRows; j++) {
				double value = information.generator.applyAsDouble(j);
				if (Double.isFinite(value)) {
					value = Math.round(value);
				}
				data[j] = value;
			}
		} else {
			for (int j = 0; j < numberOfRows; j++) {
				data[j] = information.generator.applyAsDouble(j);
			}
		}
	}


	/**
	 * Checks that the label is valid and not used by another column.
	 *
	 * @throws IllegalArgumentException
	 * 		if label is already in use or invalid
	 * @throws NullPointerException
	 * 		if label is {@code null}
	 */
	private void requireValidUnusedLabel(String label) {
		if (columnSources.containsKey(label)) {
			throw new IllegalArgumentException("Duplicate column label: " + label);
		}
		Table.requireValidLabel(label);
	}

	/**
	 * Checks that the label is used for a column.
	 *
	 * @throws IllegalArgumentException
	 * 		if the label is not in use
	 */
	private void requireUsedLabel(String label) {
		if (label == null) {
			throw new NullPointerException("Label must not be null");
		}
		if (!columnSources.containsKey(label)) {
			throw new IllegalArgumentException(UNKNOWN_LABEL_MESSAGE + label);
		}
	}
}
