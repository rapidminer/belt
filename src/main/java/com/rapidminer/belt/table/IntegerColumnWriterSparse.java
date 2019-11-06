package com.rapidminer.belt.table;

import com.rapidminer.belt.column.Column;


/**
 * A sparse {@link NumericColumnWriter} that grows its size to fit the given data. It stores integer values in a memory
 * efficient sparse format.
 *
 * @author Kevin Majchrzak
 */
final class IntegerColumnWriterSparse extends RealColumnWriterSparse {

	/**
	 * Creates a sparse column writer of the given length to create a sparse {@link Column} of type id {@link
	 * Column.TypeId#INTEGER}.
	 *
	 * @param defaultValue
	 * 		the data's (usually most common) default value.
	 */
	IntegerColumnWriterSparse(double defaultValue) {
		super(Double.isFinite(defaultValue) ? Math.round(defaultValue) : defaultValue);
	}

	/**
	 * Sets the next logical index to the given value. Finite non-integer values will be rounded.
	 */
	@Override
	protected void setNext(double value) {
		super.setNext(Double.isFinite(value) ? Math.round(value) : value);
	}

	/**
	 * Returns the buffer's {@link Column.TypeId}.
	 *
	 * @return {@link Column.TypeId#INTEGER}.
	 */
	@Override
	protected Column.TypeId type() {
		return Column.TypeId.INTEGER;
	}
}
