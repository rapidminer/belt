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

package com.rapidminer.belt.column;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;

import com.rapidminer.belt.reader.CategoricalReader;
import com.rapidminer.belt.util.IntegerFormats.Format;
import com.rapidminer.belt.util.IntegerFormats.PackedIntegers;
import com.rapidminer.belt.util.Mapping;
import com.rapidminer.belt.util.Order;
import com.rapidminer.belt.util.Sorting;


/**
 * Sparse implementation of a {@link CategoricalColumn}. It uses a more memory efficient representation of the data than
 * the default dense {@link SimpleCategoricalColumn} while still being comparably fast or faster on sparse data.
 * Caution: This implementation does not support the 2 bit and 4 bit format.
 *
 * @author Kevin Majchrzak
 */
class CategoricalSparseColumn<R> extends CategoricalColumn<R> {

	/**
	 * Column falls back to a dense column if its density is above this value for INT32 format.
	 */
	static final double MAX_DENSITY_INT32 = 0.5d;

	/**
	 * Column falls back to a dense column if its density is above this value for UINT16 format.
	 */
	static final double MAX_DENSITY_UINT16 = 0.33d;

	/**
	 * Column falls back to a dense column if its density is above this value for UINT8 format.
	 */
	static final double MAX_DENSITY_UINT8 = 0.2d;

	private static final String NULL_DATA = "Data must not be null";
	private static final String NULL_MAPPING = "Categorical dictionary must not be null";
	private static final String NOT_SUPPORTED_2BIT = "Sparse categorical column does not support 2 bit data.";
	private static final String NOT_SUPPORTED_4BIT = "Sparse categorical column does not support 4 bit data.";

	/**
	 * The internal data format. This columns supports 32, 16 and 8 bit only.
	 */
	private final Format format;

	/**
	 * The non default values in 8 bit unsigned byte format. If this column is not in 8 bit format the array will be
	 * null.
	 */
	private final byte[] byteNonDefaultValues;

	/**
	 * The non default values in 16 bit unsigned short format. If this column is not in 16 bit format the array will be
	 * null.
	 */
	private final short[] shortNonDefaultValues;

	/**
	 * The non default values in 32 bit signed int format. If this column is not in 32 bit format the array will be
	 * null.
	 */
	private final int[] intNonDefaultValues;

	/**
	 * The indices of the non-default values.
	 */
	private final int[] nonDefaultIndices;

	/**
	 * The columns most common (default) value as unsigned integer.
	 */
	private final int defaultValueAsUnsignedInt;

	/**
	 * The default value's double representation.
	 */
	private final double defaultValueAsDouble;

	/**
	 * The default value's Object representation.
	 */
	private final Object defaultValueAsObject;

	/**
	 * The columns logical size.
	 */
	private final int size;

	/**
	 * Mapping used to map from the internal integer representation to categorical values.
	 */
	private final Dictionary<R> dictionary;

	/**
	 * Creates a new CategoricalSparseColumn with the given type, defaultValue and data in form of {@link
	 * PackedIntegers}. The dictionary maps the internal integer values to their corresponding categories. Caution: This
	 * implementation does not support 2 bit or 4 bit format data.
	 *
	 * @param type
	 * 		the column type.
	 * @param bytes
	 * 		the byte data.
	 * @param dictionary
	 * 		the mapping from integers to categories.
	 * @param defaultValue
	 * 		the default value (usually the most common value as this is most efficient).
	 */
	CategoricalSparseColumn(ColumnType<R> type, PackedIntegers bytes, Dictionary<R> dictionary, byte defaultValue) {
		super(type, bytes.size());
		if (!Format.UNSIGNED_INT8.equals(bytes.format())) {
			throw new IllegalArgumentException("Sparse categorical column cannot handle byte format " + bytes.format());
		}
		this.size = bytes.size();
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		Objects.requireNonNull(bytes, NULL_DATA);
		this.defaultValueAsUnsignedInt = Byte.toUnsignedInt(defaultValue);
		defaultValueAsDouble = toDouble(defaultValueAsUnsignedInt);
		defaultValueAsObject = toObject(defaultValueAsUnsignedInt);
		this.format = bytes.format();
		this.shortNonDefaultValues = null;
		this.intNonDefaultValues = null;
		// fill non-default indices and values
		byte[] data = bytes.data();
		int numberOfNonDefaults = 0;
		for (byte i : data) {
			if (i != defaultValue) {
				numberOfNonDefaults++;
			}
		}
		nonDefaultIndices = new int[numberOfNonDefaults];
		byteNonDefaultValues = new byte[numberOfNonDefaults];
		int index = 0;
		for (int i = 0, len = data.length; i < len; i++) {
			if (data[i] != defaultValue) {
				nonDefaultIndices[index] = i;
				byteNonDefaultValues[index++] = data[i];
			}
		}
	}

	/**
	 * Creates a new CategoricalSparseColumn with the given type, defaultValue and data. The dictionary maps the intern
	 * integer values to their corresponding categories.
	 *
	 * @param type
	 * 		the column type.
	 * @param data
	 * 		the short data.
	 * @param dictionary
	 * 		the mapping from integers to categories.
	 * @param defaultValue
	 * 		the default value (usually the most common value as this is most efficient).
	 */
	CategoricalSparseColumn(ColumnType<R> type, short[] data, Dictionary<R> dictionary, short defaultValue) {
		super(type, data.length);
		this.size = data.length;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.defaultValueAsUnsignedInt = Short.toUnsignedInt(defaultValue);
		defaultValueAsDouble = toDouble(defaultValueAsUnsignedInt);
		defaultValueAsObject = toObject(defaultValueAsUnsignedInt);
		this.format = Format.UNSIGNED_INT16;
		this.byteNonDefaultValues = null;
		this.intNonDefaultValues = null;
		// fill non-default indices and values
		int numberOfNonDefaults = 0;
		for (short i : data) {
			if (i != defaultValue) {
				numberOfNonDefaults++;
			}
		}
		nonDefaultIndices = new int[numberOfNonDefaults];
		shortNonDefaultValues = new short[numberOfNonDefaults];
		int index = 0;
		for (int i = 0, len = data.length; i < len; i++) {
			if (data[i] != defaultValue) {
				nonDefaultIndices[index] = i;
				shortNonDefaultValues[index++] = data[i];
			}
		}
	}

	/**
	 * Creates a new CategoricalSparseColumn with the given type, defaultValue and data. The dictionary maps the intern
	 * integer values to their corresponding categories.
	 *
	 * @param type
	 * 		the column type.
	 * @param data
	 * 		the integer data.
	 * @param dictionary
	 * 		the mapping from integers to categories.
	 * @param defaultValue
	 * 		the default value (usually the most common value as this is most efficient).
	 */
	CategoricalSparseColumn(ColumnType<R> type, int[] data, Dictionary<R> dictionary, int defaultValue) {
		super(type, data.length);
		this.size = data.length;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.defaultValueAsUnsignedInt = defaultValue;
		defaultValueAsDouble = toDouble(defaultValueAsUnsignedInt);
		defaultValueAsObject = toObject(defaultValueAsUnsignedInt);
		this.format = Format.SIGNED_INT32;
		this.byteNonDefaultValues = null;
		this.shortNonDefaultValues = null;
		// fill non-default indices and values
		int numberOfNonDefaults = 0;
		for (int i : data) {
			if (i != defaultValue) {
				numberOfNonDefaults++;
			}
		}
		nonDefaultIndices = new int[numberOfNonDefaults];
		intNonDefaultValues = new int[numberOfNonDefaults];
		int index = 0;
		for (int i = 0, len = data.length; i < len; i++) {
			if (data[i] != defaultValue) {
				nonDefaultIndices[index] = i;
				intNonDefaultValues[index++] = data[i];
			}
		}
	}

	/**
	 * Creates a new sparse categorical column from the given sparse data (similar to {@link
	 * DoubleSparseColumn#DoubleSparseColumn(TypeId, double, int[], double[], int)}).
	 */
	CategoricalSparseColumn(ColumnType<R> type, int[] nonDefaultIndices, byte[] nonDefaultValues,
									Dictionary<R> dictionary, byte defaultValue, int size) {
		super(type, size);
		this.size = size;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.defaultValueAsUnsignedInt = Byte.toUnsignedInt(defaultValue);
		defaultValueAsDouble = toDouble(defaultValueAsUnsignedInt);
		defaultValueAsObject = toObject(defaultValueAsUnsignedInt);
		this.format = Format.UNSIGNED_INT8;
		this.shortNonDefaultValues = null;
		this.intNonDefaultValues = null;
		this.nonDefaultIndices = nonDefaultIndices;
		this.byteNonDefaultValues = nonDefaultValues;
	}

	/**
	 * Creates a new sparse categorical column from the given sparse data (similar to {@link
	 * DoubleSparseColumn#DoubleSparseColumn(TypeId, double, int[], double[], int)}).
	 */
	CategoricalSparseColumn(ColumnType<R> type, int[] nonDefaultIndices, short[] nonDefaultValues,
									Dictionary<R> dictionary, short defaultValue, int size) {
		super(type, size);
		this.size = size;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.defaultValueAsUnsignedInt = Short.toUnsignedInt(defaultValue);
		defaultValueAsDouble = toDouble(defaultValueAsUnsignedInt);
		defaultValueAsObject = toObject(defaultValueAsUnsignedInt);
		this.format = Format.UNSIGNED_INT16;
		this.byteNonDefaultValues = null;
		this.intNonDefaultValues = null;
		this.nonDefaultIndices = nonDefaultIndices;
		this.shortNonDefaultValues = nonDefaultValues;
	}

	/**
	 * Creates a new sparse categorical column from the given sparse data (similar to {@link
	 * DoubleSparseColumn#DoubleSparseColumn(TypeId, double, int[], double[], int)}).
	 */
	CategoricalSparseColumn(ColumnType<R> type, int[] nonDefaultIndices, int[] nonDefaultValues,
									Dictionary<R> dictionary, int defaultValue, int size) {
		super(type, size);
		this.size = size;
		this.dictionary = Objects.requireNonNull(dictionary, NULL_MAPPING);
		this.defaultValueAsUnsignedInt = defaultValue;
		defaultValueAsDouble = toDouble(defaultValueAsUnsignedInt);
		defaultValueAsObject = toObject(defaultValueAsUnsignedInt);
		this.format = Format.SIGNED_INT32;
		this.shortNonDefaultValues = null;
		this.byteNonDefaultValues = null;
		this.nonDefaultIndices = nonDefaultIndices;
		this.intNonDefaultValues = nonDefaultValues;
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt2(double[] array, int rowIndex) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_2BIT);
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt4(double[] array, int rowIndex) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_4BIT);
	}

	@Override
	void fillFromUInt8(double[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsDouble);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = toDouble(Byte.toUnsignedInt(byteNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromUInt16(double[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsDouble);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = toDouble(Short.toUnsignedInt(shortNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromInt32(double[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsDouble);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = toDouble(intNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt2(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_2BIT);
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt4(double[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_4BIT);
	}

	@Override
	void fillFromUInt8(double[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsDouble;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						toDouble(Byte.toUnsignedInt(byteNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromUInt16(double[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsDouble;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						toDouble(Short.toUnsignedInt(shortNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromInt32(double[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsDouble;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						toDouble(intNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt2(int[] array, int rowIndex) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_2BIT);
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt4(int[] array, int rowIndex) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_4BIT);
	}

	@Override
	void fillFromUInt8(int[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsUnsignedInt);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = Byte.toUnsignedInt(byteNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromUInt16(int[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsUnsignedInt);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = Short.toUnsignedInt(shortNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromInt32(int[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsUnsignedInt);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = intNonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt2(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_2BIT);
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt4(int[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_4BIT);
	}

	@Override
	void fillFromUInt8(int[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsUnsignedInt;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						Byte.toUnsignedInt(byteNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromUInt16(int[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsUnsignedInt;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						Short.toUnsignedInt(shortNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromInt32(int[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsUnsignedInt;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						intNonDefaultValues[nonDefaultIndex];
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt2(Object[] array, int rowIndex) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_2BIT);
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt4(Object[] array, int rowIndex) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_4BIT);
	}

	@Override
	void fillFromUInt8(Object[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsObject);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = toObject(Byte.toUnsignedInt(byteNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromUInt16(Object[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsObject);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = toObject(Short.toUnsignedInt(shortNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromInt32(Object[] array, int rowIndex) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		Arrays.fill(array, defaultValueAsObject);
		if (nonDefaultIndices.length == 0) {
			// only default values
			return;
		}

		int maxRowIndex = Math.min(rowIndex + array.length, size);
		int nonDefaultIndex = ColumnUtils.findNextIndex(nonDefaultIndices, rowIndex);

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[nonDefaultPosition - rowIndex] = toObject(intNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt2(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_2BIT);
	}

	/**
	 * This method is not supported. Trying to call it will lead to an exception.
	 *
	 * @throws UnsupportedOperationException
	 * 		if the method is called.
	 */
	@Override
	void fillFromUInt4(Object[] array, int startIndex, int arrayOffset, int arrayStepSize) {
		throw new UnsupportedOperationException(NOT_SUPPORTED_4BIT);
	}

	@Override
	void fillFromUInt8(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsObject;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						toObject(Byte.toUnsignedInt(byteNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromUInt16(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsObject;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						toObject(Short.toUnsignedInt(shortNonDefaultValues[nonDefaultIndex]));
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}

	@Override
	void fillFromInt32(Object[] array, int rowIndex, int arrayOffset, int arrayStepSize) {
		if (rowIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(rowIndex);
		}

		// fill all free slots in the array with default values
		for (int i = arrayOffset, len = array.length; i < len; i += arrayStepSize) {
			array[i] = defaultValueAsObject;
		}

		int maxRowIndex = Math.min(rowIndex + (array.length - arrayOffset - 1) / arrayStepSize + 1, size);

		// first non-default whose position in the column is greater or equal to the given row index
		int nonDefaultIndex = Arrays.binarySearch(nonDefaultIndices, rowIndex);
		if (nonDefaultIndex < 0) {
			// see documentation of binary search
			nonDefaultIndex = -nonDefaultIndex - 1;
		}

		if (nonDefaultIndex < nonDefaultIndices.length) {
			int nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			while (nonDefaultPosition < maxRowIndex) {
				array[(nonDefaultPosition - rowIndex) * arrayStepSize + arrayOffset] =
						toObject(intNonDefaultValues[nonDefaultIndex]);
				nonDefaultIndex++;
				if (nonDefaultIndex >= nonDefaultIndices.length) {
					break;
				}
				nonDefaultPosition = nonDefaultIndices[nonDefaultIndex];
			}
		} // else: only default values in the array
	}


	@Override
	Column map(int[] mapping, boolean preferView) {
		if (mapping.length == 0) {
			return stripData();
		}
		switch (format) {
			case UNSIGNED_INT8:
				return mapUInt8(mapping);
			case UNSIGNED_INT16:
				return mapUInt16(mapping);
			case SIGNED_INT32:
				return mapInt32(mapping);
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	CategoricalColumn<R> remap(Dictionary<R> newDictionary, int[] remapping) {
		switch (format) {
			case UNSIGNED_INT8:
				return new RemappedCategoricalSparseColumn<>(type(), nonDefaultIndices, byteNonDefaultValues,
						newDictionary, remapping, (byte) defaultValueAsUnsignedInt, size);
			case UNSIGNED_INT16:
				return new RemappedCategoricalSparseColumn<>(type(), nonDefaultIndices, shortNonDefaultValues,
						newDictionary, remapping, (short) defaultValueAsUnsignedInt, size);
			case SIGNED_INT32:
				return new RemappedCategoricalSparseColumn<>(type(), nonDefaultIndices, intNonDefaultValues,
						newDictionary, remapping, defaultValueAsUnsignedInt, size);
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public Format getFormat() {
		return format;
	}

	@Override
	protected PackedIntegers getByteData() {
		if (format == Format.UNSIGNED_INT8) {
			byte[] bytes = new byte[size];
			ColumnUtils.fillSparseBytesIntoByteArray(bytes, 0, (byte) defaultValueAsUnsignedInt, nonDefaultIndices,
					byteNonDefaultValues, size);
			return new PackedIntegers(bytes, Format.UNSIGNED_INT8, size);
		} else {
			return null;
		}
	}

	@Override
	protected short[] getShortData() {
		if (format == Format.UNSIGNED_INT16) {
			short[] shorts = new short[size];
			ColumnUtils.fillSparseShortsIntoShortArray(shorts, 0, (short) defaultValueAsUnsignedInt, nonDefaultIndices,
					shortNonDefaultValues, size);
			return shorts;
		} else {
			return null;
		}
	}

	@Override
	protected int[] getIntData() {
		if (format == Format.SIGNED_INT32) {
			int[] ints = new int[size];
			ColumnUtils.fillSparseIntsIntoIntArray(ints, 0, defaultValueAsUnsignedInt, nonDefaultIndices,
					intNonDefaultValues, size);
			return ints;
		} else {
			return null;
		}
	}

	@Override
	protected Dictionary<R> getDictionary() {
		return dictionary;
	}

	@Override
	protected CategoricalColumn<R> swapDictionary(Dictionary<R> newDictionary) {
		switch (format) {
			case UNSIGNED_INT8:
				return new CategoricalSparseColumn<>(type(), nonDefaultIndices, byteNonDefaultValues, newDictionary,
						(byte) defaultValueAsUnsignedInt, size);
			case UNSIGNED_INT16:
				return new CategoricalSparseColumn<>(type(), nonDefaultIndices, shortNonDefaultValues, newDictionary,
						(short) defaultValueAsUnsignedInt, size);
			case SIGNED_INT32:
				return new CategoricalSparseColumn<>(type(), nonDefaultIndices, intNonDefaultValues, newDictionary,
						defaultValueAsUnsignedInt, size);
			default:
				throw new IllegalStateException();
		}
	}

	@Override
	public int[] sort(Order order) {
		// 1) calc sorted mapping
		int[] sortedMapping = getSortedMapping(order);
		// find where default value index is in sorted result
		int newIndexForDefault = nonDefaultIndices.length;
		int defaultPosition = 0;
		for (int i = 0; i < sortedMapping.length; i++) {
			if (sortedMapping[i] == newIndexForDefault) {
				defaultPosition = i;
				break;
			}
		}
		// 2) map the non-default indices using the sorted mapping
		// (This will lead to a 0 at the defaultPosition because newIndexForDefault is out of range for the
		// nonDefaultIndices array. But that is no problem as we are not using this value.)
		int[] sortedNonDefaultIndices = Mapping.apply(nonDefaultIndices, sortedMapping);

		// 3) add the mapped non-default indices with values < the default value to the result
		int[] sortResult = new int[size];
		System.arraycopy(sortedNonDefaultIndices, 0, sortResult, 0, defaultPosition);

		// 4) add the default indices after that
		int index = defaultPosition;
		int last = -1;
		for (int i : nonDefaultIndices) {
			for (int j = last + 1; j < i; j++) {
				sortResult[index++] = j;
			}
			last = i;
		}
		// add the default indices after last non-default until size
		for (int j = last + 1; j < size; j++) {
			sortResult[index++] = j;
		}

		// 5) add the rest of the non-default indices after that
		if (defaultPosition < sortedNonDefaultIndices.length - 1) {
			System.arraycopy(sortedNonDefaultIndices, defaultPosition + 1, sortResult, index,
					sortResult.length - index);
		}

		return sortResult;
	}

	/**
	 * Returns the columns (usually most common) default categorical index.
	 */
	int getDefaultValue(){
		return defaultValueAsUnsignedInt;
	}

	/**
	 * Implements {@link #map(int[], boolean)} for the 32 bit integer format.
	 */
	private Column mapInt32(int[] mapping) {
		SparseBitmap bitMap = new SparseBitmap(defaultValueAsUnsignedInt == CategoricalReader.MISSING_CATEGORY,
				nonDefaultIndices, size);

		int numberOfNonDefaults = bitMap.countNonDefaultIndices(mapping);
		if (mapping.length * MAX_DENSITY_INT32 < numberOfNonDefaults) {
			// column is not sparse enough anymore
			return makeDenseColumn(mapping);
		}

		int[] newNonDefaultIndices = new int[numberOfNonDefaults];
		int[] newNonDefaultValues = new int[numberOfNonDefaults];
		int nonDefaultIndex = 0;
		for (int i = 0; i < mapping.length; i++) {
			int bitMapIndex = bitMap.get(mapping[i]);
			if (bitMapIndex != SparseBitmap.DEFAULT_INDEX) {
				newNonDefaultIndices[nonDefaultIndex] = i;
				if (bitMapIndex == SparseBitmap.OUT_OF_BOUNDS_INDEX) {
					newNonDefaultValues[nonDefaultIndex++] = CategoricalReader.MISSING_CATEGORY;
				} else {
					newNonDefaultValues[nonDefaultIndex++] = intNonDefaultValues[bitMapIndex];
				}
			}
		}

		return new CategoricalSparseColumn<>(type(), newNonDefaultIndices, newNonDefaultValues, dictionary,
				defaultValueAsUnsignedInt, mapping.length);
	}

	/**
	 * Implements {@link #map(int[], boolean)} for the 16 bit integer format.
	 */
	private Column mapUInt16(int[] mapping) {
		SparseBitmap bitMap = new SparseBitmap(defaultValueAsUnsignedInt ==
				CategoricalReader.MISSING_CATEGORY, nonDefaultIndices, size);

		int numberOfNonDefaults = bitMap.countNonDefaultIndices(mapping);
		if (mapping.length * MAX_DENSITY_UINT16 < numberOfNonDefaults) {
			// column is not sparse enough anymore
			return makeDenseColumn(mapping);
		}

		int[] newNonDefaultIndices = new int[numberOfNonDefaults];
		short[] newNonDefaultValues = new short[numberOfNonDefaults];
		int nonDefaultIndex = 0;
		for (int i = 0; i < mapping.length; i++) {
			int bitMapIndex = bitMap.get(mapping[i]);
			if (bitMapIndex != SparseBitmap.DEFAULT_INDEX) {
				newNonDefaultIndices[nonDefaultIndex] = i;
				if (bitMapIndex == SparseBitmap.OUT_OF_BOUNDS_INDEX) {
					newNonDefaultValues[nonDefaultIndex++] = CategoricalReader.MISSING_CATEGORY;
				} else {
					newNonDefaultValues[nonDefaultIndex++] = shortNonDefaultValues[bitMapIndex];
				}
			}
		}

		return new CategoricalSparseColumn<>(type(), newNonDefaultIndices, newNonDefaultValues, dictionary,
				(short) defaultValueAsUnsignedInt, mapping.length);
	}

	/**
	 * Implements {@link #map(int[], boolean)} for the 8 bit integer format.
	 */
	private Column mapUInt8(int[] mapping) {
		SparseBitmap bitMap = new SparseBitmap(defaultValueAsUnsignedInt ==
				CategoricalReader.MISSING_CATEGORY, nonDefaultIndices, size);

		int numberOfNonDefaults = bitMap.countNonDefaultIndices(mapping);
		if (mapping.length * MAX_DENSITY_UINT8 < numberOfNonDefaults) {
			// column is not sparse enough anymore
			return makeDenseColumn(mapping);
		}

		int[] newNonDefaultIndices = new int[numberOfNonDefaults];
		byte[] newNonDefaultValues = new byte[numberOfNonDefaults];
		int nonDefaultIndex = 0;
		for (int i = 0; i < mapping.length; i++) {
			int bitMapIndex = bitMap.get(mapping[i]);
			if (bitMapIndex != SparseBitmap.DEFAULT_INDEX) {
				newNonDefaultIndices[nonDefaultIndex] = i;
				if (bitMapIndex == SparseBitmap.OUT_OF_BOUNDS_INDEX) {
					newNonDefaultValues[nonDefaultIndex++] = CategoricalReader.MISSING_CATEGORY;
				} else {
					newNonDefaultValues[nonDefaultIndex++] = byteNonDefaultValues[bitMapIndex];
				}
			}
		}

		return new CategoricalSparseColumn<>(type(), newNonDefaultIndices, newNonDefaultValues, dictionary,
				(byte) defaultValueAsUnsignedInt, mapping.length);
	}

	/**
	 * Returns the sorted mapping for the non-default values and 1x the default value added to the end.
	 */
	private int[] getSortedMapping(Order order) {
		Comparator<R> comparator = type().comparator();
		if (comparator == null) {
			throw new UnsupportedOperationException();
		}
		Comparator<R> comparatorWithNull = Comparator.nullsLast(comparator);
		switch (format) {
			case UNSIGNED_INT8:
				return getSortedMappingUINT8(order, comparatorWithNull);
			case UNSIGNED_INT16:
				return getSortedMappingUINT16(order, comparatorWithNull);
			case SIGNED_INT32:
				return getSortedMappingINT32(order, comparatorWithNull);
			default:
				throw new IllegalStateException();
		}
	}

	/**
	 * See {@link #getSortedMapping(Order)}.
	 */
	private int[] getSortedMappingUINT8(Order order, Comparator<R> comparatorWithNull) {
		int newIndexForDefault = nonDefaultIndices.length;
		byte[] toSort = Arrays.copyOf(byteNonDefaultValues, newIndexForDefault + 1);
		toSort[newIndexForDefault] = (byte) defaultValueAsUnsignedInt;
		return Sorting.sort(toSort.length,
				(a, b) -> comparatorWithNull.compare(dictionary.get(Byte.toUnsignedInt(toSort[a])),
						dictionary.get(Byte.toUnsignedInt(toSort[b]))), order);
	}

	/**
	 * See {@link #getSortedMapping(Order)}.
	 */
	private int[] getSortedMappingUINT16(Order order, Comparator<R> comparatorWithNull) {
		int newIndexForDefault = nonDefaultIndices.length;
		short[] toSort = Arrays.copyOf(shortNonDefaultValues, newIndexForDefault + 1);
		toSort[newIndexForDefault] = (short) defaultValueAsUnsignedInt;
		return Sorting.sort(toSort.length,
				(a, b) -> comparatorWithNull.compare(dictionary.get(Short.toUnsignedInt(toSort[a])),
						dictionary.get(Short.toUnsignedInt(toSort[b]))), order);
	}

	/**
	 * See {@link #getSortedMapping(Order)}.
	 */
	private int[] getSortedMappingINT32(Order order, Comparator<R> comparatorWithNull) {
		int newIndexForDefault = nonDefaultIndices.length;
		int[] toSort = Arrays.copyOf(intNonDefaultValues, newIndexForDefault + 1);
		toSort[newIndexForDefault] = defaultValueAsUnsignedInt;
		return Sorting.sort(toSort.length,
				(a, b) -> comparatorWithNull.compare(dictionary.get(toSort[a]),
						dictionary.get(toSort[b])), order);
	}

	/**
	 * Return the corresponding double value or {@link Double#NaN} iff the given value is equal to {@link
	 * CategoricalReader#MISSING_CATEGORY}.
	 */
	private double toDouble(int i) {
		if (i == CategoricalReader.MISSING_CATEGORY) {
			return Double.NaN;
		} else {
			return i;
		}
	}

	/**
	 * Return the corresponding Object value or {@code null} iff the given value is equal to {@link
	 * CategoricalReader#MISSING_CATEGORY}.
	 */
	private Object toObject(int i) {
		return dictionary.get(i);
	}

	/**
	 * Creates a {@link SimpleTimeColumn} by applying the given mapping to this column.
	 */
	private Column makeDenseColumn(int[] mapping) {
		switch (format) {
			case UNSIGNED_INT8:
				return new SimpleCategoricalColumn<>(type(), Mapping.apply(getByteData(), mapping), dictionary);
			case UNSIGNED_INT16:
				return new SimpleCategoricalColumn<>(type(), Mapping.apply(getShortData(), mapping), dictionary);
			case SIGNED_INT32:
				return new SimpleCategoricalColumn<>(type(), Mapping.apply(getIntData(), mapping), dictionary);
			default:
				throw new IllegalStateException();
		}
	}

}
