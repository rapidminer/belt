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

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import com.rapidminer.belt.Column.TypeId;


/**
 * Utility class for storing and retrieving {@link Table}s. The file format is the following with byte order big endian:
 *
 * <ol>
 * <li>6 bytes {@link #MAGIC_NUMBER}, 1 byte {@link #MAJOR_VERSION}, 1 byte {@link #MINOR_VERSION},</li>
 * <li>4 bytes width of the table (integer), 4 bytes height of the table (integer),</li>
 * <li>width times 4 bytes containing the type of the i-th column (integer),</li>
 * <li>width times 4 bytes containing the length of the i-th column name (integer),</li>
 * <li>all the bytes of the column names encoded in utf-8,</li>
 * <li>width times height 8 bytes containing the double array of the i-th column data.</li>
 * </ol>
 *
 * @author Gisa Meier
 */
final class TableFiler {

	private static final int VERSION_NUMBER_LENGTH = 2;
	private static final int INT_LENGTH = 4;
	private static final int DOUBLE_LENGTH = 8;

	static final byte[] MAGIC_NUMBER = "RMBelt".getBytes(StandardCharsets.US_ASCII);
	static final byte MAJOR_VERSION = 0;
	static final byte MINOR_VERSION = 1;

	/** Message for file with insufficient length */
	private static final String MESSAGE_INCOMPLETE_FILE = "Incomplete file";

	// Suppress default constructor for noninstantiability
	private TableFiler() {
		throw new AssertionError();
	}

	/**
	 * Stores the given table at the given path.
	 *
	 * @param table
	 * 		the table to store
	 * @param path
	 * 		where to store the table
	 * @throws IOException
	 * 		if writing to the file fails
	 */
	static void store(Table table, Path path) throws IOException {
		long lastPosition = 0;
		try (FileChannel channel = FileChannel.open(path,
				StandardOpenOption.CREATE,
				StandardOpenOption.READ,
				StandardOpenOption.WRITE)) {

			lastPosition = writeHeader(table, lastPosition, channel);

			lastPosition = writeColumnTypes(table, lastPosition, channel, Integer.MAX_VALUE);

			lastPosition = writeColumnNames(table.labelArray(), lastPosition, channel);

			for (Column column : table.getColumns()) {
				lastPosition = column.writeToChannel(channel, lastPosition);
			}
		}
	}

	/**
	 * Loads a {@link Table} from the given path.
	 *
	 * @param path
	 * 		path to the table file
	 * @return a new table
	 * @throws IOException
	 * 		if reading the file fails
	 * @throws IllegalArgumentException
	 * 		if the file does not contain a table
	 */
	static Table load(Path path) throws IOException {
		long lastPosition = 0;
		try (FileChannel channel = FileChannel.open(path,
				StandardOpenOption.CREATE,
				StandardOpenOption.READ)) {

			int[] size = new int[2];
			lastPosition = readHeader(size, lastPosition, channel);
			TypeId[] types = new Column.TypeId[size[0]];
			lastPosition = readColumnTypes(types, lastPosition, channel, Integer.MAX_VALUE);
			String[] columnNames = new String[size[0]];
			lastPosition = readColumnNames(columnNames, lastPosition, channel);

			Column[] columns = new Column[size[0]];
			for (int i = 0; i < columns.length; i++) {
				double[] data = new double[size[1]];
				lastPosition = readDoubleArray(lastPosition, channel, data, Integer.MAX_VALUE);
				columns[i] = new DoubleArrayColumn(types[i], data);
			}
			return new Table(columns, columnNames);
		}
	}


	/**
	 * Writes the header for the given table starting at position into the channel.
	 *
	 * @param table
	 * 		the table to write
	 * @param position
	 * 		where to start writing
	 * @param channel
	 * 		the channel to write to
	 * @return the position after writing
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeHeader(Table table, long position, FileChannel channel) throws IOException {
		ByteBuffer buffer = channel.map(READ_WRITE, position,
				MAGIC_NUMBER.length + VERSION_NUMBER_LENGTH + 2 * INT_LENGTH);
		buffer.order(ByteOrder.BIG_ENDIAN);

		buffer.put(MAGIC_NUMBER);
		position += MAGIC_NUMBER.length;

		buffer.put(MAJOR_VERSION);
		buffer.put(MINOR_VERSION);
		position += VERSION_NUMBER_LENGTH;

		buffer.putInt(table.width());
		buffer.putInt(table.height());
		position += 2 * INT_LENGTH;

		return position;
	}

	/**
	 * Reads the header starting at position from the channel.
	 *
	 * @param size
	 * 		array to store width and hight in
	 * @param position
	 * 		where to start reading
	 * @param channel
	 * 		the channel to read from
	 * @return the position after reading
	 * @throws IOException
	 * 		if reading fails
	 * @throws IllegalArgumentException
	 * 		if the magic number, the major or minor version is wrong or if the file is damaged or incomplete
	 */
	static long readHeader(int[] size, long position, FileChannel channel) throws IOException {
		int length = MAGIC_NUMBER.length + VERSION_NUMBER_LENGTH + 2 * INT_LENGTH;
		if (channel.size() < position + length) {
			throw new IllegalArgumentException(MESSAGE_INCOMPLETE_FILE);
		}
		ByteBuffer buffer = channel.map(READ_ONLY, position, length);
		buffer.order(ByteOrder.BIG_ENDIAN);

		byte[] magicRead = new byte[MAGIC_NUMBER.length];
		buffer.get(magicRead);
		if (!Arrays.equals(MAGIC_NUMBER, magicRead)) {
			throw new IllegalArgumentException("Wrong file format");
		}
		position += MAGIC_NUMBER.length;

		byte result = buffer.get();
		if (result != MAJOR_VERSION) {
			throw new IllegalArgumentException("Wrong major version: " + result + ", must be " + MAJOR_VERSION);
		}
		result = buffer.get();
		if (result != MINOR_VERSION) {
			throw new IllegalArgumentException("Wrong minor version: " + result + ", must be " + MINOR_VERSION);
		}
		position += VERSION_NUMBER_LENGTH;

		int width = buffer.getInt();
		int height = buffer.getInt();
		if (width < 0 || height < 0) {
			throw new IllegalArgumentException("Damaged data");
		}
		size[0] = width;
		size[1] = height;
		position += 2 * INT_LENGTH;

		return position;
	}


	/**
	 * Writes the given column names to the channel starting at the given position.
	 *
	 * @param columnNames
	 * 		the names to write
	 * @param position
	 * 		where to start writing
	 * @param channel
	 * 		the channel to write to
	 * @return the position after writing
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeColumnNames(String[] columnNames, long position, FileChannel channel) throws IOException {
		byte[][] columnNameBytes = new byte[columnNames.length][];
		int i = 0;
		for (String columnName : columnNames) {
			columnNameBytes[i] = columnName.getBytes(StandardCharsets.UTF_8);
			i++;
		}
		int[] lengths = new int[columnNames.length];
		for (int j = 0; j < columnNameBytes.length; j++) {
			lengths[j] = columnNameBytes[j].length;
		}

		position = writeOffsetTable(position, channel, lengths, Integer.MAX_VALUE);
		position = writeByteNames(position, channel, columnNameBytes, lengths, Integer.MAX_VALUE);

		return position;
	}


	/**
	 * Reads the column names from the given channel starting at the given position.
	 *
	 * @param columnNames
	 * 		the names array to fill
	 * @param position
	 * 		where to start reading
	 * @param channel
	 * 		the channel to read from
	 * @return the position after reading
	 * @throws IOException
	 * 		if reading fails
	 * @throws IllegalArgumentException
	 * 		if the channel is too short to contain the desired information
	 */
	static long readColumnNames(String[] columnNames, long position, FileChannel channel) throws IOException {
		int[] lengths = new int[columnNames.length];
		position = readOffsetTable(position, channel, lengths, Integer.MAX_VALUE);

		byte[][] columnNameBytes = new byte[columnNames.length][];
		position = readByteNames(position, channel, columnNameBytes, lengths, Integer.MAX_VALUE);

		int i = 0;
		for (byte[] bytes : columnNameBytes) {
			columnNames[i] = new String(bytes, StandardCharsets.UTF_8);
			i++;
		}

		return position;
	}


	/**
	 * Writes the {@link Column.TypeId}s of the table into the channel, starting at the given position in blocks of maximal
	 * maximalBlock bytes.
	 *
	 * @param table
	 * 		the table to write
	 * @param position
	 * 		the position to start writing
	 * @param channel
	 * 		the channel to write into
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after writing
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeColumnTypes(Table table, long position, FileChannel channel, int maximalBlock) throws IOException {
		if (table.width() < maximalBlock / INT_LENGTH) {
			int typeLength = INT_LENGTH * table.width();
			ByteBuffer buffer = channel.map(READ_WRITE, position, typeLength);
			buffer.order(ByteOrder.BIG_ENDIAN);
			for (int i = 0; i < table.width(); i++) {
				buffer.putInt(table.column(i).type().id().ordinal());
			}
			position += typeLength;
		} else {
			long rest = table.width() * (long) INT_LENGTH;
			int maxDivisibleValue = (maximalBlock / INT_LENGTH) * INT_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_WRITE, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				int length = (int) (size / INT_LENGTH);
				for (int i = offset; i < offset + length; i++) {
					buffer.putInt(table.column(i).type().id().ordinal());
				}
				position += size;
				rest -= size;
				offset += length;
			}
		}
		return position;
	}

	/**
	 * Reads the column types from the given channel starting at the given position into the given array, reading in
	 * blocks of maximalBlock length.
	 *
	 * @param types
	 * 		the types array to fill
	 * @param position
	 * 		where to start reading
	 * @param channel
	 * 		the channel to read from
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after reading
	 * @throws IOException
	 * 		if reading fails
	 * @throws IllegalArgumentException
	 * 		if the channel is not long enough to fill the lengths array
	 */
	static long readColumnTypes(Column.TypeId[] types, long position, FileChannel channel, int maximalBlock) throws IOException {
		if (channel.size() < position + types.length * (long) INT_LENGTH) {
			throw new IllegalArgumentException(MESSAGE_INCOMPLETE_FILE);
		}
		if (types.length < maximalBlock / INT_LENGTH) {
			int typeLength = INT_LENGTH * types.length;
			ByteBuffer buffer = channel.map(READ_ONLY, position, typeLength);
			buffer.order(ByteOrder.BIG_ENDIAN);
			for (int i = 0; i < types.length; i++) {
				int ordinal = buffer.getInt();
				types[i] = Column.TypeId.values()[ordinal];
			}
			position += typeLength;
		} else {
			long rest = types.length * (long) INT_LENGTH;
			int maxDivisibleValue = (maximalBlock / INT_LENGTH) * INT_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_ONLY, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				int length = (int) (size / INT_LENGTH);
				for (int i = offset; i < offset + length; i++) {
					int ordinal = buffer.getInt();
					types[i] = TypeId.values()[ordinal];
				}
				position += size;
				rest -= size;
				offset += length;
			}
		}
		return position;
	}

	/**
	 * Writes the given lengths array into the channel in blocks of maximal maximalBlock bytes starting at the given
	 * position.
	 *
	 * @param position
	 * 		where to start writing into the channel
	 * @param channel
	 * 		the channel to write to
	 * @param lengths
	 * 		the lengths to write
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after writing
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeOffsetTable(long position, FileChannel channel, int[] lengths, int maximalBlock) throws IOException {
		if (lengths.length < maximalBlock / INT_LENGTH) {
			int offsetTableLength = INT_LENGTH * lengths.length;
			ByteBuffer buffer = channel.map(READ_WRITE, position, offsetTableLength);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.asIntBuffer().put(lengths);
			position += offsetTableLength;
		} else {
			long rest = lengths.length * (long) INT_LENGTH;
			int maxDivisibleValue = (maximalBlock / INT_LENGTH) * INT_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_WRITE, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				buffer.asIntBuffer().put(lengths, offset, (int) (size / INT_LENGTH));
				position += size;
				rest -= size;
				offset += (int) (size / INT_LENGTH);
			}
		}
		return position;
	}

	/**
	 * Reads the lengths from the given channel starting at the given position into the given array, reading in blocks
	 * of maximalBlock length.
	 *
	 * @param position
	 * 		where to start reading
	 * @param channel
	 * 		the channel to read from
	 * @param lengths
	 * 		the array to store the read lengths
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after reading
	 * @throws IOException
	 * 		if reading fails
	 * @throws IllegalArgumentException
	 * 		if the channel is not long enough to fill the lengths array
	 */
	static long readOffsetTable(long position, FileChannel channel, int[] lengths, int maximalBlock) throws IOException {
		if (channel.size() < position + lengths.length * (long) INT_LENGTH) {
			throw new IllegalArgumentException(MESSAGE_INCOMPLETE_FILE);
		}
		if (lengths.length < maximalBlock / INT_LENGTH) {
			int offsetTableLength = INT_LENGTH * lengths.length;
			ByteBuffer buffer = channel.map(READ_ONLY, position, offsetTableLength);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.asIntBuffer().get(lengths);
			position += offsetTableLength;
		} else {
			long rest = lengths.length * (long) INT_LENGTH;
			int maxDivisibleValue = (maximalBlock / INT_LENGTH) * INT_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_ONLY, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				buffer.asIntBuffer().get(lengths, offset, (int) (size / INT_LENGTH));
				position += size;
				rest -= size;
				offset += (int) (size / INT_LENGTH);
			}
		}
		return position;
	}

	/**
	 * Writes the given byte arrays of the given lengths into the channel starting at the given position and writing in
	 * blocks of size maximalBlock.
	 *
	 * @param position
	 * 		the position to start writing
	 * @param channel
	 * 		the channel to write into
	 * @param byteNames
	 * 		the byte arrays to write
	 * @param lengths
	 * 		the lenghts of the byte arrays to write
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after writing
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeByteNames(long position, FileChannel channel, byte[][] byteNames, int[] lengths, int maximalBlock)
			throws IOException {
		if (lengths.length == 0) {
			return position;
		}
		long[] totalLengths = new long[lengths.length];
		totalLengths[0] = lengths[0];
		for (int i = 1; i < lengths.length; i++) {
			totalLengths[i] = totalLengths[i - 1] + lengths[i];
		}
		if (totalLengths[totalLengths.length - 1] < maximalBlock) {
			long size = totalLengths[totalLengths.length - 1];
			ByteBuffer buffer = channel.map(READ_WRITE, position, size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			for (byte[] byteName : byteNames) {
				buffer.put(byteName);
			}
			position += size;
		} else {
			position = writeInBlocks(position, channel, byteNames, maximalBlock, totalLengths);
		}
		return position;
	}

	/**
	 * Writes the byte arrays in blocks of maximalBlock size.
	 */
	private static long writeInBlocks(long position, FileChannel channel, byte[][] byteNames, int maximalBlock,
									  long[] totalLengths) throws IOException {
		long last = 0;
		int lastIndex = -1;
		while (lastIndex < byteNames.length - 1) {
			int currentLastIndex = lastIndex + 1;
			for (int i = lastIndex + 1; i < totalLengths.length; i++) {
				if (totalLengths[i] - last < maximalBlock) {
					currentLastIndex = i;
				} else {
					break;
				}
			}
			long size = totalLengths[currentLastIndex] - last;

			ByteBuffer buffer = channel.map(READ_WRITE, position, size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			position += size;
			for (int i = lastIndex + 1; i < currentLastIndex + 1; i++) {
				buffer.put(byteNames[i]);
			}

			last = totalLengths[currentLastIndex];
			lastIndex = currentLastIndex;
		}
		return position;
	}

	/**
	 * Fills the given byte names array with specified lengths by reading from the channel starting at the given
	 * position and reading blocks of maximalBlock size.
	 *
	 * @param position
	 * 		where to start reading
	 * @param channel
	 * 		the channel to read from
	 * @param byteNames
	 * 		the array to fill
	 * @param lengths
	 * 		the lengths of the byte arrays to read
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after reading
	 * @throws IOException
	 * 		if reading fails
	 * @throws IllegalArgumentException
	 * 		if the channel is too short to contain the desired information
	 */
	static long readByteNames(long position, FileChannel channel, byte[][] byteNames, int[] lengths, int maximalBlock)
			throws IOException {
		if (lengths.length == 0) {
			return position;
		}
		long[] totalLengths = new long[lengths.length];
		totalLengths[0] = lengths[0];
		for (int i = 1; i < lengths.length; i++) {
			totalLengths[i] = totalLengths[i - 1] + lengths[i];
		}
		long totalLength = totalLengths[totalLengths.length - 1];
		if (channel.size() < position + totalLength) {
			throw new IllegalArgumentException(MESSAGE_INCOMPLETE_FILE);
		}
		if (totalLength < maximalBlock) {
			ByteBuffer buffer = channel.map(READ_ONLY, position, totalLength);
			buffer.order(ByteOrder.BIG_ENDIAN);
			for (int i = 0; i < byteNames.length; i++) {
				byteNames[i] = new byte[lengths[i]];
				buffer.get(byteNames[i]);
			}
			position += totalLength;
		} else {
			position = readInBlocks(position, channel, byteNames, lengths, maximalBlock, totalLengths);
		}
		return position;
	}

	/**
	 * Reads the byte array in blocks of maximalBlock size.
	 */
	private static long readInBlocks(long position, FileChannel channel, byte[][] byteNames, int[] lengths,
									 int maximalBlock, long[] totalLengths) throws IOException {
		long last = 0;
		int lastIndex = -1;
		while (lastIndex < byteNames.length - 1) {
			int currentLastIndex = lastIndex + 1;
			for (int i = lastIndex + 1; i < totalLengths.length; i++) {
				if (totalLengths[i] - last < maximalBlock) {
					currentLastIndex = i;
				} else {
					break;
				}
			}
			long size = totalLengths[currentLastIndex] - last;

			ByteBuffer buffer = channel.map(READ_ONLY, position, size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			position += size;
			for (int i = lastIndex + 1; i < currentLastIndex + 1; i++) {
				byteNames[i] = new byte[lengths[i]];
				buffer.get(byteNames[i]);
			}

			last = totalLengths[currentLastIndex];
			lastIndex = currentLastIndex;
		}
		return position;
	}


	/**
	 * Writes the given double array into the channel starting at the given position. Returns the position after the
	 * writing.
	 *
	 * @param position
	 * 		the position where the start writing
	 * @param channel
	 * 		the channel to write into
	 * @param data
	 * 		the data to write
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the new position after writing the data
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeDoubleArray(long position, FileChannel channel, double[] data, int maximalBlock) throws IOException {
		if (data.length < maximalBlock / DOUBLE_LENGTH) {
			int size = data.length * DOUBLE_LENGTH;
			ByteBuffer buffer = channel.map(READ_WRITE, position, size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.asDoubleBuffer().put(data);
			position += size;
		} else {
			int maxDivisibleValue = (maximalBlock / DOUBLE_LENGTH) * DOUBLE_LENGTH;
			long rest = data.length * (long) DOUBLE_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_WRITE, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				position += size;
				buffer.asDoubleBuffer().put(data, offset, (int) (size / DOUBLE_LENGTH));
				rest -= size;
				offset += (int) (size / DOUBLE_LENGTH);
			}
		}
		return position;
	}

	/**
	 * Writes the mapped double array given by the data and the mapping into the channel starting at the given position.
	 * Returns the new position after writing the data.
	 *
	 * @param position
	 * 		the position where the start writing
	 * @param channel
	 * 		the channel to write into
	 * @param data
	 * 		the data to write twisted by the mapping
	 * @param mapping
	 * 		the mapping for the data
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the new position after writing the data
	 * @throws IOException
	 * 		if writing fails
	 */
	static long writeMappedDoubleArray(long position, FileChannel channel, double[] data, int[] mapping,
									   int maximalBlock) throws IOException {
		if (mapping.length < maximalBlock / DOUBLE_LENGTH) {
			int size = mapping.length * DOUBLE_LENGTH;
			ByteBuffer buffer = channel.map(READ_WRITE, position, size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			for (int mapper : mapping) {
				if (mapper < 0 || mapper >= data.length) {
					buffer.putDouble(Double.NaN);
				} else {
					buffer.putDouble(data[mapper]);
				}
			}
			position += size;
		} else {
			int maxDivisibleValue = (maximalBlock / DOUBLE_LENGTH) * DOUBLE_LENGTH;
			long rest = mapping.length * (long) DOUBLE_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_WRITE, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				int length = (int) (size / DOUBLE_LENGTH);
				for (int i = offset; i < offset + length; i++) {
					int dataIndex = mapping[i];
					if (dataIndex < 0 || dataIndex >= data.length) {
						buffer.putDouble(Double.NaN);
					} else {
						buffer.putDouble(data[dataIndex]);
					}
				}
				position += size;
				rest -= size;
				offset += length;
			}
		}
		return position;
	}

	/**
	 * Reads the data from the channel starting at the given position in blocks of maximalBlock size.
	 *
	 * @param position
	 * 		where to start reading
	 * @param channel
	 * 		the channel to read from
	 * @param data
	 * 		the data to read
	 * @param maximalBlock
	 * 		the maximal size for the buffer to use
	 * @return the position after reading
	 * @throws IOException
	 * 		if reading fails
	 * @throws IllegalArgumentException
	 * 		if the channel is not long enough to contain the data
	 */
	static long readDoubleArray(long position, FileChannel channel, double[] data, int maximalBlock)
			throws IOException {
		if (channel.size() < data.length * (long) DOUBLE_LENGTH){
			throw new IllegalArgumentException(MESSAGE_INCOMPLETE_FILE);
		}
		if (data.length < maximalBlock / DOUBLE_LENGTH) {
			int size = data.length * DOUBLE_LENGTH;
			ByteBuffer buffer = channel.map(READ_ONLY, position, size);
			buffer.order(ByteOrder.BIG_ENDIAN);
			buffer.asDoubleBuffer().get(data);
			position += size;
		} else {
			int maxDivisibleValue = (maximalBlock / DOUBLE_LENGTH) * DOUBLE_LENGTH;
			long rest = data.length * (long) DOUBLE_LENGTH;
			int offset = 0;
			while (rest > 0) {
				long size = Math.min(rest, maxDivisibleValue);
				ByteBuffer buffer = channel.map(READ_ONLY, position, size);
				buffer.order(ByteOrder.BIG_ENDIAN);
				position += size;
				rest -= size;
				buffer.asDoubleBuffer().get(data, offset, (int) (size / DOUBLE_LENGTH));
				offset += (int) (size / DOUBLE_LENGTH);
			}
		}
		return position;
	}
}
