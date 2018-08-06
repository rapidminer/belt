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

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import com.rapidminer.belt.Column.TypeId;
import com.rapidminer.belt.util.Mapping;

/**
 * @author Gisa Meier
 */
@RunWith(Enclosed.class)
public class TableFilerTests {

	private static final double EPSILON = 1e-10;

	private static Column[] random(int width, int height) {
		Column[] columns = new Column[width];
		Arrays.setAll(columns, i -> {
			double[] data = new double[height];
			Arrays.setAll(data, j -> Math.random());
			return new DoubleArrayColumn(Column.TypeId.values()[i % TypeId.values().length], data);
		});
		return columns;
	}

	private static String[] randomLabels(int n) {
		String[] labels = new String[n];
		Arrays.setAll(labels, i -> "col" + i);
		return labels;
	}

	private static double[] random(int n) {
		double[] data = new double[n];
		Arrays.setAll(data, i -> Math.random());
		return data;
	}

	private static double[] readColumnToArray(Table table, int column) {
		double[] data = new double[table.height()];
		ColumnReader reader = new ColumnReader(table.column(column));
		for (int j = 0; j < table.height(); j++) {
			data[j] = reader.read();
		}
		return data;
	}

	private static double[][] readTableToArray(Table table) {
		double[][] result = new double[table.width()][];
		Arrays.setAll(result, i -> readColumnToArray(table, i));
		return result;
	}

	private static Column.TypeId[] readTypesToArray(Table table) {
		return IntStream.range(0, table.width()).mapToObj(i-> table.column(i).type().id()).toArray(Column.TypeId[]::new);
	}

	public static class Header {

		@Test
		public void testWrite() throws IOException {
			Column[] columns = random(2, 3);
			String[] labels = randomLabels(2);
			Table table = new Table(columns, labels);
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeHeader(table, 0, channel);
				assertEquals(2 * 8, result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			Column[] columns = new Column[0];
			String[] labels = new String[0];
			Table table = new Table(columns, labels);
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeHeader(table, 0, channel);
				assertEquals(2 * 8, result);
			}
		}


		@Test
		public void testRead() throws IOException {
			Column[] columns = random(2, 3);
			String[] labels = randomLabels(2);
			Table table = new Table(columns, labels);
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeHeader(table, 0, channel);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {

				int[] size = new int[2];
				long result = TableFiler.readHeader(size, 0, channel);
				assertEquals(2 * 8, result);
			}
		}

		@Test
		public void testReadSize() throws IOException {
			int width = 2;
			int height = 3;
			Column[] columns = random(width, height);
			String[] labels = randomLabels(width);
			Table table = new Table(columns, labels);
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeHeader(table, 0, channel);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {

				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
				assertEquals(width, size[0]);
				assertEquals(height, size[1]);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTooShort() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				byte[] rubbish = "too_short".getBytes(StandardCharsets.UTF_8);
				ByteBuffer buffer = channel.map(READ_WRITE, 0, rubbish.length);
				buffer.put(rubbish);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongMagicNumber() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				byte[] rubbish = "very_long_rubbish".getBytes(StandardCharsets.UTF_8);
				ByteBuffer buffer = channel.map(READ_WRITE, 0, rubbish.length);
				buffer.put(rubbish);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongMajorVersion() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				ByteBuffer buffer = channel.map(READ_WRITE, 0, 2 * 8);
				buffer.put(TableFiler.MAGIC_NUMBER);
				buffer.put((byte) 11);
				buffer.put(TableFiler.MINOR_VERSION);
				buffer.putDouble(1);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testWrongMinorVersion() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				ByteBuffer buffer = channel.map(READ_WRITE, 0, 2 * 8);
				buffer.put(TableFiler.MAGIC_NUMBER);
				buffer.put(TableFiler.MAJOR_VERSION);
				buffer.put((byte) 11);
				buffer.putDouble(1);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeWidth() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				ByteBuffer buffer = channel.map(READ_WRITE, 0, 2 * 8);
				buffer.put(TableFiler.MAGIC_NUMBER);
				buffer.put(TableFiler.MAJOR_VERSION);
				buffer.put(TableFiler.MINOR_VERSION);
				buffer.putInt(-1);
				buffer.putInt(1);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testNegativeHeight() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				ByteBuffer buffer = channel.map(READ_WRITE, 0, 2 * 8);
				buffer.put(TableFiler.MAGIC_NUMBER);
				buffer.put(TableFiler.MAJOR_VERSION);
				buffer.put(TableFiler.MINOR_VERSION);
				buffer.putInt(1);
				buffer.putInt(-1);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] size = new int[2];
				TableFiler.readHeader(size, 0, channel);
			}
		}

	}

	public static class ColumnTypes {

		@Test
		public void testWrite() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				Table table = Table.newTable(3)
						.addInt("int", i -> 0)
						.addReal("real", i -> 0)
						.build(Belt.defaultContext());
				long result = TableFiler.writeColumnTypes(table, 0, channel, Integer.MAX_VALUE);
				assertEquals(2 * 4, result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				Table table = Table.newTable(0).build(Belt.defaultContext());
				long result = TableFiler.writeColumnTypes(table, 0, channel, Integer.MAX_VALUE);
				assertEquals(0, result);
			}
		}

		@Test
		public void testWriteBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableBuilder builder = Table.newTable(3);
				for (int i = 0; i < 100; i++) {
					builder.addInt("int" + i, j -> 0).addReal("real" + i, j -> 0);
				}
				Table table = builder.build(Belt.defaultContext());
				long result = TableFiler.writeColumnTypes(table, 0, channel, 77);
				assertEquals(table.width() * (long) 4, result);
			}
		}

		@Test
		public void testRead() throws IOException {
			File file = File.createTempFile("test", ".data");
			Table table = Table.newTable(3)
					.addInt("int", i -> 0)
					.addReal("real", i -> 0)
					.build(Belt.defaultContext());
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnTypes(table, 0, channel, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				Column.TypeId[] types = new Column.TypeId[table.width()];
				long result = TableFiler.readColumnTypes(types, 0, channel, Integer.MAX_VALUE);
				assertEquals(table.width() * (long) 4, result);
				Column.TypeId[] expected = {TypeId.INTEGER, Column.TypeId.REAL};
				assertArrayEquals(expected, types);
			}
		}

		@Test
		public void testReadEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			Table table = Table.newTable(0).build(Belt.defaultContext());
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnTypes(table, 0, channel, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				Column.TypeId[] types = new Column.TypeId[table.width()];
				long result = TableFiler.readColumnTypes(types, 0, channel, Integer.MAX_VALUE);
				assertEquals(0, result);
				assertArrayEquals(new Column.TypeId[0], types);
			}
		}

		@Test
		public void testReadBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			TableBuilder builder = Table.newTable(3);
			for (int i = 0; i < 100; i++) {
				builder.addInt("int" + i, j -> 0).addReal("real" + i, j -> 0);
			}
			Table table = builder.build(Belt.defaultContext());
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnTypes(table, 0, channel, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				Column.TypeId[] types = new Column.TypeId[table.width()];
				long result = TableFiler.readColumnTypes(types, 0, channel, 77);
				assertEquals(table.width() * (long) 4, result);
				Column.TypeId[] expected = readTypesToArray(table);
				assertArrayEquals(expected, types);
			}
		}

		@Test
		public void testReadWrittenBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			TableBuilder builder = Table.newTable(3);
			for (int i = 0; i < 100; i++) {
				builder.addInt("int" + i, j -> 0).addReal("real" + i, j -> 0);
			}
			Table table = builder.build(Belt.defaultContext());
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnTypes(table, 0, channel, 77);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				Column.TypeId[] types = new Column.TypeId[table.width()];
				long result = TableFiler.readColumnTypes(types, 0, channel, Integer.MAX_VALUE);
				assertEquals(table.width() * (long) 4, result);
				TypeId[] expected = readTypesToArray(table);
				assertArrayEquals(expected, types);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadTooShort() throws IOException {
			File file = File.createTempFile("test", ".data");
			TableBuilder builder = Table.newTable(3);
			for (int i = 0; i < 5; i++) {
				builder.addInt("int" + i, j -> 0).addReal("real" + i, j -> 0);
			}
			Table table = builder.build(Belt.defaultContext());
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnTypes(table, 0, channel, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				Column.TypeId[] types = new Column.TypeId[table.width() + 1];
				TableFiler.readColumnTypes(types, 0, channel, Integer.MAX_VALUE);
			}
		}

	}

	public static class OffsetTable {

		@Test
		public void testWrite() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				int[] lengths = new int[]{2, 7, 11};
				long result = TableFiler.writeOffsetTable(0, channel, lengths, Integer.MAX_VALUE);
				assertEquals(lengths.length * 4, result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				int[] lengths = new int[]{};
				long result = TableFiler.writeOffsetTable(0, channel, lengths, Integer.MAX_VALUE);
				assertEquals(0, result);
			}
		}

		@Test
		public void testWriteBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				int[] lengths = new int[1000];
				Arrays.setAll(lengths, i -> i);
				long result = TableFiler.writeOffsetTable(0, channel, lengths, 99);
				assertEquals(lengths.length * (long) 4, result);
			}
		}

		@Test
		public void testRead() throws IOException {
			File file = File.createTempFile("test", ".data");
			int[] lengths = new int[]{2, 7, 11};
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeOffsetTable(0, channel, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] resultLengths = new int[lengths.length];
				long result = TableFiler.readOffsetTable(0, channel, resultLengths, Integer.MAX_VALUE);
				assertEquals(resultLengths.length * (long) 4, result);
				assertArrayEquals(lengths, resultLengths);
			}
		}

		@Test
		public void testReadEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			int[] lengths = new int[0];
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeOffsetTable(0, channel, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] resultLengths = new int[lengths.length];
				long result = TableFiler.readOffsetTable(0, channel, resultLengths, Integer.MAX_VALUE);
				assertEquals(resultLengths.length * (long) 4, result);
				assertArrayEquals(lengths, resultLengths);
			}
		}

		@Test
		public void testReadBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			int[] lengths = new int[1000];
			Arrays.setAll(lengths, i -> i);
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeOffsetTable(0, channel, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] resultLengths = new int[lengths.length];
				long result = TableFiler.readOffsetTable(0, channel, resultLengths, 99);
				assertEquals(resultLengths.length * (long) 4, result);
				assertArrayEquals(lengths, resultLengths);
			}
		}

		@Test
		public void testReadWrittenBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			int[] lengths = new int[1000];
			Arrays.setAll(lengths, i -> i);
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeOffsetTable(0, channel, lengths, 99);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] resultLengths = new int[lengths.length];
				long result = TableFiler.readOffsetTable(0, channel, resultLengths, Integer.MAX_VALUE);
				assertEquals(resultLengths.length * (long) 4, result);
				assertArrayEquals(lengths, resultLengths);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadTooShort() throws IOException {
			File file = File.createTempFile("test", ".data");
			int[] lengths = new int[]{2, 7};
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeOffsetTable(0, channel, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] resultLengths = new int[lengths.length + 1];
				TableFiler.readOffsetTable(0, channel, resultLengths, Integer.MAX_VALUE);
			}
		}

	}


	public static class ByteNames {

		@Test
		public void testWrite() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[3][];
			int[] lengths = new int[3];
			bytes[0] = "first".getBytes(StandardCharsets.UTF_8);
			lengths[0] = bytes[0].length;
			bytes[1] = "second".getBytes(StandardCharsets.UTF_8);
			lengths[1] = bytes[1].length;
			bytes[2] = "third".getBytes(StandardCharsets.UTF_8);
			lengths[2] = bytes[2].length;
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeByteNames(0, channel, bytes, lengths, Integer.MAX_VALUE);
				assertEquals(lengths[0] + lengths[1] + lengths[2], result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[0][];
			int[] lengths = new int[0];
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeByteNames(0, channel, bytes, lengths, Integer.MAX_VALUE);
				assertEquals(0, result);
			}
		}

		@Test
		public void testWriteBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[1000][];
			int[] lengths = new int[1000];

			byte[] violin = "\uD834\uDD1E".getBytes(StandardCharsets.UTF_8);
			Arrays.fill(bytes, violin);
			Arrays.fill(lengths, 4);
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeByteNames(0, channel, bytes, lengths, 99);
				assertEquals(lengths.length * (long) 4, result);
			}
		}


		@Test
		public void testRead() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[3][];
			int[] lengths = new int[3];
			bytes[0] = "first".getBytes(StandardCharsets.UTF_8);
			lengths[0] = bytes[0].length;
			bytes[1] = "second".getBytes(StandardCharsets.UTF_8);
			lengths[1] = bytes[1].length;
			bytes[2] = "third".getBytes(StandardCharsets.UTF_8);
			lengths[2] = bytes[2].length;
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeByteNames(0, channel, bytes, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				byte[][] resultBytes = new byte[lengths.length][];
				long result = TableFiler.readByteNames(0, channel, resultBytes, lengths, Integer.MAX_VALUE);
				assertEquals(lengths[0] + lengths[1] + lengths[2], result);
				assertArrayEquals(bytes, resultBytes);
			}
		}

		@Test
		public void testReadEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[0][];
			int[] lengths = new int[0];
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeByteNames(0, channel, bytes, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				byte[][] resultBytes = new byte[lengths.length][];
				long result = TableFiler.readByteNames(0, channel, resultBytes, lengths, Integer.MAX_VALUE);
				assertEquals(0, result);
				assertArrayEquals(bytes, resultBytes);
			}
		}


		@Test
		public void testReadBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[1000][];
			int[] lengths = new int[1000];

			byte[] violin = "\uD834\uDD1E".getBytes(StandardCharsets.UTF_8);
			Arrays.fill(bytes, violin);
			Arrays.fill(lengths, 4);
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeByteNames(0, channel, bytes, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				byte[][] resultBytes = new byte[lengths.length][];
				long result = TableFiler.readByteNames(0, channel, resultBytes, lengths, 99);
				assertEquals(lengths.length * (long) 4, result);
				assertArrayEquals(bytes, resultBytes);
			}
		}

		@Test
		public void testReadWrittenBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[1000][];
			int[] lengths = new int[1000];

			byte[] violin = "\uD834\uDD1E".getBytes(StandardCharsets.UTF_8);
			Arrays.fill(bytes, violin);
			Arrays.fill(lengths, 4);
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeByteNames(0, channel, bytes, lengths, 99);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				byte[][] resultBytes = new byte[lengths.length][];
				long result = TableFiler.readByteNames(0, channel, resultBytes, lengths, Integer.MAX_VALUE);
				assertEquals(lengths.length * (long) 4, result);
				assertArrayEquals(bytes, resultBytes);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadIncomplete() throws IOException {
			File file = File.createTempFile("test", ".data");
			byte[][] bytes = new byte[3][];
			int[] lengths = new int[3];
			bytes[0] = "first".getBytes(StandardCharsets.UTF_8);
			lengths[0] = bytes[0].length;
			bytes[1] = "second".getBytes(StandardCharsets.UTF_8);
			lengths[1] = bytes[1].length;
			bytes[2] = "third".getBytes(StandardCharsets.UTF_8);
			lengths[2] = bytes[2].length;
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeByteNames(0, channel, bytes, lengths, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				int[] brokenLengths = Arrays.copyOf(lengths, lengths.length + 1);
				brokenLengths[brokenLengths.length - 1] = 7;
				byte[][] resultBytes = new byte[brokenLengths.length][];
				TableFiler.readByteNames(0, channel, resultBytes, brokenLengths, Integer.MAX_VALUE);
			}
		}


	}

	public static class ColumnNames {

		@Test
		public void testWrite() throws IOException {
			File file = File.createTempFile("test", ".data");
			String[] names = new String[]{"first", "second", "third", "fourth", "fifth"};
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeColumnNames(names, 0, channel);
				assertEquals(5 + 6 + 5 + 6 + 5 + 5 * 4, result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			String[] names = new String[0];
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeColumnNames(names, 0, channel);
				assertEquals(0, result);
			}
		}


		@Test
		public void testRead() throws IOException {
			File file = File.createTempFile("test", ".data");
			String[] names = new String[]{"first", "second", "third", "fourth", "fifth"};
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnNames(names, 0, channel);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				String[] resultNames = new String[names.length];
				long result = TableFiler.readColumnNames(resultNames, 0, channel);
				assertEquals(5 + 6 + 5 + 6 + 5 + 5 * 4, result);
				assertArrayEquals(names, resultNames);
			}
		}

		@Test
		public void testReadEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			String[] names = new String[0];
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnNames(names, 0, channel);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				String[] resultNames = new String[names.length];
				long result = TableFiler.readColumnNames(resultNames, 0, channel);
				assertEquals(0, result);
				assertArrayEquals(names, resultNames);
			}
		}


		@Test
		public void testReadUtf8() throws IOException {
			File file = File.createTempFile("test", ".data");
			String[] names = new String[]{"bäüößß", "\uD834\uDD1E", "\u00D8!=\u00F8", "\u0132\u01CB\u22D1\u2AF5"};
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnNames(names, 0, channel);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				String[] resultNames = new String[names.length];
				TableFiler.readColumnNames(resultNames, 0, channel);
				assertArrayEquals(names, resultNames);
			}
		}


		@Test(expected = IllegalArgumentException.class)
		public void testReadIncomplete() throws IOException {
			File file = File.createTempFile("test", ".data");
			String[] names = new String[]{"first", "second", "third", "fourth", "fifth"};
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeColumnNames(names, 0, channel);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				String[] resultNames = new String[names.length + 1];
				TableFiler.readColumnNames(resultNames, 0, channel);
			}
		}


	}

	public static class DoubleArray {

		@Test
		public void testWrite() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[123];
			Arrays.setAll(data, i -> i);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeDoubleArray(0, channel, data, Integer.MAX_VALUE);
				assertEquals(data.length * (long) 8, result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[0];

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeDoubleArray(0, channel, data, Integer.MAX_VALUE);
				assertEquals(0, result);
			}
		}

		@Test
		public void testWriteBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[1000];
			Arrays.setAll(data, i -> i);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeDoubleArray(0, channel, data, 99);
				assertEquals(data.length * (long) 8, result);
			}
		}


		@Test
		public void testRead() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[123];
			Arrays.setAll(data, i -> i);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeDoubleArray(0, channel, data, Integer.MAX_VALUE);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[data.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
				assertEquals(data.length * (long) 8, result);
				assertArrayEquals(data, resultData, EPSILON);
			}
		}


		@Test
		public void testReadEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[0];

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeDoubleArray(0, channel, data, Integer.MAX_VALUE);
			}
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[data.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
				assertEquals(0, result);
				assertArrayEquals(data, resultData, EPSILON);
			}
		}

		@Test
		public void testReadBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[1000];
			Arrays.setAll(data, i -> i);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeDoubleArray(0, channel, data, Integer.MAX_VALUE);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[data.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, 99);
				assertEquals(data.length * (long) 8, result);
				assertArrayEquals(data, resultData, EPSILON);
			}
		}

		@Test
		public void testReadWrittenBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[1000];
			Arrays.setAll(data, i -> i);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeDoubleArray(0, channel, data, 99);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[data.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
				assertEquals(data.length * (long) 8, result);
				assertArrayEquals(data, resultData, EPSILON);
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadIncomplete() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[123];
			Arrays.setAll(data, i -> i);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeDoubleArray(0, channel, data, Integer.MAX_VALUE);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[data.length + 1];
				TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
			}
		}
	}

	public static class MappedDoubleArray {

		@Test
		public void testWrite() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[123];
			Arrays.setAll(data, i -> i);
			int[] mapping = new int[55];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(data.length));

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeMappedDoubleArray(0, channel, data, mapping, Integer.MAX_VALUE);
				assertEquals(mapping.length * (long) 8, result);
			}
		}

		@Test
		public void testWriteMissings() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[123];
			Arrays.setAll(data, i -> i);
			int[] mapping = new int[55];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(2 * data.length) - data.length);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeMappedDoubleArray(0, channel, data, mapping, Integer.MAX_VALUE);
				assertEquals(mapping.length * (long) 8, result);
			}
		}

		@Test
		public void testWriteEmpty() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[123];
			Arrays.setAll(data, i -> i);
			int[] mapping = new int[0];

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeMappedDoubleArray(0, channel, data, mapping, Integer.MAX_VALUE);
				assertEquals(0, result);
			}
		}

		@Test
		public void testWriteBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = new double[1000];
			Arrays.setAll(data, i -> i);
			int[] mapping = new int[1100];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(data.length));

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				long result = TableFiler.writeMappedDoubleArray(0, channel, data, mapping, 99);
				assertEquals(mapping.length * (long) 8, result);
			}
		}


		@Test
		public void testRead() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = random(123);
			Arrays.setAll(data, i -> i);
			int[] mapping = new int[55];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(data.length));

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeMappedDoubleArray(0, channel, data, mapping, Integer.MAX_VALUE);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[mapping.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
				assertEquals(mapping.length * (long) 8, result);
				assertArrayEquals(Mapping.apply(data, mapping), resultData, EPSILON);
			}
		}

		@Test
		public void testReadMissings() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = random(123);
			Arrays.setAll(data, i -> i);
			int[] mapping = new int[55];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(2 * data.length) - data.length);

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeMappedDoubleArray(0, channel, data, mapping, Integer.MAX_VALUE);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[mapping.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
				assertEquals(mapping.length * (long) 8, result);
				assertArrayEquals(Mapping.apply(data, mapping), resultData, EPSILON);
			}
		}

		@Test
		public void testReadWrittenBlocks() throws IOException {
			File file = File.createTempFile("test", ".data");
			double[] data = random(1000);
			int[] mapping = new int[1100];
			Random random = new Random();
			Arrays.setAll(mapping, i -> random.nextInt(data.length));

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.READ,
					StandardOpenOption.WRITE)) {
				TableFiler.writeMappedDoubleArray(0, channel, data, mapping, 99);
			}

			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
				double[] resultData = new double[mapping.length];
				long result = TableFiler.readDoubleArray(0, channel, resultData, Integer.MAX_VALUE);
				assertEquals(mapping.length * (long) 8, result);
				assertArrayEquals(Mapping.apply(data, mapping), resultData, EPSILON);
			}
		}


	}

	public static class WholeTable {

		private File file;

		@Before
		public void setFile() throws IOException {
			file = File.createTempFile("test", ".data");
		}

		@After
		public void deleteFile() throws IOException {
			//delete on exit does not work if channels are mapped to buffers, so use this hack to delete the file
			try (FileChannel channel = FileChannel.open(file.toPath(),
					StandardOpenOption.DELETE_ON_CLOSE,
					StandardOpenOption.READ)) {
			}
		}

		@Test
		public void testWrite() throws IOException {
			Column[] columns = random(2, 3);
			String[] labels = randomLabels(2);
			Table table = new Table(columns, labels);
			Table.store(table, file.toPath());
		}

		@Test
		public void testWriteEmpty() throws IOException {
			Column[] columns = new Column[0];
			String[] labels = new String[0];
			Table table = new Table(columns, labels);
			Table.store(table, file.toPath());
		}

		@Test(expected = NullPointerException.class)
		public void testWriteNullTable() throws IOException {
			Table.store(null, file.toPath());
		}

		@Test(expected = NullPointerException.class)
		public void testWriteNullPath() throws IOException {
			Column[] columns = random(2, 3);
			String[] labels = randomLabels(2);
			Table table = new Table(columns, labels);
			Table.store(table, null);
		}


		@Test
		public void testRead() throws IOException {
			Column[] columns = random(5, 37);
			String[] labels = randomLabels(5);
			Table table = new Table(columns, labels);
			Table.store(table, file.toPath());

			Table resultTable = Table.load(file.toPath());
			assertArrayEquals(readTableToArray(table), readTableToArray(resultTable));
			assertArrayEquals(readTypesToArray(table), readTypesToArray(resultTable));
			assertArrayEquals(labels, resultTable.labelArray());
		}

		@Test
		public void testReadEmpty() throws IOException {
			Column[] columns = new Column[0];
			String[] labels = new String[0];
			Table table = new Table(columns, labels);
			Table.store(table, file.toPath());

			Table resultTable = Table.load(file.toPath());
			assertArrayEquals(readTableToArray(table), readTableToArray(resultTable));
			assertArrayEquals(readTypesToArray(table), readTypesToArray(resultTable));
			assertArrayEquals(labels, resultTable.labelArray());
		}


		@Test(expected = NullPointerException.class)
		public void testReadNullPath() throws IOException {
			Table.load(null);
		}

	}

}
