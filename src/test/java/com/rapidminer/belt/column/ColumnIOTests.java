package com.rapidminer.belt.column;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.rapidminer.belt.buffer.Buffers;
import com.rapidminer.belt.column.io.DateTimeColumnBuilder;
import com.rapidminer.belt.column.io.NominalColumnBuilder;
import com.rapidminer.belt.column.io.NumericColumnBuilder;
import com.rapidminer.belt.column.io.TimeColumnBuilder;
import com.rapidminer.belt.util.IntegerFormats;


/**
 * @author Michael Knopf, Gisa Meier
 */
@RunWith(Enclosed.class)
public class ColumnIOTests {

	private static final double EPSILON = 1e-10;

	public static class InputValidationNumeric {

		private static final int SIZE = 250_000;

		private static Column numericColumn;
		private static Column dateTimeColumn;

		@BeforeClass
		public static void setupColumns() {
			numericColumn = Buffers.realBuffer(SIZE).toColumn();
			dateTimeColumn = Buffers.dateTimeBuffer(SIZE, false).toColumn();
		}

		@AfterClass
		public static void freeColumns() {
			numericColumn = null;
			dateTimeColumn = null;
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBuffer() {
			ColumnIO.putNumericDoubles(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBuffer() {
			ColumnIO.putNumericDoubles(dateTimeColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBuffer() {
			ColumnIO.putNumericDoubles(numericColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBuffer() {
			ColumnIO.putNumericDoubles(numericColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBuffer() {
			ColumnIO.putNumericDoubles(numericColumn, 0, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadNegativeLength() {
			ColumnIO.readReal(-1);
		}

		@Test(expected = NullPointerException.class)
		public void testReadNullType() {
			new NumericColumnBuilder(100, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadIncompatibleType() {
			new NumericColumnBuilder(100, ColumnType.DATETIME.id());
		}

		@Test(expected = NullPointerException.class)
		public void testReadFromNullBuffer() {
			ColumnIO.readReal(100).put(null);
		}

	}

	public static class InputValidationDateTime {

		private static final int SIZE = 250_000;

		private static Column numericColumn;
		private static Column dateTimeColumn;

		@BeforeClass
		public static void setupColumns() {
			numericColumn = Buffers.realBuffer(SIZE).toColumn();
			dateTimeColumn = Buffers.dateTimeBuffer(SIZE, false).toColumn();
		}

		@AfterClass
		public static void freeColumns() {
			numericColumn = null;
			dateTimeColumn = null;
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBuffer() {
			ColumnIO.putDateTimeLongs(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBufferNanos() {
			ColumnIO.putDateTimeNanoInts(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBuffer() {
			ColumnIO.putDateTimeLongs(numericColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBufferNanos() {
			ColumnIO.putDateTimeNanoInts(numericColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBuffer() {
			ColumnIO.putDateTimeLongs(dateTimeColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBufferNanos() {
			ColumnIO.putDateTimeNanoInts(dateTimeColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBuffer() {
			ColumnIO.putDateTimeLongs(dateTimeColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBufferNanos() {
			ColumnIO.putDateTimeNanoInts(dateTimeColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBuffer() {
			ColumnIO.putDateTimeLongs(dateTimeColumn, 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBufferNanos() {
			ColumnIO.putDateTimeNanoInts(dateTimeColumn, 0, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadNegativeLength() {
			ColumnIO.readDateTime(-1);
		}

		@Test(expected = NullPointerException.class)
		public void testReadFromNullBuffer() {
			ColumnIO.readDateTime(100)
					.putSeconds(null);
		}

		@Test(expected = NullPointerException.class)
		public void testReadFromNullBufferNanos() {
			ColumnIO.readDateTime(100)
					.putNanos(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			LongBuffer wrapper = buffer.asLongBuffer();
			wrapper.put(Instant.MIN.getEpochSecond() - 1);
			buffer.rewind();
			ColumnIO.readDateTime(100)
					.putSeconds(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange2() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			LongBuffer wrapper = buffer.asLongBuffer();
			wrapper.put(Instant.MAX.getEpochSecond() + 1);
			buffer.rewind();
			ColumnIO.readDateTime(100)
					.putSeconds(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRangeNanos() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			IntBuffer wrapper = buffer.asIntBuffer();
			wrapper.put(-1);
			buffer.rewind();
			ColumnIO.readDateTime(100)
					.putNanos(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRangeNanos2() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			IntBuffer wrapper = buffer.asIntBuffer();
			wrapper.put(Instant.MAX.getNano()+1);
			buffer.rewind();
			ColumnIO.readDateTime(100)
					.putNanos(buffer);
		}
	}

	public static class InputValidationTime {

		private static final int SIZE = 250_000;

		private static Column numericColumn;
		private static Column timeColumn;

		@BeforeClass
		public static void setupColumns() {
			numericColumn = Buffers.realBuffer(SIZE).toColumn();
			timeColumn = Buffers.timeBuffer(SIZE).toColumn();
		}

		@AfterClass
		public static void freeColumns() {
			numericColumn = null;
			timeColumn = null;
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBuffer() {
			ColumnIO.putTimeLongs(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBuffer() {
			ColumnIO.putTimeLongs(numericColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBuffer() {
			ColumnIO.putTimeLongs(timeColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBuffer() {
			ColumnIO.putTimeLongs(timeColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBuffer() {
			ColumnIO.putTimeLongs(timeColumn, 0, null);
		}


		@Test(expected = IllegalArgumentException.class)
		public void testReadNegativeLength() {
			ColumnIO.readTime(-1);
		}


		@Test(expected = NullPointerException.class)
		public void testReadFromNullBuffer() {
			ColumnIO.readTime(100)
					.put(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			LongBuffer wrapper = buffer.asLongBuffer();
			wrapper.put(-1L);
			buffer.rewind();
			ColumnIO.readTime(100)
					.put(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange2() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			LongBuffer wrapper = buffer.asLongBuffer();
			wrapper.put(86400000000000L);
			buffer.rewind();
			ColumnIO.readTime(100)
					.put(buffer);
		}
	}

	public static class InputValidationCategorical {

		private static final int SIZE = 250_000;

		private static Column numericColumn;
		private static Column nominalColumn;

		@BeforeClass
		public static void setupColumns() {
			numericColumn = Buffers.realBuffer(SIZE).toColumn();
			nominalColumn = Buffers.nominalBuffer(SIZE).toColumn();
		}

		@AfterClass
		public static void freeColumns() {
			numericColumn = null;
			nominalColumn = null;
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBuffer() {
			ColumnIO.putCategoricalIntegers(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBufferShort() {
			ColumnIO.putCategoricalShorts(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutNullColumnIntoBufferByte() {
			ColumnIO.putCategoricalBytes(null, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutTooManyValuesColumnIntoBufferShort() {
			String[] array = new String[Short.MAX_VALUE + 2];
			array[array.length - 1] = "a";
			Column col = new SimpleCategoricalColumn(ColumnType.NOMINAL, new short[10],
					new Dictionary(Arrays.asList(array)));
			ColumnIO.putCategoricalShorts(col, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutTooManyValuesColumnIntoBufferByte() {
			String[] array = new String[Byte.MAX_VALUE + 2];
			array[array.length - 1] = "a";
			Column col = new SimpleCategoricalColumn(ColumnType.NOMINAL, new short[10],
					new Dictionary(Arrays.asList(array)));
			ColumnIO.putCategoricalBytes(col, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBuffer() {
			ColumnIO.putCategoricalIntegers(numericColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBufferShort() {
			ColumnIO.putCategoricalShorts(numericColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IllegalArgumentException.class)
		public void testPutIncompatibleColumnIntoBufferByte() {
			ColumnIO.putCategoricalBytes(numericColumn, 0, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBuffer() {
			ColumnIO.putCategoricalIntegers(nominalColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBufferShort() {
			ColumnIO.putCategoricalShorts(nominalColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromNegativeOffsetIntoBufferByte() {
			ColumnIO.putCategoricalBytes(nominalColumn, -1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBuffer() {
			ColumnIO.putCategoricalIntegers(nominalColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBufferShort() {
			ColumnIO.putCategoricalShorts(nominalColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = IndexOutOfBoundsException.class)
		public void testPutFromTooLargeOffsetIntoBufferByte() {
			ColumnIO.putCategoricalBytes(nominalColumn, SIZE + 1, ByteBuffer.allocate(100));
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBuffer() {
			ColumnIO.putCategoricalIntegers(nominalColumn, 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBufferShort() {
			ColumnIO.putCategoricalShorts(nominalColumn, 0, null);
		}

		@Test(expected = NullPointerException.class)
		public void testPutIntoNullBufferByte() {
			ColumnIO.putCategoricalBytes(nominalColumn, 0, null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadNegativeLength() {
			ColumnIO.readNominal(new LinkedHashSet<>(), -1);
		}

		@Test(expected = NullPointerException.class)
		public void testReadNullDict() {
			ColumnIO.readNominal(null, 5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadEmptyDict() {
			ColumnIO.readNominal(new LinkedHashSet<>(), 5);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testReadNonNullStartDict() {
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList("a", null)), 5);
		}

		@Test(expected = NullPointerException.class)
		public void testReadFromNullBuffer() {
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putIntegers(null);
		}

		@Test(expected = NullPointerException.class)
		public void testReadFromNullBufferShort() {
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putShorts(null);
		}

		@Test(expected = NullPointerException.class)
		public void testReadFromNullBufferByte() {
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putBytes(null);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			buffer.put((byte)-1);
			buffer.rewind();
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putBytes(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRangeInt() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			IntBuffer wrapper = buffer.asIntBuffer();
			wrapper.put(-1);
			buffer.rewind();
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putIntegers(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRangeShort() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			ShortBuffer wrapper = buffer.asShortBuffer();
			wrapper.put((short)-1);
			buffer.rewind();
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putShorts(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange2() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			buffer.put((byte)5);
			buffer.rewind();
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putBytes(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange2Int() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			IntBuffer wrapper = buffer.asIntBuffer();
			wrapper.put(5);
			buffer.rewind();
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putIntegers(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testOutOfRange2Short() {
			ByteBuffer buffer = ByteBuffer.allocate(10);
			ShortBuffer wrapper = buffer.asShortBuffer();
			wrapper.put((short)5);
			buffer.rewind();
			ColumnIO.readNominal(new LinkedHashSet<>(Arrays.asList(null, "a", "b")),10)
					.putShorts(buffer);
		}

	}

	@RunWith(Parameterized.class)
	public static class NumericInput {

		@Parameter
		public ColumnType type;

		@Parameter(1)
		public ByteOrder byteOrder;

		public double[] data;

		@Parameters(name = "{0} from {1} doubles")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[] {ColumnType.REAL, ByteOrder.LITTLE_ENDIAN},
					new Object[] {ColumnType.REAL, ByteOrder.BIG_ENDIAN},
					new Object[] {ColumnType.INTEGER_53_BIT, ByteOrder.LITTLE_ENDIAN},
					new Object[] {ColumnType.INTEGER_53_BIT, ByteOrder.BIG_ENDIAN});
		}

		@Before
		public void createColumn() {
			data = new double[250_000];
			Random rng = new Random();
			if (ColumnType.REAL.equals(type)) {
				Arrays.setAll(data, i -> rng.nextDouble());
			} else {
				Arrays.setAll(data, i -> rng.nextInt(5_000_000));
			}
		}

		private NumericColumnBuilder readNumeric(int length, Column.TypeId type) {
			if (type == Column.TypeId.INTEGER_53_BIT) {
				return ColumnIO.readInteger53Bit(length);
			}
			return ColumnIO.readReal(length);
		}

		@Test
		public void testInputBuffers() {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;
			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);
			DoubleBuffer wrapper = buffer.asDoubleBuffer();

			NumericColumnBuilder builder = readNumeric(data.length, type.id());

			int written = 0;
			while (written < data.length) {
				buffer.clear();
				wrapper.clear();
				int length = Math.min(data.length - written, wrapper.capacity());
				wrapper.put(data, written, length);
				buffer.position(wrapper.position() << 3);
				buffer.flip();

				builder.put(buffer);
				written = builder.position();
			}

			Column column = builder.toColumn();
			double[] result = new double[column.size()];
			column.fill(result, 0);

			assertEquals(type, column.type());
			assertArrayEquals(data, result, EPSILON);
		}

		@Test
		public void testIncompleteColumn() throws IOException {
			byte[] bytes = new byte[data.length << 3];
			ByteBuffer wrap = ByteBuffer.wrap(bytes);
			wrap.order(byteOrder)
					.asDoubleBuffer()
					.put(data);

			wrap.limit((data.length / 2) << 3);

			NumericColumnBuilder numericColumnBuilder = readNumeric(data.length, type.id()).put(wrap);
			int numberPut = numericColumnBuilder.position();
			Column column = numericColumnBuilder.toColumn();

			double[] result = new double[column.size()];
			column.fill(result, 0);

			double[] expected = Arrays.copyOf(data, data.length);
			Arrays.fill(expected, expected.length / 2, expected.length, Double.NaN);

			assertEquals(type, column.type());
			assertEquals(data.length/2, numberPut);
			assertArrayEquals(expected, result, EPSILON);
		}

	}

	@RunWith(Parameterized.class)
	public static class TimeInput {

		@Parameter
		public ColumnType type;

		@Parameter(1)
		public ByteOrder byteOrder;

		public long[] data;

		@Parameters(name = "{0} from {1} longs")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[]{ColumnType.TIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{ColumnType.TIME, ByteOrder.BIG_ENDIAN});
		}

		@Before
		public void createColumn() {
			data = new long[250_000];
			Random rng = new Random();
			Arrays.setAll(data, i -> rng.nextInt(Integer.MAX_VALUE));
			data[42] = Long.MAX_VALUE; //set missing value
		}

		@Test
		public void testInputBuffers() {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;
			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);
			LongBuffer wrapper = buffer.asLongBuffer();

			TimeColumnBuilder builder = ColumnIO.readTime(data.length);

			int written = 0;
			while (written < data.length) {
				buffer.clear();
				wrapper.clear();
				int length = Math.min(data.length - written, wrapper.capacity());
				wrapper.put(data, written, length);
				buffer.position(wrapper.position() << 3);
				buffer.flip();

				builder.put(buffer);
				written = builder.position();
			}

			Column column = builder.toColumn();
			assertEquals(type, column.type());

			long[] result = new long[column.size()];
			((TimeColumn) column).fillNanosIntoArray(result, 0);
			assertArrayEquals(data, result);
		}

		@Test
		public void testIncompleteColumn() {
			byte[] bytes = new byte[data.length << 3];
			ByteBuffer wrap = ByteBuffer.wrap(bytes);
			wrap.order(byteOrder)
					.asLongBuffer()
					.put(data);

			wrap.limit((data.length / 2) << 3);

			TimeColumnBuilder timeColumnBuilder = ColumnIO.readTime(data.length).put(wrap);
			int numberPut = timeColumnBuilder.position();
			Column column = timeColumnBuilder.toColumn();

			assertEquals(type, column.type());

			long[] result = new long[column.size()];
			((TimeColumn) column).fillNanosIntoArray(result, 0);

			long[] expected = Arrays.copyOf(data, data.length);
			Arrays.fill(expected, expected.length / 2, expected.length, Long.MAX_VALUE);

			assertEquals(data.length / 2, numberPut);
			assertArrayEquals(expected, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class DateTimeInput {

		@Parameter
		public ColumnType type;

		@Parameter(1)
		public ByteOrder byteOrder;

		public long[] data;
		public int[] nanoData;

		@Parameters(name = "{0} from {1} longs")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[]{ColumnType.DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{ColumnType.DATETIME, ByteOrder.BIG_ENDIAN});
		}

		@Before
		public void createColumn() {
			data = new long[250_000];
			Random rng = new Random();
			Arrays.setAll(data, i -> rng.nextInt(Integer.MAX_VALUE));
			data[42] = Long.MAX_VALUE; //set missing value
			nanoData = new int[250_000];
			Arrays.setAll(nanoData, i -> rng.nextInt(Instant.MAX.getNano()));
		}

		@Test
		public void testInputBuffers() {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;
			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);
			LongBuffer wrapper = buffer.asLongBuffer();

			ByteBuffer nanoBuffer = ByteBuffer.allocate(capacity/2).order(byteOrder);
			IntBuffer nanoWrapper = nanoBuffer.asIntBuffer();

			DateTimeColumnBuilder builder = ColumnIO.readDateTime(data.length);

			int written = 0;
			while (written < data.length) {
				buffer.clear();
				wrapper.clear();
				int length = Math.min(data.length - written, wrapper.capacity());
				wrapper.put(data, written, length);
				buffer.position(wrapper.position() << 3);
				buffer.flip();

				nanoBuffer.clear();
				nanoWrapper.clear();
				int length2 = Math.min(nanoData.length - written, nanoWrapper.capacity());
				nanoWrapper.put(nanoData, written, length2);
				nanoBuffer.position(nanoWrapper.position() << 2);
				nanoBuffer.flip();

				builder.putSeconds(buffer);
				written = builder.position();
				builder.putNanos(nanoBuffer);
				int written2 = builder.nanoPosition();
				assertEquals(written, written2);
			}

			Column column = builder.toColumn();
			assertEquals(type, column.type());

			long[] result = new long[column.size()];
			((DateTimeColumn) column).fillSecondsIntoArray(result, 0);
			assertArrayEquals(data, result);

			int[] nanoResult = new int[column.size()];
			((DateTimeColumn) column).fillNanosIntoArray(nanoResult, 0);
			assertArrayEquals(nanoData, nanoResult);
		}

		@Test
		public void testInputBuffersNoNanos() {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;
			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);
			LongBuffer wrapper = buffer.asLongBuffer();

			DateTimeColumnBuilder builder = ColumnIO.readDateTime(data.length);

			int written = 0;
			while (written < data.length) {
				buffer.clear();
				wrapper.clear();
				int length = Math.min(data.length - written, wrapper.capacity());
				wrapper.put(data, written, length);
				buffer.position(wrapper.position() << 3);
				buffer.flip();
				builder.putSeconds(buffer);
				written = builder.position();
			}

			Column column = builder.toColumn();
			assertEquals(type, column.type());

			long[] result = new long[column.size()];
			((DateTimeColumn) column).fillSecondsIntoArray(result, 0);
			assertArrayEquals(data, result);
		}

		@Test
		public void testIncompleteColumn() {
			byte[] bytes = new byte[data.length << 3];
			ByteBuffer wrap = ByteBuffer.wrap(bytes);
			wrap.order(byteOrder)
					.asLongBuffer()
					.put(data);
			wrap.limit((data.length / 2) << 3);

			byte[] nanoBytes = new byte[data.length << 2];
			ByteBuffer nanoWrap = ByteBuffer.wrap(nanoBytes);
			nanoWrap.order(byteOrder)
					.asIntBuffer()
					.put(nanoData);
			nanoWrap.limit((data.length / 2) << 2);

			DateTimeColumnBuilder dateTimeColumnBuilder = ColumnIO.readDateTime(data.length);
			dateTimeColumnBuilder.putSeconds(wrap);
			int numberPut = dateTimeColumnBuilder.position();
			dateTimeColumnBuilder.putNanos(nanoWrap);
			int numberPut2 = dateTimeColumnBuilder.nanoPosition();
			assertEquals(numberPut, numberPut2);
			Column column = dateTimeColumnBuilder.toColumn();

			assertEquals(type, column.type());

			long[] result = new long[column.size()];
			((DateTimeColumn) column).fillSecondsIntoArray(result, 0);

			long[] expected = Arrays.copyOf(data, data.length);
			Arrays.fill(expected, expected.length / 2, expected.length, Long.MAX_VALUE);

			assertEquals(data.length / 2, numberPut);
			assertArrayEquals(expected, result);

			int[] nanoResult = new int[column.size()];
			((DateTimeColumn) column).fillNanosIntoArray(nanoResult, 0);

			int[] expectedNanos = Arrays.copyOf(nanoData, nanoData.length);
			Arrays.fill(expectedNanos, expectedNanos.length / 2, expectedNanos.length, 0);

			assertArrayEquals(expectedNanos, nanoResult);
		}

		@Test
		public void testIncompleteColumnNoNanos() {
			byte[] bytes = new byte[data.length << 3];
			ByteBuffer wrap = ByteBuffer.wrap(bytes);
			wrap.order(byteOrder)
					.asLongBuffer()
					.put(data);

			wrap.limit((data.length / 2) << 3);

			DateTimeColumnBuilder dateTimeColumnBuilder = ColumnIO.readDateTime(data.length).putSeconds(wrap);
			int numberPut = dateTimeColumnBuilder.position();
			Column column = dateTimeColumnBuilder.toColumn();

			assertEquals(type, column.type());

			long[] result = new long[column.size()];
			((DateTimeColumn) column).fillSecondsIntoArray(result, 0);

			long[] expected = Arrays.copyOf(data, data.length);
			Arrays.fill(expected, expected.length / 2, expected.length, Long.MAX_VALUE);

			assertEquals(data.length / 2, numberPut);
			assertArrayEquals(expected, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class NominalInput {

		@Parameter
		public ColumnType type;

		@Parameter(1)
		public ByteOrder byteOrder;

		@Parameter(2)
		public int dictSize;

		public int[] data;
		public LinkedHashSet<String> dict;


		@Parameters(name = "{0} from {1} indices with dict size {2}")
		public static Iterable<Object[]> columnImplementations() {
			int[] dictSizes = new int[]{3, 7, 42, 270, IntegerFormats.Format.UNSIGNED_INT16.maxValue() + 3};
			List<Object[]> objects = Arrays.asList(new Object[]{ColumnType.NOMINAL, ByteOrder.LITTLE_ENDIAN},
					new Object[]{ColumnType.NOMINAL, ByteOrder.BIG_ENDIAN});
			return Arrays.stream(dictSizes).boxed().flatMap(i -> objects.stream().map(o -> new Object[]{o[0], o[1],
					i}))
					.collect(Collectors.toList());
		}

		@Before
		public void createColumn() {
			data = new int[250_000];
			Random rng = new Random();
			Arrays.setAll(data, i -> rng.nextInt(dictSize));

			dict = new LinkedHashSet<>();
			dict.add(null);
			for (int i = 0; i < dictSize - 1; i++) {
				dict.add("val" + i);
			}
		}

		@Test
		public void testInputBuffers() {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;
			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);
			IntBuffer wrapper = buffer.asIntBuffer();

			NominalColumnBuilder builder = ColumnIO.readNominal(dict, data.length);

			int written = 0;
			while (written < data.length) {
				buffer.clear();
				wrapper.clear();
				int length = Math.min(data.length - written, wrapper.capacity());
				wrapper.put(data, written, length);
				buffer.position(wrapper.position() << 2);
				buffer.flip();

				builder.putIntegers(buffer);
				written = builder.position();
			}

			Column column = builder.toColumn();
			assertEquals(type, column.type());

			int[] result = new int[column.size()];
			column.fill(result, 0);
			assertArrayEquals(data, result);
		}

		@Test
		public void testInputBuffersShort() {
			if (dictSize < Short.MAX_VALUE) {
				int capacity = 8192;
				ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);
				ShortBuffer wrapper = buffer.asShortBuffer();

				NominalColumnBuilder builder = ColumnIO.readNominal(dict, data.length);

				int written = 0;
				while (written < data.length) {
					buffer.clear();
					wrapper.clear();
					int length = Math.min(data.length - written, wrapper.capacity());
					for (int i = 0; i < length; i++) {
						wrapper.put((short) data[written + i]);
					}
					buffer.position(wrapper.position() << 1);
					buffer.flip();

					builder.putShorts(buffer);
					written = builder.position();
				}

				Column column = builder.toColumn();
				assertEquals(type, column.type());

				int[] result = new int[column.size()];
				column.fill(result, 0);
				assertArrayEquals(data, result);
			}
		}

		@Test
		public void testInputBuffersByte() {
			if (dictSize < Byte.MAX_VALUE) {
				int capacity = 8192;
				ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

				NominalColumnBuilder builder = ColumnIO.readNominal(dict, data.length);

				int written = 0;
				while (written < data.length) {
					buffer.clear();
					int length = Math.min(data.length - written, buffer.capacity());
					for (int i = 0; i < length; i++) {
						buffer.put((byte) data[written + i]);
					}
					buffer.flip();

					builder.putBytes(buffer);
					written = builder.position();
				}

				Column column = builder.toColumn();
				assertEquals(type, column.type());

				int[] result = new int[column.size()];
				column.fill(result, 0);
				assertArrayEquals(data, result);
			}
		}

		@Test
		public void testInputBuffersBoolean() {
			if (dictSize < Byte.MAX_VALUE) {
				int capacity = 8192;
				ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

				NominalColumnBuilder builder = ColumnIO.readNominal(dict, data.length);

				int written = 0;
				while (written < data.length) {
					buffer.clear();
					int length = Math.min(data.length - written, buffer.capacity());
					for (int i = 0; i < length; i++) {
						buffer.put((byte) data[written + i]);
					}
					buffer.flip();

					builder.putBytes(buffer);
					written = builder.position();
				}

				try {
					Column column = builder.toBooleanColumn(1);
					assertEquals(type, column.type());

					int[] result = new int[column.size()];
					column.fill(result, 0);
					assertArrayEquals(data, result);
				} catch (IllegalArgumentException e) {
					if (dictSize > BooleanDictionary.MAXIMAL_RAW_SIZE) {
						//okay to get IllegalArgumentException
					} else {
						throw e;
					}
				}
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTooBigValue() {
			ByteBuffer buffer = ByteBuffer.allocate(10).order(byteOrder);
			IntBuffer wrapper = buffer.asIntBuffer();
			wrapper.put(dictSize);
			buffer.position(wrapper.position() << 2);
			buffer.flip();

			NominalColumnBuilder builder = ColumnIO.readNominal(dict, 2);
			builder.putIntegers(buffer);
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTooBigValueShort() {
			ByteBuffer buffer = ByteBuffer.allocate(10).order(byteOrder);
			ShortBuffer wrapper = buffer.asShortBuffer();
			wrapper.put((short) dictSize);
			buffer.position(wrapper.position() << 1);
			buffer.flip();

			NominalColumnBuilder builder = ColumnIO.readNominal(dict, 2);
			try {
				builder.putShorts(buffer);
			} catch (AssertionError e) {
				if (dictSize > Short.MAX_VALUE) {
					throw new IllegalArgumentException("right to get an assertion error");
				}
			}
		}

		@Test(expected = IllegalArgumentException.class)
		public void testTooBigValueByte() {
			ByteBuffer buffer = ByteBuffer.allocate(10).order(byteOrder);
			buffer.put((byte) dictSize);
			buffer.flip();

			NominalColumnBuilder builder = ColumnIO.readNominal(dict, 2);
			try {
				builder.putBytes(buffer);
			} catch (AssertionError e) {
				if (dictSize > Byte.MAX_VALUE) {
					throw new IllegalArgumentException("right to get an assertion error");
				}
			}
		}


		@Test
		public void testIncompleteColumn() {
			byte[] bytes = new byte[data.length << 2];
			ByteBuffer wrap = ByteBuffer.wrap(bytes);
			wrap.order(byteOrder)
					.asIntBuffer()
					.put(data);

			wrap.limit((data.length / 2) << 2);

			NominalColumnBuilder nominalColumnBuilder = ColumnIO.readNominal(dict, data.length).putIntegers(wrap);
			int numberPut = nominalColumnBuilder.position();
			Column column = nominalColumnBuilder.toColumn();

			assertEquals(type, column.type());

			int[] result = new int[column.size()];
			column.fill(result, 0);

			int[] expected = Arrays.copyOf(data, data.length);
			Arrays.fill(expected, expected.length / 2, expected.length, 0);

			assertEquals(data.length / 2, numberPut);
			assertArrayEquals(expected, result);
		}

	}


	@RunWith(Parameterized.class)
	public static class NumericOutput {

		private static final String IMPL_DOUBLE_ARRAY = "DoubleArrayColumn";
		private static final String IMPL_DOUBLE_ARRAY_INT = "DoubleArrayColumnIntSparse";
		private static final String IMPL_MAPPED_DOUBLE_ARRAY = "MappedDoubleArrayColumn";
		private static final String IMPL_MAPPED_DOUBLE_ARRAY_INT = "MappedDoubleArrayColumnInt";

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		public double[] data;
		public Column column;

		@Parameters(name = "{0} to {1} doubles")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[]{IMPL_DOUBLE_ARRAY, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_DOUBLE_ARRAY, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_DOUBLE_ARRAY_INT, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_DOUBLE_ARRAY_INT, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_MAPPED_DOUBLE_ARRAY, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_MAPPED_DOUBLE_ARRAY, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_MAPPED_DOUBLE_ARRAY_INT, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_MAPPED_DOUBLE_ARRAY_INT, ByteOrder.BIG_ENDIAN});
			}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new double[250_000];

			if (!columnImplementation.contains("Int")) {
				Arrays.setAll(data, i -> rng.nextDouble());
			} else {
				Arrays.setAll(data, i -> rng.nextDouble() > 0.7 ? rng.nextInt(5_000_000) : 42);
			}

			int[] mapping = null;
			double[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new double[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			switch (columnImplementation) {
				case IMPL_DOUBLE_ARRAY:
					column = new DoubleArrayColumn(Column.TypeId.REAL, data);
					break;
				case IMPL_DOUBLE_ARRAY_INT:
					column = new DoubleSparseColumn(Column.TypeId.INTEGER_53_BIT, 42, data);
					break;
				case IMPL_MAPPED_DOUBLE_ARRAY:
					column = new MappedDoubleArrayColumn(Column.TypeId.REAL, mappedData, mapping);
					break;
				case IMPL_MAPPED_DOUBLE_ARRAY_INT:
					column = new MappedDoubleArrayColumn(Column.TypeId.INTEGER_53_BIT, mappedData, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putNumericDoubles(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedRead() throws IOException {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;

			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			WritableByteChannel channel = Channels.newChannel(out);

			int written = 0;
			while (written < column.size()) {
				buffer.clear();
				written += ColumnIO.putNumericDoubles(column, written, buffer);
				buffer.flip();
				channel.write(buffer);
			}

			double[] result = new double[data.length];
			ByteBuffer.wrap(out.toByteArray())
					.order(byteOrder)
					.asDoubleBuffer()
					.get(result);

			assertArrayEquals(data, result, EPSILON);
		}

	}

	@RunWith(Parameterized.class)
	public static class TimeOutput {

		private static final String IMPL_SIMPLE_TIME = "SimpleTimeColumn";
		private static final String IMPL_SPARSE_TIME = "TimeSparseColumn";
		private static final String IMPL_MAPPED_TIME = "MappedTimeColumn";

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		public long[] data;
		public Column column;

		@Parameters(name = "{0} to {1} longs")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[]{IMPL_SIMPLE_TIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_SIMPLE_TIME, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_SPARSE_TIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_SPARSE_TIME, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_MAPPED_TIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_MAPPED_TIME, ByteOrder.BIG_ENDIAN});
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new long[250_000];

			if (columnImplementation.contains("Sparse")) {
				Arrays.setAll(data, i -> rng.nextDouble() > 0.7 ? rng.nextInt(Integer.MAX_VALUE) : 1234567);
			} else {
				Arrays.setAll(data, i -> rng.nextInt(Integer.MAX_VALUE));
			}

			int[] mapping = null;
			long[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new long[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			switch (columnImplementation) {
				case IMPL_SIMPLE_TIME:
					column = new SimpleTimeColumn(data);
					break;
				case IMPL_SPARSE_TIME:
					column = new TimeSparseColumn(1234567, data);
					break;
				case IMPL_MAPPED_TIME:
					column = new MappedTimeColumn(mappedData, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putTimeLongs(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedRead() throws IOException {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;

			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			WritableByteChannel channel = Channels.newChannel(out);

			int written = 0;
			while (written < column.size()) {
				buffer.clear();
				written += ColumnIO.putTimeLongs(column, written, buffer);
				buffer.flip();
				channel.write(buffer);
			}

			long[] result = new long[data.length];
			ByteBuffer.wrap(out.toByteArray())
					.order(byteOrder)
					.asLongBuffer()
					.get(result);

			assertArrayEquals(data, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class DateTimeOutput {

		private static final String IMPL_SIMPLE_DATETIME = "SimpleDateTimeColumn";
		private static final String IMPL_SPARSE_DATETIME = "DateTimeSparseColumn";
		private static final String IMPL_MAPPED_DATETIME = "MappedDateTimeColumn";

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		public long[] data;
		public Column column;

		@Parameters(name = "{0} to {1} longs")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[]{IMPL_SIMPLE_DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_SIMPLE_DATETIME, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_SPARSE_DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_SPARSE_DATETIME, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_MAPPED_DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_MAPPED_DATETIME, ByteOrder.BIG_ENDIAN});
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new long[250_000];

			if (columnImplementation.contains("Sparse")) {
				Arrays.setAll(data, i -> rng.nextDouble() > 0.7 ? rng.nextInt(Integer.MAX_VALUE) : 1234567);
			} else {
				Arrays.setAll(data, i -> rng.nextInt(Integer.MAX_VALUE));
			}

			int[] mapping = null;
			long[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new long[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			switch (columnImplementation) {
				case IMPL_SIMPLE_DATETIME:
					column = new SimpleDateTimeColumn(data);
					break;
				case IMPL_SPARSE_DATETIME:
					column = new DateTimeLowPrecisionSparseColumn(1234567, data);
					break;
				case IMPL_MAPPED_DATETIME:
					column = new MappedDateTimeColumn(mappedData, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putDateTimeLongs(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedRead() throws IOException {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;

			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			WritableByteChannel channel = Channels.newChannel(out);

			int written = 0;
			while (written < column.size()) {
				buffer.clear();
				written += ColumnIO.putDateTimeLongs(column, written, buffer);
				buffer.flip();
				channel.write(buffer);
			}

			long[] result = new long[data.length];
			ByteBuffer.wrap(out.toByteArray())
					.order(byteOrder)
					.asLongBuffer()
					.get(result);

			assertArrayEquals(data, result);
		}

		@Test
		public void testNano() throws IOException {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;

			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			WritableByteChannel channel = Channels.newChannel(out);

			int written = 0;
			while (written < column.size()) {
				buffer.clear();
				written += ColumnIO.putDateTimeNanoInts(column, written, buffer);
				buffer.flip();
				channel.write(buffer);
			}

			int[] result = new int[data.length];
			ByteBuffer.wrap(out.toByteArray())
					.order(byteOrder)
					.asIntBuffer()
					.get(result);

			assertArrayEquals(new int[data.length], result);
		}
	}

	@RunWith(Parameterized.class)
	public static class DateTimeNanoOutput {

		private static final String IMPL_SIMPLE_DATETIME = "SimpleDateTimeColumn";
		private static final String IMPL_SPARSE_DATETIME = "DateTimeSparseColumn";
		private static final String IMPL_MAPPED_DATETIME = "MappedDateTimeColumn";

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		public int[] data;
		public Column column;

		@Parameters(name = "{0} to {1} longs")
		public static Iterable<Object[]> columnImplementations() {
			return Arrays.asList(new Object[]{IMPL_SIMPLE_DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_SIMPLE_DATETIME, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_SPARSE_DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_SPARSE_DATETIME, ByteOrder.BIG_ENDIAN},
					new Object[]{IMPL_MAPPED_DATETIME, ByteOrder.LITTLE_ENDIAN},
					new Object[]{IMPL_MAPPED_DATETIME, ByteOrder.BIG_ENDIAN});
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new int[250_000];
			Arrays.setAll(data, i -> rng.nextInt(999_999_999));

			int[] mapping = null;
			int[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new int[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			switch (columnImplementation) {
				case IMPL_SIMPLE_DATETIME:
					column = new SimpleDateTimeColumn(new long[data.length], data);
					break;
				case IMPL_SPARSE_DATETIME:
					column = new DateTimeHighPrecisionSparseColumn(0, new long[data.length], data);
					break;
				case IMPL_MAPPED_DATETIME:
					column = new MappedDateTimeColumn(new long[data.length], mappedData, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putDateTimeNanoInts(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedRead() throws IOException {
			// Choose capacity such that the last batch does not fill the entire buffer
			int capacity = 8192;

			ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			WritableByteChannel channel = Channels.newChannel(out);

			int written = 0;
			while (written < column.size()) {
				buffer.clear();
				written += ColumnIO.putDateTimeNanoInts(column, written, buffer);
				buffer.flip();
				channel.write(buffer);
			}

			int[] result = new int[data.length];
			ByteBuffer.wrap(out.toByteArray())
					.order(byteOrder)
					.asIntBuffer()
					.get(result);

			assertArrayEquals(data, result);
		}

	}

	private static final String IMPL_SIMPLE_NOMINAL = "SimpleCategoricalColumn";
	private static final String IMPL_SPARSE_NOMINAL = "CategoricalSparseColumn";
	private static final String IMPL_MAPPED_NOMINAL = "MappedCategoricalColumn";
	private static final String IMPL_REMAPPED_NOMINAL = "RemappedCategoricalColumn";
	private static final String IMPL_REMAPPED_SPARSE_NOMINAL = "RemappedCategoricalSparseColumn";
	private static final String IMPL_REMAPPED_MAPPED_NOMINAL = "RemappedMappedCategoricalColumn";

	private static Iterable<Object[]> getNominalParams() {
		Object[] impls = {IMPL_SIMPLE_NOMINAL, IMPL_SPARSE_NOMINAL, IMPL_MAPPED_NOMINAL, IMPL_REMAPPED_NOMINAL,
				IMPL_REMAPPED_SPARSE_NOMINAL, IMPL_REMAPPED_MAPPED_NOMINAL};
		Object[] orders = new Object[]{ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN};

		return Arrays.stream(impls).flatMap(i -> Arrays.stream(orders).map(o -> new Object[]{i, o}))
				.collect(Collectors.toList());
	}

	private static Iterable<Object[]> getNominalParamsNoSparse() {
		Object[] impls = {IMPL_SIMPLE_NOMINAL, IMPL_MAPPED_NOMINAL, IMPL_REMAPPED_NOMINAL,
				IMPL_REMAPPED_MAPPED_NOMINAL};
		Object[] orders = new Object[]{ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN};

		return Arrays.stream(impls).flatMap(i -> Arrays.stream(orders).map(o -> new Object[]{i, o}))
				.collect(Collectors.toList());
	}

	@RunWith(Parameterized.class)
	public static class NominalOutputInt {

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		private int dictSize = 42;

		public int[] data;
		public Column column;

		@Parameters(name = "{0} to {1}")
		public static Iterable<Object[]> columnImplementations() {
			return getNominalParams();
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new int[250_000];

			if (columnImplementation.contains("Sparse")) {
				Arrays.setAll(data, i -> rng.nextDouble() > 0.7 ? rng.nextInt(dictSize) : 0);
			} else {
				Arrays.setAll(data, i -> rng.nextInt(dictSize));
			}

			int[] mapping = null;
			int[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new int[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			int[] remapping = null;
			if (columnImplementation.contains("Remapped")) {
				remapping = new int[dictSize];
				Arrays.setAll(remapping, i -> i);
			}

			switch (columnImplementation) {
				case IMPL_SIMPLE_NOMINAL:
					column = new SimpleCategoricalColumn(ColumnType.NOMINAL, data, new Dictionary(Arrays.asList(null,
							"bla")));
					break;
				case IMPL_SPARSE_NOMINAL:
					column = new CategoricalSparseColumn(ColumnType.NOMINAL, data, new Dictionary(Arrays.asList(null,
							"bla")), 0);
					break;
				case IMPL_MAPPED_NOMINAL:
					column = new MappedCategoricalColumn(ColumnType.NOMINAL, mappedData,
							new Dictionary(Arrays.asList(null, "bla")), mapping);
					break;
				case IMPL_REMAPPED_NOMINAL:
					column = new RemappedCategoricalColumn(ColumnType.NOMINAL, data, new Dictionary(Arrays.asList(null
							, "bla")), remapping);
					break;
				case IMPL_REMAPPED_SPARSE_NOMINAL:
					column = new RemappedCategoricalSparseColumn(ColumnType.NOMINAL, data,
							new Dictionary(Arrays.asList(null, "bla")), remapping, 0);
					break;
				case IMPL_REMAPPED_MAPPED_NOMINAL:
					column = new RemappedMappedCategoricalColumn(ColumnType.NOMINAL, mappedData,
							new Dictionary(Arrays.asList(null, "bla")), remapping, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putCategoricalIntegers(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedReadInt() throws IOException {
			int[] result = readAndWriteAsInts(column, byteOrder);

			assertArrayEquals(data, result);
		}

		@Test
		public void testBufferedReadShort() throws IOException {
			short[] result = readAndWriteAsShorts(column, byteOrder);

			short[] expected = new short[data.length];
			for (int i = 0; i < data.length; i++) {
				expected[i] = (short) data[i];
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadByte() throws IOException {
			byte[] result = readAndWriteAsBytes(column, byteOrder);

			byte[] expected = new byte[data.length];
			for (int i = 0; i < data.length; i++) {
				expected[i] = (byte) data[i];
			}

			assertArrayEquals(expected, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class NominalOutputShort {

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		private int dictSize = 42;

		public short[] data;
		public Column column;

		@Parameters(name = "{0} to {1}")
		public static Iterable<Object[]> columnImplementations() {
			return getNominalParams();
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new short[250_000];

			if (columnImplementation.contains("Sparse")) {
				for (int i = 0; i < data.length; i++) {
					data[i] = rng.nextDouble() > 0.7 ? (short) rng.nextInt(dictSize) : 0;
				}
			} else {
				for (int i = 0; i < data.length; i++) {
					data[i] = (short) rng.nextInt(dictSize);
				}
			}

			int[] mapping = null;
			short[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new short[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			int[] remapping = null;
			if (columnImplementation.contains("Remapped")) {
				remapping = new int[dictSize];
				Arrays.setAll(remapping, i -> i);
			}

			switch (columnImplementation) {
				case IMPL_SIMPLE_NOMINAL:
					column = new SimpleCategoricalColumn(ColumnType.NOMINAL, data, new Dictionary(Arrays.asList(null,
							"bla")));
					break;
				case IMPL_SPARSE_NOMINAL:
					column = new CategoricalSparseColumn(ColumnType.NOMINAL, data, new Dictionary(Arrays.asList(null,
							"bla")), (short) 0);
					break;
				case IMPL_MAPPED_NOMINAL:
					column = new MappedCategoricalColumn(ColumnType.NOMINAL, mappedData,
							new Dictionary(Arrays.asList(null, "bla")), mapping);
					break;
				case IMPL_REMAPPED_NOMINAL:
					column = new RemappedCategoricalColumn(ColumnType.NOMINAL, data, new Dictionary(Arrays.asList(null
							, "bla")), remapping);
					break;
				case IMPL_REMAPPED_SPARSE_NOMINAL:
					column = new RemappedCategoricalSparseColumn(ColumnType.NOMINAL, data,
							new Dictionary(Arrays.asList(null, "bla")), remapping, (short) 0);
					break;
				case IMPL_REMAPPED_MAPPED_NOMINAL:
					column = new RemappedMappedCategoricalColumn(ColumnType.NOMINAL, mappedData,
							new Dictionary(Arrays.asList(null, "bla")), remapping, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putCategoricalShorts(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedReadInt() throws IOException {
			int[] result = readAndWriteAsInts(column, byteOrder);

			int[] expected = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				expected[i] = data[i];
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadShort() throws IOException {
			short[] result = readAndWriteAsShorts(column, byteOrder);

			assertArrayEquals(data, result);
		}

		@Test
		public void testBufferedReadByte() throws IOException {
			byte[] result = readAndWriteAsBytes(column, byteOrder);

			byte[] expected = new byte[data.length];
			for (int i = 0; i < data.length; i++) {
				expected[i] = (byte) data[i];
			}

			assertArrayEquals(expected, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class NominalOutputByte {

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		private int dictSize = 17;

		public byte[] data;
		public Column column;

		@Parameters(name = "{0} to {1}")
		public static Iterable<Object[]> columnImplementations() {
			return getNominalParams();
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			data = new byte[250_000];

			if (columnImplementation.contains("Sparse")) {
				for (int i = 0; i < data.length; i++) {
					data[i] = rng.nextDouble() > 0.7 ? (byte) rng.nextInt(dictSize) : 0;
				}
			} else {
				for (int i = 0; i < data.length; i++) {
					data[i] = (byte) rng.nextInt(dictSize);
				}
			}

			int[] mapping = null;
			byte[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(data.length);
				mappedData = new byte[data.length];
				for (int i = 0; i < data.length; i++) {
					mappedData[mapping[i]] = data[i];
				}
			}

			int[] remapping = null;
			if (columnImplementation.contains("Remapped")) {
				remapping = new int[dictSize];
				Arrays.setAll(remapping, i -> i);
			}

			IntegerFormats.PackedIntegers bytes = new IntegerFormats.PackedIntegers(columnImplementation.contains(
					"Mapped") ? mappedData : data,
					IntegerFormats.Format.UNSIGNED_INT8, data.length);
			switch (columnImplementation) {
				case IMPL_SIMPLE_NOMINAL:
					column = new SimpleCategoricalColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null,
							"bla")));
					break;
				case IMPL_SPARSE_NOMINAL:
					column = new CategoricalSparseColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null,
							"bla")), (byte) 0);
					break;
				case IMPL_MAPPED_NOMINAL:
					column = new MappedCategoricalColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), mapping);
					break;
				case IMPL_REMAPPED_NOMINAL:
					column = new RemappedCategoricalColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null
							, "bla")), remapping);
					break;
				case IMPL_REMAPPED_SPARSE_NOMINAL:
					column = new RemappedCategoricalSparseColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), remapping, (byte) 0);
					break;
				case IMPL_REMAPPED_MAPPED_NOMINAL:
					column = new RemappedMappedCategoricalColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), remapping, mapping);
					break;
			}
		}

		@Test
		public void testFullByteBuffer() {
			int capacity = 1024;
			byte[] bytes = new byte[capacity];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = Byte.MAX_VALUE;
			}

			byte[] expected = Arrays.copyOf(bytes, bytes.length);

			ByteBuffer buffer = ByteBuffer.wrap(bytes).order(byteOrder);
			buffer.position(buffer.capacity());
			int numberPut = ColumnIO.putCategoricalBytes(column, 0, buffer);

			assertEquals(0, numberPut);
			assertEquals(capacity, buffer.position());
			assertArrayEquals(expected, bytes);
		}

		@Test
		public void testBufferedReadInt() throws IOException {
			int[] result = readAndWriteAsInts(column, byteOrder);

			int[] expected = new int[data.length];
			for (int i = 0; i < data.length; i++) {
				expected[i] = data[i];
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadShort() throws IOException {
			short[] result = readAndWriteAsShorts(column, byteOrder);

			short[] expected = new short[data.length];
			for (int i = 0; i < data.length; i++) {
				expected[i] = data[i];
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadByte() throws IOException {
			byte[] result = readAndWriteAsBytes(column, byteOrder);

			assertArrayEquals(data, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class NominalOutputByte4 {

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		private int dictSize = 13;

		public byte[] data;
		public Column column;

		@Parameters(name = "{0} to {1}")
		public static Iterable<Object[]> columnImplementations() {
			//no uint4 supported by sparse
			return getNominalParamsNoSparse();
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			int size = 250_000;
			data = new byte[size / 2];

			for (int i = 0; i < size; i++) {
				IntegerFormats.writeUInt4(data, i, (byte) rng.nextInt(dictSize));
			}


			int[] mapping = null;
			byte[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(size);
				mappedData = new byte[data.length];
				for (int i = 0; i < size; i++) {
					IntegerFormats.writeUInt4(mappedData, mapping[i], IntegerFormats.readUInt4(data, i));
				}
			}

			int[] remapping = null;
			if (columnImplementation.contains("Remapped")) {
				remapping = new int[dictSize];
				Arrays.setAll(remapping, i -> i);
			}

			IntegerFormats.PackedIntegers bytes = new IntegerFormats.PackedIntegers(columnImplementation.contains(
					"Mapped") ? mappedData : data,
					IntegerFormats.Format.UNSIGNED_INT4, size);
			switch (columnImplementation) {
				case IMPL_SIMPLE_NOMINAL:
					column = new SimpleCategoricalColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null,
							"bla")));
					break;
				case IMPL_MAPPED_NOMINAL:
					column = new MappedCategoricalColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), mapping);
					break;
				case IMPL_REMAPPED_NOMINAL:
					column = new RemappedCategoricalColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null
							, "bla")), remapping);
					break;
				case IMPL_REMAPPED_MAPPED_NOMINAL:
					column = new RemappedMappedCategoricalColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), remapping, mapping);
					break;
			}
		}

		@Test
		public void testBufferedReadInt() throws IOException {
			int[] result = readAndWriteAsInts(column, byteOrder);

			int[] expected = new int[data.length * 2];
			for (int i = 0; i < data.length * 2; i++) {
				expected[i] = IntegerFormats.readUInt4(data, i);
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadShort() throws IOException {
			short[] result = readAndWriteAsShorts(column, byteOrder);

			short[] expected = new short[data.length * 2];
			for (int i = 0; i < data.length * 2; i++) {
				expected[i] = (short) IntegerFormats.readUInt4(data, i);
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadByte() throws IOException {
			byte[] result = readAndWriteAsBytes(column, byteOrder);

			byte[] expected = new byte[data.length * 2];
			for (int i = 0; i < data.length * 2; i++) {
				expected[i] = (byte)IntegerFormats.readUInt4(data, i);
			}

			assertArrayEquals(expected, result);
		}

	}

	@RunWith(Parameterized.class)
	public static class NominalOutputByte2 {

		@Parameter
		public String columnImplementation;

		@Parameter(1)
		public ByteOrder byteOrder;

		private int dictSize = 3;

		public byte[] data;
		public Column column;

		@Parameters(name = "{0} to {1}")
		public static Iterable<Object[]> columnImplementations() {
			//no uint2 supported by sparse
			return getNominalParamsNoSparse();
		}

		@Before
		public void createColumn() {
			Random rng = new Random();
			int size = 250_000;
			data = new byte[size / 4];

			for (int i = 0; i < size; i++) {
				IntegerFormats.writeUInt2(data, i, (byte) rng.nextInt(dictSize));
			}


			int[] mapping = null;
			byte[] mappedData = null;
			if (columnImplementation.contains("Mapped")) {
				mapping = permutation(size);
				mappedData = new byte[data.length];
				for (int i = 0; i < size; i++) {
					IntegerFormats.writeUInt2(mappedData, mapping[i], IntegerFormats.readUInt2(data, i));
				}
			}

			int[] remapping = null;
			if (columnImplementation.contains("Remapped")) {
				remapping = new int[dictSize];
				Arrays.setAll(remapping, i -> i);
			}

			IntegerFormats.PackedIntegers bytes = new IntegerFormats.PackedIntegers(columnImplementation.contains(
					"Mapped") ? mappedData : data,
					IntegerFormats.Format.UNSIGNED_INT2, size);
			switch (columnImplementation) {
				case IMPL_SIMPLE_NOMINAL:
					column = new SimpleCategoricalColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null,
							"bla")));
					break;
				case IMPL_MAPPED_NOMINAL:
					column = new MappedCategoricalColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), mapping);
					break;
				case IMPL_REMAPPED_NOMINAL:
					column = new RemappedCategoricalColumn(ColumnType.NOMINAL, bytes, new Dictionary(Arrays.asList(null
							, "bla")), remapping);
					break;
				case IMPL_REMAPPED_MAPPED_NOMINAL:
					column = new RemappedMappedCategoricalColumn(ColumnType.NOMINAL, bytes,
							new Dictionary(Arrays.asList(null, "bla")), remapping, mapping);
					break;
			}
		}

		@Test
		public void testBufferedReadInt() throws IOException {
			int[] result = readAndWriteAsInts(column, byteOrder);

			int[] expected = new int[data.length * 4];
			for (int i = 0; i < data.length * 4; i++) {
				expected[i] = IntegerFormats.readUInt2(data, i);
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadShort() throws IOException {
			short[] result = readAndWriteAsShorts(column, byteOrder);

			short[] expected = new short[data.length * 4];
			for (int i = 0; i < data.length * 4; i++) {
				expected[i] = (short) IntegerFormats.readUInt2(data, i);
			}

			assertArrayEquals(expected, result);
		}

		@Test
		public void testBufferedReadByte() throws IOException {
			byte[] result = readAndWriteAsBytes(column, byteOrder);

			byte[] expected = new byte[data.length * 4];
			for (int i = 0; i < data.length * 4; i++) {
				expected[i] = (byte)IntegerFormats.readUInt2(data, i);
			}

			assertArrayEquals(expected, result);
		}

	}

	private static int[] readAndWriteAsInts(Column column, ByteOrder byteOrder) throws IOException {
		// Choose capacity such that the last batch does not fill the entire buffer
		int capacity = 8192;

		ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		WritableByteChannel channel = Channels.newChannel(out);

		int written = 0;
		while (written < column.size()) {
			buffer.clear();
			written += ColumnIO.putCategoricalIntegers(column, written, buffer);
			buffer.flip();
			channel.write(buffer);
		}

		int[] result = new int[250_000];
		ByteBuffer.wrap(out.toByteArray())
				.order(byteOrder)
				.asIntBuffer()
				.get(result);
		return result;
	}

	private static short[] readAndWriteAsShorts(Column column, ByteOrder byteOrder) throws IOException {
		// Choose capacity such that the last batch does not fill the entire buffer
		int capacity = 8192;

		ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		WritableByteChannel channel = Channels.newChannel(out);

		int written = 0;
		while (written < column.size()) {
			buffer.clear();
			written += ColumnIO.putCategoricalShorts(column, written, buffer);
			buffer.flip();
			channel.write(buffer);
		}

		short[] result = new short[250_000];
		ByteBuffer.wrap(out.toByteArray())
				.order(byteOrder)
				.asShortBuffer()
				.get(result);
		return result;
	}

	private static byte[] readAndWriteAsBytes(Column column, ByteOrder byteOrder) throws IOException {
		// Choose capacity such that the last batch does not fill the entire buffer
		int capacity = 8192;

		ByteBuffer buffer = ByteBuffer.allocate(capacity).order(byteOrder);

		ByteArrayOutputStream out = new ByteArrayOutputStream();
		WritableByteChannel channel = Channels.newChannel(out);

		int written = 0;
		while (written < column.size()) {
			buffer.clear();
			written += ColumnIO.putCategoricalBytes(column, written, buffer);
			buffer.flip();
			channel.write(buffer);
		}

		byte[] result = new byte[250_000];
		ByteBuffer.wrap(out.toByteArray())
				.order(byteOrder)
				.get(result);
		return result;
	}

	private static int[] permutation(int n) {
		int[] indices = new int[n];
		Arrays.setAll(indices, i -> i);
		for (int i = 0; i < n; i++) {
			int a = (int) (Math.random() * n);
			int b = (int) (Math.random() * n);
			int tmp = indices[a];
			indices[a] = indices[b];
			indices[b] = tmp;
		}
		return indices;
	}

}