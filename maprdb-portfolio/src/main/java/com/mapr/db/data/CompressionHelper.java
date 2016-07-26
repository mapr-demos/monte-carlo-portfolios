package com.mapr.db.data;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CompressionHelper {

	public static int[] integersToInts(List<Integer> integers) {
		int[] ints = new int[integers.size()];
		int i = 0;
		for (Integer n : integers) {
			ints[i++] = n;
		}
		return ints;
	}

	public static byte[] integersToBytesTest(List<Integer> integers) {
		byte[] bytes = new byte[integers.size() * TypeSize.INT32_BYTESIZE];
		int offset = 0;
		for (Integer integer : integers) {
			for (int i = 1; i <= TypeSize.INT32_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[offset++] = (byte) ((integer >> ((TypeSize.INT32_BYTESIZE - i) * 8)) & 0xff);
		}
		return bytes;
	}

	public static byte[] integersToBytes(List<Integer> integers) {
		byte[] bytes = new byte[integers.size() * TypeSize.INT32_BYTESIZE];
		int offset = 0;
		for (Integer integer : integers) {
			bytes[offset++] = (byte) ((integer >> 24) & 0xFF);
			bytes[offset++] = (byte) ((integer >> 16) & 0xFF);
			bytes[offset++] = (byte) ((integer >>  8) & 0xFF);
			bytes[offset++] = (byte) ((integer >>  0) & 0xFF);
		}
		return bytes;
	}

	public static byte[] intsToBytes(int[] ints, int startFrom, int endAt) {
			if (startFrom < 0 || startFrom > ints.length) {
				System.err.println("Returning null! startFrom: " + startFrom + " ints.length: " + ints.length);
				return null;
			}
			if (endAt < 0 || endAt > ints.length) {
				System.err.println("Returning null! endAt: " + endAt + " ints.length: " + ints.length);
				return null;
			}
			if (endAt < startFrom) {
				System.err.println("Returning null! endAt: " + endAt + " startFrom: " + startFrom);
				return null;
			}
	
			int numberOfInts = endAt - startFrom;
	
			// x86 is little Endian
			/*
			0A.0B.0C.0D.
			 |  |  |  |
			 |  |  |  |-> a + 0: 0D
			 |  |  |----> a + 1: 0C
			 |  |-------> a + 2: 0B
			 |----------> a + 3: 0A
			*/
	
			byte[] bytes = new byte[numberOfInts * TypeSize.INT32_BYTESIZE];
	
			int offset = 0;
			for (int idx = startFrom; idx < endAt; idx++) {
				bytes[offset++] = (byte) ((ints[idx] >> 24) & 0xFF);
				bytes[offset++] = (byte) ((ints[idx] >> 16) & 0xFF);
				bytes[offset++] = (byte) ((ints[idx] >>  8) & 0xFF);
				bytes[offset++] = (byte) ((ints[idx] >>  0) & 0xFF);
			}		
			return bytes;
		}

	public static int[] bytesToInts(byte[] bytes) {
		if (bytes.length % TypeSize.INT32_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.INT32_BYTESIZE + ". Length: " + bytes.length);
		}
	
		int[] ints = new int[bytes.length / TypeSize.INT32_BYTESIZE];
	
		int bits = 0;
		int byteOffset = 0;
		for (int i = 0; i < ints.length; i++) {
			bits =               (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			ints[i] = bits;
		}
		return ints;
	}

	public static long[] longsToLongs(List<Long> listOfLongs) {
		long[] longs = new long[listOfLongs.size()];
		int i = 0;
		for (Long l : listOfLongs) {
			longs[i++] = l;
		}
		return longs;
	}

	public static byte[] longsToBytesTest(List<Long> listOfLongs) {
		byte[] bytes = new byte[listOfLongs.size() * TypeSize.INT64_BYTESIZE];
		int offset = 0;
		for (Long l : listOfLongs) {
			for (int i = 1; i <= TypeSize.INT64_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[offset++] = (byte) ((l >> ((TypeSize.INT64_BYTESIZE - i) * 8)) & 0xFF);
		}
		return bytes;
	}

	public static byte[] longsToBytes(List<Long> listOfLongs) {
		byte[] bytes = new byte[listOfLongs.size() * TypeSize.INT64_BYTESIZE];
		int offset = 0;
		for (Long l : listOfLongs) {
			bytes[offset++] = (byte) ((l >> 56) & 0xFF);
			bytes[offset++] = (byte) ((l >> 48) & 0xFF);
			bytes[offset++] = (byte) ((l >> 40) & 0xFF);
			bytes[offset++] = (byte) ((l >> 32) & 0xFF);
			bytes[offset++] = (byte) ((l >> 24) & 0xFF);
			bytes[offset++] = (byte) ((l >> 16) & 0xFF);
			bytes[offset++] = (byte) ((l >>  8) & 0xFF);
			bytes[offset++] = (byte) ((l >>  0) & 0xFF);
		}
		return bytes;
	}

	public static long[] bytesToLongs(byte[] bytes) {
		if (bytes.length % TypeSize.INT64_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.INT64_BYTESIZE + ". Length: " + bytes.length);
		}
	
		long[] longs = new long[bytes.length / TypeSize.INT64_BYTESIZE];
	
		long bits = 0L;
		int byteOffset = 0;
		for (int i = 0; i < longs.length; i++) {
			bits =               (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			longs[i] = bits;
		}
		return longs;
	}

	public static double[] doublesToDoubles(List<Double> listOfDoubles) {
		double[] doubles = new double[listOfDoubles.size()];
		int i = 0;
		for (Double d : listOfDoubles) {
			doubles[i++] = d;
		}
		return doubles;
	}

	public static byte[] doublesToBytes(List<Double> doubles) {
		byte[] bytes = new byte[doubles.size() * TypeSize.DOUBLE_BYTESIZE];
		int byteOffset = 0;
		for (Double d : doubles) {
			long bits = Double.doubleToLongBits(d);
			bytes[byteOffset++] = (byte) ((bits >> 56) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >> 48) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >> 40) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >> 32) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >> 24) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >> 16) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >>  8) & 0xFF);
			bytes[byteOffset++] = (byte) ((bits >>  0) & 0xFF);
		}
		return bytes;
	}

	public static byte[] doublesToBytesTest(List<Double> doubles) {
		byte[] bytes = new byte[doubles.size() * TypeSize.DOUBLE_BYTESIZE];
		int offset = 0;
		for (Double doub : doubles) {
			long lng = Double.doubleToLongBits(doub);
			for (int i = 1; i <= TypeSize.DOUBLE_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[offset++] = (byte) ((lng >> ((TypeSize.DOUBLE_BYTESIZE - i) * 8)) & 0xff);
		}
		return bytes;
	}

	public static double[] bytesToDoubles(byte[] bytes) {
		if (bytes.length % TypeSize.DOUBLE_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.DOUBLE_BYTESIZE + ". Length: " + bytes.length);
		}
		double[] doubles = new double[bytes.length / TypeSize.DOUBLE_BYTESIZE];
	
		long bits = 0L;
		int  offset = 0;
		
		for (int i = 0; i < doubles.length; i++) {
			bits =               (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			doubles[i] = Double.longBitsToDouble(bits);
		}
		return doubles;
	}

	public static short[] shortsToShorts(List<Short> listOfShorts) {
		short[] shorts = new short[listOfShorts.size()];
		int i = 0;
		for (Short s : listOfShorts) {
			shorts[i++] = s;
		}
		return shorts;
	}

	public static byte[] shortsToBytesTest(List<Short> shorts) {
		byte[] bytes = new byte[shorts.size() * TypeSize.SHORT_BYTESIZE];
		int offset = 0;
		for (Short sh : shorts) {
			for (int i = 1; i <= TypeSize.SHORT_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[offset++] = (byte) ((sh >> ((TypeSize.SHORT_BYTESIZE - i) * 8)) & 0xff);
		}
		return bytes;
	}

	public static byte[] shortsToBytes(List<Short> shorts) {
		byte[] bytes = new byte[shorts.size() * TypeSize.SHORT_BYTESIZE];
		int offset = 0;
		for (Short s : shorts) {
			bytes[offset++] = (byte) ((s >> 8) & 0xFF);
			bytes[offset++] = (byte) ((s >> 0) & 0xFF);
		}
		return bytes;
	}

	public static short[] bytesToShorts(byte[] bytes) {
		if (bytes.length % TypeSize.SHORT_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.SHORT_BYTESIZE + ". Length: " + bytes.length);
		}
	
		short[] shorts = new short[bytes.length / TypeSize.SHORT_BYTESIZE];
	
		int bits = 0;
		int byteOffset = 0;
		for (int i = 0; i < shorts.length; i++) {
			bits =               (bytes[byteOffset++] & 0xFF);
			bits = (bits << 8) | (bytes[byteOffset++] & 0xFF);
			shorts[i] = (short)(bits & 0xFFFF);
		}
		return shorts;
	}

	public static float[] floatsToFloats(List<Float> listOfFloats) {
		float[] floats = new float[listOfFloats.size()];
		int i = 0;
		for (Float f : listOfFloats) {
			floats[i++] = f;
		}
		return floats;
	}

	public static byte[] floatsToBytesTest(List<Float> floats) {
		byte[] bytes = new byte[floats.size() * TypeSize.FLOAT_BYTESIZE];
		int offset = 0;
		for (Float f : floats) {
			int in = Float.floatToIntBits(f);
			for (int i = 1; i <= TypeSize.FLOAT_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[offset++] = (byte) ((in >> ((TypeSize.FLOAT_BYTESIZE - i) * 8)) & 0xFF);
		}
		return bytes;
	}

	public static byte[] floatsToBytes(List<Float> floats) {
		byte[] bytes = new byte[floats.size() * TypeSize.FLOAT_BYTESIZE];
		int offset = 0;
		for (Float f : floats) {
			int in = Float.floatToIntBits(f);
			bytes[offset++] = (byte) ((in >> 24) & 0xFF);
			bytes[offset++] = (byte) ((in >> 16) & 0xFF);
			bytes[offset++] = (byte) ((in >>  8) & 0xFF);
			bytes[offset++] = (byte) ((in >>  0) & 0xFF);
		}
		return bytes;
	}

	public static byte[] floatsToBytes(float[] floats, int startFrom, int endAt) {
		if (startFrom < 0 || startFrom > floats.length) {
			System.err.println("Returning null! startFrom: " + startFrom + " floats.length: " + floats.length);
			return null;
		}
		if (endAt < 0 || endAt > floats.length) {
			System.err.println("Returning null! endAt: " + endAt + " floats.length: " + floats.length);
			return null;
		}
		if (endAt < startFrom) {
			System.err.println("Returning null! endAt: " + endAt + " startFrom: " + startFrom);
			return null;
		}
	
		int numberOfFloats = endAt - startFrom;
	
		// x86 is little Endian
		/*
		 
		0A.0B.0C.0D.
		 |  |  |  |
		 |  |  |  |-> a + 0: 0D
		 |  |  |----> a + 1: 0C
		 |  |-------> a + 2: 0B
		 |----------> a + 3: 0A
		*/
	
		byte[] bytes = new byte[numberOfFloats * TypeSize.FLOAT_BYTESIZE];
		int offset = 0;
		for (int idx = startFrom; idx < endAt; idx++) {
			int fBits = Float.floatToIntBits(floats[idx]);
			bytes[offset++] = (byte) ((fBits >> 24) & 0xFF);
			bytes[offset++] = (byte) ((fBits >> 16) & 0xFF);
			bytes[offset++] = (byte) ((fBits >>  8) & 0xFF);
			bytes[offset++] = (byte) ( fBits        & 0xFF);
		}		
		return bytes;
	}

	public static float[] bytesToFloats(byte[] bytes) {
		if (bytes.length % TypeSize.FLOAT_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.FLOAT_BYTESIZE + ". Length: " + bytes.length);
		}
		float[] floats = new float[bytes.length / TypeSize.FLOAT_BYTESIZE];
	
		int bits = 0;
		int offset = 0;
		for (int i = 0; i < floats.length; i++) {
			bits =               (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			bits = (bits << 8) | (bytes[offset++] & 0xFF);
			floats[i] = Float.intBitsToFloat(bits);
		}
		return floats;
	}

	public static byte[] rleEncode(byte[] uncompressed) {
		int size = uncompressed.length;
		ByteBuffer bb = ByteBuffer.allocate(2 * size);
		bb.putInt(size);
		int zeros = 0;
		for (int i = 0; i < size; i++) {
			if (uncompressed[i] == 0) {
				if (++zeros == 255) {
					bb.putShort((short) zeros);
					zeros = 0;
				}
			} else {
				if (zeros > 0) {
					bb.putShort((short) zeros);
					zeros = 0;
				}
				bb.put(uncompressed[i]);
			}
		}
		if (zeros > 0) {
			bb.putShort((short) zeros);
			zeros = 0;
		}
		size = bb.position();
		byte[] buf = new byte[size];
		bb.rewind();
		bb.get(buf, 0, size).array();
		return buf;
	}

	public static byte[] rleDecode(byte[] compressed) {
		ByteBuffer bb = ByteBuffer.wrap(compressed);
		byte[] uncompressed = new byte[bb.getInt()];
		int pos = 0;
		while (bb.position() < bb.capacity()) {
			byte value = bb.get();
			if (value == 0) {
				bb.position(bb.position() - 1);
				pos += bb.getShort();
			} else {
				uncompressed[pos++] = value;
			}
		}
		return uncompressed;
	}

	public static void testFloats() {
		// Test floats
		float[] floats = {0.123f, 1.234f, 2.345f, -0.123f, -1.234f, -2.345f};
		ArrayList<Float> listOfFloats = new ArrayList<Float>(floats.length);
		for (float f : floats)
				listOfFloats.add(f);
		
		System.out.println("Original floats:");
		Debug.dump(floats);

		byte[] floatsToBytes = floatsToBytes(listOfFloats);
		System.out.println("floatsToBytes:");
		Debug.dump(floatsToBytes);
		
		byte[] floatsToBytesTest = floatsToBytesTest(listOfFloats);
		System.out.println("floatsToBytesTest:");
		Debug.dump(floatsToBytesTest);
		
		float[] bytesToFloats = bytesToFloats(floatsToBytes);
		System.out.println("bytesToFloats(floatsToBytes):");
		Debug.dump(bytesToFloats);
		
		float[] bytesToFloatsTest = bytesToFloats(floatsToBytesTest);
		System.out.println("bytesToFloats(floatsToBytesTest):");
		Debug.dump(bytesToFloatsTest);
	}

	public static void testDoubles() {
		double[] doubles = {0.123d, 1.234d, 2.345d, -0.123d, -1.234d, -2.345d};
		ArrayList<Double> listOfDoubles = new ArrayList<Double>(doubles.length);
		for (double d : doubles)
			listOfDoubles.add(d);
		
		System.out.println("Original doubles:");
		Debug.dump(doubles);

		byte[] doublesToBytes = doublesToBytes(listOfDoubles);
		System.out.println("doublesToBytes:");
		Debug.dump(doublesToBytes);
		
		byte[] doublesToBytesTest = doublesToBytesTest(listOfDoubles);
		System.out.println("doublesToBytesTest:");
		Debug.dump(doublesToBytesTest);
		
		double[] bytesToDoubles = bytesToDoubles(doublesToBytes);
		System.out.println("bytesToDoubles(doublesToBytes):");
		Debug.dump(bytesToDoubles);
		
		double[] bytesToDoublesTest = bytesToDoubles(doublesToBytesTest);
		System.out.println("bytesToDoubles(doublesToBytesTest):");
		Debug.dump(bytesToDoublesTest);
	}
	
	public static void testShorts() {
		short[] shorts = {12345, 23456, 32765, -12345, -23456, -32765};
		ArrayList<Short> listOfShorts = new ArrayList<Short>(shorts.length);
		for (short s : shorts)
			listOfShorts.add(s);
		
		System.out.println("Original shorts:");
		Debug.dump(shorts);
	
		byte[] shortsToBytes = shortsToBytes(listOfShorts);
		System.out.println("shortsToBytes:");
		Debug.dump(shortsToBytes);
		
		byte[] shortsToBytesTest = shortsToBytesTest(listOfShorts);
		System.out.println("shortsToBytesTest:");
		Debug.dump(shortsToBytesTest);
		
		short[] bytesToShorts = bytesToShorts(shortsToBytes);
		System.out.println("bytesToShorts(shortsToBytes):");
		Debug.dump(bytesToShorts);
	
		short[] bytesToShortsTest = bytesToShorts(shortsToBytesTest);
		System.out.println("bytesToShorts(shortsToBytesTest):");
		Debug.dump(bytesToShortsTest);
	
	}

	public static void testIntegers() {
		int[] ints = {123456987, 123456789, 234567890, -123456987, -123456789, -234567890};
		ArrayList<Integer> listOfIntegers = new ArrayList<Integer>(ints.length);
		for (int i : ints)
			listOfIntegers.add(i);
		
		System.out.println("Original integers:");
		Debug.dump(ints);
	
		byte[] integersToBytes = integersToBytes(listOfIntegers);
		System.out.println("integersToBytes:");
		Debug.dump(integersToBytes);
		
		byte[] integersToBytesTest = integersToBytesTest(listOfIntegers);
		System.out.println("integersToBytesTest:");
		Debug.dump(integersToBytesTest);
		
		int[] bytesToIntegers = bytesToInts(integersToBytes);
		System.out.println("bytesToInts(integersToBytes):");
		Debug.dump(bytesToIntegers);

		int[] bytesToIntegersTest = bytesToInts(integersToBytesTest);
		System.out.println("bytesToInts(integersToBytesTest):");
		Debug.dump(bytesToIntegersTest);

	}

	public static void testLongs() {
		long[] longs = {123456987123456987L, 123456789123456789L, 234567890234567890L, -123456987123456987L, -123456789123456789L, -234567890234567890L};
		ArrayList<Long> listOfLongs = new ArrayList<Long>(longs.length);
		for (long l : longs)
			listOfLongs.add(l);
		
		System.out.println("Original longs:");
		Debug.dump(longs);
	
		byte[] longsToBytes = longsToBytes(listOfLongs);
		System.out.println("longsToBytes:");
		Debug.dump(longsToBytes);
		
		byte[] longsToBytesTest = longsToBytesTest(listOfLongs);
		System.out.println("longsToBytesTest:");
		Debug.dump(longsToBytesTest);
		
		long[] bytesToLongs = bytesToLongs(longsToBytes);
		System.out.println("bytesToLongs(longsToBytes):");
		Debug.dump(bytesToLongs);
	
		long[] bytesToLongsTest = bytesToLongs(longsToBytesTest);
		System.out.println("bytesToLongs(longsToBytesTest):");
		Debug.dump(bytesToLongsTest);
	
	}

	public static void main(String[] args) {
		testFloats();
		System.out.println();
		testDoubles();
		System.out.println();
		testIntegers();
		System.out.println();
		testLongs();
		System.out.println();
		testShorts();
	}

}
