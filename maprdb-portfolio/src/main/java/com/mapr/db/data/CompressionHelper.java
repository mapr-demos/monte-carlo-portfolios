package com.mapr.db.data;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

public class CompressionHelper {

//	private static final int SHORT_BYTESIZE  = 2;
//	private static final int INT32_BYTESIZE  = 4;
//	private static final int INT64_BYTESIZE  = 8;
//	private static final int FLOAT_BYTESIZE  = 4;
//	private static final int DOUBLE_BYTESIZE = 8;

	public static int[] integersToInts(List<Integer> integers) {
		int[] ints = new int[integers.size()];
		int i = 0;
		for (Integer n : integers) {
			ints[i++] = n;
		}
		return ints;
	}

	public static double[] doublesToDoubles(List<Double> doubles) {
		double[] doubs = new double[doubles.size()];
		int i = 0;
		for (Double x : doubles) {
			doubs[i++] = x;
		}
		return doubs;
	}

	public static byte[] shortsToBytes(List<Short> shorts) {
		byte[] bytes = new byte[shorts.size() * TypeSize.SHORT_BYTESIZE];
		int offset = -1;
		for (Short sh : shorts) {
			for (int i = 1; i <= TypeSize.SHORT_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[i + offset] = (byte) ((sh >> ((TypeSize.SHORT_BYTESIZE - i) * 8)) & 0xff);
			offset += TypeSize.SHORT_BYTESIZE;
		}
		return bytes;
	}

	public static byte[] integersToBytes(List<Integer> integers) {
		byte[] bytes = new byte[integers.size() * TypeSize.INT32_BYTESIZE];
		int offset = -1;
		for (Integer integer : integers) {
			for (int i = 1; i <= TypeSize.INT32_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[i + offset] = (byte) ((integer >> ((TypeSize.INT32_BYTESIZE - i) * 8)) & 0xff);
			offset += TypeSize.INT32_BYTESIZE;
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

//		int offset = -1;
//		for (int idx = startFrom; idx < endAt; idx++) {
//			for (int i = 1; i <= INT32_BYTESIZE; i++)
//				// x8 because 8 bits per byte
//				bytes[i + offset] = (byte) ((ints[idx] >> ((INT32_BYTESIZE - i) * 8)) & 0xff);
//			offset += INT32_BYTESIZE;
//		}

		int offset = 0;
		for (int idx = startFrom; idx < endAt; idx++) {
			bytes[offset + 0] = (byte) ((ints[idx] >> 24) & 0xFF);
			bytes[offset + 1] = (byte) ((ints[idx] >> 16) & 0xFF);
			bytes[offset + 2] = (byte) ((ints[idx] >> 8) & 0xFF);
			bytes[offset + 3] = (byte) (ints[idx] & 0xFF);
			offset += 4;
		}		

//        ByteBuffer byteBuffer = ByteBuffer.allocate(ints.length * 4);        
//        IntBuffer intBuffer   = byteBuffer.asIntBuffer();
//        intBuffer.put(ints);
//
//        byte[] array = byteBuffer.array();
//        
        
		return bytes;
	}

	public static int[] bytesToIntegers(byte[] bytes) {
		
		if (bytes.length % TypeSize.INT32_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.INT32_BYTESIZE + ". Length: " + bytes.length);
		}

		int byteOffset = 0;
		int[] ints = new int[bytes.length / TypeSize.INT32_BYTESIZE];

		int bits = 0;
		
		for (int i = 0; i < ints.length; i++) {
			bits =               (bytes[byteOffset + 0] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 1] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 2] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 3] & 0xff);
			byteOffset += 4;
			ints[i] = bits;
		}
		
		return ints;
	}
	
	
	public static byte[] doublesToBytes(List<Double> doubles) {
		byte[] bytes = new byte[doubles.size() * TypeSize.DOUBLE_BYTESIZE];
		int offset = -1;
		for (Double doub : doubles) {
			long lng = Double.doubleToLongBits(doub);
			for (int i = 1; i <= TypeSize.DOUBLE_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[i + offset] = (byte) ((lng >> ((TypeSize.DOUBLE_BYTESIZE - i) * 8)) & 0xff);
			offset += TypeSize.DOUBLE_BYTESIZE;
		}
		return bytes;
	}

	public static double[] bytesToDoubles(byte[] bytes) {

		if (bytes.length % TypeSize.DOUBLE_BYTESIZE != 0) {
			System.err.println("Wong number of bytes! It should be a multiple of " + TypeSize.DOUBLE_BYTESIZE + ". Length: " + bytes.length);
		}
		double[] doubles = new double[bytes.length / TypeSize.DOUBLE_BYTESIZE];

		long bits = 0L;
		int byteOffset = 0;
		
		for (int i = 0; i < doubles.length; i++) {
			bits =               (bytes[byteOffset + 0] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 1] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 2] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 3] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 4] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 5] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 6] & 0xff);
			bits = (bits << 8) + (bytes[byteOffset + 7] & 0xff);
			byteOffset += 8;
			
			doubles[i] = Double.longBitsToDouble(bits);
		}
		
		return doubles;
	}
	
	public static byte[] floatsToBytes(List<Float> floats) {
		byte[] bytes = new byte[floats.size() * TypeSize.FLOAT_BYTESIZE];
		int offset = -1;
		for (Float flo : floats) {
			int in = Float.floatToIntBits(flo);
			for (int i = 1; i <= TypeSize.FLOAT_BYTESIZE; i++)
				// x 8 because 8 bits per byte
				bytes[i + offset] = (byte) ((in >> ((TypeSize.FLOAT_BYTESIZE - i) * 8)) & 0xff);
			offset += TypeSize.FLOAT_BYTESIZE;
		}
		return bytes;
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
}
