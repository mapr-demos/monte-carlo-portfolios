package com.mapr.db.data;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Compression {

	private static boolean debug = true;

	public static void deltaXorEncode(float[] uncompressed) {
		/*
		The IEEE 754 standard specifies a binary32 as having:
			Sign bit: 1 bit
			Exponent width: 8 bits
			Significand precision: 24 bits (23 explicitly stored)
		*/
		
		int[] uncompressedInts = BitManipulationHelper.floatsToInts(uncompressed, 0, uncompressed.length);
		
		int numberOfSignInts;
		if (uncompressedInts.length % 32 == 0)
			numberOfSignInts = uncompressedInts.length / 32; // Divide by 32
		else
			numberOfSignInts = 1 + uncompressedInts.length / 32; // Shift 5 bits to the right to divide by 32			
		int[] uncompressedSigns = new int [numberOfSignInts];

		int numberOfExponentInts;
		if ((uncompressedInts.length * 8) % 32 == 0) // x8 because we need 8 bits to store the exponent
			numberOfExponentInts = uncompressedInts.length * 8 / 32;
		else
			numberOfExponentInts = 1 + uncompressedInts.length * 8 / 32;
		int[] uncompressedExponents = new int [numberOfExponentInts];
		
		int numberOfSignificandInts;
		if ((uncompressedInts.length * 23) % 32 == 0) // x23 because we need 23 bits to store the significand
			numberOfSignificandInts = uncompressedInts.length * 23 / 32;
		else
			numberOfSignificandInts = 1 + uncompressedInts.length * 23 / 32;
		int[] uncompressedSignificands = new int [numberOfSignificandInts];
		
		
		int signOffset = 0;
		int exponentOffset = 0;
		int significandOffset = 0;

		// Retrieve the significands as is
		for (int idx = 0; idx < uncompressedInts.length; idx++) {
			int currentInt = uncompressedInts[idx];
			writeBits(uncompressedSignificands, currentInt & 0b00000000011111111111111111111111,         significandOffset, 23); // Mask the bits we want
			significandOffset += 23;
		}

		System.out.println("\nuncompressedSignificands (no XOR):");
		for (int significand : uncompressedSignificands)
			System.out.print("00000000000000000000000000000000".substring(Integer.toBinaryString(significand).length()) + Integer.toBinaryString(significand) + " ");

		// Transpose the significands and compute the numbers of bits set to 1 per row (i.e. column in the original data set)
		System.out.println("\nTranspose and count the significands:");

		int[] countOnes  = new int[32];
		int[] countZeros = new int[32];
		for (int i = 0; i < uncompressedSignificands.length; i += 32) {
			int[] transposedSignificands = transpose32b(Arrays.copyOfRange(uncompressedSignificands, i, i + 32));
			int offset = 0;
			for (int transposedSignificand : transposedSignificands) {
				int numberOfOnes = Integer.bitCount(transposedSignificand);
				System.out.println("00000000000000000000000000000000"
						.substring(Integer.toBinaryString(transposedSignificand).length())
						+ Integer.toBinaryString(transposedSignificand) + " has "
						+ numberOfOnes + " bits(s) set to 1");
				countOnes[offset] += numberOfOnes;
				countZeros[offset] += 32 - numberOfOnes;
				offset++;
			}
		}
		for (int i =0; i < countOnes.length; i++) {
			System.out.println("countOnes[" + i + "] = " + countOnes[i] + " countZeros[" + i + "] = " + countZeros[i] + " Ratio = " + ((float)countOnes[i] / ((float)countOnes[i] + (float)countZeros[i])));
		}
		
		// Try a XOR on the exponents and the significand
		signOffset        = 0;
		exponentOffset    = 0;
		significandOffset = 0;

		int previousInt = uncompressedInts[0];
		writeBits(uncompressedSigns, uncompressedInts[0] >>> 31, signOffset++, 1);
		writeBits(uncompressedExponents,   (uncompressedInts[0] & 0b01111111100000000000000000000000) >>> 23, exponentOffset, 8); // Mask the bits we want
		writeBits(uncompressedSignificands, uncompressedInts[0] & 0b00000000011111111111111111111111, significandOffset, 23); // Mask the bits we want
		exponentOffset += 8;
		significandOffset += 23;

		for (int idx = 1; idx < uncompressedInts.length; idx++) {
			int currentInt = uncompressedInts[idx];
			writeBits(uncompressedSigns, currentInt >>> 31, signOffset++, 1);
			writeBits(uncompressedExponents,   ((previousInt ^ currentInt) & 0b01111111100000000000000000000000) >>> 23, exponentOffset, 8); // Mask the bits we want
			writeBits(uncompressedSignificands, (previousInt ^ currentInt) & 0b00000000011111111111111111111111, significandOffset, 23); // Mask the bits we want
			exponentOffset += 8;
			significandOffset += 23;
			previousInt = currentInt;
		}
		
		if (debug) {
			System.out.println("\nuncompressedSigns:");
			for (int sign : uncompressedSigns) {
				System.out.print("00000000000000000000000000000000".substring(Integer.toBinaryString(sign).length()) + Integer.toBinaryString(sign) + " ");
			}

			System.out.println("\nuncompressedExponents (1st try on XOR):");
			for (int exponent : uncompressedExponents) {
				System.out.print("00000000000000000000000000000000".substring(Integer.toBinaryString(exponent).length()) + Integer.toBinaryString(exponent) + " ");
			}
			
			System.out.println("\nuncompressedSignificands (1st try on XOR):");
			for (int significand : uncompressedSignificands) {
			System.out.print("00000000000000000000000000000000".substring(Integer.toBinaryString(significand).length()) + Integer.toBinaryString(significand) + " ");
			}
	
		}

		// Start over to try something else
		signOffset        = 0;
		exponentOffset    = 0;
		significandOffset = 0;
		for (int idx = 0; idx < uncompressedInts.length; idx++) {
			int currentInt = uncompressedInts[idx];
			writeBits(uncompressedSigns, currentInt >>> 31, signOffset++, 1);
			writeBits(uncompressedExponents,   (currentInt & 0b01111111100000000000000000000000) >>> 23, exponentOffset, 8); // Mask the bits we want
			writeBits(uncompressedSignificands, currentInt & 0b00000000011111111111111111111111,         significandOffset, 23); // Mask the bits we want
			exponentOffset += 8;
			significandOffset += 23;
		}
		
		int previousSignificand = uncompressedSignificands[0];
		for (int idx = 1; idx < uncompressedSignificands.length; idx++) {
			int currentSignificand = uncompressedSignificands[idx];
			uncompressedSignificands[idx] = previousSignificand ^ currentSignificand;
			previousSignificand = currentSignificand;
		}
	
		System.out.println("\nuncompressedSignificands (2nd try on XOR):");
		for (int significand : uncompressedSignificands)
			System.out.print("00000000000000000000000000000000".substring(Integer.toBinaryString(significand).length()) + Integer.toBinaryString(significand) + " ");
		
	}
	
	public static long[] deltaValEncode (long[] uncompressed) {
		long min = Long.MAX_VALUE;
		long max = Long.MIN_VALUE;
		
		// Find the min and max in the set to figure out the range
		for (long v : uncompressed) {
			if (v > max)
				max = v;
			if (v < min)
				min = v;
		}
		
		// Determine the number of bits needed to encode the delta from the minimum
		long range = max - min;
		int numberOfBitsToEncode = 64 - Long.numberOfLeadingZeros(range); 
		
		if (debug)
			System.out.println("Min: " + min + " Max: " + max + " Range: " + range + " Number of bits to encode: " + numberOfBitsToEncode);

		// Create the resulting aray
		// + 1 for the first entry that stores the minimum value
		// + 1 for the number of bits used to encode each delta
		// + 1 for the number of integers when decompressing
		int numberOfCompressedLongs = 1 + (numberOfBitsToEncode * uncompressed.length) / 64;
		long[] compressed = new long[1 + 1 + 1 + numberOfCompressedLongs]; 

		if (debug)
			System.out.println("numberOfCompressedLongs: " + numberOfCompressedLongs + " uncompressed.length: " + uncompressed.length);
		
		compressed[0] = min;
		// Combine both on 64 bits
		compressed[1] = ((long)numberOfBitsToEncode ) << 32;
		compressed[1] |= uncompressed.length;
		
		int bitOffset = 2 * 64; // 2 for the 2 entries above x 64 bits each,
		for (long value : uncompressed) {
			writeBits(compressed, value - min, bitOffset , numberOfBitsToEncode);
			bitOffset += numberOfBitsToEncode;
		}
		
		return compressed;
	}

	public static long[] deltaValDecode (long[] compressed) {

		long   min                  = compressed[0];
		int    numberOfBitsToEncode = (int)(compressed[1] >>> 32);
		int    numberOfUncompressedLongs = (int)(compressed[1] & 0xFFFFFFFF);
		
		if (debug)
			System.out.println("numberOfUncompressedLongs: " + numberOfUncompressedLongs);

		long[] uncompressed         = new long[numberOfUncompressedLongs ]; // Keep only the low 32 bits

		if (debug)
			System.out.println("Min: " + min + " numberOfBitsToEncode: " + numberOfBitsToEncode + " uncompressed.length: " + uncompressed.length);

		int offsetBitsCompressed = 2 * 64; // Skip the first 2 entries above x 64 bits each
		for (int i = 0; i < uncompressed.length; i++) {
			uncompressed[i] = min + readBits(compressed, offsetBitsCompressed, numberOfBitsToEncode);
			offsetBitsCompressed += numberOfBitsToEncode;
		}

		return uncompressed;
	}
	
	public static int[] deltaValEncode (int[] uncompressed) {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		
		// Find the min and max in the set to figure out the range
		for (int v : uncompressed) {
			if (v > max)
				max = v;
			if (v < min)
				min = v;
		}
		
		// Determine the number of bits needed to encode the delta from the minimum
		int range = max - min;
		int numberOfBitsToEncode = 32 - Integer.numberOfLeadingZeros(range); 
		
		if (debug)
			System.out.println("Min: " + min + " Max: " + max + " Range: " + range + " Number of bits to encode: " + numberOfBitsToEncode);

		// Create the resulting aray
		// + 1 for the first entry that stores the minimum value
		// + 1 for the number of bits used to encode each delta
		// + 1 for the number of integers when decompressing
		int numberOfCompressedInts = 1 + (numberOfBitsToEncode * uncompressed.length) / 32;
		int[] compressed = new int[1 + 1 + 1 + numberOfCompressedInts]; 

		if (debug)
			System.out.println("numberOfCompressedInts: " + numberOfCompressedInts + " uncompressed.length: " + uncompressed.length);
		
		compressed[0] = min;
		compressed[1] = numberOfBitsToEncode;
		compressed[2] = uncompressed.length;
		
		int bitOffset = 3 * 32; // 3 for the 3 entries above x 32 bits each,
		for (int value : uncompressed) {
			writeBits(compressed, value - min, bitOffset , numberOfBitsToEncode);
			bitOffset += numberOfBitsToEncode;
		}
		
		return compressed;
	}

	public static int[] deltaValDecode (int[] compressed) {

		int min                  = compressed[0];
		int numberOfBitsToEncode = compressed[1];
		int[] uncompressed       = new int[compressed[2]];

		if (debug)
			System.out.println("Min: " + min + " numberOfBitsToEncode: " + numberOfBitsToEncode + " uncompressed.length: " + uncompressed.length);

		int offsetBitsCompressed = 3 * 32; // Skip the first 3 entries above x 32 bits each
		for (int i = 0; i < uncompressed.length; i++) {
			uncompressed[i] = min + readBits(compressed, offsetBitsCompressed, numberOfBitsToEncode);
			offsetBitsCompressed += numberOfBitsToEncode;
		}

		return uncompressed;
	}
	
    /**
     * Write a certain number of bits of an integer into an integer array
     * starting from the given start offset
     * 
     * @param out
     *                the output array
     * @param val
     *                the integer to be written
     * @param outOffset
     *                the start offset in bits in the output array
     * @param bits
     *                the number of bits to be written (bits greater or equal to 0)
     */
    public static final void writeBits(int[] out, int val, int outOffset, int bits) {
            if (bits == 0)
                    return;

            final int index = outOffset >>> 5;     // Divide by 32 (=2^5)
            final int skip  = outOffset & 0b11111; // Keep the last 5 bits
            val &= (0xFFFFFFFF >>> (32 - bits));
            out[index] |= (val << skip);
            if (32 - skip < bits)
                    out[index + 1] |= (val >>> (32 - skip));
    }

    /**
     * Read a certain number of bits of an integer into an integer array
     * starting from the given start offset
     * 
     * @param in
     *                the input array
     * @param inOffset
     *                the start offset in bits in the input array
     * @param bits
     *                the number of bits to be read, unlike writeBits(),
     *                readBits() does not deal with bits == 0 and thus bits
     *                must be greater than 0. When bits == 0, the calling functions will
     *                just skip the entire bits-bit slots without decoding them
     * @return the bits bits of the input
     */
    public static final int readBits(int[] in, final int inOffset, final int bits) {
            final int index = inOffset >>> 5;     // Divide by 32 (-2^5)
            final int skip  = inOffset & 0b11111; // Keep the last 5 bits
            int val = in[index] >>> skip;
            if (32 - skip < bits) {
                    val |= (in[index + 1] << (32 - skip));
            }
            return val & (0xFFFFFFFF >>> (32 - bits));
    }

    /**
     * Write a certain number of bits of a long into a long array
     * starting from the given start offset
     * 
     * @param out
     *                the output array
     * @param val
     *                the integer to be written
     * @param outOffset
     *                the start offset in bits in the output array
     * @param bits
     *                the number of bits to be written (bits greater or equal to 0)
     */
    public static final void writeBits(long[] out, long val, int outOffset, int numberOfbits) {
            if (numberOfbits == 0)
                    return;

            final int index = outOffset >>> 6;      // divide by 64 (=2^6)
            final int skip  = outOffset & 0b111111; // Keep the last 6 bits  
            val &= (0xFFFFFFFFFFFFFFFFL >>> (64 - numberOfbits));
            out[index] |= (val << skip);
            if (64 - skip < numberOfbits)
                    out[index + 1] |= (val >>> (64 - skip));
    }

    /**
     * Read a certain number of bits of a long into a long array
     * starting from the given start offset
     * 
     * @param in
     *                the input array
     * @param inOffset
     *                the start offset in bits in the input array
     * @param bits
     *                the number of bits to be read, unlike writeBits(),
     *                readBits() does not deal with bits == 0 and thus bits
     *                must be greater than 0. When bits == 0, the calling functions will
     *                just skip the entire bits-bit slots without decoding them
     * @return the bits bits of the input
     */
    public static final long readBits(long[] in, final int inOffset, final int numberOfBits) {
            final int index = inOffset >>> 6;      // Divide by 64 (=2^6)
            final int skip  = inOffset & 0b111111; // Keep the last 6 bits
            long val = in[index] >>> skip;
            if (64 - skip < numberOfBits) {
                    val |= (in[index + 1] << (64 - skip));
            }
            return val & (0xFFFFFFFFFFFFFFFFL >>> (64 - numberOfBits));
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

	public static int[] transpose32b(int[] input) {
		if (input.length != 32) // Not a square matrix...
			return null;
		
		int j, k;
		int m, t; // Should be unsigned

		m = 0x0000FFFF;
		for (j = 16; j != 0; j = j >>> 1, m = m ^ (m << j)) {
			for (k = 0; k < 32; k = (k + j + 1) & ~j) {
				t = (input[k] ^ (input[k + j] >>> j)) & m;
				input[k]     = input[k]     ^ t;
				input[k + j] = input[k + j] ^ (t << j);
			}
		}

		return input;
	}
	
	public static void testDeltaValWithIntegers() {
		int[] test = new int[64];
		for (int i = 0 ; i < test.length; i++)
			test[i] += 10 * 1000 * Math.random();
		
		int[]   compressed = deltaValEncode(test);
		int[] uncompressed = deltaValDecode(compressed);
		
		for (int i = 0; i < test.length; i++)
			System.out.print((i == 0 ? "" : ", ") + test[i]);
		System.out.println();
		
		System.out.println("Uncompressed:");
		for (int i = 0; i < uncompressed.length; i++)
			System.out.print((i == 0 ? "" : ", ") + uncompressed[i]);
		System.out.println();
		
		System.out.println("uncompressed.length: " + uncompressed.length + " compressed.length:" + compressed.length);
	}
	
	public static void testDeltaValWithLongs() {
		long[] test = new long[64];
		for (int i = 0 ; i < test.length; i++)
//			test[i] += (2L << 33)+ i; // 2^33 to get out of range of Integers on 32 bits
			test[i] += 10L * 1000L * Math.random();

		long[]   compressed = deltaValEncode(test);
		long[] uncompressed = deltaValDecode(compressed);
		
		for (int i = 0; i < test.length; i++)
			System.out.print((i == 0 ? "" : ", ") + test[i]);
		System.out.println();
		
		System.out.println("Uncompressed:");
		for (int i = 0; i < uncompressed.length; i++)
			System.out.print((i == 0 ? "" : ", ") + uncompressed[i]);
		System.out.println();
		
		System.out.println("uncompressed.length: " + uncompressed.length + " compressed.length:" + compressed.length);
	}
	
	public static void testDeltaXorWithFloats() {
		float[] test = new float[1024];
		test[0] = 1.1f;
		for (int i = 1 ; i < test.length; i++)
			test[i] = test[i - 1] + (float)Math.random();

		deltaXorEncode(test);
	}
	
	public static void testTranspose32b() {
		   final int[] testMatrix = {            // Test matrix.
				      0x01020304, 0x05060708, 0x090A0B0C, 0x0D0E0F00,
				      0xF0E0D0C0, 0xB0A09080, 0x70605040, 0x30201000,
				      0x00000000, 0x01010101, 0x02020202, 0x04040404,
				      0x08080808, 0x10101010, 0x20202020, 0x40404040,

				      0x80808080, 0xFFFFFFFF, 0xFEFEFEFE, 0xFDFDFDFD,
				      0xFBFBFBFB, 0xF7F7F7F7, 0xEFEFEFEF, 0xDFDFDFDF,
				      0xBFBFBFBF, 0x7F7F7F7F, 0x80000001, 0xC0000003,
				      0xE0000007, 0xF000000F, 0xF800001F, 0xFC00003F};

		   final int[] testMatrixTranspose = {            // transpose.
				      0x0C00FFBF, 0x0A017F5F, 0x0F027ECF, 0x0F047DC7,
				      0x30087BC3, 0x501077C1, 0x00206FC0, 0xF0405FC0,
				      0x0C00FF80, 0x0A017F40, 0x0F027EC0, 0x00047DC0,
				      0x30087BC0, 0x501077C0, 0xF0206FC0, 0x00405FC0,

				      0x0C00FF80, 0x0A017F40, 0x00027EC0, 0x0F047DC0,
				      0x30087BC0, 0x501077C0, 0xF0206FC0, 0xF0405FC0,
				      0x0C00FF80, 0x0A017F40, 0x00027EC1, 0x00047DC3,
				      0x60087BC7, 0xA01077CF, 0x00206FDF, 0x00405FFF};

		   System.out.println("transpose32b, forward test:");
		   
		   boolean ok = true;
		   int[] resultForward = transpose32b(testMatrix);
		   for (int i = 0; i < 32; i++)
		      if (resultForward[i] != testMatrixTranspose[i]) {
		    	  System.out.println("Error: entry[" + i + "] should be " + testMatrixTranspose[i] + " found: " + resultForward[i]);
		    	  ok = false;
		      }
		   
		   if (ok)
			   System.out.println("Passed");
		   
		   System.out.println("transpose32b, reverse test:");
		   ok = true;
		   int[] resultBackward = transpose32b(resultForward);
		   for (int i = 0; i < 32; i++)
		      if (resultBackward[i] != testMatrix[i]) {
		    	  System.out.println("Error backward: entry[" + i + "] should be " + testMatrix[i] + " found: " + resultBackward[i]);
		    	  ok = false;
		      }

		   if (ok)
			   System.out.println("Passed");
		   
	}
	
	public static void main(String[] args) {

		System.out.println("testDeltaValWithIntegers()");
		testDeltaValWithIntegers();
		System.out.println("\ntestDeltaValWithLongs()");
		testDeltaValWithLongs();
		System.out.println("\ntestDeltaXorWithFloats()");
		testDeltaXorWithFloats();
		System.out.println("\ntestTranspose32b()");
		testTranspose32b();
	}
}
