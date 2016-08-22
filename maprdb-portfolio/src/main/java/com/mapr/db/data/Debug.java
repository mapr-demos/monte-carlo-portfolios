package com.mapr.db.data;

public class Debug {

	static void dump(byte[] buffer) {
		for (int i = 0; i < buffer.length; i++)
			System.out.print(buffer[i] + " ");
		System.out.println();
	}

	static void dump(float[] floats) {
		for (int i = 0; i < floats.length; i++)
			System.out.print(floats[i] + " ");
		System.out.println();
	}

	static void dump(double[] doubles) {
		for (int i = 0; i < doubles.length; i++)
			System.out.print(doubles[i] + " ");
		System.out.println();
	}

	static void dump(short [] shorts) {
		for (int i = 0; i < shorts.length; i++)
			System.out.print(shorts[i] + " ");
		System.out.println();
	}

	static void dump(int[] ints) {
		for (int i = 0; i < ints.length; i++)
			System.out.print(ints[i] + " ");
		System.out.println();
	}

	static void dump(long[] longs) {
		for (int i = 0; i < longs.length; i++)
			System.out.print(longs[i] + " ");
		System.out.println();
	}

	static void compareByteBuffers(byte[] reference, byte[] other, String beginning) {		
        if (reference == null || other == null || reference.length != other.length ) {
        	System.err.println(beginning + " Buffers are different or one of them is empty.");
        	return;
        }

		for (int i = 0; i < reference.length; i++)
			if (reference[i] != other[i]) {
				System.err.println(beginning + " Difference detected between buffers.");
				Debug.dump(reference);
				Debug.dump(other);
				return;
			}
		
		System.out.println(beginning + " Buffers are identical.");
	}

	static void compareIntegerBuffers(int[] reference, int[] other, String beginning) {		
        if (reference == null || other == null || reference.length != other.length ) {
        	System.err.println(beginning + " Buffers are different or one of them is empty.");
        	return;
        }

		for (int i = 0; i < reference.length; i++)
			if (reference[i] != other[i]) {
				System.err.println(beginning + " Difference detected between buffers.");
				Debug.dump(reference);
				Debug.dump(other);
				return;
			}
		
		System.out.println(beginning + " Buffers are identical.");
	}
}
