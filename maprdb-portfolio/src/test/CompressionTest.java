/**
 * Created by pborne on 3/12/17.
 */
import com.mapr.db.data.CompressedDoubleArray;
import com.mapr.db.data.Compression;
import com.mapr.db.data.Debug;
import com.mapr.db.data.TypeSize;
import org.junit.Test;

import java.util.Arrays;


public class CompressionTest {

    @Test
    public void testDeltaValWithIntegers() throws Exception {
        int[] originalIntegers = new int[65536];
        for (int i = 0; i < originalIntegers.length; i++)
            originalIntegers[i] = (int)(10f * 1023f * Math.random() * (Math.random() > 0.5 ? 1: -1));

        int[] compressed   = Compression.deltaValEncode(originalIntegers);
        int[] uncompressed = Compression.deltaValDecode(compressed);

        for (int i = 0; i < originalIntegers.length; i++) {
            if (originalIntegers[i] != uncompressed[i]) {
                System.out.println("Original integers:");
                Debug.dump(originalIntegers);

                System.out.println("compressed:");
                Debug.dump(compressed);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalIntegers[" + i + "]=" + originalIntegers[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        // Basic stats
        System.out.println("Original number of integers:      " + originalIntegers.length);
        System.out.println("Original byte size of integers:   " + originalIntegers.length * TypeSize.INT32_BYTESIZE);
        System.out.println("Compressed byte size of integers: " + compressed.length * TypeSize.INT32_BYTESIZE);
        System.out.println("Compression ratio:                " + (float)(compressed.length * TypeSize.INT32_BYTESIZE) / (float)(originalIntegers.length * TypeSize.INT32_BYTESIZE));
    }

    @Test
    public void testDeltaValWithLongs() throws Exception {
        long[] originalLongs = new long[65536];
        for (int i = 0; i < originalLongs.length; i++)
            originalLongs[i] = (long)(10f * 1000f * Math.random() * (Math.random() > 0.5 ? 1f : -1f));

        long[] compressed   = Compression.deltaValEncode(originalLongs);
        long[] uncompressed = Compression.deltaValDecode(compressed);

        System.out.println("Testing with small values:");
        for (int i = 0; i < originalLongs.length; i++) {
            if (originalLongs[i] != uncompressed[i]) {
                System.out.println("Original longs:");
                Debug.dump(originalLongs);

                System.out.println("compressed:");
                Debug.dump(compressed);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalLongs[" + i + "]=" + originalLongs[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        // Basic stats
        System.out.println("Original number of integers:      " + originalLongs.length);
        System.out.println("Original byte size of integers:   " + originalLongs.length * TypeSize.INT64_BYTESIZE);
        System.out.println("Compressed byte size of integers: " + compressed.length * TypeSize.INT64_BYTESIZE);
        System.out.println("Compression ratio:                " + (float)(compressed.length) / (float)(originalLongs.length));

        for (int i = 0; i < originalLongs.length; i++) {
			originalLongs[i] += (2L << 37)+ i; // 2^37 to get out of range of Integers on 32 bits
            originalLongs[i] *= Math.random() > 0.5 ? 1L : -1L;
        }

        compressed   = Compression.deltaValEncode(originalLongs);
        uncompressed = Compression.deltaValDecode(compressed);

        System.out.println("Testing with large values:");
        for (int i = 0; i < originalLongs.length; i++) {
            if (originalLongs[i] != uncompressed[i]) {
                System.out.println("Original longs:");
                Debug.dump(originalLongs);

                System.out.println("compressed:");
                Debug.dump(compressed);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalLongs[" + i + "]=" + originalLongs[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        // Basic stats
        System.out.println("Original number of integers:      " + originalLongs.length);
        System.out.println("Original byte size of integers:   " + originalLongs.length * TypeSize.INT64_BYTESIZE);
        System.out.println("Compressed byte size of integers: " + compressed.length * TypeSize.INT64_BYTESIZE);
        System.out.println("Compression ratio:                " + (float)(compressed.length) / (float)(originalLongs.length));
    }

    @Test
    public void testDeltaXorWithFloats32() throws Exception {
        float[] originalFloats = new float[8192];

        // Test when all the values are positive
        System.out.println("");
        System.out.println("Using only positive values");
        originalFloats[0] = 1.1f;
        for (int i = 1; i < originalFloats.length; i++)
            originalFloats[i] = originalFloats[0] + (float) Math.random() ;

        CompressedDoubleArray compressed = Compression.deltaXorEncode32(originalFloats);
        float[]              uncompressed = Compression.deltaXorDecode32(compressed);

        for (int i = 0; i < originalFloats.length; i++) {
            if (originalFloats[i] != uncompressed[i]) {
                System.out.println("Original floats:");
                Debug.dump(originalFloats);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalFloats[" + i + "]=" + originalFloats[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        // Mix of positive/negative numbers
        System.out.println("");
        System.out.println("Using a mix of positive/negative numbers (not sorted)");
        for (int i = 1; i < originalFloats.length; i++)
            originalFloats[i] *= Math.random() > 0.5 ? 1f : -1f;

        compressed   = Compression.deltaXorEncode32(originalFloats);
        uncompressed = Compression.deltaXorDecode32(compressed);

        for (int i = 0; i < originalFloats.length; i++) {
            if (originalFloats[i] != uncompressed[i]) {
                System.out.println("Original floats:");
                Debug.dump(originalFloats);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalFloats[" + i + "]=" + originalFloats[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        System.out.println("");
        System.out.println("Using a mix of positive/negative numbers (sorted)");
        Arrays.sort(originalFloats);

        compressed   = Compression.deltaXorEncode32(originalFloats);
        uncompressed = Compression.deltaXorDecode32(compressed);

        for (int i = 0; i < originalFloats.length; i++) {
            if (originalFloats[i] != uncompressed[i]) {
                System.out.println("Original floats:");
                Debug.dump(originalFloats);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalFloats[" + i + "]=" + originalFloats[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

    }

    @Test
    public void testDeltaXorWithFloats64() throws Exception {
        double[] originalDoubles = new double[8192];

        // Test when all the values are positive
        System.out.println("");
        System.out.println("Using only positive values");
        originalDoubles[0] = 1.1d;
        for (int i = 1; i < originalDoubles.length; i++)
            originalDoubles[i] = originalDoubles[0] + Math.random();

        CompressedDoubleArray compressed = Compression.deltaXorEncode64(originalDoubles);
        double[]             uncompressed = Compression.deltaXorDecode64(compressed);

        for (int i = 0; i < originalDoubles.length; i++) {
            if (originalDoubles[i] != uncompressed[i]) {
                System.out.println("Original doubles:");
                Debug.dump(originalDoubles);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalDoubles[" + i + "]=" + originalDoubles[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        // Mix of positive/negative numbers
        System.out.println("");
        System.out.println("Using a mix of positive/negative numbers (not sorted)");
        for (int i = 1; i < originalDoubles.length; i++)
            originalDoubles[i] *= Math.random() > 0.5d ? 1.0d : -1.0d;

        compressed   = Compression.deltaXorEncode64(originalDoubles);
        uncompressed = Compression.deltaXorDecode64(compressed);

        for (int i = 0; i < originalDoubles.length; i++) {
            if (originalDoubles[i] != uncompressed[i]) {
                System.out.println("Original doubles:");
                Debug.dump(originalDoubles);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalDoubles[" + i + "]=" + originalDoubles[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

        System.out.println("");
        System.out.println("Using a mix of positive/negative numbers (sorted)");
        Arrays.sort(originalDoubles);

        compressed   = Compression.deltaXorEncode64(originalDoubles);
        uncompressed = Compression.deltaXorDecode64(compressed);

        for (int i = 0; i < originalDoubles.length; i++) {
            if (originalDoubles[i] != uncompressed[i]) {
                System.out.println("Original doubles:");
                Debug.dump(originalDoubles);

                System.out.println("uncompressed:");
                Debug.dump(uncompressed);

                throw new RuntimeException("Values are different: originalDoubles[" + i + "]=" + originalDoubles[i] +
                        " uncompressed[" + i + "]=" + uncompressed[i]);
            }
        }

    }


}
