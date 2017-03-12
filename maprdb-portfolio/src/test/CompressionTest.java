/**
 * Created by pborne on 3/12/17.
 */
import com.mapr.db.data.CompressedFloatArray;
import com.mapr.db.data.Compression;
import com.mapr.db.data.Debug;
import org.junit.Test;

import java.util.Arrays;


public class CompressionTest {

    @Test
    public void testDeltaValWithIntegers() throws Exception {
        int[] originalIntegers = new int[64];
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
    }

    @Test
    public void testDeltaValWithLongs() throws Exception {
        long[] originalLongs = new long[64];
        for (int i = 0; i < originalLongs.length; i++)
            originalLongs[i] = (long)(10f * 1000f * Math.random() * (Math.random() > 0.5 ? 1f : -1f));

        long[] compressed   = Compression.deltaValEncode(originalLongs);
        long[] uncompressed = Compression.deltaValDecode(compressed);

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

        for (int i = 0; i < originalLongs.length; i++) {
			originalLongs[i] += (2L << 37)+ i; // 2^37 to get out of range of Integers on 32 bits
            originalLongs[i] *= Math.random() > 0.5 ? 1L : -1L;
        }

        compressed   = Compression.deltaValEncode(originalLongs);
        uncompressed = Compression.deltaValDecode(compressed);

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
    }

    @Test
    public void testDeltaXorWithFloats() throws Exception {
        float[] originalFloats = new float[65536];

        // Test when all the values are positive
        System.out.println("");
        System.out.println("Using only positive values");
        originalFloats[0] = 1.1f;
        for (int i = 1; i < originalFloats.length; i++)
            originalFloats[i] = originalFloats[0] + (float) Math.random() ;

        CompressedFloatArray   compressed = Compression.deltaXorEncode(originalFloats);
        float[]              uncompressed = Compression.deltaXorDecode(compressed);

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

        compressed   = Compression.deltaXorEncode(originalFloats);
        uncompressed = Compression.deltaXorDecode(compressed);

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

        compressed   = Compression.deltaXorEncode(originalFloats);
        uncompressed = Compression.deltaXorDecode(compressed);

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


}
