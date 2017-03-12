/**
 * Created by pborne on 3/11/17.
 */

import com.mapr.db.data.BitManipulationHelper;
import com.mapr.db.data.Debug;
import com.mapr.db.data.DebugGeneric;
import org.junit.Test;

import java.util.ArrayList;

public class BitManipulationHelperTest {

    static int numberOfFloats = 20;
    static int numberOfDoubles = 20;
    static int numberOfIntegers = 200;

    static float magnitude = 10000.0f;
    static double magnitudeD = 10000.0d;
    static float magnitudeI = 10000;

    @Test
    public void testFloatsToBytes() throws Exception {
        ArrayList<Float> originalFloats = new ArrayList<>(numberOfFloats);
        for (int i = 0; i < numberOfFloats; i++)
            originalFloats.add((float) Math.random() * magnitude);

        byte[] floatsToBytes = BitManipulationHelper.floatsToBytes(originalFloats);
        float[] bytesToFloats = BitManipulationHelper.bytesToFloats(floatsToBytes);

        for (int i = 0; i < originalFloats.size(); i++) {
            if (originalFloats.get(i) != bytesToFloats[i]) {
                DebugGeneric<Float> d = new DebugGeneric<>();
                System.out.println("Original floats:");
                d.dump(originalFloats);

                System.out.println("floatsToBytes:");
                Debug.dump(floatsToBytes);

                System.out.println("bytesToFloats:");
                Debug.dump(bytesToFloats);

                throw new RuntimeException("Values are different: originalFloats[" + i + "]=" + originalFloats.get(i) +
                        " bytesToFloats[" + i + "]=" + bytesToFloats[i]);
            }
        }
    }

    @Test
    public void testFloatsToInts() throws Exception {
        float[] originalFloats = new float[numberOfFloats];
        for (int i = 0; i < numberOfFloats; i++)
            originalFloats[i] = (float) Math.random() * magnitude;

        int[] floatsToInts = BitManipulationHelper.floatsToInts(originalFloats, 0, originalFloats.length);
        float[] intsToFloats = BitManipulationHelper.intsToFloats(floatsToInts);

        for (int i = 0; i < originalFloats.length; i++) {
            if (originalFloats[i] != intsToFloats[i]) {
                System.out.println("Original floats:");
                Debug.dump(originalFloats);

                System.out.println("floatsToInts:");
                Debug.dump(floatsToInts);

                System.out.println("intsToFloats:");
                Debug.dump(intsToFloats);

                throw new RuntimeException("Values are different: originalFloats[" + i + "]=" + originalFloats[i] +
                        " intsToFloats[" + i + "]=" + intsToFloats[i]);
            }
        }
    }

    @Test
    public void testFloatsToBytesLoop() throws Exception {
        ArrayList<Float> originalFloats = new ArrayList<>(numberOfFloats);
        for (int i = 0; i < numberOfFloats; i++)
            originalFloats.add((float) Math.random() * magnitude);

        byte[] floatsToBytesLoop = BitManipulationHelper.floatsToBytesLoop(originalFloats);
        float[] bytesToFloats = BitManipulationHelper.bytesToFloats(floatsToBytesLoop);

        for (int i = 0; i < originalFloats.size(); i++) {
            if (originalFloats.get(i) != bytesToFloats[i]) {
                DebugGeneric<Float> d = new DebugGeneric<>();
                System.out.println("Original floats:");
                d.dump(originalFloats);

                System.out.println("floatsToBytesLoop:");
                Debug.dump(floatsToBytesLoop);

                System.out.println("bytesToFloats:");
                Debug.dump(bytesToFloats);

                throw new RuntimeException("Values are different: originalFloats[" + i + "]=" + originalFloats.get(i) +
                        " bytesToFloats[" + i + "]=" + bytesToFloats[i]);
            }
        }
    }

    @Test
    public void testDoublesToBytes() throws Exception {
        ArrayList<Double> originalDoubles = new ArrayList<>(numberOfDoubles);
        for (int i = 0; i < numberOfDoubles; i++)
            originalDoubles.add(Math.random() * magnitudeD);

        byte[] doublesToBytes = BitManipulationHelper.doublesToBytes(originalDoubles);
        double[] bytesToDoubles = BitManipulationHelper.bytesToDoubles(doublesToBytes);

        for (int i = 0; i < originalDoubles.size(); i++) {
            if (originalDoubles.get(i) != bytesToDoubles[i]) {
                DebugGeneric<Double> d = new DebugGeneric<>();
                System.out.println("Original floats:");
                d.dump(originalDoubles);

                System.out.println("doublesToBytes:");
                Debug.dump(doublesToBytes);

                System.out.println("bytesToDoubles:");
                Debug.dump(bytesToDoubles);

                throw new RuntimeException("Values are different: originalDoubles[" + i + "]=" + originalDoubles.get(i) +
                        " bytesToDoubles[" + i + "]=" + bytesToDoubles[i]);
            }
        }
    }

    @Test
    public void testDoublesToBytesLoop() throws Exception {
        ArrayList<Double> originalDoubles = new ArrayList<>(numberOfDoubles);
        for (int i = 0; i < numberOfDoubles; i++)
            originalDoubles.add(Math.random() * magnitudeD);

        byte[] doublesToBytes = BitManipulationHelper.doublesToBytesLoop(originalDoubles);
        double[] bytesToDoubles = BitManipulationHelper.bytesToDoubles(doublesToBytes);

        for (int i = 0; i < originalDoubles.size(); i++) {
            if (originalDoubles.get(i) != bytesToDoubles[i]) {
                DebugGeneric<Double> d = new DebugGeneric<>();
                System.out.println("Original floats:");
                d.dump(originalDoubles);

                System.out.println("doublesToBytes:");
                Debug.dump(doublesToBytes);

                System.out.println("bytesToDoubles:");
                Debug.dump(bytesToDoubles);

                throw new RuntimeException("Values are different: originalDoubles[" + i + "]=" + originalDoubles.get(i) +
                        " bytesToDoubles[" + i + "]=" + bytesToDoubles[i]);
            }
        }
    }

    @Test
    public void testDoublesToLongs() throws Exception {
        double[] originalDoubles = new double[numberOfFloats];
        for (int i = 0; i < numberOfDoubles; i++)
            originalDoubles[i] = Math.random() * magnitudeD;

        long[] doublesToLongs = BitManipulationHelper.doublesToLongs(originalDoubles, 0, originalDoubles.length);
        double[] longsToDoubles = BitManipulationHelper.longsToDoubles(doublesToLongs);

        for (int i = 0; i < originalDoubles.length; i++) {
            if (originalDoubles[i] != longsToDoubles[i]) {
                System.out.println("Original doubles:");
                Debug.dump(originalDoubles);

                System.out.println("doublesToLongs:");
                Debug.dump(doublesToLongs);

                System.out.println("longsToDoubles:");
                Debug.dump(longsToDoubles);

                throw new RuntimeException("Values are different: originalDoubles[" + i + "]=" + originalDoubles[i] +
                        " bytesToDoubles[" + i + "]=" + longsToDoubles[i]);
            }
        }
    }

    @Test
    public void testIntegersToBytes() throws Exception {
        ArrayList<Integer> originalIntegers = new ArrayList<Integer>(numberOfIntegers);
        for (int i = 0; i < numberOfDoubles; i++)
            originalIntegers.add((int) (Math.random() * magnitudeI));

        byte[] integersToBytes = BitManipulationHelper.integersToBytes(originalIntegers);
        int[] bytesToIntegers = BitManipulationHelper.bytesToInts(integersToBytes);

        for (int i = 0; i < originalIntegers.size(); i++) {
            if (originalIntegers.get(i) != bytesToIntegers[i]) {
                DebugGeneric<Integer> d = new DebugGeneric<>();
                System.out.println("Original integers:");
                d.dump(originalIntegers);

                System.out.println("integersToBytes:");
                Debug.dump(integersToBytes);

                System.out.println("bytesToInts(integersToBytes):");
                Debug.dump(bytesToIntegers);

                throw new RuntimeException("Values are different: originalIntegers[" + i + "]=" + originalIntegers.get(i) +
                        " bytesToDoubles[" + i + "]=" + bytesToIntegers[i]);
            }
        }
    }

    @Test
    public void testIntegersToBytesLoop() throws Exception {
        ArrayList<Integer> originalIntegers = new ArrayList<Integer>(numberOfIntegers);
        for (int i = 0; i < numberOfDoubles; i++)
            originalIntegers.add((int) (Math.random() * magnitudeI));

        byte[] integersToBytes = BitManipulationHelper.integersToBytesloop(originalIntegers);
        int[] bytesToIntegers = BitManipulationHelper.bytesToInts(integersToBytes);

        for (int i = 0; i < originalIntegers.size(); i++) {
            if (originalIntegers.get(i) != bytesToIntegers[i]) {
                DebugGeneric<Integer> d = new DebugGeneric<>();
                System.out.println("Original integers:");
                d.dump(originalIntegers);

                System.out.println("integersToBytes:");
                Debug.dump(integersToBytes);

                System.out.println("bytesToInts(integersToBytes):");
                Debug.dump(bytesToIntegers);

                throw new RuntimeException("Values are different: originalIntegers[" + i + "]=" + originalIntegers.get(i) +
                        " bytesToDoubles[" + i + "]=" + bytesToIntegers[i]);
            }
        }
    }
}
