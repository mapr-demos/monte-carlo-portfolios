package com.mapr.db.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.Inflater;
import java.util.Random;

public class Compression {

    private static boolean debug = false;
    private static boolean stats = true;
    private static boolean compare = false;

    /*
	The IEEE 754 standard specifies a float 32 as having:
		Sign:         1 bit
		Exponent:     8 bits
		Significand: 24 bits (23 explicitly stored)
	*/
    private static int signBitWidth32          =  1;
    private static int exponentBitWidth32      =  8;
    private static int significandBitWidth32   = 23;
    private static int floatingPointBitWidth32 = 32;

    private static final int signBitMask32        = 0b10000000000000000000000000000000;
    private static final int exponentBitMask32    = 0b01111111100000000000000000000000;
    private static final int significandBitMask32 = 0b00000000011111111111111111111111;

    /*
    The IEEE 754 standard specifies a float 64 as having:
        Sign:         1 bit
        Exponent:    11 bits
        Significand: 53 bits (52 explicitly stored)
    */
    private static int signBitWidth64          =  1;
    private static int exponentBitWidth64      = 11;
    private static int significandBitWidth64   = 52;
    private static int floatingPointBitWidth64 = 64;

    private static final long signBitMask64        = 0b1000000000000000000000000000000000000000000000000000000000000000L;
    private static final long exponentBitMask64    = 0b0111111111110000000000000000000000000000000000000000000000000000L;
    private static final long significandBitMask64 = 0b0000000000001111111111111111111111111111111111111111111111111111L;

    public static CompressedDoubleArray deltaXorEncode32(float[] uncompressed) {

		/* 
		 * We retrieve the three components of an IEEE 754 float (32 bits)
		 * The signs are retrieved as is and then compressed (ZIP or GZIP)
		 * The exponents and significands are XOR'ed and then compressed (ZIP or GZIP)
		 */

        int[] uncompressedInts = BitManipulationHelper.floatsToInts(uncompressed, 0, uncompressed.length);

        int numberOfSignInts;
        if ((uncompressedInts.length * signBitWidth32) % floatingPointBitWidth32 == 0)
            numberOfSignInts = uncompressedInts.length * signBitWidth32 / floatingPointBitWidth32; // Divide by 32 bits per float
        else
            numberOfSignInts = 1 + uncompressedInts.length * signBitWidth32 / floatingPointBitWidth32; // +1 to make room for the extra bits that won't fit
        int[] uncompressedSigns = new int[numberOfSignInts];

        int numberOfExponentInts;
        if ((uncompressedInts.length * exponentBitWidth32) % floatingPointBitWidth32 == 0)             // x8 because the exponent is stored on 8 bits
            numberOfExponentInts = uncompressedInts.length * exponentBitWidth32 / floatingPointBitWidth32; // Divide by 32 bits per float
        else
            numberOfExponentInts = 1 + uncompressedInts.length * exponentBitWidth32 / floatingPointBitWidth32; // +1 to make room for the extra bits that won't fit
        int[] uncompressedExponents = new int[numberOfExponentInts];

        int numberOfSignificandInts;
        if ((uncompressedInts.length * significandBitWidth32) % floatingPointBitWidth32 == 0)            // x23 because the significand is stored on 23 bits
            numberOfSignificandInts = uncompressedInts.length * significandBitWidth32 / floatingPointBitWidth32; // Divide by 32 bits per float
        else
            numberOfSignificandInts = 1 + uncompressedInts.length * significandBitWidth32 / floatingPointBitWidth32; // +1 to make room for the extra bits that won't fit
        int[] uncompressedSignificands = new int[numberOfSignificandInts];

        int signOffset        = 0;
        int exponentOffset    = 0;
        int significandOffset = 0;

        // XOR the exponents and the significands
        int previousInt = uncompressedInts[0];
        writeBits(uncompressedSigns, uncompressedInts[0] >>> (floatingPointBitWidth32 - signBitWidth32), signOffset, signBitWidth32, false);
        writeBits(uncompressedExponents, (uncompressedInts[0] & exponentBitMask32) >>> significandBitWidth32, exponentOffset, exponentBitWidth32, false); // Mask the bits we want
        writeBits(uncompressedSignificands, uncompressedInts[0] & significandBitMask32, significandOffset, significandBitWidth32, false); // Mask the bits we want

        if (debug) {
            binaryPrint("  compress raw exp: ", uncompressedInts[0] & exponentBitMask32);
            binaryPrint("  compress raw sig: ", uncompressedInts[0] & significandBitMask32);
        }

        signOffset        += signBitWidth32;
        exponentOffset    += exponentBitWidth32;
        significandOffset += significandBitWidth32;

        for (int idx = 1; idx < uncompressedInts.length; idx++) {
            int currentInt = uncompressedInts[idx];
            // Push the bit sign all the way. The triple chevron is so we push 0 from the MSB
            writeBits(uncompressedSigns, currentInt >>> (floatingPointBitWidth32 - signBitWidth32), signOffset, signBitWidth32, false);
            // Mask the bits we want
            writeBits(uncompressedExponents, ((previousInt ^ currentInt) & exponentBitMask32) >>> significandBitWidth32, exponentOffset, exponentBitWidth32, false);
            if (debug) {
                binaryPrint("  compress raw exp: ", currentInt & exponentBitMask32);
                binaryPrint("  compress XOR exp: ", (previousInt ^ currentInt) & exponentBitMask32);
            }
            // XOR with the previous integer
            writeBits(uncompressedSignificands, (previousInt ^ currentInt) & significandBitMask32, significandOffset, significandBitWidth32, false);

            if (debug) {
                binaryPrint("  compress raw sig: ", currentInt & significandBitMask32);
                binaryPrint("  compress XOR sig: ", (previousInt ^ currentInt) & significandBitMask32);
            }

            signOffset        += signBitWidth32;
            exponentOffset    += exponentBitWidth32;
            significandOffset += significandBitWidth32;
            previousInt = currentInt;
        }

        if (debug) {
            uncompressedInts[0] &= exponentBitMask32;
            previousInt = uncompressedInts[0];
            for (int idx = 1; idx < uncompressedInts.length; idx++) {
                int currentInt = uncompressedInts[idx];
                uncompressedInts[idx] = (previousInt ^ currentInt) & exponentBitMask32;
                previousInt = currentInt;
            }
        }

        // Let's deflate those arrays independently

        // -------------------------------------------------------
        // ---------------------- Signs --------------------------
        // -------------------------------------------------------

        byte[] bestCompressedSigns = null, bestCompressedExponents = null, bestCompressedSignificands = null;
        CompressionAlgorithms bestSignsAlgorithm = null, bestExponentsAlgorithm = null, bestSignificandsAlgorithm = null;

        int uncompressedSize = 0;
        int gzipSize = 0;
        int zipSize = 0;

        if (stats) {
            System.out.println("\nuncompressedSigns: size = " + uncompressedSigns.length * TypeSize.FLOAT_BYTESIZE + " bytes"); // x4 because we use an array of ints (32 bits = 4 bytes)
            uncompressedSize += uncompressedSigns.length * TypeSize.FLOAT_BYTESIZE;
        }

        byte[] gzipCompressedSigns;
        try {
            gzipCompressedSigns = compressGzip(BitManipulationHelper.intsToBytes(uncompressedSigns, 0, uncompressedSigns.length));
            bestCompressedSigns = gzipCompressedSigns;
            bestSignsAlgorithm = CompressionAlgorithms.GZIP;
            if (stats) {
                System.out.println("GZIP: size = " + gzipCompressedSigns.length + " bytes");
                gzipSize += gzipCompressedSigns.length;
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        byte[] zipCompressedSigns;
        try {
            zipCompressedSigns = compressZip(BitManipulationHelper.intsToBytes(uncompressedSigns, 0, uncompressedSigns.length));
            if (bestCompressedSigns == null || zipCompressedSigns.length < bestCompressedSigns.length) {
                bestCompressedSigns = zipCompressedSigns;
                bestSignsAlgorithm = CompressionAlgorithms.ZIP;
            }
            if (stats) {
                System.out.println("ZIP:  size = " + zipCompressedSigns.length + " bytes");
                zipSize += zipCompressedSigns.length;
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        // -------------------------------------------------------
        // ---------------------- Exponents ----------------------
        // -------------------------------------------------------
        if (stats) {
            System.out.println("\nuncompressedExponents: size = " + uncompressedExponents.length * TypeSize.FLOAT_BYTESIZE + " bytes"); // x4 because we use an array of ints.
            uncompressedSize += uncompressedExponents.length * TypeSize.FLOAT_BYTESIZE;
        }

        byte[] gzipCompressedExponents;
        try {
            gzipCompressedExponents = compressGzip(BitManipulationHelper.intsToBytes(uncompressedExponents, 0, uncompressedExponents.length));
            bestCompressedExponents = gzipCompressedExponents;
            bestExponentsAlgorithm = CompressionAlgorithms.GZIP;
            if (stats) {
                System.out.println("GZIP: size = " + gzipCompressedExponents.length + " bytes");
                gzipSize += gzipCompressedExponents.length;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] zipCompressedExponents;
        try {
            zipCompressedExponents = compressZip(BitManipulationHelper.intsToBytes(uncompressedExponents, 0, uncompressedExponents.length));
            if (bestCompressedExponents == null || zipCompressedExponents.length < bestCompressedExponents.length) {
                bestCompressedExponents = zipCompressedExponents;
                bestExponentsAlgorithm = CompressionAlgorithms.ZIP;
            }
            if (stats) {
                System.out.println("ZIP:  size = " + zipCompressedExponents.length + " bytes");
                zipSize += zipCompressedExponents.length;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // ----------------------------------------------------------
        // ---------------------- Significands ----------------------
        // ----------------------------------------------------------
        if (stats) {
            System.out.println("\nuncompressedSignificands: size = " + uncompressedSignificands.length * TypeSize.FLOAT_BYTESIZE + " bytes"); // x4 because we use an array of ints.
            uncompressedSize += uncompressedSignificands.length * TypeSize.FLOAT_BYTESIZE;
        }

        byte[] gzipCompressedSignificands;
        try {
            gzipCompressedSignificands = compressGzip(BitManipulationHelper.intsToBytes(uncompressedSignificands, 0, uncompressedSignificands.length));
            bestCompressedSignificands = gzipCompressedSignificands;
            bestSignificandsAlgorithm = CompressionAlgorithms.GZIP;
            if (stats) {
                System.out.println("GZIP: size = " + gzipCompressedSignificands.length + " bytes");
                gzipSize += gzipCompressedSignificands.length;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] zipCompressedSignificands;
        try {
            zipCompressedSignificands = compressZip(BitManipulationHelper.intsToBytes(uncompressedSignificands, 0, uncompressedSignificands.length));
            if (bestCompressedSignificands == null || zipCompressedSignificands.length < bestCompressedSignificands.length) {
                bestCompressedSignificands = zipCompressedSignificands;
                bestSignificandsAlgorithm = CompressionAlgorithms.ZIP;
            }
            if (stats) {
                System.out.println("ZIP:  size = " + zipCompressedSignificands.length + " bytes");
                zipSize += zipCompressedSignificands.length;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (stats) {
            System.out.println("Total uncompressed: " + uncompressedSize);
            System.out.println("Total gzip:         " + gzipSize + " ratio: " + gzipSize * 1.0f / uncompressedSize);
            System.out.println("Total  zip:         " + zipSize + " ratio: " + zipSize * 1.0f / uncompressedSize);
        }

        return new CompressedDoubleArray(bestCompressedSigns,
                bestCompressedExponents,
                bestCompressedSignificands,
                bestSignsAlgorithm,
                bestExponentsAlgorithm,
                bestSignificandsAlgorithm,
                uncompressed.length,
                CompressedDoubleArray.WIDTH.THIRTY_TWO); // We did everything for 32-bit floats

    }

    public static CompressedDoubleArray deltaXorEncode64(double[] uncompressed) {

		/*
		 * We retrieve the three components of an IEEE 754 float (64 bits)
		 * The signs are retrieved as is and then compressed (ZIP or GZIP)
		 * The exponents and significands are XOR'ed and then compressed (ZIP or GZIP)
		 */

        long[] uncompressedLongs = BitManipulationHelper.doublesToLongs(uncompressed, 0, uncompressed.length);

        int numberOfLongs;
        if ((uncompressedLongs.length * signBitWidth64) % floatingPointBitWidth64 == 0)
            numberOfLongs = uncompressedLongs.length * signBitWidth64 / floatingPointBitWidth64;
        else
            numberOfLongs = 1 + uncompressedLongs.length * signBitWidth64 / floatingPointBitWidth64; // +1 to make room for the extra bits that won't fit
        long[] uncompressedSigns = new long[numberOfLongs];

        int numberOfExponentLongs;
        if ((uncompressedLongs.length * exponentBitWidth64) % floatingPointBitWidth64 == 0)
            numberOfExponentLongs = uncompressedLongs.length * exponentBitWidth64 / floatingPointBitWidth64;
        else
            numberOfExponentLongs = 1 + uncompressedLongs.length * exponentBitWidth64 / floatingPointBitWidth64; // +1 to make room for the extra bits that won't fit
        long[] uncompressedExponents = new long[numberOfExponentLongs];

        int numberOfSignificandLongs;
        if ((uncompressedLongs.length * significandBitWidth64) % floatingPointBitWidth64 == 0)
            numberOfSignificandLongs = uncompressedLongs.length * significandBitWidth64 / floatingPointBitWidth64;
        else
            numberOfSignificandLongs = 1 + uncompressedLongs.length * significandBitWidth64 / floatingPointBitWidth64; // +1 to make room for the extra bits that won't fit
        long[] uncompressedSignificands = new long[numberOfSignificandLongs];

        int signOffset        = 0;
        int exponentOffset    = 0;
        int significandOffset = 0;

        // XOR the exponents and the significands
        long previousLong = uncompressedLongs[0];
        writeBits(uncompressedSigns, uncompressedLongs[0] >>> (floatingPointBitWidth64 - signBitWidth64), signOffset, signBitWidth64, false);
        writeBits(uncompressedExponents, (uncompressedLongs[0] & exponentBitMask64) >>> significandBitWidth64, exponentOffset, exponentBitWidth64, false); // Mask the bits we want
        writeBits(uncompressedSignificands, uncompressedLongs[0] & significandBitMask64, significandOffset, significandBitWidth64, false); // Mask the bits we want

        if (debug) {
            binaryPrint("  compress raw exp: ", uncompressedLongs[0] & exponentBitMask64);
            binaryPrint("  compress raw sig: ", uncompressedLongs[0] & significandBitMask64);
        }

        signOffset        += signBitWidth64;
        exponentOffset    += exponentBitWidth64;
        significandOffset += significandBitWidth64;

        for (int idx = 1; idx < uncompressedLongs.length; idx++) {
            long currentLong = uncompressedLongs[idx];
            // Push the bit sign all the way. The triple chevron is so we push 0 from the MSB
            writeBits(uncompressedSigns, currentLong >>> (floatingPointBitWidth64 - signBitWidth64), signOffset, signBitWidth64, false);
            // Mask the bits we want
            writeBits(uncompressedExponents, ((previousLong ^ currentLong) & exponentBitMask64) >>> significandBitWidth64, exponentOffset, exponentBitWidth64, false);
            if (debug) {
                binaryPrint("  compress raw exp: ", currentLong & exponentBitMask64);
                binaryPrint("  compress XOR exp: ", (previousLong ^ currentLong) & exponentBitMask64);
            }
            // XOR with the previous integer
            writeBits(uncompressedSignificands, (previousLong ^ currentLong) & significandBitMask64, significandOffset, significandBitWidth64, false);

            if (debug) {
                binaryPrint("  compress raw sig: ", currentLong & significandBitMask64);
                binaryPrint("  compress XOR sig: ", (previousLong ^ currentLong) & significandBitMask64);
            }

            signOffset        += signBitWidth64;
            exponentOffset    += exponentBitWidth64;
            significandOffset += significandBitWidth64;
            previousLong = currentLong;
        }

        if (debug) {
            uncompressedLongs[0] &= exponentBitMask64;
            previousLong = uncompressedLongs[0];
            for (int idx = 1; idx < uncompressedLongs.length; idx++) {
                long currentLong = uncompressedLongs[idx];
                uncompressedLongs[idx] = (previousLong ^ currentLong) & exponentBitMask64;
                previousLong = currentLong;
            }
        }

        // Let's deflate those arrays independently

        // -------------------------------------------------------
        // ---------------------- Signs --------------------------
        // -------------------------------------------------------

        byte[] bestCompressedSigns = null, bestCompressedExponents = null, bestCompressedSignificands = null;
        CompressionAlgorithms bestSignsAlgorithm = null, bestExponentsAlgorithm = null, bestSignificandsAlgorithm = null;

        int uncompressedSize = 0;
        int gzipSize = 0;
        int zipSize = 0;

        if (stats) {
            System.out.println("\nuncompressedSigns: size = " + uncompressedSigns.length * TypeSize.DOUBLE_BYTESIZE + " bytes"); // x4 because we use an array of ints (32 bits = 4 bytes)
            uncompressedSize += uncompressedSigns.length * TypeSize.DOUBLE_BYTESIZE;
        }

        byte[] gzipCompressedSigns;
        try {
            gzipCompressedSigns = compressGzip(BitManipulationHelper.longsToBytes(uncompressedSigns, 0, uncompressedSigns.length));
            bestCompressedSigns = gzipCompressedSigns;
            bestSignsAlgorithm = CompressionAlgorithms.GZIP;
            if (stats) {
                System.out.println("GZIP: size = " + gzipCompressedSigns.length + " bytes");
                gzipSize += gzipCompressedSigns.length;
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        byte[] zipCompressedSigns;
        try {
            zipCompressedSigns = compressZip(BitManipulationHelper.longsToBytes(uncompressedSigns, 0, uncompressedSigns.length));
            if (bestCompressedSigns == null || zipCompressedSigns.length < bestCompressedSigns.length) {
                bestCompressedSigns = zipCompressedSigns;
                bestSignsAlgorithm = CompressionAlgorithms.ZIP;
            }
            if (stats) {
                System.out.println("ZIP:  size = " + zipCompressedSigns.length + " bytes");
                zipSize += zipCompressedSigns.length;
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }

        // -------------------------------------------------------
        // ---------------------- Exponents ----------------------
        // -------------------------------------------------------
        if (stats) {
            System.out.println("\nuncompressedExponents: size = " + uncompressedExponents.length * TypeSize.DOUBLE_BYTESIZE + " bytes"); // x4 because we use an array of ints.
            uncompressedSize += uncompressedExponents.length * TypeSize.DOUBLE_BYTESIZE;
        }

        byte[] gzipCompressedExponents;
        try {
            gzipCompressedExponents = compressGzip(BitManipulationHelper.longsToBytes(uncompressedExponents, 0, uncompressedExponents.length));
            bestCompressedExponents = gzipCompressedExponents;
            bestExponentsAlgorithm = CompressionAlgorithms.GZIP;
            if (stats) {
                System.out.println("GZIP: size = " + gzipCompressedExponents.length + " bytes");
                gzipSize += gzipCompressedExponents.length;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        byte[] zipCompressedExponents;
        try {
            zipCompressedExponents = compressZip(BitManipulationHelper.longsToBytes(uncompressedExponents, 0, uncompressedExponents.length));
            if (bestCompressedExponents == null || zipCompressedExponents.length < bestCompressedExponents.length) {
                bestCompressedExponents = zipCompressedExponents;
                bestExponentsAlgorithm = CompressionAlgorithms.ZIP;
            }
            if (stats) {
                System.out.println("ZIP:  size = " + zipCompressedExponents.length + " bytes");
                zipSize += zipCompressedExponents.length;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // ----------------------------------------------------------
        // ---------------------- Significands ----------------------
        // ----------------------------------------------------------
        if (stats) {
            System.out.println("\nuncompressedSignificands: size = " + uncompressedSignificands.length * TypeSize.DOUBLE_BYTESIZE + " bytes"); // x4 because we use an array of ints.
            uncompressedSize += uncompressedSignificands.length * TypeSize.DOUBLE_BYTESIZE;
        }

        byte[] gzipCompressedSignificands;
        try {
            gzipCompressedSignificands = compressGzip(BitManipulationHelper.longsToBytes(uncompressedSignificands, 0, uncompressedSignificands.length));
            bestCompressedSignificands = gzipCompressedSignificands;
            bestSignificandsAlgorithm = CompressionAlgorithms.GZIP;
            if (stats) {
                System.out.println("GZIP: size = " + gzipCompressedSignificands.length + " bytes");
                gzipSize += gzipCompressedSignificands.length;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        byte[] zipCompressedSignificands;
        try {
            zipCompressedSignificands = compressZip(BitManipulationHelper.longsToBytes(uncompressedSignificands, 0, uncompressedSignificands.length));
            if (bestCompressedSignificands == null || zipCompressedSignificands.length < bestCompressedSignificands.length) {
                bestCompressedSignificands = zipCompressedSignificands;
                bestSignificandsAlgorithm = CompressionAlgorithms.ZIP;
            }
            if (stats) {
                System.out.println("ZIP:  size = " + zipCompressedSignificands.length + " bytes");
                zipSize += zipCompressedSignificands.length;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (stats) {
            System.out.println("Total uncompressed: " + uncompressedSize);
            System.out.println("Total gzip:         " + gzipSize + " ratio: " + gzipSize * 1.0f / uncompressedSize);
            System.out.println("Total  zip:         " + zipSize + " ratio: " + zipSize * 1.0f / uncompressedSize);
        }

        return new CompressedDoubleArray(bestCompressedSigns,
                bestCompressedExponents,
                bestCompressedSignificands,
                bestSignsAlgorithm,
                bestExponentsAlgorithm,
                bestSignificandsAlgorithm,
                uncompressed.length,
                CompressedDoubleArray.WIDTH.SIXTY_FOUR); // We did everything for 64-bit floats

    }

    public static float[] deltaXorDecode32(CompressedDoubleArray compressed) throws Exception {

        if (compressed == null)
            return null;

        if (compressed.width == null || compressed.width != CompressedDoubleArray.WIDTH.THIRTY_TWO) {
            System.err.println("Wrong format. Should be " + CompressedDoubleArray.WIDTH.THIRTY_TWO + " bits.");
            return null;
        }

        if (compressed.uncompressedArrayLength <= 0) {
            System.err.println("Wrong length. Should be greater than 0. Length = " + compressed.uncompressedArrayLength);
            return null;
        }

        // decompress the 3 components

        int[] decompressedSigns;
        switch (compressed.signsAlgorithm) {
            case NONE:
                decompressedSigns = BitManipulationHelper.bytesToInts(compressed.compressedSigns);
                break;
            case ZIP:
                decompressedSigns = BitManipulationHelper.bytesToInts(uncompressZip(compressed.compressedSigns));
                break;
            case GZIP:
                decompressedSigns = BitManipulationHelper.bytesToInts(uncompressGzip(compressed.compressedSigns));
                break;
            default:
                System.err.println("Unknown compression algorithm for signs.");
                return null;
        }

        int[] decompressedExponents;
        switch (compressed.exponentsAlgorithm) {
            case NONE:
                decompressedExponents = BitManipulationHelper.bytesToInts(compressed.compressedExponents);
                break;
            case ZIP:
                decompressedExponents = BitManipulationHelper.bytesToInts(uncompressZip(compressed.compressedExponents));
                break;
            case GZIP:
                decompressedExponents = BitManipulationHelper.bytesToInts(uncompressGzip(compressed.compressedExponents));
                break;
            default:
                System.err.println("Unknown compression algorithm for exponents.");
                return null;
        }

        int[] decompressedSignificands;
        switch (compressed.significandsAlgorithm) {
            case NONE:
                decompressedSignificands = BitManipulationHelper.bytesToInts(compressed.compressedSignificands);
                break;
            case ZIP:
                decompressedSignificands = BitManipulationHelper.bytesToInts(uncompressZip(compressed.compressedSignificands));
                break;
            case GZIP:
                decompressedSignificands = BitManipulationHelper.bytesToInts(uncompressGzip(compressed.compressedSignificands));
                break;
            default:
                System.err.println("Unknown compression algorithm for significands.");
                return null;
        }

        // Rebuild the array of floats
        int signOffset        = 0;
        int exponentOffset    = 0;
        int significandOffset = 0;

        int[] decompressedAsInts = new int[compressed.uncompressedArrayLength];

        for (int i = 0; i < decompressedAsInts.length; i++) {
            int decompressedSign        = readBits(decompressedSigns,               signOffset, signBitWidth32);
            int decompressedExponent    = readBits(decompressedExponents,       exponentOffset, exponentBitWidth32);
            int decompressedSignificand = readBits(decompressedSignificands, significandOffset, significandBitWidth32);

            decompressedSign     <<= (floatingPointBitWidth32 - signBitWidth32);
            decompressedExponent <<= (floatingPointBitWidth32 - signBitWidth32 - exponentBitWidth32);
            decompressedAsInts[i]  = decompressedSign | decompressedExponent | decompressedSignificand;
            signOffset        += signBitWidth32;
            exponentOffset    += exponentBitWidth32;
            significandOffset += significandBitWidth32;
        }

        int previousInt         = decompressedAsInts[0];
        int previousExponent    = previousInt & exponentBitMask32;
        int previousSignificand = previousInt & significandBitMask32;

        // XOR the exponents and significands
        for (int i = 1; i < decompressedAsInts.length; i++) {
            int currentSign        = decompressedAsInts[i] & signBitMask32;
            int currentExponent    = decompressedAsInts[i] & exponentBitMask32;
            int currentSignificand = decompressedAsInts[i] & significandBitMask32;

            decompressedAsInts[i] = currentSign | (previousExponent ^ currentExponent) | (previousSignificand ^ currentSignificand);
            previousExponent      = previousExponent ^ currentExponent;
            previousSignificand  = previousSignificand ^ currentSignificand;
        }

        return BitManipulationHelper.intsToFloats(decompressedAsInts);
    }

    public static double[] deltaXorDecode64(CompressedDoubleArray compressed) throws Exception {

        if (compressed == null)
            return null;

        if (compressed.width == null || compressed.width != CompressedDoubleArray.WIDTH.SIXTY_FOUR) {
            System.err.println("Wrong format. Should be " + CompressedDoubleArray.WIDTH.SIXTY_FOUR + " bits.");
            return null;
        }

        if (compressed.uncompressedArrayLength <= 0) {
            System.err.println("Wrong length. Should be greater than 0. Length = " + compressed.uncompressedArrayLength);
            return null;
        }

        // decompress the 3 components
        long[] decompressedSigns;
        switch (compressed.signsAlgorithm) {
            case NONE:
                decompressedSigns = BitManipulationHelper.bytesToLongs(compressed.compressedSigns);
                break;
            case ZIP:
                decompressedSigns = BitManipulationHelper.bytesToLongs(uncompressZip(compressed.compressedSigns));
                break;
            case GZIP:
                decompressedSigns = BitManipulationHelper.bytesToLongs(uncompressGzip(compressed.compressedSigns));
                break;
            default:
                System.err.println("Unknown compression algorithm for signs.");
                return null;
        }

        long[] decompressedExponents;
        switch (compressed.exponentsAlgorithm) {
            case NONE:
                decompressedExponents = BitManipulationHelper.bytesToLongs(compressed.compressedExponents);
                break;
            case ZIP:
                decompressedExponents = BitManipulationHelper.bytesToLongs(uncompressZip(compressed.compressedExponents));
                break;
            case GZIP:
                decompressedExponents = BitManipulationHelper.bytesToLongs(uncompressGzip(compressed.compressedExponents));
                break;
            default:
                System.err.println("Unknown compression algorithm for exponents.");
                return null;
        }

        long[] decompressedSignificands;
        switch (compressed.significandsAlgorithm) {
            case NONE:
                decompressedSignificands = BitManipulationHelper.bytesToLongs(compressed.compressedSignificands);
                break;
            case ZIP:
                decompressedSignificands = BitManipulationHelper.bytesToLongs(uncompressZip(compressed.compressedSignificands));
                break;
            case GZIP:
                decompressedSignificands = BitManipulationHelper.bytesToLongs(uncompressGzip(compressed.compressedSignificands));
                break;
            default:
                System.err.println("Unknown compression algorithm for significands.");
                return null;
        }

        // Rebuild the array of doubles
        int signOffset        = 0;
        int exponentOffset    = 0;
        int significandOffset = 0;

        long[] decompressedAsLongs = new long[compressed.uncompressedArrayLength];

        for (int i = 0; i < decompressedAsLongs.length; i++) {
            long decompressedSign        = readBits(decompressedSigns,               signOffset,        signBitWidth64);
            long decompressedExponent    = readBits(decompressedExponents,       exponentOffset,    exponentBitWidth64);
            long decompressedSignificand = readBits(decompressedSignificands, significandOffset, significandBitWidth64);

            decompressedSign     <<= (floatingPointBitWidth64 - signBitWidth64);
            decompressedExponent <<= (floatingPointBitWidth64 - signBitWidth64 - exponentBitWidth64);
            decompressedAsLongs[i] = decompressedSign | decompressedExponent | decompressedSignificand;

            signOffset        += signBitWidth64;
            exponentOffset    += exponentBitWidth64;
            significandOffset += significandBitWidth64;
        }

        long previousLong        = decompressedAsLongs[0];
        long previousExponent    = previousLong & exponentBitMask64;
        long previousSignificand = previousLong & significandBitMask64;

        // XOR the exponents and significands
        for (int i = 1; i < decompressedAsLongs.length; i++) {
            long currentSign        = decompressedAsLongs[i] & signBitMask64;
            long currentExponent    = decompressedAsLongs[i] & exponentBitMask64;
            long currentSignificand = decompressedAsLongs[i] & significandBitMask64;

            decompressedAsLongs[i] = currentSign | (previousExponent ^ currentExponent) | (previousSignificand ^ currentSignificand);
            previousExponent       = previousExponent ^ currentExponent;
            previousSignificand    = previousSignificand ^ currentSignificand;
        }

        return BitManipulationHelper.longsToDoubles(decompressedAsLongs);
    }

    private static byte[] compressGzip(final byte[] input) throws IOException {
        if (input == null || input.length == 0)
            return null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(input, 0, input.length);
        gzipOutputStream.close();
        return outputStream.toByteArray();
    }

    private static byte[] uncompressGzip(final byte[] input) throws IOException {
        if (input == null || input.length == 0)
            return null;
        ByteArrayInputStream inputStream = new ByteArrayInputStream(input);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        GZIPInputStream gzipInputStream = new GZIPInputStream(inputStream);
        byte[] tmp = new byte[32 * 1024]; // Hard coded for now: 32 kiloBytes
        while (true) {
            int read = gzipInputStream.read(tmp);
            if (read < 0)
                break;
            buffer.write(tmp, 0, read);
        }
        gzipInputStream.close();
        return buffer.toByteArray();
    }

    private static byte[] compressZip(final byte[] input) throws Exception {
        if (input == null || input.length == 0)
            return null;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Deflater deflater = new Deflater();
        deflater.setLevel(Deflater.BEST_COMPRESSION);
        deflater.setInput(input);
        deflater.finish();
        byte[] tmp = new byte[32 * 1024]; // 32 kiloBytes
        try {
            while (!deflater.finished()) {
                int size = deflater.deflate(tmp);
                outputStream.write(tmp, 0, size);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                if (outputStream != null)
                    outputStream.close();
            } catch (Exception ex) {
                throw ex;
            }
        }

        return outputStream.toByteArray();
    }

    private static byte[] uncompressZip(final byte[] input) throws Exception {
        if (input == null || input.length == 0)
            return null;
        ByteArrayOutputStream outputStream = null;
        Inflater inflater = new Inflater();
        inflater.setInput(input);
        outputStream = new ByteArrayOutputStream();
        byte[] tmp = new byte[32 * 1024];
        try {
            while (!inflater.finished()) {
                int size = inflater.inflate(tmp);
                outputStream.write(tmp, 0, size);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            try {
                if (outputStream != null)
                    outputStream.close();
            } catch (Exception ex) {
                throw ex;
            }
        }

        return outputStream.toByteArray();
    }

    /**
     * @param uncompressedSignificands
     */
    private static void countOnesAndZerosTransposed32(int[] uncompressedSignificands) {
        int[] countOnes = new int[32];
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
        for (int i = 0; i < countOnes.length; i++) {
            System.out.println("countOnes[" + i + "] = " + countOnes[i] + " countZeros[" + i + "] = " + countZeros[i]
                    + " Ratio = " + ((float) countOnes[i] / ((float) countOnes[i] + (float) countZeros[i])));
        }
    }

    private static void countOnesAndZeros(int[] input) {
        int countOnes = 0;
        int countZeros = 0;
        for (int value : input) {
            int numberOfOnes = Integer.bitCount(value);
            System.out.println("00000000000000000000000000000000".substring(Integer.toBinaryString(value).length())
                    + Integer.toBinaryString(value) + " has " + numberOfOnes + " bits(s) set to 1");
            countOnes += numberOfOnes;
            countZeros += 32 - numberOfOnes;
        }
        System.out.println("countOnes = " + countOnes + " countZeros = " + countZeros + " Ratio = "
                + ((float) countOnes / ((float) countOnes + (float) countZeros)));
    }

    public static long[] deltaValEncode(long[] uncompressed) {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;

        // Find the min and max in the set to figure out the range
        for (long v : uncompressed) {
            max = v > max ? v : max;
            min = v < min ? v : min;
        }

        // Determine the number of bits needed to encode the range
        long range = max - min;
        int numberOfBitsToEncode = TypeSize.INT64_BITSIZE - Long.numberOfLeadingZeros(range);

        if (debug)
            System.out.println("Min: " + min + " Max: " + max + " Range: " + range + " Number of bits to encode: " + numberOfBitsToEncode);

        // Create the resulting array
        // + 1 for the first entry that stores the minimum value
        // + 1 for the number of bits used to encode each delta
        // + 1 for the number of longs when decompressing
        int numberOfCompressedLongs = 1 + (numberOfBitsToEncode * uncompressed.length) / TypeSize.INT64_BITSIZE;
        long[] compressed = new long[1 + 1 + 1 + numberOfCompressedLongs];

        if (debug)
            System.out.println("numberOfCompressedLongs: " + numberOfCompressedLongs + " uncompressed.length: " + uncompressed.length);

        compressed[0] = min;
        // Combine both on 64 bits
        compressed[1] = ((long) numberOfBitsToEncode) << 32;
        compressed[1] |= uncompressed.length;

        int bitOffset = 2 * TypeSize.INT64_BITSIZE; // 2 for the 2 entries above x 64 bits each,
        for (long value : uncompressed) {
            writeBits(compressed, value - min, bitOffset, numberOfBitsToEncode);
            bitOffset += numberOfBitsToEncode;
        }

        return compressed;
    }

    public static long[] deltaValDecode(long[] compressed) {

        long min = compressed[0];
        int numberOfBitsToEncode = (int) (compressed[1] >>> 32);
        int numberOfUncompressedLongs = (int) (compressed[1] & 0xFFFFFFFF); // Keep only the low 32 bits

        if (debug)
            System.out.println("numberOfUncompressedLongs: " + numberOfUncompressedLongs);

        long[] uncompressed = new long[numberOfUncompressedLongs];

        if (debug)
            System.out.println("Min: " + min + " numberOfBitsToEncode: " + numberOfBitsToEncode + " uncompressed.length: " + uncompressed.length);

        int offsetBitsCompressed = 2 * TypeSize.INT64_BITSIZE; // Skip the first 2 entries above x 64 bits each
        for (int i = 0; i < uncompressed.length; i++) {
            uncompressed[i] = min + readBits(compressed, offsetBitsCompressed, numberOfBitsToEncode);
            offsetBitsCompressed += numberOfBitsToEncode;
        }

        return uncompressed;
    }

    public static int[] deltaValEncode(int[] uncompressed) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        // Find the min and max in the set to figure out the range
        for (int v : uncompressed) {
            max = v > max ? v : max;
            min = v < min ? v : min;
        }

        // Determine the number of bits needed to encode the delta from the minimum
        int range = max - min;
        int numberOfBitsToEncode = TypeSize.INT32_BITSIZE - Integer.numberOfLeadingZeros(range);

        if (debug)
            System.out.println("Min: " + min + " Max: " + max + " Range: " + range + " Number of bits to encode: " + numberOfBitsToEncode);

        // Create the resulting array
        // + 1 for the first entry that stores the minimum value
        // + 1 for the number of bits used to encode each delta
        // + 1 for the number of integers when decompressing
        int numberOfCompressedInts = 1 + (numberOfBitsToEncode * uncompressed.length) / TypeSize.INT32_BITSIZE;
        int[] compressed = new int[1 + 1 + 1 + numberOfCompressedInts];

        if (debug)
            System.out.println("numberOfCompressedInts: " + numberOfCompressedInts + " uncompressed.length: " + uncompressed.length);

        compressed[0] = min;
        compressed[1] = numberOfBitsToEncode;
        compressed[2] = uncompressed.length;

        int bitOffset = 3 * TypeSize.INT32_BITSIZE; // 3 for the 3 entries above x 32 bits each,
        for (int value : uncompressed) {
            writeBits(compressed, value - min, bitOffset, numberOfBitsToEncode, false);
            bitOffset += numberOfBitsToEncode;
        }

        return compressed;
    }

    public static int[] deltaValDecode(int[] compressed) {

        int min = compressed[0];
        int numberOfBitsToEncode = compressed[1];
        int[] uncompressed = new int[compressed[2]];

        if (debug)
            System.out.println("Min: " + min + " numberOfBitsToEncode: " + numberOfBitsToEncode + " uncompressed.length: " + uncompressed.length);

        int offsetBitsCompressed = 3 * TypeSize.INT32_BITSIZE; // Skip the first 3 entries above x 32 bits each
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
     * @param out       the output array
     * @param val       the integer to be written
     * @param outOffset the start offset in bits in the output array
     * @param bits      the number of bits to be written (bits greater or equal to 0)
     */
    private static void writeBits(int[] out, int val, int outOffset, int bits, boolean printInfo) {
        if (bits <= 0 || bits > 32) // Working with ints, the number of bits to write must be positive and LTE 32
            return;

        final int index = outOffset >>> 5;     // Divide by 32 (=2^5)
        final int skip  = outOffset & 0b11111; // Modulo 32

        if (printInfo) {
            System.out.println();
            System.out.println("outOffset: " + outOffset + " index: " + index + " skip: " + skip + " bits: " + bits);
            System.out.println("             00000000011111111112222222222333");
            System.out.println("             12345678901234567890123456789012");
            System.out.println("             -------|-------|-------|--------");
        }

        val &= (0xFFFFFFFF >>> (32 - bits));
        if (printInfo) {
            System.out.println("val:         " + "00000000000000000000000000000000".substring(Integer.toBinaryString(val).length()) + Integer.toBinaryString(val));
            System.out.println("out[index]:  " + "00000000000000000000000000000000".substring(Integer.toBinaryString(out[index]).length()) + Integer.toBinaryString(out[index]));
        }
        out[index] |= (val << skip);
        if (printInfo) {
            System.out.println("out[index]:  " + "00000000000000000000000000000000".substring(Integer.toBinaryString(out[index]).length()) + Integer.toBinaryString(out[index]));
        }
        if (32 - skip < bits) {
            out[index + 1] |= (val >>> (32 - skip));
            if (printInfo) {
                System.out.println(
                        "out[index+1]:" + "00000000000000000000000000000000".substring(Integer.toBinaryString(out[index + 1]).length()) + Integer.toBinaryString(out[index + 1]));
            }
        }
    }

    /**
     * Write a certain number of bits of an integer into an integer array
     * starting from the given start offset
     *
     * @param out       the output array
     * @param val       the integer to be written
     * @param outOffset the start offset in bits in the output array
     * @param bits      the number of bits to be written (bits greater or equal to 0)
     */
    private static void writeBits(long[] out, long val, int outOffset, int bits, boolean printInfo) {
        if (bits <= 0 || bits > 64) // Working with ints, the number of bits to write must be positive and LTE 64
            return;

        final int index = outOffset >>> 6;     // Divide by 64 (=2^6)
        final int skip  = outOffset & 0b111111; // Modulo 64

        if (printInfo) {
            System.out.println();
            System.out.println("outOffset: " + outOffset + " index: " + index + " skip: " + skip + " bits: " + bits);
            System.out.println("             0000000001111111111222222222233333333334444444444555555555566666");
            System.out.println("             1234567890123456789012345678901234567890123456789012345678901234");
            System.out.println("             -------|-------|-------|-------|-------|-------|-------|--------");
        }

        val &= (0xFFFFFFFFFFFFFFFFL >>> (64 - bits));
        if (printInfo) {
            System.out.println("val:         " + "0000000000000000000000000000000000000000000000000000000000000000".substring(Long.toBinaryString(val).length()) + Long.toBinaryString(val));
            System.out.println("out[index]:  " + "0000000000000000000000000000000000000000000000000000000000000000".substring(Long.toBinaryString(out[index]).length()) + Long.toBinaryString(out[index]));
        }
        out[index] |= (val << skip);
        if (printInfo) {
            System.out.println("out[index]:  " + "0000000000000000000000000000000000000000000000000000000000000000".substring(Long.toBinaryString(out[index]).length()) + Long.toBinaryString(out[index]));
        }
        if (64 - skip < bits) {
            out[index + 1] |= (val >>> (64 - skip));
            if (printInfo) {
                System.out.println(
                        "out[index+1]:" + "0000000000000000000000000000000000000000000000000000000000000000".substring(Long.toBinaryString(out[index + 1]).length()) + Long.toBinaryString(out[index + 1]));
            }
        }
    }

    public static final void newwriteBits(int[] out, int bits, int outOffset, int bitsToImport) {
        if (bitsToImport == 0)
            return;

		/*
		 * Divide by 32 to get the index of element [i] bit shifting 5 bits to
		 * the right, pumping 0 in. 32 = 2^5
		 */
        final int index = outOffset >>> 5;
        final int spaceLeft = 32 - (outOffset & 0b11111); // Modulo 32 (=2^5)

		/*
		 * The objective is to pack a given number of bits at some position in
		 * an array. Since there may not be enough room in element [i] we have
		 * to store some of the bits into element [i + 1] also. To do this, we
		 * want to break down (if needed) the bits at the input into two sets of
		 * bits that we can then store in element [i] and [i + 1].
		 *
		 * We also want to be somewhat efficient by doing bit manipulations that
		 * don't need to use loops (as in loop over the bits and copy them one by
		 * one) or if/then/else constructs since they would then rely on branch
		 * prediction in the CPU.
		 *
		 * The input is stored in a 32-bit integer and we are asked to copy a
		 * certain number of bits defined by the value in bitsToImport.
		 *
		 * The variable spaceLeft contains the number of bits available to store
		 * into element [i] based on the value passed in offset.
		 *
		 * The approach is to build two variables that, when put together
		 * represent the bits to load, with the number of leading bits set to 0
		 * equal to (32 - spaceLeft) because those bits are already in use in
		 * element [i].
		 *
		 * Once we have built those two variables, we can store the bits into
		 * the elements by using a logical "or" with element [i] and
		 * element [i + 1].
		 */

		/*
		 * maskSign is all 0's or all 1's depending on the sign of spaceLeft -
		 * bitsToImport It is used to mask the operation that is irrelevant when
		 * we do a logical "or"
		 */
        int maskSign = ((spaceLeft - bitsToImport) >> 31);

		/*
		 * Use when spaceLeft >= bitsToImport maskLeft will have (32 -
		 * spaceLeft) bits set to 0 (MSB) and will be used to mask the part we
		 * need to store in element[i] to avoid touching the bits already in use
		 * in element [i].
		 *
		 * Note the triple chevron (>>>) that is used to pump 0 from the bit
		 * sign when shifting to the right.
		 */
        int maskLeft = 0xFFFFFFFF >>> (32 - spaceLeft);

		/*
		 * Compute the number of bits to rotate left or right, depending on the
		 * space left and the number of bits to load. The maskSign variable will
		 * zero out one of them every time.
		 */
        int rotateRightGTE = (bitsToImport - spaceLeft) & maskSign;
        int rotateLeftLTZ = (spaceLeft - bitsToImport) & ~maskSign;

        System.out.println();
        System.out.println("bitsToImport:   " + bitsToImport + " spaceLeft: " + spaceLeft);
        System.out.println("rotateRightGTE: " + rotateRightGTE);
        System.out.println("rotateLeftLTZ:  " + rotateLeftLTZ);

		/*
		 * bitsLeft contains the first part of the bits that will be stored in
		 * element [i] We compute both cases (rotate left and rotate right) and
		 * we use the maskSign value to completely 0 out the one we don't want.
		 * The logical "or" allows us to combine those (with one of them being
		 * 32 bits set to 0) and we finally mask the result with maskLeft to set
		 * to 0 the bits that are not to be touched in element [i].
		 */
        int bitsLeft = (((bits >> rotateRightGTE) & maskSign) | ((bits << rotateLeftLTZ) & ~maskSign)) & maskLeft;

		/*
		 * bitsRight contains the second part (if there is not enough space left
		 * in element [i]) of the bits we need to store in element [i + 1].
		 * Since there is at least one bit available for storage in element [i],
		 * we are guaranteed that there will be at most 31 bits to store in
		 * element [i + 1].
		 *
		 * When there is not enough enough space to store all the bits in
		 * element [i], (spaceLeft - bitsToImport) < 0 and we use a left shift
		 * with a negative value. This has the interesting effect to move the
		 * bits from the left side in (sign side) and set every other bit to 0.
		 *
		 * We also mask with the maskSign variable so that, if there is enough
		 * space in element [i], we want all bits in bitsRight to be set to 0 so
		 * as not to pollute element [i + 1] which would then show up when we do
		 * a logical "or" at the next call.
		 */
        int bitsRight = (bits << (spaceLeft - bitsToImport)) & maskSign;

        out[index] |= bitsLeft;
        out[index + 1] |= bitsRight;
    }

    /**
     * Read a certain number of bits of an integer into an integer array
     * starting from the given start offset
     *
     * @param in       the input array
     * @param inOffset the start offset in bits in the input array
     * @param bits     the number of bits to be read, unlike writeBits(),
     *                 readBits() does not deal with bits == 0 and thus bits
     *                 must be greater than 0. When bits == 0, the calling functions will
     *                 just skip the entire bits-bit slots without decoding them
     * @return the bits bits of the input
     */
    private static int readBits(int[] in, final int inOffset, final int bits) {
        final int index = inOffset >>> 5;     // Divide by 32 (2^5)
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
     * @param out          the output array
     * @param val          the integer to be written
     * @param outOffset    the start offset in bits in the output array
     * @param numberOfBits the number of bits to be written (bits greater or equal to 0)
     */
    private static void writeBits(long[] out, long val, int outOffset, int numberOfBits) {
        if (numberOfBits <= 0 || numberOfBits > 64)
            return;

        final int index = outOffset >>> 6;      // divide by 64 (=2^6)
        final int skip  = outOffset & 0b111111; // Modulo 64
        val &= (0xFFFFFFFFFFFFFFFFL >>> (64 - numberOfBits));
        out[index] |= (val << skip);
        if (64 - skip < numberOfBits)
            out[index + 1] |= (val >>> (64 - skip));
    }

    /**
     * Read a certain number of bits of a long into a long array
     * starting from the given start offset
     *
     * @param in           the input array
     * @param inOffset     the start offset in bits in the input array
     * @param numberOfBits the number of bits to be read, unlike writeBits(),
     *                     readBits() does not deal with bits == 0 and thus bits
     *                     must be greater than 0. When bits == 0, the calling functions will
     *                     just skip the entire bits-bit slots without decoding them
     * @return the bits bits of the input
     */
    private static long readBits(long[] in, final int inOffset, final int numberOfBits) {
        final int index = inOffset >>> 6;      // Divide by 64 (=2^6)
        final int skip  = inOffset & 0b111111; // Modulo 64
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
                input[k] = input[k] ^ t;
                input[k + j] = input[k + j] ^ (t << j);
            }
        }

        return input;
    }

    /**
     * @param information String to print out at the beginning
     * @param integer     Integer whose binary representation is to be added
     */
    private static void binaryPrint(String information, int integer) {
        System.out.println(information + "00000000000000000000000000000000".substring(Integer.toBinaryString(integer).length()) + Integer.toBinaryString(integer));
    }

    /**
     * @param information String to print out at the beginning
     * @param integer     Integer whose binary representation is to be added
     */
    private static void binaryPrint(String information, long integer) {
        System.out.println(information + "0000000000000000000000000000000000000000000000000000000000000000".substring(Long.toBinaryString(integer).length()) + Long.toBinaryString(integer));
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

    public static void testBitStorage32() {
        Random rand = new Random();
        int  bits = rand.nextInt(Integer.MAX_VALUE);
        int spaceLeft = rand.nextInt(32);

        for (int bitsToImport = 7; bitsToImport <= 32; bitsToImport += 5) {
			/*
			 * The objective is to pack a given number of bits at some position in an array.
			 * Since there may not be enough room in element [i] we have to store some of the bits
			 * into element [i + 1] also.
			 * To do this, we want to break down (if needed) the bits at the input into two
			 * sets of bits that we can then store in element [i] and [i + 1].
			 * 
			 * We also want to be somewhat efficient by doing bit manipulations that dont need
			 * to use loops (as in loop over the bits and copy them one by one) or if/then/else
			 * constructs since they would then rely on branch prediction in the CPU.
			 * 
			 * The input is stored in a 32-bit integer and we are asked to copy a certain
			 * number of bits defined by the value in bitsToImport.
			 * 
			 * The variable spaceLeft contains the number of bits available to store into element [i]
			 * based on the value passed in offset.
			 *
			 * The approach is to build two variables that, when put together represent the bits 
			 * to load, with the number of leading bits set to 0 equal to (32 - spaceLeft) because
			 * those bits are already in use in element [i].
			 * 
			 * Once we have built those two variables, we can store the bits into the elements by
			 * using a logical "or" with element [i] and element [i + 1].
			 */
			
			/* 
			 * maskSign is all 0's or all 1's depending on the sign of spaceLeft - bitsToImport
			 * It is used to mask the operation that is irrelevant when we do a logical "or"
			 */
            int maskSign = ((spaceLeft - bitsToImport) >> 31);

			/*
			 *  Use when spaceLeft >= bitsToImport
			 *  maskLeft will have (32 - spaceLeft) bits set to 0 (MSB) and will be used to mask
			 *  the part we need to store in element[i] to avoid touching the bits already in use
			 *  in element [i].
			 *  
			 *  Note the triple chevron (>>>) that is used to pump 0 from the bit sign when shifting
			 *  to the right.
			 */
            int maskLeft = 0xFFFFFFFF >>> (32 - spaceLeft);
			
			/*
			 * Compute the number of bits to rotate left or right, depending on the space left
			 * and the number of bits to load.
			 * The maskSign variable will zero out one of them every time.
			 */
            int rotateRightGTE = (bitsToImport - spaceLeft) & maskSign;
            int rotateLeftLTZ = (spaceLeft - bitsToImport) & ~maskSign;

            System.out.println();
            System.out.println("bitsToImport:   " + bitsToImport + " spaceLeft: " + spaceLeft);
            System.out.println("rotateRightGTE: " + rotateRightGTE);
            System.out.println("rotateLeftLTZ:  " + rotateLeftLTZ);

			/*
			 * bitsLeft contains the first part of the bits that will be stored in element [i]
			 * We compute both cases (rotate left and rotate right) and we use the maskSign value
			 * to completely 0 out the one we don't want.
			 * The logical "or" allows us to combine those (with one of them being 32 bits set to 0)
			 * and we finally mask the result with maskLeft to set to 0 the bits that are not to be 
			 * touched in element [i].
			 */
            int bitsLeft = (((bits >> rotateRightGTE) & maskSign) | ((bits << rotateLeftLTZ) & ~maskSign)) & maskLeft;

			/*
			 * bitsRight contains the second part (if there is not enough space left in element [i]) of the bits we
			 * need to store in element [i + 1].
			 * Since there is at least one bit available for storage in element [i], we are guaranteed that there will
			 * be at most 31 bits to store in element [i + 1].
			 * 
			 * When there is not enough enough space to store all the bits in element [i], (spaceLeft - bitsToImport) < 0
			 * and we use a left shift with a negative value. This has the interesting effect to move the bits from the
			 * left side in (sign side) and set every other bit to 0.
			 * 
			 * We also mask with the maskSign variable so that, if there is enough space in element [i], we want all bits
			 * in bitsRight to be set to 0 so as not to pollute element [i + 1] which would then show up when we do a logical
			 * "or" at the next call.
			 */
            int bitsRight = (bits << (spaceLeft - bitsToImport)) & maskSign;

            System.out.println("             00000000011111111112222222222333");
            System.out.println("             12345678901234567890123456789012");
            System.out.println("             -------|-------|-------|--------");
            System.out.println("bits:        " + "00000000000000000000000000000000".substring(Integer.toBinaryString(bits).length()) + Integer.toBinaryString(bits));
            System.out.println("bitsLeft:    " + "00000000000000000000000000000000".substring(Integer.toBinaryString(bitsLeft).length()) + Integer.toBinaryString(bitsLeft));
            System.out.println("bitsRight:   " + "00000000000000000000000000000000".substring(Integer.toBinaryString(bitsRight).length()) + Integer.toBinaryString(bitsRight));
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println("\ntestTranspose32b()");
        testTranspose32b();

        testBitStorage32();
    }
}
