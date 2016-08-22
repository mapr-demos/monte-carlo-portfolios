package com.mapr.db.data;

public class CompressedFloatArray {

	public enum WIDTH {
	    THIRTY_TWO,
	    SIXTY_FOUR
	}
	
	public final int uncompressedArrayLength;

	public final byte[] compressedSigns;
	public final byte[] compressedExponents;
	public final byte[] compressedSignificands;

	public final CompressionAlgorithms signsAlgorithm;
	public final CompressionAlgorithms exponentsAlgorithm;
	public final CompressionAlgorithms significandsAlgorithm;

	public final WIDTH width;
	
	public CompressedFloatArray(	byte[] compressedSigns,
									byte[] compressedExponents,
									byte[] compressedSignificands,
									CompressionAlgorithms signsAlgorithm,
									CompressionAlgorithms exponentsAlgorithm,
									CompressionAlgorithms significandsAlgorithm,
									int uncompressedArrayLength,
									WIDTH width) {

		this.compressedSigns         = compressedSigns;
		this.compressedExponents     = compressedExponents;
		this.compressedSignificands  = compressedSignificands;

		this.signsAlgorithm          = signsAlgorithm;
		this.exponentsAlgorithm      = exponentsAlgorithm;
		this.significandsAlgorithm   = significandsAlgorithm;

		this.uncompressedArrayLength = uncompressedArrayLength;
		this.width                   = width;
	}
}
