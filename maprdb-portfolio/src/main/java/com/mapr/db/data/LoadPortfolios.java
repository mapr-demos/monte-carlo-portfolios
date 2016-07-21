package com.mapr.db.data;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.kohsuke.args4j.*;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import org.xerial.snappy.Snappy;

import com.mapr.db.Admin;
import com.mapr.db.FamilyDescriptor;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;
import com.mapr.db.exceptions.DBException;

import org.ojai.Document;
import org.ojai.store.exceptions.*;
import com.mapr.db.impl.FamilyDescriptorImpl;

import me.lemire.integercompression.*;

//import net.jpountz.lz4.LZ4Compressor;
//import net.jpountz.lz4.LZ4Factory;

public class LoadPortfolios {

	private static final int COLUMN_BLOCK_SIZE = 8192;
	private static final String HELP = "Usage: -filepath <path to input file> -format JSON|TSV|CSV [-jsonKey <field name>] -table <Mapr-DB table path> [-verbose] [-debug] [-check] [-help]";
	private String filePath;
	private Format fileFormat;
	private String tablePath;
	private String jsonKey;
	private boolean verbose;
	private boolean check;
	private boolean checkSnappy;
	private boolean debug;

	private Table table;
	private String start_date;
	private String end_date;

	// Compressor used for the weights
	private String floatCompressorName = "snappy";

	// This codec seems to be usually giving the best compression ratio for instruments
	private IntegerCODEC integerCodec = new Composition(new BinaryPacking(), new VariableByte());
	private String integerCodecName = "bpacking+variablebyte";

	public static void main(String[] args) throws IOException {

		Options opts = new Options();
		CmdLineParser parser = new CmdLineParser(opts);
		try {
			parser.parseArgument(args);
			if (opts.help) {
				System.err.println(HELP);
				System.exit(1);
			}
		} catch (CmdLineException e) {
			System.err.println(HELP);
			System.exit(1);
		}

		new LoadPortfolios().load(opts);
	}

	public void load(Options options) throws IOException {
		if (options == null) {
			System.err.println("No command line options received. Exiting.");
			System.exit(1);
		}

		if (options.filepath == null || options.filepath.equals("")) {
			System.err.println("The file path is empty. Exiting.");
			System.exit(1);
		} else {
			filePath = options.filepath;
		}

		if (options.format == null) {
			System.err.println("No file format specified, using JSON.");
			fileFormat = Format.JSON;
		} else if (!options.format.equalsIgnoreCase("JSON")) {
			System.err.println("Only JSON file format is supported. Exiting");
			System.exit(1);
		} else {
			if (options.format.equalsIgnoreCase("JSON")) {
				fileFormat = Format.JSON;

				if (options.jsonKey == null || options.jsonKey.equals("")) {
					System.err.println("The key is empty. Exiting.");
					System.exit(1);
				} else {
					jsonKey = options.jsonKey;
				}
			}

			if (options.format.equalsIgnoreCase("CSV"))
				fileFormat = Format.CSV;
			if (options.format.equalsIgnoreCase("TSV"))
				fileFormat = Format.TSV;
		}

		if (options.table == null || options.table.equals("")) {
			System.err.println("The table name is empty. Exiting.");
			System.exit(1);
		} else {
			tablePath = options.table;
		}

		if (options.verbose)
			verbose = options.verbose;

		if (options.check)
			check = options.check;

		if (options.checkSnappy)
			checkSnappy = options.checkSnappy;

		if (options.debug)
			debug = options.debug;

		if (verbose) {
			System.err.println("--- Parsing Results ---");
			System.err.println("File path:   " + filePath);
			System.err.println("File format: " + fileFormat);
			System.err.println("Key name:    " + jsonKey);
			System.err.println("Table path:  " + tablePath);
			System.err.println("Verbose:     " + verbose);
			System.err.println("Check:       " + check);
			System.err.println("Debug:       " + debug);
		}

		 table = getTable(tablePath);

		if (fileFormat == Format.JSON)
			loadFromJSON(options);
	}

	private Table getTable(String tableName) {
		if (verbose)
			System.err.println("\n========== getTable " + tableName + " ==========");

		try {
			if (!MapRDB.tableExists(tableName)) {
				if (verbose)
					System.err.println("\n========== getTable " + tableName + " doesn't exist. ==========");

				TableDescriptor tableDescriptor = MapRDB.newTableDescriptor(tableName);
				FamilyDescriptor fd = new FamilyDescriptorImpl();
				if (fd != null) {
					fd.setCompression(FamilyDescriptor.Compression.None);
					fd.setInMemory(false);
					fd.setName("test");
					fd.setTTL(0);
				}
				
				try {
					Admin admin = MapRDB.newAdmin();
					admin.createTable(tableDescriptor);
					admin.close();
				} catch (Exception ex) {
					System.out.println("Exception: " + ex.getMessage());
				}

				// Create the table if not already present
				Table retTable = MapRDB.getTable(tableName);
				retTable.getTableDescriptor().addFamily(fd);

				for (FamilyDescriptor family : retTable.getTableDescriptor().getFamilies() )
					System.out.println("family = " + family);

				return retTable;
			} else {
				if (verbose)
					System.out.println("\n\n========== getTable " + tableName + " exists. Getting it. ==========");
				return MapRDB.getTable(tableName); // get the table
			}
		} catch (DBException e) {
			System.err.println("Exception: " + e.getMessage());
		}
		return null;
	}

	public void loadFromJSON(Options options) {
		int depth = 0;
		try {
			JsonFactory jFactory = new JsonFactory();
			JsonParser jParser = jFactory.createParser(new File(filePath));

			String idElement = null;
			String idValue = null;

			if (options.jsonKey != null && !options.jsonKey.equals("")) {
				idElement = options.jsonKey;
				if (verbose)
					System.err.println("Loading from JSON. Key name: " + idElement);
			} else {
				System.err.println("Don't know the name of the field to use as the key.");
				return;
			}

			ArrayList<Integer> instruments = null;
			Integer min_instrument = Integer.MAX_VALUE;
			Integer max_instrument = Integer.MIN_VALUE;

			ArrayList<Double> weights = null;
			Double min_weight = Double.MAX_VALUE;
			Double max_weight = Double.MIN_VALUE;

			Document document = null;
						
			while (jParser.nextToken() != null) {

				String fieldName = jParser.getCurrentName();

				switch (jParser.getCurrentToken()) {

				case START_OBJECT:
					if (depth == 0) {
						System.out.println();
						if (verbose)
							System.err.println("New MapRDB Document created");
						// Reset everything
						document = MapRDB.newDocument();
						min_instrument = Integer.MAX_VALUE;
						max_instrument = Integer.MIN_VALUE;
						min_weight = Double.MAX_VALUE;
						max_weight = Double.MIN_VALUE;
						start_date = null;
						end_date = null;
					}
					depth++;
					break;

				case END_OBJECT:
					depth--;
					// When the depth reaches zero on an end of object, we are done.
					// At this point, we can call insert() or insertandreplace()
					if (depth == 0) {
						if (idValue != null) {
							if (verbose)
								System.err.println("Setting ID for document: id=" + idValue);

							// Set the metadata + document ID
							document.set("_id", idValue);
							Document metadata = MapRDB.newDocument()
									.set("min_instrument", min_instrument)
									.set("max_instrument", max_instrument)
									.set("min_weight", min_weight)
									.set("max_weight", max_weight);
							document.set("metadata", metadata);
							
							if (verbose) {
								System.err.println("Compressing Instruments");
								System.err.println("Size instruments.size():             " + instruments.size());
							}

							byte[] compressedInstruments = getCompressedInstruments(instruments);
							String instrumentCompressorName;
							
							if (compressedInstruments.length < instruments.size() * TypeSize.INT32_BYTESIZE) {
								document.set("instruments", getInstrumentDocument(instruments.size(), compressedInstruments, integerCodecName));
								instrumentCompressorName = integerCodecName;
							}
							else {
								document.set("instruments", getInstrumentDocument(instruments.size(), CompressionHelper.integersToBytes(instruments), "none"));
								instrumentCompressorName = "none";
							}

							if (debug) {
								System.out.println("loadFromJson: Content of compressedInstruments instrumentCompressorName: " + instrumentCompressorName);
								dumpBuffer(compressedInstruments);
							}

							// Compress the weights
							if (verbose)
								System.err.println("Compressing weights");
							
							byte[] compressedWeights = getCompressedWeights(weights);
//							if (debug) {
//								System.out.println("loadFromJson: Content of compressedWeights");
//								dumpBuffer(compressedWeights);
//							}
							String weightCompressorName;
							if (compressedWeights.length < weights.size() * TypeSize.DOUBLE_BYTESIZE) {
								document.set("weights", getWeightDocument(weights.size(), compressedWeights, floatCompressorName));
								weightCompressorName = floatCompressorName;
							}
							else {
								document.set("weights", getWeightDocument(weights.size(), CompressionHelper.doublesToBytes(weights), "none"));
								weightCompressorName = "none";
							}
								// Insert document in MapRDB
							if (verbose)
								System.err.println("Inserting document into MapRDB");
							
							if (table == null)
								System.err.println("The table object is null! Cannot insert");
							else
								try {
									if (check)
										System.out.println(document);

									table.insertOrReplace(document);

									if (check) {
										System.err.println("Checking compressed instruments");
										if (instrumentCompressorName.equals(integerCodecName))
											checkInstrumentsInMapRDB(idValue, instruments, compressedInstruments);
										else
											checkInstrumentsInMapRDB(idValue, instruments, CompressionHelper.integersToBytes(instruments));

										System.err.println("Checking compressed weights");
										if (weightCompressorName.equals(floatCompressorName))
											checkWeightsInMapRDB(idValue, weights, compressedWeights);
										else
											checkWeightsInMapRDB(idValue, weights, CompressionHelper.doublesToBytes(weights));
									}
								} catch (DocumentExistsException dee) {
									System.err.println("Exception during insert : " + dee.getMessage());
								}

							if (verbose)
								System.err.println("Clearing instruments");
							instruments.clear();

							if (verbose)
								System.err.println("Clearing weights");
							weights.clear();

						} else {
							System.err.println("Couldn't determine the ID for the document. NOT inserting.");
						}
					}
					break;

				case START_ARRAY:
					if (fieldName.equals("instruments") && instruments == null && weights == null) {
						// We create new arrays to store the columnar data (uncompressed)
						instruments = new ArrayList<Integer>(COLUMN_BLOCK_SIZE);
						weights     = new ArrayList<Double>(COLUMN_BLOCK_SIZE);
						if (verbose)
							System.err.println("New ArrayList<Integer> & ArrayList<Double> created");
					}
					depth++;
					break;

				case END_ARRAY:
					depth--;
					break;
				
				// Not used
				case FIELD_NAME:
				case NOT_AVAILABLE:
				case VALUE_EMBEDDED_OBJECT:
					break;

				// These actually add things to the array or object
				case VALUE_NULL:
					if (fieldName != null) {
						document.setNull(fieldName);
					}
					break;

				case VALUE_NUMBER_FLOAT:
					if (fieldName != null) {
						if (fieldName.equals("weight")) {
							double weight = jParser.getDoubleValue();
							weights.add(weight);
							if (weight > max_weight)
								max_weight = weight;
							if (weight < min_weight)
								min_weight = weight;
						}
					}
					break;

				case VALUE_NUMBER_INT:
					if (fieldName != null) {
						if (fieldName.equals(idElement)) {
							idValue = jParser.getValueAsString();
						} else if (fieldName.equals("instrument")) {
							Integer instrument = jParser.getIntValue();
							instruments.add(instrument);
							if (instrument > max_instrument)
								max_instrument = instrument;
							if (instrument < min_instrument)
								min_instrument = instrument;
						} else
							document.set(fieldName, jParser.getLongValue());
					}
					break;

				case VALUE_STRING:
					if (fieldName != null) {
						if (fieldName.equals(idElement))
							idValue = jParser.getText();
						if (fieldName.equals("start"))
							start_date = jParser.getText();
						if (fieldName.equals("end"))
							end_date = jParser.getText();
					} else {
						document.set(fieldName, jParser.getText());
					}
					break;

				case VALUE_FALSE:
				case VALUE_TRUE:
					if (fieldName != null) {
						document.set(fieldName, jParser.getBooleanValue());
					}
					break;
				}

//				if (debug) {
//					for (int i = 0; i < depth; i++)
//						System.out.print("-");
//					System.out.println("> " + jParser.getCurrentToken().toString() + ": " + fieldName);
//				}
			}
			jParser.close();
			if (table != null) {
				if (verbose)
					System.err.println("Flushing");
				table.flush();
			}
			else
				System.err.println("table is null. Cannot flush.");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void checkWeightsInMapRDB(String idValue, ArrayList<Double> weights, byte[] referenceWeights) {

		Document   record                 = table.findById(idValue);
		String     algo                   = record.getString("weights.algo");
		ByteBuffer retrievedWeightsBuffer = record.getBinary("weights.compressed");
		int numberOfDoubles               = record.getInt("weights.uncompressedDoubleSize");
		int compressedByteSize            = record.getInt("weights.compressedByteSize");

		retrievedWeightsBuffer.rewind();
		byte[] retrievedWeights = Arrays.copyOf(retrievedWeightsBuffer.array(), compressedByteSize);

		compareByteBuffers(referenceWeights, retrievedWeights, "Weight buffers: ");
		checkWeights(algo, numberOfDoubles, weights, retrievedWeights);
	}

	private void checkInstrumentsInMapRDB(String idValue, ArrayList<Integer> instruments, byte[] referenceInstruments) {

		Document record = table.findById(idValue);

		String     algo               = record.getString("instruments.algo");
		ByteBuffer retrievedBuffer    = record.getBinary("instruments.compressed");
		int        numberOfIntegers   = record.getInt("instruments.uncompressedIntegerSize");
		int        compressedByteSize = record.getInt("instruments.compressedByteSize");

		retrievedBuffer.rewind();
		byte[] retrievedInstruments = Arrays.copyOf(retrievedBuffer.array(), compressedByteSize);

		compareByteBuffers(referenceInstruments, retrievedInstruments, "Instrument buffers: ");
		checkInstruments(algo, numberOfIntegers, instruments, retrievedInstruments);
	}
	
	private void compareByteBuffers(byte[] reference, byte[] other, String beginning) {		
        if (reference == null || other == null || reference.length != other.length ) {
        	System.err.println(beginning + "Buffers are different or one of them is empty.");
        	return;
        }

		for (int i = 0; i < reference.length; i++)
			if (reference[i] != other[i]) {
				System.err.println(beginning + "Difference detected between buffers.");
				dumpBuffer(reference);
				dumpBuffer(other);
				return;
			}
		
		System.err.println(beginning + "Buffers are identical.");
	}
	
	private void checkWeights(String algo, int numberOfDoubles, ArrayList<Double> weights, byte[] retrievedWeights) {

		String beginning = "Check weights: ";
		
        double[] referenceDoubles = CompressionHelper.doublesToDoubles(weights);
		if (algo == null || algo.equals("")) {
			System.err.println(beginning + "Cannot retrieve the algorithm used to compress the weights!");
			return;
		}
		if (!algo.equals("snappy") && !algo.equals("none")) {
			System.err.println(beginning + "Unknown algorithm used for weights: " + algo);
			return;
		}
		
		if (retrievedWeights == null || retrievedWeights.length == 0) {
			System.err.println(beginning + "The buffer for the weights is either null or empty!");
			return;
		}
		
		if (numberOfDoubles <= 0 || numberOfDoubles != referenceDoubles.length) {
			System.err.println(beginning + "numberOfDoubles is incorrect: " + numberOfDoubles + " referenceWeights.length: " + referenceDoubles.length);
			return;
		}

		double[] decompressedWeights = null;
		
		if (algo.equals("snappy")) {
			try {
				decompressedWeights = Snappy.uncompressDoubleArray(retrievedWeights);
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
		}

		if (algo.equals("none")) {
			decompressedWeights = CompressionHelper.bytesToDoubles(retrievedWeights);
		}
//		if (debug) {
//			System.out.println("checkWeights: " + "decompressedWeights.length: " + decompressedWeights.length + " referenceDoubles.length: " + referenceDoubles.length);
//		}

		for (int index = 0; index < referenceDoubles.length; index++) {
			if (referenceDoubles[index] != decompressedWeights[index]) {
				System.err.println(beginning + "Weights are different! index: " + index + " referenceWeights: " + referenceDoubles[index] + " decompressedWeights: " + decompressedWeights[index]);
				return;
			}
		}
		
		System.err.println(beginning + "Weights are the same!");
		
	}

	private void checkInstruments(String algo, int numberOfIntegers, ArrayList<Integer> instruments, byte[] retrievedInstruments) {

		String beginning = "Check instruments: ";
		
        int[] referenceIntegers = CompressionHelper.integersToInts(instruments);
		if (algo == null || algo.equals("")) {
			System.err.println(beginning + "Cannot retrieve the algorithm used to compress the instruments!");
			return;
		}
		if (!algo.equals("bpacking+variablebyte") && !algo.equals("none")) {
			System.err.println(beginning + "Unknown algorithm used for instruments: " + algo);
			return;
		}
		
		if (retrievedInstruments == null || retrievedInstruments.length == 0) {
			System.err.println(beginning + "The buffer for the instruments is either null or empty!");
			return;
		}
		
		if (numberOfIntegers <= 0 || numberOfIntegers != referenceIntegers.length) {
			System.err.println(beginning + "numberOfIntegers is incorrect: " + numberOfIntegers + " referenceIntegers.length: " + referenceIntegers.length);
			return;
		}

		int[] decompressedInstruments = null;
		
		if (algo.equals("bpacking+variablebyte")) {
			decompressedInstruments = getDecompressedInstruments(retrievedInstruments, numberOfIntegers);
		}

		if (algo.equals("none")) {
			decompressedInstruments = CompressionHelper.bytesToIntegers(retrievedInstruments);
		}
//		if (debug) {
//			System.out.println("checkWeights: " + "decompressedWeights.length: " + decompressedWeights.length + " referenceDoubles.length: " + referenceDoubles.length);
//		}

		for (int index = 0; index < referenceIntegers.length; index++) {
			if (referenceIntegers[index] != decompressedInstruments[index]) {
				System.err.println(beginning + "Instruments are different! index: " + index + " referenceIntegers: " + referenceIntegers[index] + " decompressedWeights: " + decompressedInstruments[index]);
				return;
			}
		}
		
		System.err.println(beginning + "Instruments are the same!");
		
	}

	private Document getWeightDocument(int numberOfWeights, byte[] compressedWeights, String algo) {
		Document document = MapRDB.newDocument();
		document.set("uncompressedDoubleSize", numberOfWeights)
				.set("compressed", compressedWeights)
				.set("compressedByteSize", compressedWeights.length)
				.set("algo", algo);

		return document;
	}

	private byte[] getCompressedWeights(ArrayList<Double> weights) {

		double[] uncompressedWeights = CompressionHelper.doublesToDoubles(weights);
		byte[]   compressedWeights;
		try {
			compressedWeights = Snappy.compress(uncompressedWeights);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		if (checkSnappy) {
			System.out.println("getCompressedWeights: Content of compressedWeights");
			dumpBuffer(compressedWeights);

			double[] decompressedWeights;
			try {
				decompressedWeights = Snappy.uncompressDoubleArray(compressedWeights);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}

			for (int i = 0; i < uncompressedWeights.length; i++) {
				if (decompressedWeights[i] != uncompressedWeights[i]) {
					System.out.println("getCompressedWeights: difference after decompression index: " + i + " uncompressedWeights[i]: " + uncompressedWeights[i] + " decompressedWeights[i]: " +decompressedWeights[i]);
					break;
				}
			}
		}

        return compressedWeights;
	}

	private void dumpBuffer(byte[] buffer) {
		for (int i = 0; i < buffer.length; i++)
			System.out.print(buffer[i] + " ");
		System.out.println();
	}
	
	private Document getInstrumentDocument(int numberOfInstruments, byte[] instruments, String algo) {

		Document document = MapRDB.newDocument();
		document.set("uncompressedIntegerSize", numberOfInstruments)
				.set("compressed", instruments)
				.set("compressedByteSize", instruments.length)
				.set("algo", algo);
		
		return document;
	}

	private byte[] getCompressedInstruments(ArrayList<Integer> instruments) {

		int[] uncompressedInstruments = CompressionHelper.integersToInts(instruments);
		int[]   compressedInstruments = new int[4 * uncompressedInstruments.length + 1024];
		
		IntWrapper inpos  = new IntWrapper(0);
		IntWrapper outpos = new IntWrapper(0);
		        
		integerCodec.compress(uncompressedInstruments,
							  inpos,
							  uncompressedInstruments.length - inpos.get(), 
							  compressedInstruments,
							  outpos);
        
		return CompressionHelper.intsToBytes(compressedInstruments, 0, outpos.get() + 1);
	}

	private int[] getDecompressedInstruments(byte[] compressedInstruments, int numberOfIntegers) {

    	
        /**
         * Uncompress data from an array to another array.
         * 
         * Both inpos and outpos parameters are modified to indicate new
         * positions after read/write.
         * 
         * @param in
         *                array containing data in compressed form
         * @param inpos
         *                where to start reading in the array
         * @param inlength
         *                length of the compressed data (ignored by some
         *                schemes)
         * @param out
         *                array where to write the compressed output
         * @param outpos
         *                where to write the compressed output in out

           public void uncompress(int[] in, IntWrapper inpos, int inlength, int[] out, IntWrapper outpos);
         */
        
		int[]      decompressedInstruments = new int[numberOfIntegers];
    	IntWrapper inpos  = new IntWrapper(0);
    	IntWrapper outpos = new IntWrapper(0);
        
    	int[] compressedInstrumentsAsInts = CompressionHelper.bytesToIntegers(compressedInstruments);
    	
		integerCodec.uncompress(compressedInstrumentsAsInts,
								inpos,
								compressedInstrumentsAsInts.length, 
								decompressedInstruments,
								outpos);
		
		return decompressedInstruments;
	}

	public static enum Format {
		JSON, TSV, CSV
	}

	private static class Options {

		@Option(name = "-help")
		private boolean help = false;

		@Option(name = "-verbose")
		private boolean verbose = false;

		// Check the data
		@Option(name = "-check")
		private boolean check = false;

		@Option(name = "-checkSnappy")
		private boolean checkSnappy = false;

		@Option(name = "-debug")
		private boolean debug = false;

		@Option(name = "-filepath")
		private String filepath = null;

		@Option(name = "-format")
		private String format = null;

		@Option(name = "-table")
		private String table = null;

		@Option(name = "-jsonKey")
		private String jsonKey = null;

		@Argument()
		List<String> files;
	}
}
