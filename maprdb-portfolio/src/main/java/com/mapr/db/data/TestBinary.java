package com.mapr.db.data;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.ojai.Document;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;

public class TestBinary {

	public static void main(String[] args) {

		double[] doubleArray = { 1234.1, 2345.1, 3456.1 };
		System.out.println("Content of doubleArray:");
		System.out.println(Arrays.toString(doubleArray));
		System.out.println();

		
		
		
		
		System.out.println("Converting to a ByteBuffer");
		// Doubles to bytes
		byte[] bytes = new byte[doubleArray.length * 8];
		ByteBuffer buf = ByteBuffer.wrap(bytes);
		for (double d : doubleArray)
			buf.putDouble(d);

		System.out.println("Content of ByteBuffer:");
		for (int i = 0; i < buf.capacity(); i++)
			System.out.print(buf.get(i) + " ");
		System.out.println();
		System.out.println();
		
		
        buf.flip();

		
//		System.out.println("Converting back to doubles:");
//		// Bytes to doubles
//		ByteBuffer buf2 = ByteBuffer.wrap(bytes);
//		double[] doubles = new double[bytes.length / 8];
//		for (int i = 0; i < doubles.length; i++)
//			doubles[i] = buf2.getDouble(i * 8);
//		System.out.println(Arrays.toString(doubles));

		
		// Create the table with:
		// maprcli table create -path /mapr/demo.mapr.com/user/pborne/testBinary
		// -tabletype json -insertionorder false

		Table table = MapRDB.getTable("/mapr/demo.mapr.com/user/pborne/testBinary");

		// Insert into MapR-DB
		System.out.println("Inserting into MapR-DB");
		Document document = MapRDB.newDocument();
		document.setId("1");
		document.set("buffer", buf);
	
		Document otherDocument = MapRDB.newDocument();
		otherDocument.set("otherbuffer", buf);
		document.set("other", otherDocument);
		System.out.println("Inserted document: " + document);
		
		table.insertOrReplace(document);
		
		Document documentRetrieved = table.findById("1");
		System.out.println("Retrieved document: " + documentRetrieved);

		ByteBuffer retrievedBuffer = documentRetrieved.getBinary("buffer");

		System.out.println("Content of retrievedBuffer:");
		for (int i = 0; i < retrievedBuffer.capacity(); i++)
			System.out.print(retrievedBuffer.get(i) + " ");
		System.out.println();

		ByteBuffer otherRetrievedBuffer = documentRetrieved.getBinary("other.otherbuffer");

		System.out.println("Content of otherRetrievedBuffer:");
		for (int i = 0; i < otherRetrievedBuffer.capacity(); i++)
			System.out.print(otherRetrievedBuffer.get(i) + " ");
		System.out.println();

		
	}

}
