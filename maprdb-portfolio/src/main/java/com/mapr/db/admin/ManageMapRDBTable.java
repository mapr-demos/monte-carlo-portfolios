package com.mapr.db.admin;

import java.io.IOException;
import java.util.Arrays;

import com.mapr.db.Admin;
import com.mapr.db.FamilyDescriptor;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.TableDescriptor;

public class ManageMapRDBTable {

    public static final String SIM_TABLE_PATH = "/apps/simulations";
    public static final String SIM_RES_TABLE_PATH = "/apps/simulation_results";
    public static final String PORTFOLIO_TABLE_PATH = "/apps/portfolios";

	public static void main(String[] args) throws IOException {

		new ManageMapRDBTable().run();
		
	}

	private void run() throws IOException {
		
		System.out.println("Creating table " + SIM_TABLE_PATH);
		this.createTable(SIM_TABLE_PATH);

		System.out.println("Creating table " + SIM_RES_TABLE_PATH);
		this.createTable(SIM_RES_TABLE_PATH);
		
		System.out.println("Creating table " + PORTFOLIO_TABLE_PATH);
		this.createTable(PORTFOLIO_TABLE_PATH);
		
		System.out.println("Done.");
	}

	private void createTable(String table_name) throws IOException {
        // delete table
        if (MapRDB.tableExists(table_name)) {
            MapRDB.deleteTable(table_name);
        }

        // Admin Tool
        Admin admin = MapRDB.newAdmin();

        // Create a table descriptor
        TableDescriptor tableDescriptor = MapRDB.newTableDescriptor()
                .setPath(table_name)  // set the Path of the table in MapR-FS
                .setSplitSize(512)    // Size in mebibyte (Mega Binary Bytes)
                .setBulkLoad(false);   // Created with Bulk mode by default

        // Configuration of the default Column Family, used to store JSON element by default
        FamilyDescriptor familyDesc = MapRDB.newDefaultFamilyDescriptor()
                .setCompression(FamilyDescriptor.Compression.None)
                .setInMemory(true); // To tell the DB to keep these value in RAM as much as possible
        tableDescriptor.addFamily(familyDesc);

        // Create a new column family to store specific JSON attributes
//        familyDesc = MapRDB.newFamilyDescriptor()
//                .setName("clicks")
//                .setJsonFieldPath("clicks")
//                .setCompression(FamilyDescriptor.Compression.ZLIB)  // compression for this CF
//                .setInMemory(false);

//        tableDescriptor.addFamily(familyDesc);

        admin.createTable(tableDescriptor);
    }


	private void deleteTable(String tableName) throws IOException {
		if (MapRDB.tableExists(tableName)) {
			MapRDB.deleteTable(tableName);
		}
	}

	/**
	 * Print table information such as Name, Path and Tablets information
	 *
	 * @param tableName
	 *            The table to describe
	 * @throws IOException
	 *             If anything goes wrong accessing the table
	 */
	private void printTableInformation(String tableName) throws IOException {
		Table table = MapRDB.getTable(tableName);
		System.out.println("\n=============== TABLE INFO ===============");
		System.out.println(" Table Name : " + table.getName());
		System.out.println(" Table Path : " + table.getPath());
		System.out.println(" Table Infos : " + Arrays.toString(table.getTabletInfos()));
		System.out.println("==========================================\n");
	}
}
