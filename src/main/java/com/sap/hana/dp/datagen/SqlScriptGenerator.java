package com.sap.hana.dp.datagen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.IOUtils;

public class SqlScriptGenerator {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: SqlScriptGenerator <input-table-list-filename> <output-filename>");
            System.exit(1);
        }

        String inputFile = args[0];
        String outputFile = args[1];
        BufferedReader reader = null;
        PrintWriter printer = null;
        String tablename = null;
        try {
            reader = new BufferedReader(new FileReader(inputFile));
            printer = new PrintWriter(new FileWriter(outputFile));
            while ((tablename = reader.readLine()) != null) {
                StringBuilder sb = new StringBuilder();
                sb.append("create virtual table system.VT_")
                    .append(tablename)
                    .append(" at \"ORACLENTSRC\".\"<NULL>\".\"<NULL>\".\"LR_USER.")
                    .append(tablename).append("\";\n");
                sb.append("create column table system.")
                    .append(tablename)
                    .append(" like system.VT_")
                    .append(tablename)
                    .append(";\n");
                sb.append("create remote subscription system.SUBS_")
                    .append(tablename)
                    .append(" on system.VT_")
                    .append(tablename)
                    .append(" with schema changes target table system.")
                    .append(tablename)
                    .append(";\n");
                sb.append("alter remote subscription system.SUBS_")
                    .append(tablename)
                    .append(" queue;\n");
                sb.append("alter remote subscription system.SUBS_")
                    .append(tablename)
                    .append(" distribute;\n");
                printer.println(sb.toString());
                printer.flush();
            }
        } finally {
            IOUtils.closeQuietly(printer);
            IOUtils.closeQuietly(reader);
        }
    }

}
