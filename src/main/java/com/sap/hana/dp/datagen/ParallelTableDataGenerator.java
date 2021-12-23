package com.sap.hana.dp.datagen;

public class ParallelTableDataGenerator {

//	private static final long BASE_START_VALUE = 100000000; // to prevent numeric overflow
	public static void main(String[] args) throws Exception {
		if (args.length != 10) {
            System.err.println("Usage: ParallelTableDataGenerator "
            		+ "<parallel-num-per-table> "
            		+ "<db-connection-info-prop-filename> "
            		+ "<table-list-filename> "
            		+ "<delete-threshold> "
            		+ "<delete-on-start> "
            		+ "<max-insert-count-per-thread> "
            		+ "<max-number-of-rows-to-gen-per-run> "
            		+ "<max-update-count-per-thread> "
            		+ "<enable-log>"
            		+ "<specify-values>");
            System.exit(1);
        }

        int parallelNum = Integer.valueOf(args[0]) > 1 ? Integer.valueOf(args[0]) : 1;
        for (int parallel = 1; parallel <= parallelNum; ++parallel) {
        	String[] perArgs = new String[]{args[1],args[2],args[3],args[4],
        			String.valueOf(Integer.valueOf(args[5]) * (parallel-1)),
        			args[5],args[6],args[7], args[8], args[9]};
        	new Thread(new TableDataGeneratorMain(perArgs)).start();
        }
	}

}
