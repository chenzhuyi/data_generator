package com.sap.hana.dp.datagen;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;

public class TableDataGeneratorMain implements Runnable {
	private static volatile boolean gracefullyExited = false;
	private static volatile boolean jvmStopped = false;
	private final String[] args;
	public TableDataGeneratorMain(String[] args) {
		this.args = args;
	}

	public void run() {

        int i = 0;
        final String connectionInfoPropFilename = args[i++];
        final String tableListFilename = args[i++];
        final long deleteThreshold = Long.valueOf(args[i++]);
        final boolean deleteOnStart = Boolean.valueOf(args[i++]);
        final long startHintValue = Long.valueOf(args[i++]);
        final long maxInsertCountPerThread = Long.valueOf(args[i++]);
        final int maxNumberOfRowsToGeneratePerRun = Integer.valueOf(args[i++]);
        final long maxUpdateCountPerThread = Long.valueOf(args[i++]);
        final boolean enableLog = Boolean.valueOf(args[i++]);
        final String specifyValues = args[i++];
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
        	@Override
        	public void run() {
        		System.out.println("JVM is shutting down.");
        		jvmStopped = true;
        		while (!gracefullyExited) {
        			try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
					}
        		}
        		System.out.println("JVM is down.");
        	}
        });
        
        Properties props = new Properties();
        try {
			props.load(new FileReader(connectionInfoPropFilename));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
 
        HanaJdbcConnectionFactory factory = new HanaJdbcConnectionFactory(props);
        
//        OracleJdbcConnectionFactory factory = new OracleJdbcConnectionFactory(props);
        
//        TeradataJdbcConnectionFactory factory = new TeradataJdbcConnectionFactory(props);

        BufferedReader reader = null;
        List<String> tables = null;
        try {
            reader = new BufferedReader(new FileReader(tableListFilename));
            Collector<String, ?, List<String>> collector = Collectors.toList();
            tables = reader.lines().collect(collector);
        } catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
            IOUtils.closeQuietly(reader);
        }
        
        Map<String, String> values = new HashMap<String, String>();
        List<String> lines = null;
        try {
            reader = new BufferedReader(new FileReader(specifyValues));
            Collector<String, ?, List<String>> lineCollector = Collectors.toList();
            lines = reader.lines().collect(lineCollector);
            
            for (String line : lines) {
            	String[] lineSplits = line.split("=");
            	values.put(lineSplits[0], lineSplits[1]);
            }
            
        } catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
            IOUtils.closeQuietly(reader);
        }

        List<Thread> threads = new ArrayList<Thread>();
        i = 0;
        for (String table : tables) {
        	threads.add(createAndStartGenerator(factory, table,
        			deleteThreshold, deleteOnStart, startHintValue,
        			maxInsertCountPerThread, maxNumberOfRowsToGeneratePerRun, maxUpdateCountPerThread,
        			enableLog, values));
            ++i;
        }

        i = 0;
        for (Thread t : threads) {
            try {
				t.join();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
            System.out.println("Thread " + t.getId() + " exited.");
            ++i;
        }
        gracefullyExited = true;
	}

    public static Thread createAndStartGenerator(JdbcConnectionFactory factory, String table,
    		long deleteThreshold, boolean deleteOnStart, long startHintValue, long maxInsertCountPerThread, int maxNumberOfRowsToGeneratePerRun, 
    		long maxUpdateCountPerThread, boolean enableLog, Map<String, String> values) {
        TableDataGenerator gen = new TableDataGenerator(factory, table,
        		deleteThreshold, deleteOnStart, startHintValue, maxInsertCountPerThread, maxNumberOfRowsToGeneratePerRun, maxUpdateCountPerThread,
        		jvmStopped, enableLog, values);
        Thread thr = new Thread(gen);
        StringBuilder sb = new StringBuilder("Starting thread ").append(thr.getId()).append(" with tables [").append(table).append("]");
        if (deleteOnStart) {
        	sb.append(". Delete all at first");
        }
        if (maxInsertCountPerThread == 0 && maxUpdateCountPerThread == 0 && deleteThreshold > 0) {
        	sb.append(". Delete randomly only");
        }
        if (maxInsertCountPerThread > 0 ) {
        	sb.append(". Will insert around ").append(maxInsertCountPerThread).append(" rows");
        }
        if (maxUpdateCountPerThread > 0) {
        	sb.append(". Will update around ").append(maxUpdateCountPerThread).append(" rows");
        }
        System.out.println(sb);
        thr.start();
        return thr;
    }
}
