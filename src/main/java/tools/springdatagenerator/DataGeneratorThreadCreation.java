package tools.springdatagenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;

import lombok.extern.slf4j.Slf4j;
import tools.springdatagenerator.beans.DataSourceConfig;
import tools.springdatagenerator.beans.RuntimeConfig;

@Slf4j
public class DataGeneratorThreadCreation implements Runnable {

    private static volatile boolean gracefullyExited = false;
    private static volatile boolean jvmStopped = false;
    private final RuntimeConfig runtimeConfig;
    private final long startHintValue;
    private final JdbcTemplate jdbcTemplate;

    public DataGeneratorThreadCreation(DataSourceConfig dataSourceConfig,
            RuntimeConfig runtimeConfig, long startHintValue) {
        this.runtimeConfig = runtimeConfig;
        this.startHintValue = startHintValue;
        this.jdbcTemplate = new JdbcTemplate(
                dataSourceConfig.dataSourceProperties().initializeDataSourceBuilder().build());
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("JVM is shutting down.");
                jvmStopped = true;
                while (!gracefullyExited) {
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                    }
                }
                log.info("JVM is down.");
            }
        });

        List<String> tables = Arrays.asList(runtimeConfig.tableList.split(","));
        Map<String, String> values = new HashMap<String, String>();
        for (String line : runtimeConfig.specifyValues.split(",")) {
            String[] lineSplits = line.split("=");
            values.put(lineSplits[0], lineSplits[1]);
        }

        List<Thread> threads = new ArrayList<Thread>();
        for (String table : tables) {
            threads.add(createAndStartGenerator(table, values));
        }

        for (Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            log.info("Thread " + t.getId() + " exited.");
        }
        gracefullyExited = true;
	}

    public Thread createAndStartGenerator(String table, Map<String, String> values) {
        DataGeneratorTask gen = new DataGeneratorTask(jdbcTemplate, runtimeConfig,
                table, startHintValue,
                values, jvmStopped);
        Thread thr = new Thread(gen);
        StringBuilder sb = new StringBuilder("Starting thread ")
                .append(thr.getId()).append(" with tables [").append(table).append("]");
        if (runtimeConfig.needTruncate) {
            sb.append(". Delete all at first");
        }
        if (runtimeConfig.maxInsertCountPerThread == 0
                && runtimeConfig.maxUpdateCountPerThread == 0
                && runtimeConfig.deleteThreshold > 0) {
            sb.append(". Delete randomly only");
        }
        if (runtimeConfig.maxInsertCountPerThread > 0 ) {
            sb.append(". Will insert around ").append(runtimeConfig.maxInsertCountPerThread).append(" rows");
        }
        if (runtimeConfig.maxUpdateCountPerThread > 0) {
            sb.append(". Will update around ").append(runtimeConfig.maxUpdateCountPerThread).append(" rows");
        }
        log.info(sb.toString());
        thr.start();
        return thr;
    }
}
