package tools.springdatagenerator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;

import tools.springdatagenerator.beans.DataSourceConfig;
import tools.springdatagenerator.beans.RuntimeConfig;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class,
    DataSourceTransactionManagerAutoConfiguration.class,
    JdbcTemplateAutoConfiguration.class})
public class SpringDataGeneratorApplication implements ApplicationRunner {
    @Autowired
    private RuntimeConfig runtimeConfig;

    @Autowired
    private DataSourceConfig dataSourceConfig;

    public static void main(String[] args) {
        SpringApplication.run(SpringDataGeneratorApplication.class, args); 
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        int parallelNum = runtimeConfig.threads;
        for (int parallel = 1; parallel <= parallelNum; ++parallel) {
            new Thread(new DataGeneratorThreadCreation(dataSourceConfig, runtimeConfig,
                    Long.valueOf(runtimeConfig.maxInsertCountPerThread) * (parallel-1))).start();
        }
    }

}
