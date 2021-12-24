package tools.springdatagenerator.beans;

import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:application.properties")
public class DataSourceConfig {

    @Bean
    @ConfigurationProperties(prefix="datagenerator.datasource")
    public DataSourceProperties dataSourceProperties() {
        return new DataSourceProperties();
    }

}
