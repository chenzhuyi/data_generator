package com.sap.hana.dp.datagen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class OracleJdbcConnectionFactory implements JdbcConnectionFactory {
    public static final String ORACLE_JDBC_CONNECTION_URL = "jdbc:oracle:thin:@";
    private Properties properties = null;

    public OracleJdbcConnectionFactory(Properties properties) {
    	this.properties = properties;
    }

	public Connection newConnection() throws SQLException {
	    StringBuilder url = new StringBuilder(ORACLE_JDBC_CONNECTION_URL);

        // if cdb_enabled=true (12c multitenant) jdbc:oracle:thin:@HOST:PORT/DB_SERVICE_NAME
        // if cdb_enabled=false                  jdbc:oracle:thin:@HOST:PORT:DB_NAME
	    url.append(properties.get(DB_HOST));
	    url.append(":");
	    url.append(properties.get(DB_PORT));
	    url.append(":");
	    url.append(properties.get(DB_NAME));

	    Properties props = new Properties();
	    props.put("user", properties.getProperty(DB_USERNAME));
	    props.put("password", properties.getProperty(DB_PASSWORD));
	    return DriverManager.getConnection(url.toString(), props);
	}
}
