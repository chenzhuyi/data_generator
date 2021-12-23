package com.sap.hana.dp.datagen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class HanaJdbcConnectionFactory implements JdbcConnectionFactory {
    public static final String HANA_JDBC_CONNECTION_URL = "jdbc:sap://";
    private Properties properties = null;

    public HanaJdbcConnectionFactory(Properties properties) {
    	this.properties = properties;
    }
    
	public Connection newConnection() throws SQLException {
	    StringBuilder url = new StringBuilder(HANA_JDBC_CONNECTION_URL);

	    url.append(properties.get(DB_HOST));
	    url.append(":");
	    url.append(properties.get(DB_PORT));
	    url.append("/?databaseName=");
	    url.append(properties.get(DB_NAME));
	    if (properties.get(CONN_PROPERTIES) != null && !((String) properties.get(CONN_PROPERTIES)).isEmpty()) {
	    	url.append("&").append(properties.get(CONN_PROPERTIES));
	    }

	    String user = properties.getProperty(DB_USERNAME);
	    String password = properties.getProperty(DB_PASSWORD);
	    return DriverManager.getConnection(url.toString(), user, password);
	}
}
