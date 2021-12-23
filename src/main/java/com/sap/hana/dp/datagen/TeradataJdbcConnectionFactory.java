package com.sap.hana.dp.datagen;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TeradataJdbcConnectionFactory implements JdbcConnectionFactory {
    private Properties properties = null;

    public TeradataJdbcConnectionFactory(Properties properties) {
    	this.properties = properties;
    }
    
	public Connection newConnection() throws SQLException {
//	    StringBuilder url = new StringBuilder(HANA_JDBC_CONNECTION_URL);
//
//	    url.append(properties.get(DB_HOST));
//	    url.append(":");
//	    url.append(properties.get(DB_PORT));
//	    url.append("/?databaseName=");
//	    url.append(properties.get(DB_NAME));
		String url = "jdbc:teradata://10.48.153.53/DATABASE=DPTUSER3,TMODE=ANSI,CHARSET=UTF8,LOGMECH=TD2,DBS_PORT=1025";

	    String user = "DPTUSER3";
	    String password = "Sybase123";
	    return DriverManager.getConnection(url, user, password);
	}
}
