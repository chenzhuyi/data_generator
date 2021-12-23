package com.sap.hana.dp.datagen;

import java.sql.Connection;
import java.sql.SQLException;

interface JdbcConnectionFactory {
    static final String DB_USERNAME = "db_username";
    static final String DB_PASSWORD = "db_password";
    static final String DB_HOST = "db_host";
    static final String DB_PORT = "db_port";
    static final String DB_NAME = "db_name";
    static final String CONN_PROPERTIES = "conn_properties";

	Connection newConnection() throws SQLException;
}
