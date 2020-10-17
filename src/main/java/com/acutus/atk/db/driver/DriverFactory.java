package com.acutus.atk.db.driver;

import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jaspervdb on 2016/11/22.
 */
public class DriverFactory {

    private static Map<String, AbstractDriver> helperMap = new HashMap<>();

    @SneakyThrows
    public static AbstractDriver getDriver(Connection connection) {
        if (!helperMap.containsKey(connection.getMetaData().getUserName())) {
            helperMap.put(connection.getMetaData().getUserName(), getDriver(connection, connection.getMetaData().getDatabaseProductName()));
        }
        return helperMap.get(connection.getMetaData().getUserName());
    }

    @SneakyThrows
    public static AbstractDriver getDriver(DataSource dataSource) {
        try (Connection connection = dataSource.getConnection()) {
            return getDriver(connection);
        }
    }

    private static AbstractDriver getDriver(Connection connection, String productName) throws SQLException {
        switch (productName) {
            case "Oracle":
                throw new UnsupportedOperationException("Not implemented");
            case "MySQL":
                return new MysqlDriver().init(connection);
            case "Microsoft SQL Server":
                throw new UnsupportedOperationException("Not implemented");
            case "HSQL Database Engine":
                throw new UnsupportedOperationException("Not implemented");
            case "PostgreSQL":
                return new PostgresqlDriver();
            case "Apache Derby":
                throw new UnsupportedOperationException("Not implemented");
            default:
                throw new RuntimeException(String.format("DB not supported %s", productName));
        }
    }
}
