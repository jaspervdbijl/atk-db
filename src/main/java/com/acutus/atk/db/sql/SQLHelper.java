package com.acutus.atk.db.sql;

import com.acutus.atk.util.Call;
import lombok.SneakyThrows;

import javax.sql.DataSource;
import java.sql.Connection;

public class SQLHelper {

    @SneakyThrows
    public static void run(DataSource dataSource, Call.One<Connection> call) {
        try (Connection con = dataSource.getConnection()) {
            call.call(con);
        }
    }

    @SneakyThrows
    public static <T> T runAndReturn(DataSource dataSource, Call.OneRet<Connection, T> call) {
        try (Connection con = dataSource.getConnection()) {
            return call.call(con);
        }
    }


}
