package com.acutus.atk.db.sql;

import com.acutus.atk.util.Call;
import lombok.SneakyThrows;

import javax.sql.ConnectionEvent;
import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class SQLHelper {

    @SneakyThrows
    public static void run(DataSource dataSource, Call.Two<Connection,Void> call) {
        try (Connection con = dataSource.getConnection()) {
            call.call(con);
        }
    }

    @SneakyThrows
    public static <T> T runAndReturn(DataSource dataSource, Call.Two<Connection,T> call) {
        try (Connection con = dataSource.getConnection()) {
            return call.call(con);
        }
    }



}
