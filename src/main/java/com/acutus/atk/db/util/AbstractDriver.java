package com.acutus.atk.db.util;

import com.acutus.atk.db.sql.SQLHelper;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractDriver {

    @SneakyThrows
    public List<String> getPrimaryKeys(Connection connection, String tableName) {
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(connection.getCatalog()
                , connection.getSchema(), tableName)) {
            List<List> keys = SQLHelper.query(rs, new Class[]{String.class}, new String[]{"COLUMN_NAME"});
            return keys.stream().map(s -> (String) s.get(0)).collect(Collectors.toList());
        }
    }

}
