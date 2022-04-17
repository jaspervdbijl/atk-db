package com.acutus.atk.db.fe;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.util.collection.Tuple1;
import lombok.Synchronized;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;

import static com.acutus.atk.db.sql.SQLHelper.*;

public class TableVersionHelper {

    private static Boolean EXISTS_CHECKED = false;

    public static boolean shouldUpgrade(Connection connection, AbstractAtkEntity entity) {
        Optional<Integer> version = getTableVersion(connection, entity.getTableName());
        return (!version.isPresent() || entity.version() == 0 || version.get() > entity.version());
    }

    @Synchronized("EXISTS_CHECKED")
    public static Optional<Integer> getTableVersion(Connection connection, String tableName) {
        if (!EXISTS_CHECKED) {
            EXISTS_CHECKED = true;
            createIfNotExists(connection);
        }
        List<Tuple1<Integer>> version = query(connection, Integer.class, "select version from atk_table_version where name = ?", tableName);
        return version.isEmpty() ? Optional.empty() : Optional.of(version.get(0).getFirst());
    }

    public static void setTableVersion(Connection connection, AbstractAtkEntity entity) {
        if (getTableVersion(connection, entity.getTableName()).isPresent()) {
            executeUpdate(connection, "update atk_table_version set version = ? where name = ?"
                    , entity.version(), entity.getTableName());
        } else {
            executeUpdate(connection, "insert into atk_table_version(name,version) values(?,?)"
                    , entity.version(), entity.getTableName());
        }
    }

    public static void createIfNotExists(Connection connection) {
        if (!DriverFactory.getDriver(connection).doesTableExist(connection, "atk_table_version")) {
            execute(connection, "create table atk_table_version(name varchar(250),version int)");
        }
    }
}
