package com.acutus.atk.db.fe;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFieldList;
import com.acutus.atk.db.util.AbstractDriver;
import com.acutus.atk.db.util.DriverFactory;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import java.sql.*;
import java.util.Optional;

import static com.acutus.atk.db.constants.EnvProperties.DB_FE_ALLOW_DROP;
import static com.acutus.atk.db.constants.EnvProperties.DB_FE_STRICT;
import static com.acutus.atk.db.sql.SQLHelper.execute;

@Log
public class FEHelper {

    public static void maintainDataDefinition(Connection connection, AbstractAtkEntity entity) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        if (!driver.doesTableExist(connection, entity.getTableName())) {
            createTable(connection, entity);
        } else {
            updateTable(connection, entity);
        }
    }

    public static void createTableIfNoExists(Connection connection, AbstractAtkEntity entity) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        if (!driver.doesTableExist(connection, entity.getTableName())) {
            createTable(connection, entity);
        }
    }

    private static void logAndExecute(Connection connection, String sql) {
        log.warning(sql);
        execute(connection, sql);

    }

    public static void createTable(Connection connection, AbstractAtkEntity entity) {
        logAndExecute(connection, DriverFactory.getDriver(connection).getCreateSql(entity));
    }

    @SneakyThrows
    public static void updateTable(Connection connection, AbstractAtkEntity entity) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        try (Statement smt = connection.createStatement()) {
            try (ResultSet rs = smt.executeQuery(String.format("select * from %s", entity.getTableName()))) {
                updateTable(connection, driver, entity, rs.getMetaData());
            }
        }
    }

    @SneakyThrows
    private static void updateTable(Connection connection, AbstractDriver driver, AbstractAtkEntity entity
            , ResultSetMetaData meta) {
        Strings colNames = new Strings();
        for (int i = 0; i < meta.getColumnCount(); i++) {
            colNames.add(meta.getColumnName(i + 1));
            Optional<AtkEnField> atkField = entity.getEnFields().getByColName(meta.getColumnName(i + 1));
            if (atkField.isPresent()) {
                boolean typeMatch =
                        driver.getFieldType(atkField.get()).equalsIgnoreCase(meta.getColumnTypeName(i + 1))
                                || atkField.get().getType().getName().equals(meta.getColumnClassName(i + 1));
                boolean sizeMatch = Clob.class.equals(atkField.get().getColumnType(driver))
                        || Blob.class.equals(atkField.get().getColumnType(driver))
                        || atkField.get()
                        .getColLength() == meta.getColumnDisplaySize(i + 1);
                boolean nullMatch = atkField.get().isNullable() == (meta.isNullable(i + 1) == 1);
                if (!(typeMatch && (sizeMatch || !DB_FE_STRICT.get()) && nullMatch)) {
                    // alter the column
                    logAndExecute(connection, DriverFactory.getDriver(connection)
                            .getAlterColumnDefinition(atkField.get()));
                }
            } else {
                // drop the extra column
                Assert.isTrue(DB_FE_ALLOW_DROP.get(), "Dropping of columns %s not allowed"
                        , atkField.get().getTableAndColName());
                logAndExecute(connection, DriverFactory.getDriver(connection)
                        .getDropColumnColumnDefinition(atkField.get()));
            }
        }
        // add all the missing columns
        AtkEnFieldList toAdd = entity.getEnFields().clone();
        toAdd.removeIf(p -> colNames.contains(p.getColName()));
        for (AtkEnField field : toAdd) {
            logAndExecute(connection, DriverFactory.getDriver(connection)
                    .getAddColumnColumnDefinition(field));
        }
    }


}
