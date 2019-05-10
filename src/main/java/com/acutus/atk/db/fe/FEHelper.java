package com.acutus.atk.db.fe;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFields;
import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.fe.indexes.Indexes;
import com.acutus.atk.db.fe.keys.FrKeys;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import lombok.SneakyThrows;
import lombok.extern.java.Log;

import javax.persistence.Enumerated;
import java.sql.*;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.acutus.atk.db.constants.EnvProperties.DB_FE_ALLOW_DROP;
import static com.acutus.atk.db.constants.EnvProperties.DB_FE_STRICT;
import static com.acutus.atk.db.sql.SQLHelper.execute;
import static com.acutus.atk.util.AtkUtil.handle;

@Log
public class FEHelper {

    public static void maintainDataDefinition(Connection connection, List<Class<? extends AbstractAtkEntity>> classes) {
        List<AbstractAtkEntity> entities = classes.stream()
                .map(c -> handle(() -> c.newInstance()))
                .collect(Collectors.toList());
        maintainDataDefinition(connection, entities.toArray(new AbstractAtkEntity[]{}));
    }

    public static void maintainDataDefinition(Connection connection, Class<? extends AbstractAtkEntity>... classes) {
        maintainDataDefinition(connection, Arrays.asList(classes));
    }

    @SneakyThrows
    private static void maintainDataDefinition(Connection connection, AbstractAtkEntity... entities) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        // maintain schema's
        for (AbstractAtkEntity entity : entities) {
            if (!driver.doesTableExist(connection, entity.getTableName())) {
                createTable(connection, entity);
            } else {
                maintainTable(connection, entity);
            }
        }
        // PK
        for (AbstractAtkEntity entity : entities) {
            maintainPrimaryKeys(connection, driver, entity);
        }

        // FK
        for (AbstractAtkEntity entity : entities) {
            maintainForeignKeys(connection, driver, entity);
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
    public static void maintainTable(Connection connection, AbstractAtkEntity entity) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        try (Statement smt = connection.createStatement()) {
            try (ResultSet rs = smt.executeQuery(String.format("select * from %s", entity.getTableName()))) {
                maintainTable(connection, driver, entity, rs.getMetaData());
            }
        }
    }

    @SneakyThrows
    private static void maintainTable(Connection connection, AbstractDriver driver, AbstractAtkEntity entity
            , ResultSetMetaData meta) {
        Strings colNames = new Strings();
        for (int i = 0; i < meta.getColumnCount(); i++) {
            colNames.add(meta.getColumnName(i + 1));
            Optional<AtkEnField> atkField = entity.getEnFields().getByColName(meta.getColumnName(i + 1));
            if (atkField.isPresent()) {
                boolean typeMatch =
                        driver.getFieldType(atkField.get()).equalsIgnoreCase(meta.getColumnTypeName(i + 1))
                                || atkField.get().getColumnType(driver).getName().equals(meta.getColumnClassName(i + 1));
                boolean sizeMatch = Clob.class.equals(atkField.get().getColumnType(driver))
                        || Blob.class.equals(atkField.get().getColumnType(driver))
                        || Date.class.isAssignableFrom(atkField.get().getColumnType(driver))
                        || Temporal.class.isAssignableFrom(atkField.get().getColumnType(driver))
                        || atkField.get().getField().getAnnotation(Enumerated.class) != null
                        || atkField.get().getColLength() == meta.getColumnDisplaySize(i + 1);
                boolean nullMatch = atkField.get().isNullable() == (meta.isNullable(i + 1) == 1);
                if (!(typeMatch && (sizeMatch || !DB_FE_STRICT.get()) && nullMatch)) {
                    // alter the column
                    logAndExecute(connection, DriverFactory.getDriver(connection)
                            .getAlterColumnDefinition(atkField.get()));
                }
            } else {
                // drop the extra column
                Assert.isTrue(DB_FE_ALLOW_DROP.get(), "Dropping of columns %s not allowed"
                        , meta.getColumnName(i + 1));
                logAndExecute(connection, DriverFactory.getDriver(connection)
                        .getDropColumnColumnDefinition(entity.getTableName(), meta.getColumnName(i + 1)));
            }
        }
        // add all the missing columns
        AtkEnFields toAdd = entity.getEnFields().clone();
        toAdd.removeIf(p -> colNames.contains(p.getColName()));
        for (AtkEnField field : toAdd) {
            logAndExecute(connection, DriverFactory.getDriver(connection)
                    .getAddColumnColumnDefinition(field));
        }
    }

    private static void maintainPrimaryKeys(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        Strings dbPks = driver.getPrimaryKeys(connection, entity.getTableName());

        AtkEnFields pkToAdd = entity.getEnFields().getIds()
                .removeWhen(f -> dbPks.containsIgnoreCase(f.getColName()));
        if (!pkToAdd.isEmpty()) {
            logAndExecute(connection, driver.getAddPrimaryKeyDefinition(pkToAdd));
        }

    }

    /**
     * add missing Foreign keys, replace mismatching keys, drop redundant keys
     *
     * @param connection
     * @param driver
     * @param entity
     */
    private static void maintainForeignKeys(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        // **** Foreign Keys

        FrKeys dbKeys = FrKeys.load(driver.getForeignKeys(connection, entity.getTableName()));
        AtkEnFields enKeys = entity.getEnFields().getForeignKeys();

        // add missing
        AtkEnFields missing = enKeys.removeWhen(k -> dbKeys.containsField(k));
        missing.stream().forEach(k -> logAndExecute(connection, driver.addForeignKey(k)));

        // remove redundant
        FrKeys remove = dbKeys.removeWhen(k -> k.isPresentIn(enKeys));
        remove.stream().forEach(k -> logAndExecute(connection, driver.dropForeignKey(entity.getTableName(), k)));
    }

    @SneakyThrows
    public void maintainIndexes(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        Indexes indexes = driver.getIndexes(connection, entity.getTableName());
        // add missing
    }


}
