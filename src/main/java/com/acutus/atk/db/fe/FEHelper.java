package com.acutus.atk.db.fe;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFields;
import com.acutus.atk.db.AtkEnIndex;
import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.db.fe.indexes.Index;
import com.acutus.atk.db.fe.indexes.Indexes;
import com.acutus.atk.db.fe.keys.FrKeys;
import com.acutus.atk.db.processor.Populate;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.reflection.Reflect;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.StringUtils;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.collection.One;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.Enumerated;
import java.lang.reflect.Field;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.acutus.atk.db.constants.EnvProperties.DB_FE_ALLOW_DROP;
import static com.acutus.atk.db.constants.EnvProperties.DB_FE_STRICT;
import static com.acutus.atk.db.sql.SQLHelper.execute;
import static com.acutus.atk.util.AtkUtil.handle;

@Slf4j
public class FEHelper {

    public static void maintainDataDefinition(Connection connection, List<Class<? extends AbstractAtkEntity>> classes) {
        List<AbstractAtkEntity> entities = classes.stream()
                .map(c -> handle(() -> (AbstractAtkEntity) c.getConstructors()[0].newInstance()))
                .collect(Collectors.toList());
        maintainDataDefinition(connection, entities.toArray(new AbstractAtkEntity[]{}));
    }

    public static void maintainDataDefinition(Connection connection, Class<? extends AbstractAtkEntity>... classes) {
        maintainDataDefinition(connection, Arrays.asList(classes));
    }

    private static void createRecordLogTableIfNotExists(Connection connection) {
        if (!DriverFactory.getDriver(connection).doesTableExist(connection, "atk_db_record")) {
            SQLHelper.execute(connection, "create table atk_db_record(table_name varchar(200), checksum varchar(100), modified_time datetime DEFAULT CURRENT_TIMESTAMP)");
        }
    }

    private static boolean recordMismatch(Connection connection, AbstractAtkEntity entity) {
        Optional<One<String>> hash = SQLHelper.queryOne(connection, String.class, "select checksum from atk_db_record where lower(table_name) = ?"
                , entity.getTableName().toLowerCase());
        return !hash.isPresent() || !hash.get().getFirst().equalsIgnoreCase(entity.getMd5Hash());
    }

    @SneakyThrows
    private static void maintainChecksum(Connection connection, AbstractAtkEntity entity) {
        log.info("Maintain checksum {}", entity.getTableName());
        SQLHelper.executeUpdate(connection, "delete from atk_db_record where lower(table_name) = ? ", entity.getTableName());
        SQLHelper.executeUpdate(connection, "insert into atk_db_record(table_name,checksum) values (?,?) ", entity.getTableName(), entity.getMd5Hash());
    }

    @SneakyThrows
    private static void maintainDataDefinition(Connection connection, AbstractAtkEntity... entities) {
        createRecordLogTableIfNotExists(connection);

        // filter
        entities = Arrays.stream(entities)
                .filter(c -> c.maintainEntity())
                .filter(c -> recordMismatch(connection, c))
                .collect(Collectors.toList()).toArray(new AbstractAtkEntity[]{});

        AbstractDriver driver = DriverFactory.getDriver(connection);
        // maintain schema's
        for (AbstractAtkEntity entity : entities) {
            log.info("Maintain {}", entity.getTableName());

            if (entity.maintainForeignKeys()) {
                maintainForeignKeysDrop(connection, driver, entity);
            }

            if (entity.maintainEntity() && !driver.doesTableExist(connection, entity.getTableName())) {
                createTable(connection, entity);
            } else if (entity.maintainEntity()) {
                maintainTable(driver, connection, entity);
            }
        }
        // PK
        for (AbstractAtkEntity entity : entities) {
            if (entity.maintainColumns()) {
                maintainPrimaryKeys(connection, driver, entity);
            }
        }

        // FK
        for (AbstractAtkEntity entity : entities) {
            if (entity.maintainForeignKeys()) {
                maintainForeignKeysAdd(connection, driver, entity);
            }
        }

        // INDEXES
        for (AbstractAtkEntity entity : entities) {
            if (entity.maintainIndex()) {
                maintainIndexes(connection, driver, entity);
            }
        }
        // update checksums
        for (AbstractAtkEntity entity : entities) {
            maintainChecksum(connection, entity);
        }
    }

    public static void createTableIfNoExists(Connection connection, AbstractAtkEntity entity) {
        AbstractDriver driver = DriverFactory.getDriver(connection);
        if (!driver.doesTableExist(connection, entity.getTableName())) {
            createTable(connection, entity);
        }
    }

    private static void logAndExecute(Connection connection, String sql) {
        if (sql == null || sql.isEmpty()) return;
        log.warn(sql);
        execute(connection, sql);
    }

    @SneakyThrows
    private static void populateValues(Connection connection, AbstractAtkEntity entity, Map source) {
        entity = entity.getClass().getConstructor().newInstance();
        for (Field field : Reflect.getFields(entity.getClass()).filter(f -> source.containsKey(f.getName()))) {
            if (field.getType().isEnum()) {
                field.set(entity, field.getType().getMethod("valueOf", String.class).invoke(null, source.get(field.getName())));
            } else {
                field.set(entity, source.get(field.getName()));
            }
        }
        entity.persist().insert(connection);
    }

    @SneakyThrows
    public static void createTable(Connection connection, AbstractAtkEntity entity) {
        log.info("Create entity {}", entity.getTableName());
        logAndExecute(connection, DriverFactory.getDriver(connection).getCreateSql(entity));
        Populate populate = entity.getClass().getAnnotation(Populate.class);
        if (populate != null) {
            new ObjectMapper().readValue(Thread.currentThread().getContextClassLoader().getResourceAsStream(populate.value()), List.class)
                    .stream().forEach(o -> populateValues(connection, entity, (Map) o));
        }
    }

    @SneakyThrows
    public static void maintainTable(AbstractDriver driver, Connection connection, AbstractAtkEntity entity) {
        try (Statement smt = connection.createStatement()) {
            try (ResultSet rs = smt.executeQuery(driver.limit(String.format("select * from %s", entity.getTableName()),1))) {
                maintainTable(connection, driver, entity, rs.getMetaData());
                connection.commit();
            }
        }
    }

    @SneakyThrows
    private static boolean classTypeMatch(AbstractDriver driver, AtkEnField field, ResultSetMetaData meta, int i) {
        return field.getColumnType(driver).getName().equals(meta.getColumnClassName(i + 1))
                || field.getColumnType(driver).equals(LocalTime.class) && meta.getColumnClassName(i + 1).equalsIgnoreCase("java.sql.Time")
                || field.getColumnType(driver).equals(LocalDate.class) && meta.getColumnClassName(i + 1).equalsIgnoreCase("java.sql.Date")
                || field.getColumnType(driver).equals(LocalDateTime.class) && meta.getColumnClassName(i + 1).equalsIgnoreCase("java.sql.Timestamp");

    }

    @SneakyThrows
    private static void maintainTable(Connection connection, AbstractDriver driver, AbstractAtkEntity entity, ResultSetMetaData meta) {

        FrKeys dbKeys = driver.shouldDropConstraintPriorToAlter() ? FrKeys.load(driver.getForeignKeys(connection, entity.getTableName())) : null;

        Strings colNames = new Strings();
        for (int i = 0; i < meta.getColumnCount(); i++) {
            colNames.add(meta.getColumnName(i + 1));
            Optional<AtkEnField> atkField = entity.getEnFields().getByColName(meta.getColumnName(i + 1));
            if (atkField.isPresent()) {
                if (entity.maintainEntityColumn(atkField.get())) {
                    boolean typeMatch =
                            driver.getFieldType(atkField.get()).equalsIgnoreCase(meta.getColumnTypeName(i + 1)) ||
                                    classTypeMatch(driver, atkField.get(), meta, i) ||
                                    atkField.get().getColumnDefinitionType().equalsIgnoreCase(meta.getColumnTypeName(i + 1));

                    if (!typeMatch) {
                        log.info("Type mismatch {}.{}",entity.getTableName(),atkField.get().getColName());
                    }

                    boolean sizeMatch = Clob.class.equals(atkField.get().getColumnType(driver))
                            || Blob.class.equals(atkField.get().getColumnType(driver))
                            || Date.class.isAssignableFrom(atkField.get().getColumnType(driver))
                            || Temporal.class.isAssignableFrom(atkField.get().getColumnType(driver))
                            || Boolean.class.isAssignableFrom(atkField.get().getColumnType(driver))
                            || Number.class.isAssignableFrom(atkField.get().getColumnType(driver))
                            || Character.class.isAssignableFrom(atkField.get().getColumnType(driver))
                            || atkField.get().getField().getAnnotation(Enumerated.class) != null
                            || atkField.get().getColLength() == meta.getColumnDisplaySize(i + 1);

                    boolean nullMatch = atkField.get().isNullable() == (meta.isNullable(i + 1) == 1);

                    // Check default
                    DatabaseMetaData md = connection.getMetaData();
                    String columnDefaultVal = "";
                    try (ResultSet rs = md.getColumns(connection.getCatalog(), md.getUserName(), entity.getTableName(), atkField.get().getColName())) {
                        if (rs.next()) {
                            columnDefaultVal = StringUtils.defaultString(rs.getString("COLUMN_DEF"));

                            if (!columnDefaultVal.isEmpty() && atkField.get().getField().getType().isAssignableFrom(Boolean.class)) {
                                columnDefaultVal = Boolean.valueOf(columnDefaultVal).toString();
                            }
                        }
                    }
                    boolean defaultMatch = driver.getColumnDefinitionDefault(atkField.get()).equalsIgnoreCase(columnDefaultVal);

                    if (!(typeMatch && (sizeMatch || !DB_FE_STRICT.get()) && nullMatch) || !defaultMatch) {
                        // alter the column

                        // drop any f-key constraints before changing the column size - this might only be necessary for mysql

                        if (driver.shouldDropConstraintPriorToAlter() && dbKeys.containsField(atkField.get())) {
                            String sql = driver.dropForeignKey(entity.getTableName(), dbKeys.get(dbKeys.indexOf(atkField.get())));
                            if (entity.maintainForeignKeys()) {
                                logAndExecute(connection, sql);
                            } else {
                                log.warn("Maintain FKeys disabled for {}. Did not execute: {}", entity.getTableName(), sql);
                            }
                        }
                        String sql = DriverFactory.getDriver(connection).getAlterColumnDefinition(atkField.get());
                        if (entity.maintainColumns()) {
                            logAndExecute(connection, sql);
                        } else {
                            log.warn("Maintain Columns disabled for {}. Did not execute: {}", entity.getTableName(), sql);
                        }
                    }
                }
            } else if (entity.maintainColumns() && entity.maintainColumnsFilter().length == 0) {
                // drop the extra column
                String sql = DriverFactory.getDriver(connection).getDropColumnColumnDefinition(entity.getTableName(), meta.getColumnName(i + 1));
                Assert.isTrue(DB_FE_ALLOW_DROP.get(), "Dropping of columns %s not allowed", meta.getColumnName(i + 1));
                if (entity.maintainColumns()) {
                    logAndExecute(connection, sql);
                } else {
                    log.warn("Maintain Columns disabled for {}. Did not execute: {}", entity.getTableName(), sql);
                }
            }
        }
        // add all the missing columns
        AtkEnFields toAdd = entity.getEnFields().clone();
        toAdd.removeIf(p -> colNames.contains(p.getColName()) || !entity.maintainEntityColumn(p));
        for (AtkEnField field : toAdd) {
            String sql = DriverFactory.getDriver(connection).getAddColumnColumnDefinition(field);
            if (entity.maintainColumns()) {
                logAndExecute(connection, sql);
            } else {
                log.warn("Maintain Columns disabled for {}. Did not execute: {}", entity.getTableName(), sql);
            }
        }
    }

    private static void maintainPrimaryKeys(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        log.info("Maintain PK {}", entity.getTableName());
        Strings dbPks = driver.getPrimaryKeys(connection, entity.getTableName());

        AtkEnFields pkToAdd = entity.getEnFields().getIds()
                .removeWhen(f -> dbPks.containsIgnoreCase(f.getColName())
                        || !entity.maintainEntityColumn(f));
        if (!pkToAdd.isEmpty()) {
            logAndExecute(connection, driver.getAddPrimaryKeyDefinition(pkToAdd));
            logAndExecute(connection, DriverFactory.getDriver(connection).addAutoIncrementPK(pkToAdd));
        }

    }

    private static void maintainForeignKeysDrop(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        FrKeys dbKeys = FrKeys.load(driver.getForeignKeys(connection, entity.getTableName()));
        AtkEnFields enKeys = entity.getEnFields().getForeignKeys();
        // remove redundant
        FrKeys remove = dbKeys.removeWhen(k -> k.isPresentIn(enKeys));
        remove.stream().forEach(k -> logAndExecute(connection, driver.dropForeignKey(entity.getTableName(), k)));
    }

    /**
     * add missing Foreign keys, replace mismatching keys, drop redundant keys
     *
     * @param connection
     * @param driver
     * @param entity
     */
    private static void maintainForeignKeysAdd(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        log.info("Maintain FK {}", entity.getTableName());
        FrKeys dbKeys = FrKeys.load(driver.getForeignKeys(connection, entity.getTableName()));
        AtkEnFields enKeys = entity.getEnFields().getForeignKeys();

        // add missing
        AtkEnFields missing = enKeys.removeWhen(k -> dbKeys.containsField(k));
        missing.stream().forEach(k -> logAndExecute(connection, driver.addForeignKey(k)));

    }

    @SneakyThrows
    public static void maintainIndexes(Connection connection, AbstractDriver driver, AbstractAtkEntity entity) {
        log.info("Maintain Index {}", entity.getTableName());
        Indexes indexes = driver.getIndexes(connection, entity.getTableName());

        FrKeys dbKeys = FrKeys.load(driver.getForeignKeys(connection, entity.getTableName()));

        indexes.removeIf(i -> dbKeys.getFkColNames().containsIgnoreCase(i.getColumns().toString(",")));

        // add missing
        List<AtkEnIndex> missing = entity.getIndexes()
                .stream()
                .filter(i -> !indexes.getByName(i.getName()).isPresent())
                .collect(Collectors.toList());

        for (AtkEnIndex i : missing) {
            logAndExecute(connection, driver.getCreateIndex(entity, i));
        }

        Indexes redundant = indexes.stream()
                .filter(i -> !entity.getIndexes().getByName(i.getINDEX_NAME()).isPresent())
                .collect(Collectors.toCollection(Indexes::new));
        for (Index index : redundant) {
            logAndExecute(connection, driver.getDropIndex(entity, index));
        }

        Indexes mismatch = indexes.stream()
                .filter(i -> entity.getIndexes().getByName(i.getINDEX_NAME()).isPresent()
                        && !i.equals(entity.getIndexes().getByName(i.getINDEX_NAME()).get()))
                .collect(Collectors.toCollection(Indexes::new));

        for (Index index : mismatch) {
            logAndExecute(connection, driver.getDropIndex(entity, index));
            logAndExecute(connection, driver.getCreateIndex(entity
                    , entity.getIndexes().getByName(index.getINDEX_NAME()).get()));
        }

    }


}
