package com.acutus.atk.db.driver;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFields;
import com.acutus.atk.db.AtkEnIndex;
import com.acutus.atk.db.annotations.Default;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.fe.indexes.Index;
import com.acutus.atk.db.fe.indexes.Indexes;
import com.acutus.atk.db.fe.keys.FrKey;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.StringUtils;
import com.acutus.atk.util.Strings;
import lombok.SneakyThrows;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.Lob;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.acutus.atk.db.annotations.ForeignKey.Action.NoAction;

public abstract class AbstractDriver {

    public AbstractDriver init(Connection connection) {
        return this;
    }

    @SneakyThrows
    public ResultSet getForeignKeys(Connection connection, String tableName) {
        DatabaseMetaData dm = connection.getMetaData();
        return dm.getImportedKeys(connection.getCatalog(), connection.getSchema(), tableName);
    }

    @SneakyThrows
    public Strings getPrimaryKeys(Connection connection, String tableName) {
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(connection.getCatalog()
                , connection.getSchema(), tableName)) {
            List<List> keys = SQLHelper.query(rs, new Class[]{String.class}, new String[]{"COLUMN_NAME"});
            return keys.stream().map(s -> (String) s.get(0)).collect(Collectors.toCollection(Strings::new));
        }
    }

    @SneakyThrows
    public Indexes getIndexes(Connection connection, String tableName) {
        return removePrimaryIndexes(
                Indexes.load(connection.getMetaData().getIndexInfo(connection.getCatalog()
                        , connection.getSchema(), tableName, false, false)));
    }

    public Indexes removePrimaryIndexes(Indexes indices) {
        return indices.stream().filter(i -> !i.getINDEX_NAME().equalsIgnoreCase("primary"))
                .collect(Collectors.toCollection(Indexes::new));
    }


    @SneakyThrows
    public boolean doesTableExist(Connection connection, String tableName) {
        try (Statement smt = connection.createStatement()) {
            try {
                try (ResultSet rs = smt.executeQuery(limit(String.format("select * from %s", tableName),1))) {
                    return true;
                }
            } catch (SQLException sqle) {
                return false;
            }
        }
    }

    @SneakyThrows
    public String getCreateSql(AbstractAtkEntity entity) {
        return String.format("create table %s (%s)"
                , entity.getTableName()
                , entity.getEnFields().stream()
                        .map(f -> String.format("%s %s", f.getColName(), getColumnDefinition(f)))
                        .reduce((s1, s2) -> s1 + "," + s2).get());
    }

    @SneakyThrows
    public String addAutoIncrementPK(AtkEnFields ids) {

        Optional<AtkEnField> atkEnFieldOptional =
                ids.stream().filter(field ->
                        field.getField().isAnnotationPresent(GeneratedValue.class) &&
                                field.getType().isAssignableFrom(Integer.class) || field.getType().isAssignableFrom(Long.class)).findFirst();

        if (atkEnFieldOptional.isPresent()) {
            return String.format("alter table %s modify %s int auto_increment;", ids.get(0).getEntity().getTableName(),
                    atkEnFieldOptional.get().getColName());
        } else {
            return null;

        }
    }

    public String getColumnDefinition(AtkEnField field) {
        Optional<Column> column = field.getColumn();
        if (column.isPresent() && !StringUtils.isEmpty(column.get().columnDefinition())) {
            return column.get().columnDefinition().toLowerCase().indexOf("null") > 0 ? column.get().columnDefinition() :
                    String.format("%s %s", column.get().columnDefinition(), field.isNullable() ? "null" : "not null");
        }
        return String.format("%s %s", getFieldType(field), field.isNullable() ? "null" : "not null");
    }

    public String getColumnDefinitionDefault(AtkEnField field) {
        String defaultFieldValue = getColumnDefinition(field);

        if (StringUtils.isEmpty(defaultFieldValue) || defaultFieldValue.toLowerCase().indexOf("default") == -1) {
            return "";
        }

        String result =
                defaultFieldValue.substring(defaultFieldValue.toLowerCase().indexOf("default")).
                        replace("default", "").
                        replace("DEFAULT", "").
                        replaceAll("not null", "").
                        replaceAll("null", "").
                        replaceAll("'", "").trim();

        if (field.getField().getType().isAssignableFrom(Boolean.class)) {
            return Boolean.valueOf(result).toString();
        }

        return result;
    }

    public boolean shouldDropConstraintPriorToAlter() {
        return true;
    }

    public String getAlterColumnDefinition(AtkEnField field) {
        return String.format("alter table %s modify column %s %s", field.getEntity().getTableName()
                , field.getColName(), getColumnDefinition(field));
    }

    public String getAddColumnColumnDefinition(AtkEnField field) {
        return String.format("alter table %s add column %s %s", field.getEntity().getTableName()
                , field.getColName(), getColumnDefinition(field));
    }

    public String getDropColumnColumnDefinition(String table, String column) {
        return String.format("alter table %s drop column %s", table, column);
    }

    public String getAddPrimaryKeyDefinition(AtkEnFields ids) {
        Assert.isTrue(!ids.isEmpty(), "Expected non empty list");
        return String.format("alter table %s add primary key (%s)", ids.get(0).getEntity().getTableName()
                , ids.getColNames().toString(","));
    }

    @SneakyThrows
    public String addForeignKey(AtkEnField field) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public String dropForeignKey(String tableName, FrKey frKey) {
        return String.format("alter table %s drop foreign key %s", tableName, frKey.getFK_NAME());
    }

    public String getCascadeRule(ForeignKey.Action action) {
        if (action.equals(ForeignKey.Action.Cascade)) {
            return "cascade";
        } else if (action.equals(NoAction)) {
            return "no action";
        } else if (action.equals(ForeignKey.Action.SetNull)) {
            return "set null";
        } else if (action.equals(ForeignKey.Action.SetDefault)) {
            return "set default";
        } else if (action.equals(ForeignKey.Action.Restrict)) {
            return "restrict";
        } else {
            throw new UnsupportedOperationException("Unknown cascade type " + action);
        }
    }

    public String getDeferRule(ForeignKey.Deferrability deferrable) {
        return "";
    }


    // Indexes

    public String getCreateIndex(AbstractAtkEntity entity, AtkEnIndex index) {
        return String.format("create %s index %s on %s (%s)", index.isUnique() ? "unique" : "", index.getName()
                , entity.getTableName(), index.getFields().getColNames().toString(","));
    }

    public String getDropIndex(AbstractAtkEntity entity, Index index) {
        return String.format("drop index %s on %s", index.getINDEX_NAME(), entity.getTableName());
    }

    /**
     * default database varchar length
     *
     * @return
     */
    public int getMaxVarcharLength() {
        return 4096;
    }

    private boolean isClob(AtkEnField field) {
        Lob lob = field.getField().getAnnotation(Lob.class);
        return lob != null || field.getColLength() >= getMaxVarcharLength();
    }

    public String getFieldType(AtkEnField field) {
        GeneratedValue generated = field.getField().getAnnotation(GeneratedValue.class);
        String unsigned = (generated  != null ? " unsigned" : "");
        Optional<Column> column = field.getColumn();
        Class type = field.getColumnType(this);
        if (String.class.equals(type)) {
            return String.format("varchar(%d)", column.isPresent() ? column.get().length() : 255);
        } else if (Clob.class.equals(type)) {
            return "longtext";
        } else if (Integer.class.equals(type)) {
            return "int" + unsigned;
        } else if (field.getField().isAnnotationPresent(ForeignKey.class) && (Long.class.equals(type) || BigInteger.class.equals(type))) {
            return "int unsigned";
        } else if (Long.class.equals(type) || BigInteger.class.equals(type)) {
            return "bigint" + unsigned;
        } else if (BigDecimal.class.equals(type) || Double.class.equals(type)) {
            return "double";
        } else if (Float.class.equals(type)) {
            return "float";
        } else if (Short.class.equals(type)) {
            return "shortint" + unsigned ;
        } else if (Boolean.class.equals(type)) {
            return "bool";
        } else if (Character.class.equals(type)) {
            return "char";
        } else if (Blob.class.equals(type) || Byte[].class.equals(type) || byte[].class.equals(type)) {
            return "longblob";
        } else if (Timestamp.class.equals(type) || LocalDateTime.class.equals(type)) {
            return getFieldTypeForTimestamp(column);
        } else if (java.sql.Date.class.equals(type) || LocalDate.class.equals(type)) {
            return getFieldTypeForDate(column);
        } else if (java.sql.Time.class.equals(type) || LocalTime.class.equals(type)) {
            return getFieldTypeForTime(column);
        } else {
            throw new UnsupportedOperationException("Unsupported type " + type);
        }
    }

    public String getFieldTypeForString(Optional<Column> column) {
        return String.format("varchar(%d)", column.isPresent() ? column.get().length() : 255);
    }

    public String getFieldTypeForClob(Optional<Column> column) {
        return "clob";
    }

    public String getFieldTypeForTimestamp(Optional<Column> column) {
        return "datetime";
    }

    public String getFieldTypeForDate(Optional<Column> column) {
        return "date";
    }

    public String getFieldTypeForTime(Optional<Column> column) {
        return "time";
    }

    public abstract <T> T getLastInsertValue(Connection connection, Class<T> clazz);

    public abstract String limit(String sql, int limit);

    @SneakyThrows
    public String getColMetadataDefault(ResultSet rs) {
        return StringUtils.defaultString(rs.getString("COLUMN_DEF"));
    }

}
