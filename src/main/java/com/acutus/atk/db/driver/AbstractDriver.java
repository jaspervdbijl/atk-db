package com.acutus.atk.db.driver;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFields;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.fe.indexes.Indexes;
import com.acutus.atk.db.fe.keys.FrKey;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.StringUtils;
import com.acutus.atk.util.Strings;
import lombok.SneakyThrows;

import javax.persistence.Column;
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
        return Indexes.load(connection.getMetaData().getIndexInfo(null, null, tableName, false, false));
    }

    @SneakyThrows
    public boolean doesTableExist(Connection connection, String tableName) {
        try (Statement smt = connection.createStatement()) {
            try {
                try (ResultSet rs = smt.executeQuery(String.format("select * from %s", tableName))) {
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

    public String getColumnDefinition(AtkEnField field) {
        Optional<Column> column = field.getColumn();
        if (column.isPresent() && !StringUtils.isEmpty(column.get().columnDefinition())) {
            return column.get().columnDefinition();
        }
        return String.format("%s %s", getFieldType(field), field.isNullable() ? "null" : "not null");
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

    /**
     * default database varchar length
     *
     * @return
     */
    public int getMaxVarcharLength() {
        return 4000;
    }

    private boolean isClob(AtkEnField field) {
        Lob lob = field.getField().getAnnotation(Lob.class);
        return lob != null || field.getColLength() >= getMaxVarcharLength();
    }

    public String getFieldType(AtkEnField field) {
        Optional<Column> column = field.getColumn();
        if (String.class.equals(field.getType()) && !isClob(field)) {
            return String.format("varchar(%d)", column.isPresent() ? column.get().length() : 255);
        } else if (String.class.equals(field.getType()) && isClob(field)) {
            return "longtext";
        } else if (Integer.class.equals(field.getType())) {
            return "int";
        } else if (Long.class.equals(field.getType()) || BigInteger.class.equals(field.getType())) {
            return "long";
        } else if (BigDecimal.class.equals(field.getType()) || Double.class.equals(field.getType())) {
            return "double";
        } else if (Float.class.equals(field.getType())) {
            return "float";
        } else if (Short.class.equals(field.getType())) {
            return "shortint";
        } else if (Boolean.class.equals(field.getType())) {
            return "bool";
        } else if (byte[].class.equals(field.getType()) || Byte[].class.equals(field.getType())) {
            return "blob";
        } else if (Byte[].class.equals(field.getType())) {
            return String.format("varchar2(%d)", column.isPresent() ? column.get().length() : 255);
        } else if (Timestamp.class.equals(field.getType()) || LocalDateTime.class.equals(field.getType())) {
            return getFieldTypeForTimestamp(column);
        } else if (java.sql.Date.class.equals(field.getType()) || LocalDate.class.equals(field.getType())) {
            return getFieldTypeForDate(column);
        } else if (java.sql.Time.class.equals(field.getType()) || LocalTime.class.equals(field.getType())) {
            return getFieldTypeForTime(column);
        } else {
            throw new UnsupportedOperationException("Unsupported type " + field.getType());
        }
    }

    public String getFieldTypeForString(Optional<Column> column) {
        return String.format("varchar(%d)", column.isPresent() ? column.get().length() : 255);
    }

    public String getFieldTypeForClob(Optional<Column> column) {
        return "clob";
    }

    public String getFieldTypeForTimestamp(Optional<Column> column) {
        return "timestamp";
    }

    public String getFieldTypeForDate(Optional<Column> column) {
        return "date";
    }

    public String getFieldTypeForTime(Optional<Column> column) {
        return "time";
    }

    public abstract <T> T getLastInsertValue(Connection connection, Class<T> clazz);

}
