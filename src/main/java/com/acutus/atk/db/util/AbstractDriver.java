package com.acutus.atk.db.util;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.util.StringUtils;
import lombok.SneakyThrows;

import javax.persistence.Column;
import javax.persistence.Lob;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class AbstractDriver {

    public AbstractDriver init(Connection connection) {
        return this;
    }

    @SneakyThrows
    public List<String> getPrimaryKeys(Connection connection, String tableName) {
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(connection.getCatalog()
                , connection.getSchema(), tableName)) {
            List<List> keys = SQLHelper.query(rs, new Class[]{String.class}, new String[]{"COLUMN_NAME"});
            return keys.stream().map(s -> (String) s.get(0)).collect(Collectors.toList());
        }
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

    public String getDropColumnColumnDefinition(AtkEnField field) {
        return String.format("alter table %s drop column %s", field.getEntity().getTableName()
                , field.getColName());
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
        } else {
            throw new UnsupportedOperationException("Unsupported type " + field.getType());
        }
    }

    public abstract <T> T getLastInsertValue(Connection connection, Class<T> clazz);

}
