package com.acutus.atk.db.driver;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.collection.Tuple1;
import lombok.SneakyThrows;

import javax.persistence.Column;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.acutus.atk.db.annotations.ForeignKey.Action.NoAction;
import static com.acutus.atk.db.sql.SQLHelper.query;
import static com.acutus.atk.db.sql.SQLHelper.runAndReturn;

public class MysqlDriver extends AbstractDriver {

    public <T> T getLastInsertValue(Connection connection, Class<T> clazz) {
        List<Tuple1<T>> id = query(connection, clazz, "select LAST_INSERT_ID()");
        Assert.isTrue(!id.isEmpty(), "No inserted id found");
        return id.get(0).getFirst();
    }

    @Override
    public String limit(String sql, int limit, int offset) {
        return String.format("%s limit %d,%d", sql, offset, limit);
    }

    @Override
    public String getTableCharset(Connection connection, String tableName) throws SQLException {
        return SQLHelper.queryOne(connection,String.class,"SELECT CCSA.character_set_name FROM information_schema.`TABLES` T" +
                ", information_schema.`COLLATION_CHARACTER_SET_APPLICABILITY` CCSA "
                + "WHERE CCSA.collation_name = T.table_collation "
                + "AND T.table_schema = ? and T.TABLE_NAME = ?",connection.getCatalog(),tableName).get().getFirst();
    }

    @Override
    public String setCharset(String tableName, String charset) {
        return "ALTER TABLE "+tableName+" CONVERT TO CHARACTER SET " + charset;
    }

    @Override
    public String replace(String column, String text, String replace) {
        return String.format("replace(%s,%s,%s)d", column, text, replace);
    }

    @SneakyThrows
    public String addForeignKey(AtkEnField field) {
        ForeignKey key = field.getField().getAnnotation(ForeignKey.class);
        AbstractAtkEntity fEntity = (AbstractAtkEntity) key.table().newInstance();

        return String.format("alter table %s add foreign key (%s) references %s (%s) %s ",
                field.getEntity().getTableName(),
                field.getColName(),
                fEntity.getTableName(),
                key.field() != null && !key.field().isEmpty() ? key.field() :
                        fEntity.getEnFields().getIds().getColNames().toString(","),
                (key.onDeleteAction() != NoAction ? ("on delete " + getCascadeRule(key.onDeleteAction())) : " ")
                        + " " + getDeferRule(key.deferrable()));
    }

    @Override
    public String getFieldTypeForClob(Optional<Column> column) {
        return "longtext";
    }

    @SneakyThrows
    public String getColMetadataDefault(ResultSet rs) {
        String def = super.getColMetadataDefault(rs);
        return "0000-00-00 00:00:00".equals(def) ? "" : def;
    }

    @Override
    public Optional<Long> getSlaveLag(DataSource dataSource) {
        return runAndReturn(dataSource, c -> {
            try (Statement smt = c.createStatement()) {
                try (ResultSet rs = smt.executeQuery("show slave status")) {
                    List<List> values = SQLHelper.query(rs,new Class[]{String.class,String.class,Long.class},
                            new String[]{"Slave_IO_Running", "Slave_SQL_Running", "Seconds_Behind_Master"});
                    return values.isEmpty()
                            ? Optional.empty()
                            : "Yes".equals(values.get(0).get(0)) && "Yes".equals(values.get(0).get(1))
                            ? Optional.of((Long) values.get(0).get(2))
                            : Optional.empty();

                }
            }
        });
    }

    @Override
    public List<String> createSequence(String name, int start, int cache) {
        return Arrays.asList("create table if not exists seq_" + name + " (id INT NOT NULL)",
                "INSERT INTO seq_" + name + " VALUES (0))");
    }

    @Override
    public Integer nextSequence(Connection c, String name) {
        SQLHelper.executeUpdate(c,"UPDATE seq_"+name+" SET id=LAST_INSERT_ID(id+1)");
        return SQLHelper.queryOne(c,Integer.class,"SELECT LAST_INSERT_ID()").get().getFirst();
    }

    @Override
    public String getCreateSql(AbstractAtkEntity entity) {
        return super.getCreateSql(entity) + (!entity.charset().isEmpty() ? "  DEFAULT CHARACTER SET " + entity.charset() : "");
    }
}
