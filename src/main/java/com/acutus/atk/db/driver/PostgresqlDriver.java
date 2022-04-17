package com.acutus.atk.db.driver;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFields;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.sql.SQLHelper;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.collection.Tuple1;
import lombok.SneakyThrows;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.acutus.atk.db.annotations.ForeignKey.Action.NoAction;
import static com.acutus.atk.db.sql.SQLHelper.query;
import static com.acutus.atk.db.sql.SQLHelper.runAndReturn;

public class PostgresqlDriver extends AbstractDriver {

    public <T> T getLastInsertValue(Connection connection, Class<T> clazz) {
        List<Tuple1<T>> id = query(connection, clazz, "select LAST_INSERT_ID()");
        Assert.isTrue(!id.isEmpty(), "No inserted id found");
        return id.get(0).getFirst();
    }

    @Override
    public String limit(String sql, int limit) {
        return String.format("%s limit %d", sql, limit);
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
    public String getAlterColumnDefinition(AtkEnField field) {
        String colDef = getColumnDefinition(field);
        // tmp hack
        if (colDef.endsWith(" null")) {
            colDef = colDef.substring(0,colDef.length() - " null".length());
        }
        if (colDef.endsWith(" not null")) {
            colDef = colDef.substring(0,colDef.length() - " not null".length());
        }
        return String.format("alter table %s alter column %s type %s", field.getEntity().getTableName()
                , field.getColName(), colDef);
    }


    @Override
    public String getFieldTypeForClob(Optional<Column> column) {
        return "longtext";
    }

    @Override
    public List<String> createSequence(String name, int start, int cache) {
        return Arrays.asList("create sequence IF NOT EXISTS  " + name + " start " + start + " cache " + cache);
    }

    @SneakyThrows
    public String addAutoIncrementPK(AtkEnFields ids) {

        Optional<AtkEnField> atkEnFieldOptional =
                ids.stream().filter(field ->
                        field.getField().isAnnotationPresent(GeneratedValue.class) &&
                                field.getType().isAssignableFrom(Integer.class) || field.getType().isAssignableFrom(Long.class)).findFirst();

        if (atkEnFieldOptional.isPresent()) {
            return String.format("alter table %s alter %s type SERIAL", ids.get(0).getEntity().getTableName(),
                    atkEnFieldOptional.get().getColName());
        } else {
            return null;

        }
    }

    @Override
    public Integer nextSequence(Connection c, String name) {
        return SQLHelper.queryOne(c,Integer.class,"SELECT nextval('"+name+"');").get().getFirst();
    }

    @Override
    public Integer nextSequence(DataSource dataSource, String name) {
        return runAndReturn(dataSource,c -> nextSequence(c,name));
    }

}
