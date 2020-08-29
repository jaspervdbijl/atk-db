package com.acutus.atk.db.driver;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.collection.One;
import lombok.SneakyThrows;

import javax.persistence.Column;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;

import static com.acutus.atk.db.annotations.ForeignKey.Action.NoAction;
import static com.acutus.atk.db.sql.SQLHelper.query;

public class MysqlDriver extends AbstractDriver {

    public <T> T getLastInsertValue(Connection connection, Class<T> clazz) {
        List<One<T>> id = query(connection, clazz, "select LAST_INSERT_ID()");
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
    public String getFieldTypeForClob(Optional<Column> column) {
        return "longtext";
    }

}
