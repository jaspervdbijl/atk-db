package com.acutus.atk.db;

import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.driver.AbstractDriver;
import com.acutus.atk.db.driver.DriverFactory;
import com.acutus.atk.entity.AtkField;
import com.acutus.atk.entity.AtkFieldList;
import lombok.SneakyThrows;

import javax.persistence.Column;
import javax.persistence.Enumerated;
import javax.persistence.Id;
import javax.persistence.Lob;
import java.lang.reflect.Field;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.util.Optional;

import static com.acutus.atk.db.sql.SQLHelper.mapFromRs;
import static com.acutus.atk.db.util.AtkEnUtil.unwrapEnumerated;
import static com.acutus.atk.util.StringUtils.isEmpty;
import static javax.persistence.EnumType.STRING;

public class AtkEnField<T, R extends AbstractAtkEntity> extends AtkField<T, R> {

    public AtkEnField(Field field, R entity) {
        super(field, entity);
    }

    @Override
    public R getEntity() {
        return super.getEntity();
    }

    public String getColName() {
        Column column = getField().getAnnotation(Column.class);
        return column != null && !isEmpty(column.name()) ? column.name() : getField().getName();
    }

    public String getTableAndColName() {
        return String.format("%s.%s", getEntity().getTableName(), getColName());
    }


    // TODO - expand to include complex ids
    public boolean isId() {
        return getField().getAnnotation(Id.class) != null;
    }

    public boolean isForeignKey() {
        return getField().getAnnotation(ForeignKey.class) != null;
    }

    public Optional<Column> getColumn() {
        return Optional.ofNullable(getField().getAnnotation(Column.class));
    }

    public int getColLength() {
        return getColumn().isPresent() ? getColumn().get().length() : 255;
    }

    public boolean isNullable() {
        return (getColumn().isPresent() ? getColumn().get().nullable() : true) && !isId();
    }

    public Class getColumnType(AbstractDriver driver) {
        if (byte[].class.equals(getType()) || Byte[].class.equals(getType())) {
            return Blob.class;
        }
        if (String.class.equals(getType())
                && (getField().getAnnotation(Lob.class) != null || getColLength() >= driver.getMaxVarcharLength())) {
            return Clob.class;
        }
        Enumerated enumerated = getField().getAnnotation(Enumerated.class);
        if (enumerated != null) {
            return enumerated.value().equals(STRING) ? String.class : Integer.class;
        }
        return getType();
    }

    @SneakyThrows
    public void setFromRs(AbstractDriver driver, ResultSet rs) {
        set((T) unwrapEnumerated(getField(), mapFromRs(rs, getColumnType(driver), getTableAndColName())));
    }
}
