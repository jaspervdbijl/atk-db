package com.acutus.atk.db.fe.keys;

import com.acutus.atk.db.AbstractAtkEntity;
import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFieldList;
import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.fe.FEHelper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import static com.acutus.atk.db.constants.EnvProperties.DB_FE_STRICT;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FrKey implements FEHelper.PopulateStringsFromResultSet {

    private String PKTABLE_CAT;
    private String PKTABLE_SCHEM;
    private String PKTABLE_NAME;
    private String PKCOLUMN_NAME;
    private String FKTABLE_CAT;
    private String FKTABLE_SCHEM;
    private String FKTABLE_NAME;
    private String FKCOLUMN_NAME;
    private String KEY_SEQ;
    private String UPDATE_RULE;
    private Integer DELETE_RULE;
    private String FK_NAME;
    private String PK_NAME;
    private Integer DEFERRABILITY;

    public static String getFKeyConstraintName(AbstractAtkEntity fEntity, AtkEnField field, ForeignKey key) {
        return key.name().isEmpty()
                ? String.format("%s_%s_FK", fEntity.getTableName(), field.getColName())
                : key.name();
    }


    @SneakyThrows
    public boolean equals(AtkEnField field) {
        ForeignKey fKey = field.getField().getAnnotation(ForeignKey.class);
        return fKey != null ?
                DB_FE_STRICT.get()
                        ? equalsStrict(fKey.table(), field, fKey)
                        : equals(fKey.table()
                        , field, fKey) : false;
    }

    @SneakyThrows
    public boolean equals(Class<? extends AbstractAtkEntity> eClass, AtkEnField field, ForeignKey key) {
        AbstractAtkEntity fEntity = eClass.newInstance();
        return (getPKCOLUMN_NAME().equalsIgnoreCase(field.getColName())) &&
                getPKTABLE_NAME().equalsIgnoreCase(field.getEntity().getTableName()) &&
                getFKCOLUMN_NAME().equalsIgnoreCase(fEntity.getEnFields().getSingleId().getColName()) ||
                (getFKCOLUMN_NAME().equalsIgnoreCase(field.getColName()) &&
                        getPKTABLE_NAME().equalsIgnoreCase(fEntity.getTableName()) &&
                        getPKCOLUMN_NAME().equalsIgnoreCase(fEntity.getEnFields().getSingleId().getColName()));
    }

    public boolean equalsStrict(Class<? extends AbstractAtkEntity> eClass, AtkEnField field, ForeignKey key) {
        return equals(eClass, field, key) && key.onDeleteAction().getCode() == getDELETE_RULE() &&
                getDEFERRABILITY() == key.deferrable().getCode();
    }

    public boolean isPresentIn(AtkEnFieldList fKeys) {
        return fKeys.stream().filter(f -> equals(f)).findAny().isPresent();
    }

}
