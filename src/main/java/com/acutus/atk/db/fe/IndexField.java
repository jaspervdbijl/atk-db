package com.acutus.atk.db.fe;


import lombok.Data;

/**
 * Created by jaspervdbijl on 2016/12/19.
 */
@Data
public class IndexField extends Object {

    private String TABLE_CAT;
    private String TABLE_SCHEM;
    private String TABLE_NAME;
    private String COLUMN_NAME;
    private String KEY_SEQ;
    private String PK_NAME;
}
