package com.acutus.atk.db.fe.indexes;


import com.acutus.atk.db.fe.FEHelper;
import lombok.Data;

@Data
public class Index implements FEHelper.PopulateStringsFromResultSet {

    private String TABLE_CAT;
    private String TABLE_SCHEM;
    private String TABLE_NAME;
    private String COLUMN_NAME;
    private String KEY_SEQ;
    private String PK_NAME;
}
