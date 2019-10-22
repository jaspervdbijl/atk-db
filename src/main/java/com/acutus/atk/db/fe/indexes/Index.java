package com.acutus.atk.db.fe.indexes;


import com.acutus.atk.db.AtkEnIndex;
import com.acutus.atk.util.Strings;
import lombok.Data;

@Data
public class Index {

    private String INDEX_NAME;
    private String TABLE_CAT;
    private String TABLE_SCHEM;
    private String TABLE_NAME;
    private String COLUMN_NAME;
    private Boolean NON_UNIQUE;

    private transient Strings columns = new Strings();


    public boolean equals(AtkEnIndex idx) {
        return idx.getName().equalsIgnoreCase(INDEX_NAME)
                && idx.getFields().getColNames().equalsIgnoreOrderIgnoreCase(columns)
                && idx.isUnique() == !NON_UNIQUE;
    }
}
