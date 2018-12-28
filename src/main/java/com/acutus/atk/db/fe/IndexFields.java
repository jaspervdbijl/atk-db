package com.acutus.atk.db.fe;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by jaspervdbijl on 2016/12/19.
 */
public class IndexFields extends ArrayList<IndexField> {

    public List<String> getPrimaryKeyNames() {
        return stream().filter(i -> i.getPK_NAME() != null).map(i -> i.getCOLUMN_NAME()).collect(Collectors.toList());
    }


}
