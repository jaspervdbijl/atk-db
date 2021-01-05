package com.acutus.atk.db.entity;

import com.acutus.atk.entity.processor.Alternate;
import lombok.Data;

@Data
public class PersonV2 {

    private String id;
    private String name;
    @Alternate("surname")
    private String surnameV2;


}
