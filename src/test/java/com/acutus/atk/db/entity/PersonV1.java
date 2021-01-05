package com.acutus.atk.db.entity;

import com.acutus.atk.entity.processor.Alternate;
import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Lob;

@Data
public class PersonV1 {

    private String id;
    private String name;
    @Alternate("surname")
    private String surnameV1;


}
