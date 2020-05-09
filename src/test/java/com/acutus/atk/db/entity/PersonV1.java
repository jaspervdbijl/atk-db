package com.acutus.atk.db.entity;

import lombok.Data;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Lob;

@Data
public class PersonV1 {

    private String id;
    private String name;
    private String surname;


}
