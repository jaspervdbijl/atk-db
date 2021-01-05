package com.acutus.atk.db.entity;

import com.acutus.atk.db.processor.AtkEntity;
import com.acutus.atk.entity.processor.Alternate;
import com.acutus.atk.util.Strings;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Lob;

@AtkEntity(daoClass = {PersonV1.class,PersonV2.class})
public class Person {

    @Id
    @Column(length = 50)
    private String id;

    @Column(length = 50)
    private String name;

    @Column(length = 5000)
    @Lob
    private String surname;


}
