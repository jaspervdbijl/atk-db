package com.acutus.atk.db.entity;

import com.acutus.atk.db.processor.AtkEntity;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Lob;

@AtkEntity
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
