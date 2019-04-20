package com.acutus.atk.db.entity;

import com.acutus.atk.db.annotations.ForeignKey;
import com.acutus.atk.db.processor.AtkEntity;

import javax.persistence.Column;
import javax.persistence.Id;

@AtkEntity
public class Vehicle {

    @Id
    @Column(length = 50)
    private String id;

    @Column(length = 50)
    private String regNo;

    @ForeignKey(table = Person.class, field = "id", name = "personIdFk")
    private String personId;

    public void tester() {
        System.out.println("Hello");
    }

    public static void main(String[] args) {
        System.out.println("@For (".replaceAll("\\s\\(", "("));
    }
}
