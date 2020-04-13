//package com.acutus.atk.db.fe;
//
//import ch.vorburger.exec.ManagedProcessException;
//import ch.vorburger.mariadb4j.DB;
//import com.acutus.atk.db.entity.PersonEntity;
//import com.acutus.atk.db.entity.VehicleEntity;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//
//public class FEHelperTest {
//
//    static DB db;
//    static Connection conn;
//
//    @BeforeAll
//    public static void setup() throws ManagedProcessException, SQLException {
//        db = DB.newEmbeddedDB(3306);
//        conn = DriverManager.getConnection("jdbc:mysql://localhost/test", "root", "");
//    }
//
//    @Test
//    void maintainTable() {
//        FEHelper.maintainDataDefinition(conn, PersonEntity.class, VehicleEntity.class);
//    }
//
//}
