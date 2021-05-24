package com.acutus.atk.db.util;

import lombok.SneakyThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class UserContextHolder {

    private static ThreadLocal<String> USER_LOCAL = new ThreadLocal<>();

    public static void setUsername(String username) {
        USER_LOCAL.set(username);
    }

    public static String getUsername() {
        return USER_LOCAL.get();
    }

    @SneakyThrows
    public static void main(String[] args) {
        System.out.println(Arrays.stream(Files.readString(Path.of("/Users/jaspervdbijl/Downloads/tables.sh")).split("\n")).reduce((s1, s2) -> s1+" " + s2).get());
    }


}
