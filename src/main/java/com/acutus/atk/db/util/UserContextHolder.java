package com.acutus.atk.db.util;

public class UserContextHolder {

    private static ThreadLocal<String> USER_LOCAL = new ThreadLocal<>();

    public static void setUsername(String username) {
        USER_LOCAL.set(username);
    }

    public static String getUsername() {
        return USER_LOCAL.get();
    }
}
