package com.acutus.atk.db.processor;

public class ProcessorHelper {

    public static final String EXECUTE_METHOD = "public int _EXECUTE_(javax.sql.DataSource dataSource,Object ... params) {\n" +
            "        return runAndReturn(dataSource,c -> {\n" +
            "            try (PreparedStatement ps = c.prepareStatement(\"_SQL_\")) {\n" +
            "                if (params != null) {\n" +
            "                    for (int i = 0; i < params.length; i++) {\n" +
            "                        ps.setObject(i + 1, params[i]);\n" +
            "                    }\n" +
            "                }\n" +
            "                return ps.executeUpdate();\n" +
            "            }\n" +
            "        });\n" +
            "    }";

    /*

    public int execute(javax.sql.DataSource dataSource,Object ... params) {
        return runAndReturn(dataSource,c -> {
            try (PreparedStatement ps = c.prepareStatement("_SQL_")) {
                if (params != null) {
                    for (int i = 0; i < params.length; i++) {
                        ps.setObject(i + 1, params[i]);
                    }
                }
                return ps.executeUpdate();
            }
        });
    }

*/
    public static String getExecuteMethod(String methodName, String sql) {
        return EXECUTE_METHOD.replace("_EXECUTE_", methodName).replace("_SQL_", sql);
    }

    public static void main(String[] args) {
        System.out.println(getExecuteMethod("deleteOld", "delete from"));
    }
}
