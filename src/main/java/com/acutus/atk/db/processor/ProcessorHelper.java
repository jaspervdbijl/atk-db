package com.acutus.atk.db.processor;

import com.acutus.atk.db.AbstractAtkEntity;

import javax.lang.model.type.TypeMirror;
import javax.sql.DataSource;
import java.util.Optional;

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

    public static String QUERY_METHOD = "public static java.util.Optional<_TYPE_> _METHOD_NAME_(javax.sql.DataSource dataSource, Object ... params) {\n" +
            "        return new _TYPE_().query().get(dataSource,\"_SQL_\",params);\n" +
            "    }";

    public static String QUERY_ALL_METHOD = "public static java.util.List<_TYPE_> _METHOD_NAME_(javax.sql.DataSource dataSource, Object ... params) {\n" +
            "        return new _TYPE_().query().getAll(dataSource,\"_SQL_\",params);\n" +
            "    }";

    public static String getQueryMethod(String extName, TypeMirror type ,String methodName, String sql) {
        return QUERY_METHOD.replace("_TYPE_", type.toString()+extName)
                .replace("_METHOD_NAME_", methodName).replace("_SQL_", sql);
    }

    public static String getQueryAllMethod(String extName, TypeMirror type, String methodName, String sql) {
        String className = type.toString();
        className = className.substring(className.indexOf("<")+1);
        className = className.substring(0,className.indexOf(">")) + extName;
        return QUERY_ALL_METHOD.replace("_TYPE_", className)
                .replace("_METHOD_NAME_", methodName).replace("_SQL_", sql);
    }

    public static String getExecuteMethod(String methodName, String sql) {
        return EXECUTE_METHOD.replace("_EXECUTE_", methodName).replace("_SQL_", sql);
    }

    public static void main(String[] args) {
        System.out.println(getExecuteMethod("deleteOld", "delete from"));
    }

}
