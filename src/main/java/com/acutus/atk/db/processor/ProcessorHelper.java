package com.acutus.atk.db.processor;

import javax.lang.model.type.TypeMirror;
import javax.persistence.FetchType;

import static com.acutus.atk.util.StringUtils.isEmpty;
import static com.acutus.atk.util.StringUtils.isNotEmpty;

public class ProcessorHelper {

    public static final String EXECUTE_METHOD = "   public static int _EXECUTE_(javax.sql.DataSource dataSource,Object ... params) {\n" +
            "       return runAndReturn(dataSource,c -> {\n" +
            "           return _EXECUTE_(c,params);" +
            "        });\n" +
            "    }";

    public static final String EXECUTE_METHOD_CONNECTION = "    public static int _EXECUTE_(java.sql.Connection c,Object ... params) {\n" +
            "       try (PreparedStatement ps = c.prepareStatement(_SQL_)) {\n" +
            "           if (params != null) {\n" +
            "               for (int i = 0; i < params.length; i++) {\n" +
            "                   ps.setObject(i + 1, params[i]);\n" +
            "               }\n" +
            "           }\n" +
            "           return ps.executeUpdate();\n" +
            "       } catch (java.sql.SQLException e) {\n" +
            "           throw new RuntimeException(e);\n" +
            "       }\n" +
            "   }";

    public static String QUERY_METHOD = "\tpublic static java.util.Optional<_TYPE_> _METHOD_NAME_(javax.sql.DataSource dataSource, Object ... params) {\n" +
            "        return new _TYPE_().query().get(dataSource,\"_SQL_\",params);\n" +
            "    }\n\n" +
            "\tpublic static java.util.Optional<_TYPE_> _METHOD_NAME_(java.sql.Connection connection, Object ... params) {\n" +
            "        return new _TYPE_().query().get(connection,\"_SQL_\",params);\n" +
            "    }";

    public static String QUERY_PRIM = "\tpublic static java.util.Optional<One<_TYPE_>> _METHOD_NAME_(javax.sql.DataSource dataSource, Object ... params) {\n" +
            "        return queryOne(dataSource,_TYPE_.class,\"_SQL_\",params);\n" +
            "    }\n\n" +
            "\tpublic static java.util.Optional<One<_TYPE_>> _METHOD_NAME_(java.sql.Connection connection, Object ... params) {\n" +
            "        return queryOne(connection,_TYPE_.class,\"_SQL_\",params);\n" +
            "    }";

    public static String QUERY_ALL_METHOD = "\tpublic static AtkEntities<_TYPE_> _METHOD_NAME_(javax.sql.DataSource dataSource, Object ... params) {\n" +
            "        return new _TYPE_().query()._GET_ALL_METHOD_(dataSource,\"_SQL_\",params);\n" +
            "    }\n\n"+
            "\tpublic static AtkEntities<_TYPE_> _METHOD_NAME_(java.sql.Connection connection, Object ... params) {\n" +
            "        return new _TYPE_().query()._GET_ALL_METHOD_(connection,\"_SQL_\",params);\n" +
            "    }";

    public static String QUERY_ALL_PRIM = "\tpublic static java.util.List<One<_TYPE_>> _METHOD_NAME_(javax.sql.DataSource dataSource, Object ... params) {\n" +
            "        return query(dataSource,_TYPE_.class,\"_SQL_\",params);\n" +
            "    }\n\n" +
            "\tpublic static java.util.List<One<_TYPE_>> _METHOD_NAME_(java.sql.Connection connection, Object ... params) {\n" +
            "        return query(connection,_TYPE_.class,\"_SQL_\",params);\n" +
            "    }\n\n";

    private static boolean isClassPrimitive(String className) {
        return (className.startsWith("java.lang.") || className.startsWith("java.math."));
    }

    public static String getQueryMethod(String extName, TypeMirror type ,String methodName, String sql) {
        extName = isClassPrimitive(type.toString())? "" : extName;
        return ((isClassPrimitive(type.toString()))? QUERY_PRIM : QUERY_METHOD)
                .replace("_TYPE_", type.toString()+extName)
                .replace("_METHOD_NAME_", methodName).replace("_SQL_", sql);
    }

    public static String getQueryAllMethod(FetchType fetchType, String extName, TypeMirror type, String methodName, String sql) {
        extName = isClassPrimitive(type.toString())? "" : extName;
        String className = type.toString();
        className = className.substring(className.indexOf("<")+1);
        className = className.substring(0,className.indexOf(">"));
        String classNameAndExt = className + extName;
        return (isClassPrimitive(type.toString())? QUERY_ALL_PRIM : QUERY_ALL_METHOD)
                .replace("_TYPE_", classNameAndExt)
                .replace("_METHOD_NAME_", methodName).replace("_SQL_", sql)
                .replace("_GET_ALL_METHOD_",FetchType.EAGER.equals(fetchType) ? "getAll" : "getAll");
    }

    public static String getExecuteMethod(String methodName, Execute execute) {
        String sql = isNotEmpty(execute.resource())
                ? String.format("getCachedResource(\"%s\")",execute.resource())
                : String.format("\"%s\"",execute.value());
        return EXECUTE_METHOD_CONNECTION.replace("_EXECUTE_", methodName).replace("_SQL_", sql) + "\n\n" +
                EXECUTE_METHOD.replace("_EXECUTE_", methodName).replace("_SQL_", sql) + "\n";
    }

}
