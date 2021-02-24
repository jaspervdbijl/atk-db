package com.acutus.atk.db.sql;

import com.acutus.atk.db.constants.SQLConstants;
import com.acutus.atk.util.Assert;
import com.acutus.atk.util.Strings;
import com.acutus.atk.util.call.CallOne;
import com.acutus.atk.util.call.CallOneRet;
import com.acutus.atk.util.collection.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.acutus.atk.db.constants.SQLConstants.RS_FUNC_INT_MAP;
import static com.acutus.atk.db.constants.SQLConstants.RS_FUNC_INT_STR;
import static com.acutus.atk.util.AtkUtil.handle;
import static com.acutus.atk.util.StringUtils.isNotEmpty;

@Slf4j
public class SQLHelper {

    static {
        SQLConstants.init();
    }

    @SneakyThrows
    public static void run(DataSource dataSource, CallOne<Connection> call) {
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            try {
                call.call(con);
                con.commit();
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
                con.rollback();
                throw ex instanceof RuntimeException
                        ? (RuntimeException) ex :
                        new RuntimeException(ex.getMessage(), ex);
            }
        }
    }

    @SneakyThrows
    public static <T> T runAndReturn(DataSource dataSource, CallOneRet<Connection, T> call) {
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            T value = call.call(con);
            con.commit();
            return value;
        }
    }

    @SneakyThrows
    public static <T> T mapFromRs(ResultSet rs, Class<T> type, int index) {
        Assert.isTrue(RS_FUNC_INT_MAP.containsKey(type), "Type not supported %s", type);
        return (T) RS_FUNC_INT_MAP.get(type).invoke(rs, index);
    }

    @SneakyThrows
    private static <T> T unwrap(Class<T> type, Object value) {
        try {
            if (value == null) return (T) value;
            if (type.equals(value.getClass())) return (T) value;
            if (Clob.class.equals(type)) return (T) value;
            if (Blob.class.equals(type)) return (T) value;
            if (Character.class.equals(type))
                return (T) (isNotEmpty(value.toString()) ? value.toString().charAt(0) : null);
            if (LocalDateTime.class.equals(type) && value.getClass().equals(Timestamp.class))
                return (T) ((Timestamp) value).toLocalDateTime();
            if (LocalDate.class.equals(type) && value.getClass().equals(Timestamp.class))
                return (T) ((Timestamp) value).toLocalDateTime().toLocalDate();
            if (LocalTime.class.equals(type) && value.getClass().equals(Time.class))
                return (T) ((Time) value).toLocalTime();
            throw new UnsupportedOperationException(
                    String.format("Could not unwrap types from %s to %s", type.getName(), value.getClass().getName()));
        } catch (Exception ex) {
            log.warn("Error unwrapping type {} value {}", type, value);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
    }

    @SneakyThrows
    public static <T> T mapFromRs(ResultSet rs, Class<T> type, String colName) {
        Assert.isTrue(RS_FUNC_INT_STR.containsKey(type), "Type not supported %s", type);

        try {
            return (T) unwrap(type, RS_FUNC_INT_STR.get(type).invoke(rs, colName));
        } catch (Exception sqlException) {
            return (T) unwrap(type, RS_FUNC_INT_STR.get(type).invoke(rs, colName.substring(colName.indexOf(".") + 1)));
        }
    }

    /**
     * @param rs
     * @param type     the expected class type
     * @param colNames optional. if not specified will retrieve from index 0
     * @return
     */
    @SneakyThrows
    public static List<List> query(ResultSet rs, Class type[], String colNames[]) {
        List values = new ArrayList();
        while (rs.next()) {
            values.add(IntStream.range(0, type.length)
                    .mapToObj(i -> colNames == null
                            ? mapFromRs(rs, type[i], i + 1)
                            : mapFromRs(rs, type[i], colNames[i]))
                    .collect(Collectors.toList()));
        }
        return values;
    }

    public static List<List> query(ResultSet rs, Class type[]) {
        return query(rs, type, null);
    }

    @SneakyThrows
    public static PreparedStatement prepare(Connection connection, String query, Object... params) {
        PreparedStatement ps = connection.prepareStatement(query);
        if (params != null) {
            IntStream.range(0, params.length).forEach(i -> handle(() -> ps.setObject(i + 1, params[i])));
        }
        return ps;
    }

    @SneakyThrows
    public static List<List> query(Connection connection, Class type[], String query, Object... params) {
        try (PreparedStatement ps = prepare(connection, query, params)) {
            try (ResultSet rs = ps.executeQuery()) {
                long s1 =0 ,s2 = 0;
                try {
                    s1 = System.currentTimeMillis();
                    return query(rs, type);

                } finally {
                    s2 = System.currentTimeMillis();
                    if (s2 - s1 > 1000) {
                        System.out.println("Query SLow " + ((s2-s1) / 1000) + " "+ query);
                    }
                }
            }
        }
    }

    public static List query(DataSource dataSource, Class type[], String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, type, query, params));
    }

    @SneakyThrows
    private static Collectable getInstance(Class<? extends Collectable> collectionClass, List values) {
        return collectionClass.getConstructor().newInstance().initFromList(values);
    }

    private static List<? extends Collectable> query(Connection connection, Class<? extends Collectable> colClass
            , Class types[], String query, Object... params) {
        return query(connection, types, query, params)
                .stream()
                .map(a -> getInstance(colClass, a))
                .collect(Collectors.toList());
    }

    public static <A> List<One<A>> query(Connection connection, Class<A> type, String query, Object... params) {
        return (List<One<A>>) query(connection, One.class, new Class[]{type}, query, params);
    }

    public static <A> List<A> reMap(List<One<A>> map) {
        return map.stream().map(a -> a.getFirst()).collect(Collectors.toList());
    }

    public static <A> List<One<A>> query(DataSource dataSource, Class<A> type, String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, type, query, params));
    }

    public static <A> Optional<One<A>> queryOne(Connection connection, Class<A> type, String query, Object... params) {
        List<One<A>> list = query(connection, type, query, params);
        return Optional.ofNullable(!list.isEmpty() ? list.get(0) : null);
    }

    public static <A> Optional<One<A>> queryOne(DataSource dataSource, Class<A> type, String query, Object... params) {
        return runAndReturn(dataSource, connection -> queryOne(connection, type, query, params));
    }


    public static <A, B> List<Two<A, B>> query(Connection connection, Class<A> t1, Class<B> t2, String query, Object... params) {
        return (List<Two<A, B>>) query(connection, Two.class, new Class[]{t1, t2}, query, params);
    }

    public static <A, B> List<Two<A, B>> query(DataSource dataSource, Class<A> t1, Class<B> t2, String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, t1, t2, query, params));
    }

    public static <A, B, C> List<Three<A, B, C>> query(Connection connection, Class<A> t1, Class<B> t2
            , Class<C> t3, String query, Object... params) {
        return (List<Three<A, B, C>>) query(connection, Three.class, new Class[]{t1, t2, t3}, query, params);
    }

    public static <A, B, C> List<Three<A, B, C>> query(DataSource dataSource, Class<A> t1, Class<B> t2
            , Class<C> t3, String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, t1, t2, t3, query, params));
    }

    public static <A, B, C, D> List<Four<A, B, C, D>> query(Connection connection, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, String query, Object... params) {
        return (List<Four<A, B, C, D>>) query(connection, Four.class, new Class[]{t1, t2, t3, t4}, query, params);
    }

    public static <A, B, C, D> List<Four<A, B, C, D>> query(DataSource dataSource, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, t1, t2, t3, t4, query, params));
    }

    public static <A, B, C, D, E> List<Five<A, B, C, D, E>> query(Connection connection, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, Class<E> t5, String query, Object... params) {
        return (List<Five<A, B, C, D, E>>) query(connection, Five.class, new Class[]{t1, t2, t3, t4, t5}, query, params);
    }

    public static <A, B, C, D, E> List<Five<A, B, C, D, E>> query(DataSource dataSource, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, Class<E> t5, String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, t1, t2, t3, t4, t5, query, params));
    }

    public static <A, B, C, D, E, F> List<Six<A, B, C, D, E, F>> query(Connection connection, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, Class<E> t5, Class<F> t6, String query, Object... params) {
        return (List<Six<A, B, C, D, E, F>>) query(connection, Six.class, new Class[]{t1, t2, t3, t4, t5, t6}, query, params);
    }

    public static <A, B, C, D, E, F> List<Six<A, B, C, D, E, F>> query(DataSource dataSource, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, Class<E> t5, Class<F> t6, String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, t1, t2, t3, t4, t5, t6, query, params));
    }

    public static <A, B, C, D, E, F, G> List<Seven<A, B, C, D, E, F, G>> query(Connection connection, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, Class<E> t5, Class<F> t6, Class<G> t7,String query, Object... params) {
        return (List<Seven<A, B, C, D, E, F, G>>) query(connection, Seven.class, new Class[]{t1, t2, t3, t4, t5, t6, t7}, query, params);
    }

    public static <A, B, C, D, E, F,G> List<Seven<A, B, C, D, E, F,G>> query(DataSource dataSource, Class<A> t1, Class<B> t2
            , Class<C> t3, Class<D> t4, Class<E> t5, Class<F> t6, Class<G> t7,String query, Object... params) {
        return runAndReturn(dataSource, connection -> query(connection, t1, t2, t3, t4, t5, t6,t7, query, params));
    }

    @SneakyThrows
    public static int executeUpdate(Connection connection, String sql, Object... params) {
        try (PreparedStatement ps = prepare(connection, sql, params)) {
            return ps.executeUpdate();
        }
    }

    public static void executeUpdate(DataSource dataSource, String sql, Object... params) {
        run(dataSource, c -> executeUpdate(c, sql, params));
    }


    @SneakyThrows
    public static void execute(Connection connection, String sql) {
        try (Statement smt = connection.createStatement()) {
            smt.execute(sql);
        }
    }

    public static void execute(DataSource dataSource, String sql) {
        run(dataSource, c -> execute(c, sql));
    }

    public static Strings intoWords(String sql) {
        return Strings.asList(sql.replaceAll("\\p{Cntrl}", " ").replace(",", " , ").split("\\s+"));
    }

    public static String alias(Strings tNames, String sql, char letter) {
        sql = sql.replaceAll("\\p{Cntrl}", " ").replace("   ", " ").replace("  ", " ");
        String head = sql.substring(0, sql.indexOf(" from "));
        sql = sql.substring(sql.indexOf(" from "));

        for (int i = 0; i < tNames.size(); i++) {
            sql = sql.replace(tNames.get(i) + "_", ((char) (letter + i)) + "_");
            sql = sql.replace(tNames.get(i) + ".", ((char) (letter + i)) + ".");
            sql = sql.replace(tNames.get(i), tNames.get(i) + " " + ((char) (letter + i)));
            sql = sql.replace(((char) (letter + i)) + "_", tNames.get(i) + "_");
        }
        return head + sql;
    }

    public static void main(String[] args) {
        System.out.println(alias(Strings.asList("vendor_payment", "invoice", "booking"),
                "select * from vendor_payment,invoice,booking where vendor_payment.invoice_id = invoice.id and invoice.id = booking.invoice_id and vendor_payment.status = ? and invoice.user_id = ? order by vendor_payment.schedule_date desc", 'a'));
    }

}
