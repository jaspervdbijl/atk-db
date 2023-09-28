package com.acutus.atk.db.sql;

import com.acutus.atk.db.AtkEnField;
import com.acutus.atk.db.AtkEnFields;
import static com.acutus.atk.db.util.AtkEnUtil.wrapForPreparedStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@NoArgsConstructor
public class Filter {

  public enum Type {
    CUSTOM, AND, OR, NOT
  }

  private AtkEnFields fields;
  private Filter s1, s2;
  private Type type;

  @Getter
  private String customSql;
  @Getter
  private Object customParams[];

  public Filter(Type type, AtkEnField[] fields) {
    this(type, new AtkEnFields(fields));
  }

  public Filter(Type type, AtkEnFields fields) {
    this.type = type;
    this.fields = new AtkEnFields(fields);
  }

  public Filter(String sql, Object... params) {
    this.type = Type.CUSTOM;
    this.customSql = sql;
    this.customParams = params;
  }

  public Filter(Type type, Filter s1, Filter s2) {
    this.type = type;
    this.s1 = s1;
    this.s2 = s2;
  }

  public boolean isCustom() {
    return type == Type.CUSTOM;
  }

  public boolean isEmpty() {
    return type == null;
  }

  public static Filter and(AtkEnField... fields) {
    return new Filter(Type.AND, fields);
  }

  public static Filter and(Filter s1, Filter s2) {
    return new Filter(Type.AND, s1, s2);
  }

  public static Filter or(AtkEnField... fields) {
    return new Filter(Type.OR, fields);
  }

  public static Filter or(Filter s1, Filter s2) {
    return new Filter(Type.OR, s1, s2);
  }

  public static Filter not(AtkEnField... fields) {
    return new Filter(Type.NOT, fields);
  }

  public static Filter not(Filter s1, Filter s2) {
    return new Filter(Type.NOT, s1, s2);
  }

  public String getSql() {
    if (fields != null && !fields.isEmpty()) {
      return fields.stream().map(f -> f.getTableAndColName() + " "
              + (f.get() == null ? " is null " : " = ? "))
          .reduce((a, b) -> a + " and " + b).get();
    } else if (s1 != null) {
      return String.format("((%s) %s (%s))", s1.getSql(), type.name().toLowerCase(), s2.getSql());
    } else {
      // find all
      return "1 = 1";
    }
  }

  @SneakyThrows
  private void set(PreparedStatement ps, AtomicInteger index) {
    if (fields != null) {
      for (AtkEnField f : fields) {
        if (f.get() != null) {
          ps.setObject(index.getAndIncrement(), wrapForPreparedStatement(f));
        }
      }
    } else if (s1 != null) {
      s1.set(ps, index);
      s2.set(ps, index);
    }
  }

  @SneakyThrows
  public PreparedStatement prepare(PreparedStatement ps) {
    if (type == Type.CUSTOM) {
      int index = 0;
      for (int i = 0; customParams != null && i < customParams.length; i++) {
        if (customParams[i] instanceof List) {
          for (Object item : ((List<?>) customParams[i])) {
            ps.setObject(i + 1 + index, item);
            index = index + 1;
          }
        } else if (customParams[i] instanceof Object[]) {
          for (Object item : ((Object[]) customParams[i])) {
            ps.setObject(i + 1 + index, item);
            index = index + 1;
          }

        } else {
          ps.setObject(i + 1 + index, customParams[i]);
        }
      }
    } else {
      set(ps, new AtomicInteger(1));
    }
    return ps;
  }

}
