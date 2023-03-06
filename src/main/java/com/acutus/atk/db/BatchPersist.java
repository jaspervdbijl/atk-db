package com.acutus.atk.db;

import com.acutus.atk.util.Assert;
import com.acutus.atk.util.collection.Tuple2;
import com.acutus.atk.util.collection.Tuple4;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static com.acutus.atk.db.Persist.PERSIST_CALLBACK;
import static com.acutus.atk.db.sql.SQLHelper.*;
import static com.acutus.atk.db.util.AtkEnUtil.wrapForPreparedStatement;
import static com.acutus.atk.util.AtkUtil.handle;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class BatchPersist<T extends AbstractAtkEntity> {

    private AtkEntities<T> values;

    @SneakyThrows
    public int[] insert(Connection c) {
        if (!values.isEmpty()) {
            T entity = values.get(0);
            Tuple4<AtkEnFields, AtkEnFields, Boolean, String> prepared = entity.persist().prepareInsert();
            try (PreparedStatement ps = c.prepareStatement(prepared.getFourth())) {
                values.stream().forEach(v -> v.persist().batchInsert(ps));
                ps.clearParameters();
                return ps.executeBatch();
            }
        }
        return new int[0];
    }

    public int[] insert(DataSource dataSource) {
        return runAndReturn(dataSource, c -> insert(c));
    }

    @SneakyThrows
    public int[] update(Connection connection) {
        if (!values.isEmpty()) {
            // use the first entry to prepare the statement. assumption is that the update fields will remain static
            Tuple2<AtkEnFields,AtkEnFields> uFieldAndValue = values.get(0).persist()
                    .getUpdateFieldsAndValues(values.get(0),values.get(0).getEnFields().getSet(),true);
            try (PreparedStatement ps = values.get(0).persist()
                    .prepareBatchPreparedStatement(connection, uFieldAndValue)) {
                for (T entity : values) {
                    uFieldAndValue = entity.persist()
                            .getUpdateFieldsAndValues(entity,entity.getEnFields().getSet(),true);

                    prepare(ps, Persist.wrapForPreparedStatement(uFieldAndValue.getSecond()).toArray(new Object[]{}));
                    ps.addBatch();
                    PERSIST_CALLBACK.ifPresent(c -> handle(() -> c.call(connection, entity, false)));
                    entity.getEnFields().reset();
                }
                return ps.executeBatch();
            }
        }
        return new int[]{};
    }

    @SneakyThrows
    public int[] update(DataSource dataSource) {
        return runAndReturn(dataSource, c -> update(c));
    }
}
