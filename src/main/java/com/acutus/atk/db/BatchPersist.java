package com.acutus.atk.db;

import com.acutus.atk.util.collection.Four;
import com.google.auto.service.AutoService;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import javax.persistence.MapKeyColumn;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;

import static com.acutus.atk.db.sql.SQLHelper.runAndReturn;

@Slf4j
@AllArgsConstructor@NoArgsConstructor
public class BatchPersist<T extends AbstractAtkEntity> {

    private AtkEntities<T> values;

    @SneakyThrows
    public int[] insert(Connection c) {
        if (!values.isEmpty()) {
            T entity = values.get(0);
            Four<AtkEnFields, AtkEnFields, Boolean, String> prepared = entity.persist().prepareInsert();
            try (PreparedStatement ps = c.prepareStatement(prepared.getFourth())) {
                values.stream().forEach(v -> v.persist().batchInsert(ps));
                ps.clearParameters();
                return ps.executeBatch();
            }
        }
        return new int[0];
    }

    public int[] insert(DataSource dataSource) {
        return runAndReturn(dataSource,c -> insert(c));
    }
}
