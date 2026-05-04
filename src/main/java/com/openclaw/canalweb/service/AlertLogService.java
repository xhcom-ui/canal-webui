package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.AlertLog;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AlertLogService {
    private final JdbcClient jdbcClient;

    public AlertLogService(DataSource dataSource) {
        this.jdbcClient = JdbcClient.create(dataSource);
    }

    public void error(String moduleName, String refId, String title, String content) {
        jdbcClient.sql("""
                INSERT INTO alert_log (alert_level, module_name, ref_id, alert_title, alert_content)
                VALUES ('ERROR', :moduleName, :refId, :title, :content)
                """)
                .param("moduleName", moduleName)
                .param("refId", refId == null ? "" : refId)
                .param("title", title)
                .param("content", content == null ? "" : content)
                .update();
    }

    public List<AlertLog> list(Integer acknowledged, String moduleName, String refId) {
        return list(acknowledged, moduleName, refId, 300);
    }

    public List<AlertLog> list(Integer acknowledged, String moduleName, String refId, int limit) {
        StringBuilder sql = new StringBuilder("""
                SELECT id, alert_level, module_name, ref_id, alert_title, alert_content,
                       acknowledged, acknowledge_time, create_time
                FROM alert_log WHERE 1 = 1
                """);
        if (acknowledged != null) {
            sql.append(" AND acknowledged = :acknowledged");
        }
        if (moduleName != null && !moduleName.isBlank()) {
            sql.append(" AND module_name = :moduleName");
        }
        if (refId != null && !refId.isBlank()) {
            sql.append(" AND ref_id = :refId");
        }
        sql.append(" ORDER BY id DESC LIMIT :limit");
        var spec = jdbcClient.sql(sql.toString());
        if (acknowledged != null) {
            spec = spec.param("acknowledged", acknowledged);
        }
        if (moduleName != null && !moduleName.isBlank()) {
            spec = spec.param("moduleName", moduleName);
        }
        if (refId != null && !refId.isBlank()) {
            spec = spec.param("refId", refId);
        }
        spec = spec.param("limit", boundedLimit(limit));
        return spec.query(AlertLog.class).list();
    }

    public List<AlertLog> recentUnacknowledged(int limit) {
        return jdbcClient.sql("""
                SELECT id, alert_level, module_name, ref_id, alert_title, alert_content,
                       acknowledged, acknowledge_time, create_time
                FROM alert_log
                WHERE acknowledged = 0
                ORDER BY id DESC
                LIMIT :limit
                """)
                .param("limit", Math.max(1, Math.min(limit, 50)))
                .query(AlertLog.class)
                .list();
    }

    public long unacknowledgedCount() {
        return jdbcClient.sql("SELECT COUNT(*) FROM alert_log WHERE acknowledged = 0")
                .query(Long.class)
                .single();
    }

    public void acknowledge(Long id) {
        jdbcClient.sql("""
                UPDATE alert_log
                SET acknowledged = 1, acknowledge_time = :time
                WHERE id = :id
                """)
                .param("id", id)
                .param("time", LocalDateTime.now())
                .update();
    }

    public int acknowledgeBatch(List<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return 0;
        }
        int updated = 0;
        for (Long id : ids) {
            updated += jdbcClient.sql("""
                    UPDATE alert_log
                    SET acknowledged = 1, acknowledge_time = :time
                    WHERE id = :id AND acknowledged = 0
                    """)
                    .param("id", id)
                    .param("time", LocalDateTime.now())
                    .update();
        }
        return updated;
    }

    public int acknowledgeByFilter(Integer acknowledged, String moduleName, String refId) {
        StringBuilder sql = new StringBuilder("""
                UPDATE alert_log
                SET acknowledged = 1, acknowledge_time = :time
                WHERE acknowledged = 0
                """);
        if (acknowledged != null) {
            sql.append(" AND acknowledged = :acknowledged");
        }
        if (moduleName != null && !moduleName.isBlank()) {
            sql.append(" AND module_name = :moduleName");
        }
        if (refId != null && !refId.isBlank()) {
            sql.append(" AND ref_id = :refId");
        }
        var spec = jdbcClient.sql(sql.toString()).param("time", LocalDateTime.now());
        if (acknowledged != null) {
            spec = spec.param("acknowledged", acknowledged);
        }
        if (moduleName != null && !moduleName.isBlank()) {
            spec = spec.param("moduleName", moduleName);
        }
        if (refId != null && !refId.isBlank()) {
            spec = spec.param("refId", refId);
        }
        return spec.update();
    }

    public Map<String, Object> stats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("unacknowledged", unacknowledgedCount());
        stats.put("acknowledged", jdbcClient.sql("SELECT COUNT(*) FROM alert_log WHERE acknowledged = 1").query(Long.class).single());
        stats.put("total", jdbcClient.sql("SELECT COUNT(*) FROM alert_log").query(Long.class).single());
        stats.put("byModule", jdbcClient.sql("""
                SELECT module_name AS moduleName, COUNT(*) AS count
                FROM alert_log
                WHERE acknowledged = 0
                GROUP BY module_name
                ORDER BY count DESC
                """)
                .query((rs, rowNum) -> Map.of(
                        "moduleName", rs.getString("moduleName"),
                        "count", rs.getLong("count")
                ))
                .list());
        return stats;
    }

    private int boundedLimit(int limit) {
        return Math.max(1, Math.min(limit, 5000));
    }
}
