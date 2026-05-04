package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.OperationLog;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;

@Service
public class OperationLogService {
    private final JdbcClient jdbcClient;

    public OperationLogService(DataSource dataSource) {
        this.jdbcClient = JdbcClient.create(dataSource);
    }

    public void record(String moduleName, String actionName, String refId, String content) {
        jdbcClient.sql("""
                INSERT INTO operation_log (module_name, action_name, ref_id, log_content)
                VALUES (:module, :action, :refId, :content)
                """)
                .param("module", moduleName)
                .param("action", actionName)
                .param("refId", refId == null ? "" : refId)
                .param("content", content == null ? "" : content)
                .update();
    }

    public List<OperationLog> list(String moduleName, String refId) {
        return list(moduleName, refId, 300);
    }

    public List<OperationLog> list(String moduleName, String refId, int limit) {
        StringBuilder sql = new StringBuilder("""
                SELECT id, module_name, action_name, ref_id, log_content, create_time
                FROM operation_log WHERE 1 = 1
                """);
        if (moduleName != null && !moduleName.isBlank()) {
            sql.append(" AND module_name = :moduleName");
        }
        if (refId != null && !refId.isBlank()) {
            sql.append(" AND ref_id = :refId");
        }
        sql.append(" ORDER BY id DESC LIMIT :limit");
        var spec = jdbcClient.sql(sql.toString());
        if (moduleName != null && !moduleName.isBlank()) {
            spec = spec.param("moduleName", moduleName);
        }
        if (refId != null && !refId.isBlank()) {
            spec = spec.param("refId", refId);
        }
        spec = spec.param("limit", boundedLimit(limit));
        return spec.query(OperationLog.class).list();
    }

    public List<OperationLog> recent(int limit) {
        return jdbcClient.sql("""
                SELECT id, module_name, action_name, ref_id, log_content, create_time
                FROM operation_log
                ORDER BY id DESC
                LIMIT :limit
                """)
                .param("limit", Math.max(1, Math.min(limit, 50)))
                .query(OperationLog.class)
                .list();
    }

    private int boundedLimit(int limit) {
        return Math.max(1, Math.min(limit, 5000));
    }
}
