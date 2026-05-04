package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.SyncTaskLog;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;

@Service
public class TaskLogService {
    private final JdbcClient jdbcClient;
    private final AlertLogService alertLogService;

    public TaskLogService(DataSource dataSource, AlertLogService alertLogService) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.alertLogService = alertLogService;
    }

    public void info(String taskId, String content) {
        append(taskId, "INFO", content);
    }

    public void error(String taskId, String content) {
        append(taskId, "ERROR", content);
        alertLogService.error("task", taskId, "任务异常", content);
    }

    public void append(String taskId, String level, String content) {
        jdbcClient.sql("INSERT INTO sync_task_log (task_id, log_level, log_content) VALUES (:taskId, :level, :content)")
                .param("taskId", taskId)
                .param("level", level)
                .param("content", content)
                .update();
    }

    public List<SyncTaskLog> listByTask(String taskId) {
        return listByTask(taskId, 200);
    }

    public List<SyncTaskLog> listByTask(String taskId, int limit) {
        return jdbcClient.sql("""
                SELECT id, task_id, log_level, log_content, create_time
                FROM sync_task_log WHERE task_id = :taskId ORDER BY id DESC LIMIT :limit
                """)
                .param("taskId", taskId)
                .param("limit", boundedLimit(limit))
                .query(SyncTaskLog.class)
                .list();
    }

    public long count() {
        return jdbcClient.sql("SELECT COUNT(*) FROM sync_task_log").query(Long.class).single();
    }

    private int boundedLimit(int limit) {
        return Math.max(1, Math.min(limit, 5000));
    }
}
