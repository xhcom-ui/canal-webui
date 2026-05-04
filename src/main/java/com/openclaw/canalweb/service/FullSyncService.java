package com.openclaw.canalweb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openclaw.canalweb.domain.DatasourceConfig;
import com.openclaw.canalweb.dto.TaskDetail;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class FullSyncService {
    private final JdbcClient jdbcClient;
    private final SyncTaskService syncTaskService;
    private final DatasourceService datasourceService;
    private final TaskLogService taskLogService;
    private final OperationLogService operationLogService;
    private final ObjectMapper objectMapper;

    public FullSyncService(DataSource dataSource, SyncTaskService syncTaskService, DatasourceService datasourceService,
                           TaskLogService taskLogService, OperationLogService operationLogService,
                           ObjectMapper objectMapper) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.syncTaskService = syncTaskService;
        this.datasourceService = datasourceService;
        this.taskLogService = taskLogService;
        this.operationLogService = operationLogService;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Path execute(String taskId, String reason) {
        TaskDetail detail = syncTaskService.detail(taskId);
        if (!SyncTaskService.requiresFullSync(detail.task().syncMode())) {
            throw new IllegalArgumentException("当前同步模式不是 FULL 或 FULL_INCREMENTAL，不能执行全量同步");
        }
        DatasourceConfig datasource = datasourceService.findByKey(detail.task().dataSourceKey())
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + detail.task().dataSourceKey()));
        String sql = detail.task().syncSql();
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("全量同步需要配置 syncSql");
        }
        LocalDateTime start = LocalDateTime.now();
        Path output = outputFile(taskId, start);
        long count = 0;
        long failures = 0;
        try {
            Files.createDirectories(output.getParent());
            String url = "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                    .formatted(datasource.host(), datasource.port(), datasource.dbName());
            try (var connection = DriverManager.getConnection(url, datasource.username(), datasource.password());
                 var statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                 BufferedWriter writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                statement.setFetchSize(Integer.MIN_VALUE);
                try (ResultSet rs = statement.executeQuery(sql)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    int columns = meta.getColumnCount();
                    while (rs.next()) {
                        writer.write(rowJson(rs, meta, columns));
                        writer.newLine();
                        count++;
                    }
                }
            }
        } catch (Exception ex) {
            failures = 1;
            markMetrics(taskId, count, failures, start, output);
            taskLogService.error(taskId, "全量同步失败: " + ex.getMessage());
            operationLogService.record("task", "full-sync-error", taskId, ex.getMessage());
            throw new IllegalStateException("全量同步失败: " + ex.getMessage(), ex);
        }
        markMetrics(taskId, count, failures, start, output);
        String message = "全量同步完成，原因: %s，记录数: %d，文件: %s".formatted(reason, count, output.toAbsolutePath());
        taskLogService.info(taskId, message);
        operationLogService.record("task", "full-sync", taskId, message);
        return output;
    }

    public List<String> dueFullTaskIds(LocalDateTime since) {
        return jdbcClient.sql("""
                SELECT id FROM sync_task
                WHERE task_status = 'RUNNING'
                  AND sync_mode IN ('FULL', 'FULL_INCREMENTAL')
                  AND cron_expression IS NOT NULL
                  AND cron_expression <> ''
                  AND (last_schedule_time IS NULL OR last_schedule_time < :since)
                ORDER BY update_time ASC
                """)
                .param("since", since)
                .query(String.class)
                .list();
    }

    public void markScheduled(String taskId) {
        jdbcClient.sql("UPDATE sync_task SET last_schedule_time = :time, update_time = :time WHERE id = :id")
                .param("id", taskId)
                .param("time", LocalDateTime.now())
                .update();
    }

    public Map<String, Object> preview(String taskId, int limit) {
        TaskDetail detail = syncTaskService.detail(taskId);
        int rowLimit = Math.max(1, Math.min(limit, 100));
        Map<String, Object> result = new LinkedHashMap<>();
        String file = detail.task().fullSyncFile();
        result.put("file", file);
        result.put("exists", false);
        result.put("size", 0);
        result.put("lineCount", 0);
        result.put("limit", rowLimit);
        result.put("rows", List.of());
        result.put("rawLines", List.of());
        if (file == null || file.isBlank()) {
            result.put("message", "尚未执行全量同步，暂无结果文件");
            return result;
        }

        Path path = Path.of(file).toAbsolutePath().normalize();
        if (!isAllowedFullSyncPath(path)) {
            throw new IllegalArgumentException("全量结果文件不在允许的运行目录内");
        }
        if (!Files.isRegularFile(path)) {
            result.put("message", "全量结果文件不存在或不可读取");
            return result;
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        List<String> rawLines = new ArrayList<>();
        long lineCount = 0;
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineCount++;
                if (rows.size() < rowLimit && rawLines.size() < rowLimit) {
                    try {
                        Map<String, Object> row = objectMapper.readValue(line, new TypeReference<>() {
                        });
                        rows.add(row);
                    } catch (Exception ex) {
                        rawLines.add(line);
                    }
                }
            }
            result.put("exists", true);
            result.put("size", Files.size(path));
            result.put("lineCount", lineCount);
            result.put("rows", rows);
            result.put("rawLines", rawLines);
            result.put("message", rawLines.isEmpty() ? "读取成功" : "部分行不是标准 JSON，已按原文展示");
            return result;
        } catch (Exception ex) {
            throw new IllegalStateException("读取全量结果失败: " + ex.getMessage(), ex);
        }
    }

    private void markMetrics(String taskId, long count, long failures, LocalDateTime start, Path output) {
        long delay = Duration.between(start, LocalDateTime.now()).toMillis();
        jdbcClient.sql("""
                UPDATE sync_task
                SET total_count = total_count + :count,
                    fail_count = fail_count + :failures,
                    last_delay_ms = :delay,
                    full_sync_file = :file,
                    update_time = :time
                WHERE id = :id
                """)
                .param("id", taskId)
                .param("count", count)
                .param("failures", failures)
                .param("delay", delay)
                .param("file", output.toAbsolutePath().toString())
                .param("time", LocalDateTime.now())
                .update();
    }

    private static Path outputFile(String taskId, LocalDateTime start) {
        String fileName = "%s-%s.jsonl".formatted(taskId, start.toString().replace(":", "").replace(".", ""));
        return Path.of("canal-runtime", "generated", "full-sync", fileName);
    }

    private static boolean isAllowedFullSyncPath(Path path) {
        Path baseDir = Path.of("canal-runtime", "generated", "full-sync").toAbsolutePath().normalize();
        if (path.startsWith(baseDir)) {
            return true;
        }
        String normalized = path.toString().replace('\\', '/');
        return normalized.contains("/canal-runtime/generated/full-sync/");
    }

    private static String rowJson(ResultSet rs, ResultSetMetaData meta, int columns) throws Exception {
        StringBuilder builder = new StringBuilder("{");
        for (int index = 1; index <= columns; index++) {
            if (index > 1) {
                builder.append(',');
            }
            builder.append('"').append(escape(meta.getColumnLabel(index))).append('"').append(':');
            Object value = rs.getObject(index);
            if (value == null) {
                builder.append("null");
            } else if (value instanceof Number || value instanceof Boolean) {
                builder.append(value);
            } else {
                builder.append('"').append(escape(String.valueOf(value))).append('"');
            }
        }
        return builder.append('}').toString();
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
