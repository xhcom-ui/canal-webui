package com.openclaw.canalweb.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openclaw.canalweb.domain.DatasourceConfig;
import com.openclaw.canalweb.domain.TaskFieldMapping;
import com.openclaw.canalweb.domain.TaskTargetConfig;
import com.openclaw.canalweb.dto.TaskDetail;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class FullSyncService {
    private static final Pattern TEMPLATE_FIELD_PATTERN = Pattern.compile("\\{([A-Za-z0-9_]+)}");

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
        EsFullSyncWriter esWriter = null;
        try {
            Files.createDirectories(output.getParent());
            if ("ES".equalsIgnoreCase(detail.task().targetType())) {
                esWriter = new EsFullSyncWriter(detail);
            }
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
                        Map<String, Object> row = rowMap(rs, meta, columns);
                        writer.write(objectMapper.writeValueAsString(row));
                        writer.newLine();
                        if (esWriter != null) {
                            esWriter.add(row);
                        }
                        count++;
                    }
                }
            }
            if (esWriter != null) {
                esWriter.flush();
            }
        } catch (Exception ex) {
            failures = 1;
            markMetrics(taskId, count, failures, start, output);
            taskLogService.error(taskId, "全量同步失败: " + ex.getMessage());
            operationLogService.record("task", "full-sync-error", taskId, ex.getMessage());
            throw new IllegalStateException("全量同步失败: " + ex.getMessage(), ex);
        }
        markMetrics(taskId, count, failures, start, output);
        String targetMessage = esWriter == null ? "" : "，ES写入: %d".formatted(esWriter.written());
        String message = "全量同步完成，原因: %s，记录数: %d%s，文件: %s"
                .formatted(reason, count, targetMessage, output.toAbsolutePath());
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

    private static Map<String, Object> rowMap(ResultSet rs, ResultSetMetaData meta, int columns) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 1; index <= columns; index++) {
            row.put(meta.getColumnLabel(index), rs.getObject(index));
        }
        return row;
    }

    private final class EsFullSyncWriter {
        private final HttpClient httpClient = HttpClient.newHttpClient();
        private final String bulkUrl;
        private final String documentIdTemplate;
        private final String securityAuth;
        private final int commitBatch;
        private final List<TaskFieldMapping> mappings;
        private final List<String> lines = new ArrayList<>();
        private int pendingRows = 0;
        private long written = 0;

        private EsFullSyncWriter(TaskDetail detail) {
            Map<String, String> config = targetConfigMap(detail.targetConfig());
            String index = firstText(config, "indexName", "index");
            if (index.isBlank()) {
                throw new IllegalArgumentException("ES 全量写入需要配置 indexName");
            }
            this.bulkUrl = firstEsHost(config.get("hosts")) + "/" + index + "/_bulk";
            this.documentIdTemplate = valueOrDefault(firstText(config, "documentId", "idField"), "{id}");
            this.securityAuth = config.getOrDefault("securityAuth", "");
            this.commitBatch = intValue(config.get("commitBatch"), 3000);
            this.mappings = detail.fieldMappings().stream()
                    .filter(item -> item.enabled() == null || Boolean.TRUE.equals(item.enabled()))
                    .toList();
        }

        private void add(Map<String, Object> row) throws Exception {
            Map<String, Object> document = document(row);
            String id = documentId(row);
            Map<String, Object> indexMeta = id.isBlank()
                    ? Map.of("index", Map.of())
                    : Map.of("index", Map.of("_id", id));
            lines.add(objectMapper.writeValueAsString(indexMeta));
            lines.add(objectMapper.writeValueAsString(document));
            pendingRows++;
            if (pendingRows >= commitBatch) {
                flush();
            }
        }

        private void flush() throws Exception {
            if (lines.isEmpty()) {
                return;
            }
            String body = String.join("\n", lines) + "\n";
            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(bulkUrl))
                    .header("Content-Type", "application/x-ndjson")
                    .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
            if (securityAuth != null && !securityAuth.isBlank()) {
                builder.header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString(securityAuth.getBytes(StandardCharsets.UTF_8)));
            }
            HttpResponse<String> response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException("ES bulk 写入失败，HTTP " + response.statusCode() + ": " + response.body());
            }
            Map<String, Object> result = objectMapper.readValue(response.body(), new TypeReference<>() {
            });
            if (Boolean.TRUE.equals(result.get("errors"))) {
                throw new IllegalStateException("ES bulk 写入存在失败项: " + response.body());
            }
            written += pendingRows;
            pendingRows = 0;
            lines.clear();
        }

        private long written() {
            return written;
        }

        private Map<String, Object> document(Map<String, Object> row) {
            Map<String, Object> document = new LinkedHashMap<>();
            if (mappings.isEmpty()) {
                document.putAll(row);
                return document;
            }
            for (TaskFieldMapping mapping : mappings) {
                document.put(mapping.targetField(), row.get(mapping.sourceField()));
            }
            return document;
        }

        private String documentId(Map<String, Object> row) {
            String id = documentIdTemplate;
            Matcher matcher = TEMPLATE_FIELD_PATTERN.matcher(id);
            StringBuffer buffer = new StringBuffer();
            while (matcher.find()) {
                Object value = row.get(matcher.group(1));
                matcher.appendReplacement(buffer, Matcher.quoteReplacement(value == null ? "" : String.valueOf(value)));
            }
            matcher.appendTail(buffer);
            if (buffer.length() != id.length() || id.contains("{")) {
                return buffer.toString();
            }
            Object value = row.get(id);
            return value == null ? "" : String.valueOf(value);
        }
    }

    private static Map<String, String> targetConfigMap(List<TaskTargetConfig> configs) {
        Map<String, String> result = new LinkedHashMap<>();
        for (TaskTargetConfig config : configs) {
            result.put(config.configKey(), config.configValue());
        }
        return result;
    }

    private static String firstText(Map<String, String> config, String... keys) {
        for (String key : keys) {
            String value = config.get(key);
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }

    private static String firstEsHost(String hosts) {
        String first = valueOrDefault(hosts, "127.0.0.1:9200").split(",")[0].trim();
        return first.contains("://") ? first : "http://" + first;
    }

    private static String valueOrDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static int intValue(String value, int defaultValue) {
        try {
            return value == null || value.isBlank() ? defaultValue : Math.max(1, Integer.parseInt(value.trim()));
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }
}
