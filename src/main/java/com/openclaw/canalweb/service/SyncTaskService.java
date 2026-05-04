package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.SyncTask;
import com.openclaw.canalweb.domain.TaskFieldMapping;
import com.openclaw.canalweb.domain.TaskTargetConfig;
import com.openclaw.canalweb.dto.DashboardStats;
import com.openclaw.canalweb.dto.FieldMappingRequest;
import com.openclaw.canalweb.dto.TaskDetail;
import com.openclaw.canalweb.dto.TaskSaveRequest;
import com.openclaw.canalweb.dto.TargetTestRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class SyncTaskService {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern TEMPLATE_FIELD_PATTERN = Pattern.compile("\\{([A-Za-z0-9_]+)}");
    private static final String DEFAULT_CLI_PATH = "/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/opt/local/bin:/opt/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin";
    private record CommandResult(int exitCode, String output) {}

    private final JdbcClient jdbcClient;
    private final DatasourceService datasourceService;
    private final CanalAdapterService canalAdapterService;
    private final CanalRuntimeService canalRuntimeService;
    private final TaskLogService taskLogService;
    private final OperationLogService operationLogService;
    private final ConfigVersionService configVersionService;
    private final AlertLogService alertLogService;

    public SyncTaskService(DataSource dataSource, DatasourceService datasourceService,
                           CanalAdapterService canalAdapterService, CanalRuntimeService canalRuntimeService,
                           TaskLogService taskLogService, OperationLogService operationLogService,
                           ConfigVersionService configVersionService, AlertLogService alertLogService) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.datasourceService = datasourceService;
        this.canalAdapterService = canalAdapterService;
        this.canalRuntimeService = canalRuntimeService;
        this.taskLogService = taskLogService;
        this.operationLogService = operationLogService;
        this.configVersionService = configVersionService;
        this.alertLogService = alertLogService;
    }

    public List<SyncTask> list() {
        return list(null);
    }

    public List<SyncTask> list(String status) {
        String statusCondition = status == null || status.isBlank() ? "" : " WHERE task_status = :status";
        var spec = jdbcClient.sql("""
                SELECT id, task_name, description, data_source_key, sync_sql, target_type, sync_mode,
                       cron_expression, batch_size, task_status, total_count, fail_count, last_delay_ms,
                       last_start_time, last_stop_time, last_schedule_time, full_sync_file, create_time, update_time
                FROM sync_task
                """ + statusCondition + " ORDER BY update_time DESC, create_time DESC");
        if (status != null && !status.isBlank()) {
            spec = spec.param("status", status);
        }
        return spec.query(SyncTask.class).list();
    }

    public TaskDetail detail(String id) {
        SyncTask task = find(id);
        return new TaskDetail(task, fieldMappings(id), targetConfig(id));
    }

    public TaskSaveRequest exportRequest(String id) {
        TaskDetail detail = detail(id);
        Map<String, String> config = targetConfigMap(detail.targetConfig());
        List<FieldMappingRequest> mappings = detail.fieldMappings().stream()
                .map(item -> new FieldMappingRequest(item.sourceField(), item.targetField(), item.fieldType(),
                        item.primaryKey(), item.nullableField(), item.enabled(), item.defaultValue(),
                        item.transformExpr(), item.formatPattern(), item.fieldOptions()))
                .toList();
        return new TaskSaveRequest(detail.task().id(), detail.task().taskName(), detail.task().description(),
                detail.task().dataSourceKey(), detail.task().syncSql(), detail.task().targetType(),
                detail.task().syncMode(), detail.task().cronExpression(), detail.task().batchSize(),
                mappings, config);
    }

    public Map<String, Object> previewConfig(String id) {
        TaskDetail detail = detail(id);
        var datasource = datasourceService.findByKey(detail.task().dataSourceKey())
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + detail.task().dataSourceKey()));
        String taskSpec = canalAdapterService.buildCanalConfig(
                detail.task(), datasource, detail.fieldMappings(), detail.targetConfig());
        CanalAdapterService.AdapterRuntimeFiles runtimeFiles = canalAdapterService.buildRuntimeFiles(
                detail.task(), datasource, detail.fieldMappings(), detail.targetConfig());
        Map<String, Object> preview = new HashMap<>();
        preview.put("taskSpec", taskSpec);
        preview.put("destination", runtimeFiles.destination());
        preview.put("outerAdapter", runtimeFiles.outerAdapterBlock());
        preview.put("mappingDir", runtimeFiles.mappingDir());
        preview.put("mappingFile", runtimeFiles.mappingFile());
        preview.put("mappingContent", runtimeFiles.mappingContent());
        return preview;
    }

    public Map<String, Object> resourcePlan(String id) {
        TaskDetail detail = detail(id);
        Map<String, String> config = targetConfigMap(detail.targetConfig());
        String type = detail.task().targetType() == null ? "LOGGER" : detail.task().targetType().toUpperCase();
        Map<String, String> sourceTypes = sourceColumnTypes(detail, config);
        List<Map<String, Object>> resources = switch (type) {
            case "REDIS" -> redisResourcePlan(config, detail.fieldMappings());
            case "MYSQL", "RDB" -> rdbResourcePlan(config, detail.fieldMappings(), sourceTypes, "MySQL");
            case "PGSQL", "POSTGRESQL" -> rdbResourcePlan(config, detail.fieldMappings(), sourceTypes, "PostgreSQL");
            case "ES", "ELASTICSEARCH" -> esResourcePlan(config, detail.fieldMappings(), sourceTypes);
            case "KAFKA" -> kafkaResourcePlan(config);
            case "ROCKETMQ" -> rocketMqResourcePlan(config);
            case "RABBITMQ" -> rabbitMqResourcePlan(config);
            case "PULSAR", "PULSARMQ" -> pulsarResourcePlan(config);
            case "HBASE" -> hbaseResourcePlan(config);
            case "TABLESTORE" -> tablestoreResourcePlan(config);
            default -> List.of(planItem("说明", "INFO", "当前目标端不需要提前创建外部资源", ""));
        };
        List<Map<String, Object>> checks = targetResourceChecks(type, config, detail.fieldMappings());
        if (!resources.isEmpty() && !checks.isEmpty()) {
            resources.get(0).put("checks", checks);
        }
        long blockerCount = severityCount(checks, "BLOCKER");
        long warnCount = severityCount(checks, "WARN");
        long readyCount = checks.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", id);
        result.put("targetType", type);
        result.put("sourceTypes", sourceTypes);
        result.put("count", resources.size());
        result.put("checkCount", checks.size());
        result.put("readyCount", readyCount);
        result.put("blockerCount", blockerCount);
        result.put("warnCount", warnCount);
        result.put("ok", blockerCount == 0);
        result.put("checks", checks);
        result.put("resources", resources);
        return result;
    }

    public Map<String, Object> previewSql(TaskSaveRequest request, int limit) {
        if (request.dataSourceKey() == null || request.dataSourceKey().isBlank()) {
            throw new IllegalArgumentException("请选择数据源");
        }
        if (request.syncSql() == null || request.syncSql().isBlank()) {
            throw new IllegalArgumentException("请先填写同步 SQL");
        }
        String sql = request.syncSql().trim();
        String normalized = sql.toLowerCase(java.util.Locale.ROOT);
        if (!normalized.startsWith("select") && !normalized.startsWith("with")) {
            throw new IllegalArgumentException("SQL 预览仅允许 SELECT/WITH 查询");
        }
        var datasource = datasourceService.findByKey(request.dataSourceKey())
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + request.dataSourceKey()));
        int rowLimit = Math.max(1, Math.min(limit, 100));
        String previewSql = "SELECT * FROM (" + stripTrailingSemicolon(sql) + ") canal_web_preview LIMIT " + rowLimit;
        String url = "jdbc:mysql://%s:%d/%s?connectTimeout=5000&socketTimeout=10000&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                .formatted(datasource.host(), datasource.port(), datasource.dbName());
        try (var connection = DriverManager.getConnection(url, datasource.username(), datasource.password());
             var statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(previewSql)) {
            var meta = rs.getMetaData();
            List<Map<String, Object>> columns = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                Map<String, Object> column = new LinkedHashMap<>();
                column.put("name", meta.getColumnLabel(i));
                column.put("type", meta.getColumnTypeName(i));
                columns.add(column);
            }
            List<Map<String, Object>> rows = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                rows.add(row);
            }
            return Map.of("columns", columns, "rows", rows, "limit", rowLimit);
        } catch (Exception ex) {
            throw new IllegalArgumentException("SQL 预览失败: " + ex.getMessage(), ex);
        }
    }

    public Map<String, Object> validateMappings(List<FieldMappingRequest> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        List<FieldMappingRequest> normalized = normalizeMappings(mappings);
        addCheck(checks, "字段映射", !normalized.isEmpty(),
                "映射 " + normalized.size() + " 个字段", "至少配置一个字段映射");
        addCheck(checks, "启用字段", normalized.stream().anyMatch(item -> item.enabled() == null || Boolean.TRUE.equals(item.enabled())),
                "启用 " + normalized.stream().filter(item -> item.enabled() == null || Boolean.TRUE.equals(item.enabled())).count() + " 个字段",
                "至少启用一个字段");
        addCheck(checks, "主键字段", normalized.stream().anyMatch(item -> Boolean.TRUE.equals(item.primaryKey())),
                "主键 " + normalized.stream().filter(item -> Boolean.TRUE.equals(item.primaryKey())).count() + " 个",
                "建议至少配置一个主键或业务 key");
        addCheck(checks, "属性参数", true, "JSON 合法", "fieldOptions 已通过 JSON Object 校验");
        long okCount = checks.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        long blockerCount = severityCount(checks, "BLOCKER");
        return Map.of(
                "ok", blockerCount == 0,
                "okCount", okCount,
                "total", checks.size(),
                "blockerCount", blockerCount,
                "checks", checks,
                "mappings", normalized
        );
    }

    @Transactional
    public TaskDetail save(TaskSaveRequest request) {
        String id = request.id() == null || request.id().isBlank()
                ? UUID.randomUUID().toString().replace("-", "").substring(0, 24)
                : request.id();
        String targetType = normalizeTargetType(request.targetType());
        List<FieldMappingRequest> normalizedMappings = normalizeMappings(request.fieldMappings());
        boolean existed = exists(id);
        if (existed) {
            jdbcClient.sql("""
                    UPDATE sync_task
                    SET task_name = :taskName, description = :description, data_source_key = :dataSourceKey,
                        sync_sql = :syncSql, target_type = :targetType, sync_mode = :syncMode,
                        cron_expression = :cronExpression, batch_size = :batchSize, update_time = :updateTime
                    WHERE id = :id
                    """)
                    .params(taskParams(id, request))
                    .param("updateTime", LocalDateTime.now())
                    .update();
        } else {
            jdbcClient.sql("""
                    INSERT INTO sync_task
                    (id, task_name, description, data_source_key, sync_sql, target_type, sync_mode,
                     cron_expression, batch_size, task_status, update_time)
                    VALUES
                    (:id, :taskName, :description, :dataSourceKey, :syncSql, :targetType, :syncMode,
                     :cronExpression, :batchSize, 'STOPPED', :updateTime)
                    """)
                    .params(taskParams(id, request))
                    .param("updateTime", LocalDateTime.now())
                    .update();
        }

        replaceMappings(id, normalizedMappings);
        replaceTargetConfig(id, request.targetConfig());
        TaskSaveRequest snapshot = new TaskSaveRequest(id, request.taskName(), request.description(),
                request.dataSourceKey(), request.syncSql(), targetType, request.syncMode(),
                request.cronExpression(), request.batchSize(), normalizedMappings, request.targetConfig());
        configVersionService.snapshot("task", id, configVersionService.snapshotPayload("task", snapshot));
        operationLogService.record("task", existed ? "update" : "create", id, "任务配置已保存: " + request.taskName());
        taskLogService.info(id, "任务配置已保存");
        return detail(id);
    }

    @Transactional
    public TaskDetail cloneTask(String id) {
        TaskSaveRequest source = exportRequest(id);
        String clonedName = nextCloneName(source.taskName());
        TaskSaveRequest cloneRequest = new TaskSaveRequest(null, clonedName,
                source.description() == null || source.description().isBlank()
                        ? "复制自任务 " + id
                        : source.description() + " / 复制自任务 " + id,
                source.dataSourceKey(), source.syncSql(), source.targetType(), source.syncMode(),
                source.cronExpression(), source.batchSize(), source.fieldMappings(), source.targetConfig());
        TaskDetail cloned = save(cloneRequest);
        operationLogService.record("task", "clone", cloned.task().id(), "任务已从 " + id + " 复制");
        taskLogService.info(cloned.task().id(), "任务已从 " + id + " 复制，默认状态为 STOPPED");
        return cloned;
    }

    @Transactional
    public TaskDetail rollback(com.openclaw.canalweb.domain.ConfigVersion version) {
        if (!"task".equals(version.configType())) {
            throw new IllegalArgumentException("配置版本类型不是同步任务");
        }
        TaskSaveRequest request = configVersionService.readSnapshotPayload(version, TaskSaveRequest.class);
        TaskDetail detail = save(request);
        operationLogService.record("task", "rollback", detail.task().id(),
                "任务配置已回滚到版本 v" + version.versionNo());
        taskLogService.info(detail.task().id(), "任务配置已回滚到版本 v" + version.versionNo());
        return detail;
    }

    @Transactional
    public void start(String id) {
        Map<String, Object> diagnostics = diagnostics(id);
        long blockerCount = ((Number) diagnostics.getOrDefault("blockerCount", 0)).longValue();
        if (blockerCount > 0) {
            throw new IllegalArgumentException("任务自检存在 " + blockerCount + " 个阻断项，请先处理后再启动");
        }
        TaskDetail detail = detail(id);
        String mode = normalizeSyncMode(detail.task().syncMode());
        if (requiresFullSync(mode)) {
            runFullSnapshot(detail, "start");
        }
        if (!requiresIncremental(mode)) {
            canalRuntimeService.deleteTaskRuntimeFiles(id);
            updateStatus(id, "RUNNING", true);
            operationLogService.record("task", "start", id, "FULL 任务已启动并完成全量快照");
            taskLogService.info(id, "FULL 任务已启动，已完成全量快照，不挂载 Canal Adapter 增量配置");
            return;
        }
        var datasource = datasourceService.findByKey(detail.task().dataSourceKey())
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + detail.task().dataSourceKey()));
        String config = canalAdapterService.buildCanalConfig(
                detail.task(), datasource, detail.fieldMappings(), detail.targetConfig());
        CanalAdapterService.AdapterRuntimeFiles runtimeFiles = canalAdapterService.buildRuntimeFiles(
                detail.task(), datasource, detail.fieldMappings(), detail.targetConfig());
        var file = canalRuntimeService.saveTaskAdapterSpec(id, config);
        canalRuntimeService.saveTaskAdapterRuntimeFiles(id, runtimeFiles);
        configVersionService.snapshot("task-adapter", id, config);
        canalRuntimeService.refreshRuntime(true);
        updateStatus(id, "RUNNING", true);
        String message = "FULL_INCREMENTAL".equals(mode)
                ? "任务已完成全量快照并启动增量链路，任务插件配置: " + file.toAbsolutePath()
                : "任务已启动，Canal 运行时已刷新，任务插件配置: " + file.toAbsolutePath();
        operationLogService.record("task", "start", id,
                "FULL_INCREMENTAL".equals(mode) ? "任务已完成全量快照并刷新 Canal Runtime" : "任务已启动并刷新 Canal Runtime");
        taskLogService.info(id, message);
    }

    @Transactional
    public void stop(String id) {
        find(id);
        canalRuntimeService.deleteTaskRuntimeFiles(id);
        canalRuntimeService.refreshRuntime(true);
        updateStatus(id, "STOPPED", false);
        operationLogService.record("task", "stop", id, "任务已停止并卸载 Canal Adapter 运行配置");
        taskLogService.info(id, "任务已停止，Canal Adapter 运行配置已卸载");
    }

    @Transactional
    public void batchStart(List<String> ids) {
        for (String id : ids) {
            start(id);
        }
    }

    @Transactional
    public void batchStop(List<String> ids) {
        for (String id : ids) {
            stop(id);
        }
    }

    @Transactional
    public void delete(String id) {
        jdbcClient.sql("DELETE FROM task_field_mapping WHERE task_id = :id").param("id", id).update();
        jdbcClient.sql("DELETE FROM task_target_config WHERE task_id = :id").param("id", id).update();
        jdbcClient.sql("DELETE FROM sync_task_log WHERE task_id = :id").param("id", id).update();
        jdbcClient.sql("DELETE FROM sync_task WHERE id = :id").param("id", id).update();
        canalRuntimeService.deleteTaskRuntimeFiles(id);
        canalRuntimeService.refreshRuntime(false);
        operationLogService.record("task", "delete", id, "任务已删除");
    }

    public Map<String, Object> monitor(String id) {
        SyncTask task = find(id);
        long logCount = jdbcClient.sql("SELECT COUNT(*) FROM sync_task_log WHERE task_id = :id")
                .param("id", id).query(Long.class).single();
        long errorCount = jdbcClient.sql("SELECT COUNT(*) FROM sync_task_log WHERE task_id = :id AND log_level = 'ERROR'")
                .param("id", id).query(Long.class).single();
        Map<String, Object> monitor = new HashMap<>();
        monitor.put("taskId", id);
        monitor.put("status", task.taskStatus());
        monitor.put("logCount", logCount);
        monitor.put("errorCount", errorCount);
        monitor.put("totalCount", task.totalCount() == null ? 0 : task.totalCount());
        monitor.put("failCount", task.failCount() == null ? 0 : task.failCount());
        monitor.put("delayMs", task.lastDelayMs() == null ? 0 : task.lastDelayMs());
        monitor.put("lastStartTime", task.lastStartTime());
        monitor.put("lastStopTime", task.lastStopTime());
        monitor.put("lastScheduleTime", task.lastScheduleTime());
        monitor.put("fullSyncFile", task.fullSyncFile());
        return monitor;
    }

    public Map<String, Object> runtime(String id) {
        TaskDetail detail = detail(id);
        Map<String, Object> monitor = monitor(id);
        Map<String, Object> preview;
        try {
            preview = previewConfig(id);
        } catch (Exception ex) {
            preview = Map.of("error", ex.getMessage());
        }
        Map<String, String> config = targetConfigMap(detail.targetConfig());
        List<com.openclaw.canalweb.domain.SyncTaskLog> logs = taskLogService.listByTask(id);
        var lastLog = logs.stream().findFirst().orElse(null);
        var lastError = logs.stream().filter(item -> "ERROR".equalsIgnoreCase(item.logLevel())).findFirst().orElse(null);
        Map<String, Object> runtime = new LinkedHashMap<>();
        runtime.put("taskId", id);
        runtime.put("status", detail.task().taskStatus());
        runtime.put("targetType", detail.task().targetType());
        runtime.put("syncMode", detail.task().syncMode());
        runtime.put("destination", preview.getOrDefault("destination", ""));
        runtime.put("adapterKey", config.getOrDefault("adapterKey", id));
        runtime.put("mappingDir", preview.getOrDefault("mappingDir", ""));
        runtime.put("mappingFile", preview.getOrDefault("mappingFile", ""));
        runtime.put("runtimeFiles", runtimeFiles(id, preview));
        runtime.put("destinationStatus", adapterDestinationStatus(String.valueOf(preview.getOrDefault("destination", ""))));
        runtime.put("logCount", monitor.get("logCount"));
        runtime.put("errorCount", monitor.get("errorCount"));
        runtime.put("lastStartTime", monitor.get("lastStartTime"));
        runtime.put("lastStopTime", monitor.get("lastStopTime"));
        runtime.put("lastScheduleTime", monitor.get("lastScheduleTime"));
        runtime.put("lastLog", lastLog);
        runtime.put("lastError", lastError);
        runtime.put("recentLogs", logs.stream().limit(8).toList());
        runtime.put("suggestion", runtimeSuggestion(detail.task().taskStatus(), lastError, preview,
                (Map<String, Object>) runtime.get("destinationStatus"), (List<Map<String, Object>>) runtime.get("runtimeFiles")));
        return runtime;
    }

    public Map<String, Object> diagnostics(String id) {
        TaskDetail detail = detail(id);
        List<Map<String, Object>> checks = new ArrayList<>();
        addCheck(checks, "任务名称", hasText(detail.task().taskName()), value(detail.task().taskName()), "用于业务识别任务");
        addCheck(checks, "同步 SQL", hasText(detail.task().syncSql()), sqlSummary(detail.task().syncSql()), "建议使用生成 SQL 或确认 SELECT 语句有效");
        addCheck(checks, "批大小", detail.task().batchSize() != null && detail.task().batchSize() > 0,
                value(detail.task().batchSize()), "batchSize 需要大于 0");
        String mode = normalizeSyncMode(detail.task().syncMode());
        addCheck(checks, "同步模式", isSupportedSyncMode(mode), mode,
                "支持 INCREMENTAL、FULL、FULL_INCREMENTAL");
        if (requiresFullSync(mode)) {
            addCheck(checks, "全量执行", hasText(detail.task().syncSql()),
                    sqlSummary(detail.task().syncSql()), "按 syncSql 读取源库并生成 JSONL 快照");
            addCheck(checks, "全量调度", hasText(detail.task().cronExpression()),
                    value(detail.task().cronExpression()), "FULL/FULL_INCREMENTAL 可配置 Cron，手动全量不强制");
        }
        if (requiresIncremental(mode)) {
            addCheck(checks, "增量链路", true, mode, "由 Canal Server/Adapter 消费 binlog");
        } else {
            addCheck(checks, "增量链路", true, "未启用", "FULL 只生成全量快照，不挂载 Adapter 增量配置");
        }
        var datasource = datasourceService.findByKey(detail.task().dataSourceKey()).orElse(null);
        addCheck(checks, "数据源存在", datasource != null, detail.task().dataSourceKey(), "任务必须绑定已存在数据源");
        if (datasource != null) {
            addCheck(checks, "数据源启用", datasource.status() != null && datasource.status() == 1,
                    datasource.status() != null && datasource.status() == 1 ? "已启用" : "已禁用", "禁用数据源不会生成 Canal 实例配置");
        }
        addCheck(checks, "字段映射", !detail.fieldMappings().isEmpty(),
                "映射 " + detail.fieldMappings().size() + " 个字段", "目标端映射依赖字段配置");
        boolean mappingValid = detail.fieldMappings().stream()
                .allMatch(item -> hasText(item.sourceField()) && hasText(item.targetField()));
        addCheck(checks, "字段映射完整", mappingValid, mappingValid ? "字段完整" : "存在空字段", "源字段和目标字段都不能为空");
        List<String> duplicateTargets = duplicateFields(detail.fieldMappings().stream()
                .filter(item -> item.enabled() == null || Boolean.TRUE.equals(item.enabled()))
                .map(TaskFieldMapping::targetField)
                .toList());
        addCheck(checks, "目标字段唯一", duplicateTargets.isEmpty(),
                duplicateTargets.isEmpty() ? "目标字段无重复" : "重复 " + duplicateTargets,
                "启用字段映射的目标字段应保持唯一，避免覆盖写入");
        List<String> invalidOptions = detail.fieldMappings().stream()
                .filter(item -> hasText(item.fieldOptions()) && !validJsonObject(item.fieldOptions()))
                .map(item -> valueOrDefault(item.targetField(), item.sourceField()))
                .toList();
        addCheck(checks, "字段属性 JSON", invalidOptions.isEmpty(),
                invalidOptions.isEmpty() ? "属性参数合法" : "异常字段 " + invalidOptions,
                "fieldOptions 必须是 JSON Object，例如 {\"messageKey\":true}");
        Map<String, String> targetConfig = targetConfigMap(detail.targetConfig());
        if (datasource != null) {
            checks.addAll(fieldTypeChecks(detail, targetConfig));
        }
        try {
            Map<String, Object> targetResult = testTarget(new TargetTestRequest(detail.task().targetType(), targetConfig));
            addCheck(checks, "目标端连接", true, String.valueOf(targetResult.getOrDefault("message", "连接检查通过")), detail.task().targetType());
        } catch (Exception ex) {
            addCheck(checks, "目标端连接", "LOGGER".equalsIgnoreCase(detail.task().targetType()), ex.getMessage(), detail.task().targetType());
        }
        checks.addAll(targetResourceChecks(detail.task().targetType(), targetConfig, detail.fieldMappings()));
        Map<String, Object> preview = Map.of();
        if (datasource != null && requiresIncremental(mode)) {
            try {
                preview = previewConfig(id);
                addCheck(checks, "Adapter 配置生成", true, "生成成功", String.valueOf(preview.getOrDefault("mappingFile", "")));
            } catch (Exception ex) {
                addCheck(checks, "Adapter 配置生成", false, ex.getMessage(), "检查目标端参数和字段映射");
            }
        } else if (!requiresIncremental(mode)) {
            addCheck(checks, "Adapter 配置生成", true, "FULL 模式跳过", "全量快照不需要 Adapter runtime 文件");
        }
        long okCount = checks.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        long blockerCount = severityCount(checks, "BLOCKER");
        long warnCount = severityCount(checks, "WARN");
        long infoCount = severityCount(checks, "INFO");
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("taskId", id);
        result.put("taskName", detail.task().taskName());
        result.put("targetType", detail.task().targetType());
        result.put("syncMode", detail.task().syncMode());
        result.put("okCount", okCount);
        result.put("total", checks.size());
        result.put("blockerCount", blockerCount);
        result.put("warnCount", warnCount);
        result.put("infoCount", infoCount);
        result.put("ok", blockerCount == 0);
        result.put("checks", checks);
        result.put("preview", preview);
        return result;
    }

    public Map<String, Object> testTarget(TargetTestRequest request) {
        String type = request.targetType() == null ? "LOGGER" : request.targetType().toUpperCase();
        Map<String, String> config = request.targetConfig() == null ? Map.of() : request.targetConfig();
        try {
            switch (type) {
                case "LOGGER" -> {
                    return successTarget("目标端连接检查通过");
                }
                case "KAFKA" -> {
                    testEndpoint(config.getOrDefault("bootstrapServers", config.getOrDefault("servers", "127.0.0.1:9092")));
                    requireText(config, "topic", "缺少 Kafka Topic");
                    requireText(config, "messageFormat", "缺少 Kafka 消息格式");
                    return successTarget("Kafka 地址可连接");
                }
                case "ROCKETMQ" -> {
                    testEndpoint(config.getOrDefault("nameServer", config.getOrDefault("namesrvAddr", "127.0.0.1:9876")));
                    requireText(config, "topic", "缺少 RocketMQ Topic");
                    requireText(config, "producerGroup", "缺少 RocketMQ Producer Group");
                    return successTarget("RocketMQ NameServer 可连接");
                }
                case "RABBITMQ" -> {
                    testHostPort(config.getOrDefault("host", "127.0.0.1"), intValue(config.get("port"), 5672));
                    requireText(config, "topic", "缺少 RabbitMQ Topic/Queue 约定");
                    return successTarget("RabbitMQ 地址可连接");
                }
                case "PULSAR", "PULSARMQ" -> {
                    testEndpoint(config.getOrDefault("serviceUrl", config.getOrDefault("serverUrl", "pulsar://127.0.0.1:6650")));
                    requireText(config, "topic", "缺少 Pulsar Topic");
                    requireText(config, "subscriptionName", "缺少 Pulsar Subscription");
                    return successTarget("Pulsar Broker 可连接");
                }
                case "HBASE" -> {
                    testEndpoint(config.getOrDefault("zkQuorum", "127.0.0.1") + ":" + config.getOrDefault("zkClientPort", "2181"));
                    requireText(config, "hbaseTable", "缺少 HBase 表名");
                    requireText(config, "family", "缺少 HBase Column Family");
                    requireText(config, "rowKey", "缺少 HBase RowKey");
                    return successTarget("目标端连接检查通过");
                }
                case "TABLESTORE" -> {
                    requireText(config, "endpoint", "缺少 Tablestore Endpoint");
                    requireText(config, "instanceName", "缺少 Tablestore Instance");
                    requireText(config, "tableName", "缺少 Tablestore 表名");
                    requireText(config, "primaryKey", "缺少 Tablestore 主键字段");
                    return successTarget("Tablestore 配置检查通过");
                }
                case "ES", "ELASTICSEARCH" -> {
                    testEndpoint(config.getOrDefault("hosts", "127.0.0.1:9200"));
                    return successTarget("Elasticsearch 地址可连接");
                }
                case "REDIS" -> {
                    testHostPort(config.getOrDefault("host", "127.0.0.1"), intValue(config.get("port"), 6379));
                    requireText(config, "keyPattern", "缺少 Redis Key 模板");
                    requireText(config, "valueType", "缺少 Redis 数据结构");
                    return successTarget("Redis 地址可连接");
                }
                case "MYSQL", "PGSQL", "POSTGRESQL", "CLICKHOUSE", "ORACLE" -> {
                    testJdbc(config);
                    return successTarget("JDBC 目标端连接成功");
                }
                default -> throw new IllegalArgumentException("暂不支持的目标端类型: " + type);
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("目标端连接失败: " + ex.getMessage(), ex);
        }
    }

    private List<Map<String, Object>> targetResourceChecks(String targetType, Map<String, String> config,
                                                           List<TaskFieldMapping> mappings) {
        String type = targetType == null ? "LOGGER" : targetType.toUpperCase();
        return switch (type) {
            case "REDIS" -> redisResourceChecks(config, mappings);
            case "MYSQL", "RDB" -> rdbResourceChecks(config, mappings, "MySQL");
            case "PGSQL", "POSTGRESQL" -> rdbResourceChecks(config, mappings, "PostgreSQL");
            case "ES", "ELASTICSEARCH" -> esResourceChecks(config, mappings);
            case "KAFKA" -> kafkaResourceChecks(config, mappings);
            case "ROCKETMQ" -> rocketMqResourceChecks(config, mappings);
            case "RABBITMQ" -> rabbitMqResourceChecks(config, mappings);
            case "PULSAR", "PULSARMQ" -> pulsarResourceChecks(config, mappings);
            case "HBASE" -> hbaseResourceChecks(config, mappings);
            case "TABLESTORE" -> tablestoreResourceChecks(config, mappings);
            default -> List.of();
        };
    }

    private List<Map<String, Object>> redisResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String keyPattern = firstText(config, "keyPattern", "key_pattern");
        String valueType = firstText(config, "valueType", "value_type");
        String valueMapping = firstText(config, "valueMapping", "value_mapping");
        String ttl = firstText(config, "ttlSeconds", "ttl_seconds", "expireSeconds");
        String deletePolicy = firstText(config, "deletePolicy", "delete_policy");
        addCheck(checks, "Redis Key 模板", hasText(keyPattern), value(keyPattern), "例如 user:{id}，模板字段必须来自源字段映射");
        addCheck(checks, "Redis Key 字段", templateFieldsExist(keyPattern, sourceFields(mappings)),
                templateFieldMessage(keyPattern), "检查 keyPattern 中的 {字段名}");
        addCheck(checks, "Redis 数据结构", Set.of("STRING", "HASH", "JSON").contains(upper(valueType)),
                value(valueType), "支持 STRING/HASH/JSON");
        addCheck(checks, "Redis Value 字段", hasText(valueMapping) || "STRING".equals(upper(valueType)),
                hasText(valueMapping) ? valueMapping : "STRING 可使用单字段或序列化内容", "HASH/JSON 建议显式配置 valueMapping");
        addCheck(checks, "Redis TTL", nonNegativeInteger(ttl), valueOrZero(ttl), "ttlSeconds 必须为非负整数，0 表示不过期");
        addCheck(checks, "Redis 删除策略", Set.of("DELETE_KEY", "MARK_DELETED", "IGNORE").contains(upper(deletePolicy)),
                value(deletePolicy), "源库 DELETE 事件的处理方式");
        try {
            CommandResult result = redisPing(config);
            addCheck(checks, "Redis 服务可用", result.exitCode() == 0 && result.output().contains("PONG"),
                    result.output(), "使用 redis-cli PING 验证 host/port/database/password");
        } catch (Exception ex) {
            addCheck(checks, "Redis 服务可用", false, ex.getMessage(), "确认 Redis 已启动，且 redis-cli 可执行");
        }
        return checks;
    }

    private List<Map<String, Object>> rdbResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings,
                                                        String label) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String database = firstText(config, "targetDatabase", "target_database", "targetSchema", "target_schema");
        String table = firstText(config, "targetTableName", "target_table", "targetTable");
        String pk = firstText(config, "primaryKey", "primary_key", "targetPk");
        String writeMode = firstText(config, "writeMode", "write_mode");
        addCheck(checks, label + " 目标库/Schema", hasText(database), value(database), "需要提前创建目标库或 Schema");
        addCheck(checks, label + " 目标表", hasText(table), value(table), "需要提前创建目标表");
        addCheck(checks, label + " 主键/唯一键", hasText(pk), value(pk), "UPSERT/UPDATE 模式必须配置主键或唯一键");
        addCheck(checks, label + " 写入模式", Set.of("INSERT", "UPSERT", "UPDATE").contains(upper(writeMode)),
                value(writeMode), "支持 INSERT/UPSERT/UPDATE");
        if (hasText(config.get("jdbcUrl")) && hasText(table)) {
            try (Connection connection = openJdbc(config)) {
                Set<String> columns = tableColumns(connection, database, table);
                addCheck(checks, label + " 库表存在", !columns.isEmpty(), qualifiedName(database, table),
                        "目标表不存在时请先执行建库建表 SQL");
                Set<String> missing = missingTargetColumns(mappings, columns);
                addCheck(checks, label + " 字段覆盖", missing.isEmpty(), missing.isEmpty() ? "字段覆盖完整" : "缺少 " + missing,
                        "目标表字段需要覆盖字段映射中的目标字段");
                addCheck(checks, label + " 主键字段", !hasText(pk) || columns.contains(pk), value(pk),
                        "目标主键字段必须存在于目标表");
            } catch (Exception ex) {
                addCheck(checks, label + " 资源检查", false, ex.getMessage(), "确认 JDBC、库名、表名和账号权限");
            }
        }
        return checks;
    }

    private List<Map<String, Object>> esResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String index = firstText(config, "indexName", "index");
        String documentId = firstText(config, "documentId", "document_id", "idField");
        String mapping = firstText(config, "mapping");
        String writeMode = firstText(config, "writeMode", "write_mode");
        addCheck(checks, "ES 索引名", hasText(index), value(index), "需要提前创建索引");
        addCheck(checks, "ES Document ID", hasText(documentId) && templateFieldsExist(documentId, sourceFields(mappings)),
                value(documentId), "例如 {id}，字段必须来自源字段映射");
        addCheck(checks, "ES 写入模式", Set.of("INDEX", "UPDATE", "UPSERT").contains(upper(writeMode)),
                value(writeMode), "支持 INDEX/UPDATE/UPSERT");
        addCheck(checks, "ES Mapping JSON", validMappingJson(mapping), hasText(mapping) ? "mapping 已填写" : "未填写",
                "建议显式创建 mapping，避免动态 mapping 类型漂移");
        if (hasText(index)) {
            try {
                int status = esHeadStatus(config.getOrDefault("hosts", "127.0.0.1:9200"), index);
                addCheck(checks, "ES 索引存在", status >= 200 && status < 300, "HTTP " + status,
                        "索引不存在时请先创建索引和 mapping");
            } catch (Exception ex) {
                addCheck(checks, "ES 索引检查", false, ex.getMessage(), "确认 ES 地址和认证信息");
            }
        }
        return checks;
    }

    private List<Map<String, Object>> kafkaResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String topic = firstText(config, "topic");
        String messageKey = firstText(config, "messageKey", "message_key");
        String partitionKey = firstText(config, "partitionKey", "partition_key");
        String format = firstText(config, "messageFormat", "message_format");
        String group = firstText(config, "consumerGroup", "consumer_group");
        Set<String> sourceFields = sourceFields(mappings);
        addCheck(checks, "Kafka Topic", hasText(topic), value(topic), "Topic 需要提前创建");
        addCheck(checks, "Kafka Message Key", !hasText(messageKey) || templateFieldsExist(messageKey, sourceFields),
                value(messageKey), "例如 {id}，模板字段必须来自源字段映射");
        addCheck(checks, "Kafka 分区字段", !hasText(partitionKey) || sourceFields.contains(partitionKey),
                value(partitionKey), "用于保证同一业务主键进入同一分区");
        addCheck(checks, "Kafka 消息格式", Set.of("JSON", "CANAL_JSON").contains(upper(format)),
                value(format), "支持 JSON/CANAL_JSON");
        addCheck(checks, "Kafka 消费组", hasText(group), value(group), "登记下游 consumer group，避免多个业务共用同一 group");
        if (hasText(topic)) {
            try {
                CommandResult result = kafkaTopics(config);
                boolean exists = topicExists(result.output(), topic);
                addCheck(checks, "Kafka Topic 存在", result.exitCode() == 0 && exists,
                        result.exitCode() == 0 ? (exists ? topic : "未找到 " + topic) : result.output(),
                        "Topic 不存在时请先执行 Topic 创建命令");
            } catch (Exception ex) {
                addCheck(checks, "Kafka Topic 存在", false, ex.getMessage(), "确认 Kafka 已启动，且 kafka-topics 可执行");
            }
        }
        return checks;
    }

    private List<Map<String, Object>> rocketMqResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = mqResourceChecks("RocketMQ", config, mappings);
        String nameServer = firstText(config, "nameServer", "namesrvAddr");
        String producerGroup = firstText(config, "producerGroup", "producer_group");
        addCheck(checks, "RocketMQ NameServer", hasText(nameServer), value(nameServer), "例如 127.0.0.1:9876");
        addCheck(checks, "RocketMQ Producer Group", hasText(producerGroup), value(producerGroup), "生产者组需要唯一且稳定");
        return checks;
    }

    private List<Map<String, Object>> rabbitMqResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = mqResourceChecks("RabbitMQ", config, mappings);
        String host = firstText(config, "host");
        String port = firstText(config, "port");
        String exchange = firstText(config, "exchange");
        String routingKey = firstText(config, "routingKey", "routing_key");
        addCheck(checks, "RabbitMQ Host", hasText(host), valueOrDefault(host, "127.0.0.1"), "RabbitMQ broker 地址");
        addCheck(checks, "RabbitMQ Port", nonNegativeInteger(port), valueOrDefault(port, "5672"), "默认 5672");
        addCheck(checks, "RabbitMQ Exchange", hasText(exchange), value(exchange), "建议提前创建 exchange");
        addCheck(checks, "RabbitMQ Routing Key", hasText(routingKey), value(routingKey), "用于路由到队列");
        return checks;
    }

    private List<Map<String, Object>> pulsarResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = mqResourceChecks("Pulsar", config, mappings);
        String serviceUrl = firstText(config, "serviceUrl", "serverUrl");
        String subscription = firstText(config, "subscriptionName", "consumerGroup");
        addCheck(checks, "Pulsar Service URL", hasText(serviceUrl), value(serviceUrl), "例如 pulsar://127.0.0.1:6650");
        addCheck(checks, "Pulsar Subscription", hasText(subscription), value(subscription), "登记下游 subscription，避免多个业务共用");
        return checks;
    }

    private List<Map<String, Object>> mqResourceChecks(String label, Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String topic = firstText(config, "topic");
        String messageKey = firstText(config, "messageKey", "message_key");
        String partitionKey = firstText(config, "partitionKey", "partition_key");
        String format = firstText(config, "messageFormat", "message_format");
        Set<String> sourceFields = sourceFields(mappings);
        addCheck(checks, label + " Topic", hasText(topic), value(topic), "Topic 需要提前创建");
        addCheck(checks, label + " Message Key", !hasText(messageKey) || templateFieldsExist(messageKey, sourceFields),
                value(messageKey), "例如 {id}，模板字段必须来自源字段映射");
        addCheck(checks, label + " 分区字段", !hasText(partitionKey) || sourceFields.contains(partitionKey),
                value(partitionKey), "用于保证同一业务主键进入同一分区");
        addCheck(checks, label + " 消息格式", Set.of("JSON", "CANAL_JSON").contains(upper(format)),
                value(format), "支持 JSON/CANAL_JSON");
        return checks;
    }

    private List<Map<String, Object>> hbaseResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String zkQuorum = firstText(config, "zkQuorum");
        String zkPort = firstText(config, "zkClientPort");
        String hbaseTable = firstText(config, "hbaseTable", "tableName");
        String family = firstText(config, "family");
        String rowKey = firstText(config, "rowKey");
        Set<String> sourceFields = sourceFields(mappings);
        addCheck(checks, "HBase ZK Quorum", hasText(zkQuorum), value(zkQuorum), "例如 hbase-zk-1,hbase-zk-2");
        addCheck(checks, "HBase ZK 端口", nonNegativeInteger(zkPort), valueOrZero(zkPort), "默认 2181");
        addCheck(checks, "HBase 表名", hasText(hbaseTable), value(hbaseTable), "Namespace:Table 或 Table");
        addCheck(checks, "HBase Column Family", hasText(family), value(family), "需要提前创建 Column Family");
        addCheck(checks, "HBase RowKey", hasText(rowKey) && rowKeyFieldsExist(rowKey, sourceFields),
                value(rowKey), "支持 id 或 id,type，字段必须来自源字段映射");
        addCheck(checks, "HBase 字段映射", mappings != null && !mappings.isEmpty(),
                mappings == null ? "未配置" : String.valueOf(mappings.size()), "需要数据库字段到 qualifier 的映射");
        return checks;
    }

    private List<Map<String, Object>> tablestoreResourceChecks(Map<String, String> config, List<TaskFieldMapping> mappings) {
        List<Map<String, Object>> checks = new ArrayList<>();
        String endpoint = firstText(config, "endpoint");
        String instanceName = firstText(config, "instanceName");
        String tableName = firstText(config, "tableName", "targetTable");
        String primaryKey = firstText(config, "primaryKey", "targetPk");
        addCheck(checks, "Tablestore Endpoint", hasText(endpoint), value(endpoint), "例如 https://instance.cn-hangzhou.ots.aliyuncs.com");
        addCheck(checks, "Tablestore Instance", hasText(instanceName), value(instanceName), "实例名不能为空");
        addCheck(checks, "Tablestore 表名", hasText(tableName), value(tableName), "表需要提前创建");
        addCheck(checks, "Tablestore 主键", hasText(primaryKey) && sourceFields(mappings).contains(primaryKey),
                value(primaryKey), "主键字段必须来自源字段映射");
        addCheck(checks, "Tablestore 字段映射", mappings != null && !mappings.isEmpty(),
                mappings == null ? "未配置" : String.valueOf(mappings.size()), "需要源字段到属性列的映射");
        return checks;
    }

    private List<Map<String, Object>> redisResourcePlan(Map<String, String> config, List<TaskFieldMapping> mappings) {
        String keyPattern = firstText(config, "keyPattern", "key_pattern");
        String valueType = firstText(config, "valueType", "value_type");
        String ttl = firstText(config, "ttlSeconds", "ttl_seconds", "expireSeconds");
        String fields = mappings == null || mappings.isEmpty() ? "id" : mappings.stream()
                .map(TaskFieldMapping::targetField)
                .filter(SyncTaskService::hasText)
                .reduce((left, right) -> left + "," + right)
                .orElse("id");
        String script = """
                # Redis 资源无需建表，启动前确认以下写入契约
                key_pattern=%s
                value_type=%s
                value_fields=%s
                ttl_seconds=%s
                delete_policy=%s
                redis-cli -h %s -p %s -n %s PING
                """.formatted(
                value(keyPattern),
                value(firstText(Map.of("valueType", valueType), "valueType")),
                fields,
                hasText(ttl) ? ttl : "0",
                value(firstText(config, "deletePolicy", "delete_policy")),
                config.getOrDefault("host", "127.0.0.1"),
                config.getOrDefault("port", "6379"),
                config.getOrDefault("database", "0")
        );
        return List.of(planItem("Redis Key/Value 契约", "CHECK", "确认 key/value/TTL/delete 策略", script));
    }

    private List<Map<String, Object>> rdbResourcePlan(Map<String, String> config, List<TaskFieldMapping> mappings,
                                                      Map<String, String> sourceTypes, String label) {
        String database = firstText(config, "targetDatabase", "targetSchema", "target_database", "target_schema");
        String table = firstText(config, "targetTableName", "targetTable", "target_table");
        String pk = firstText(config, "primaryKey", "targetPk", "primary_key");
        String qualified = hasText(database) && hasText(table) && !table.contains(".") ? database + "." + table : value(table);
        String columns = sqlColumns(mappings, sourceTypes, pk, "PostgreSQL".equals(label));
        String script = "PostgreSQL".equals(label)
                ? """
                CREATE SCHEMA IF NOT EXISTS %s;

                CREATE TABLE IF NOT EXISTS %s (
                %s
                );
                """.formatted(valueOrDefault(database, "public"), qualified, columns)
                : """
                CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

                CREATE TABLE IF NOT EXISTS `%s`.`%s` (
                %s
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """.formatted(valueOrDefault(database, "target_db"), valueOrDefault(database, "target_db"),
                valueOrDefault(table, "target_table"), columns);
        return List.of(planItem(label + " 建库建表 SQL", "SQL", "提交 DBA 审核后执行", script));
    }

    private List<Map<String, Object>> esResourcePlan(Map<String, String> config, List<TaskFieldMapping> mappings,
                                                     Map<String, String> sourceTypes) {
        String index = firstText(config, "indexName", "index");
        String mapping = firstText(config, "mapping");
        String generatedMapping = hasText(mapping) && validMappingJson(mapping) ? mapping : generatedEsMapping(mappings, sourceTypes);
        String script = """
                PUT /%s
                {
                  "mappings": %s
                }
                """.formatted(valueOrDefault(index, "target_index"), generatedMapping);
        return List.of(planItem("Elasticsearch 索引和 Mapping", "HTTP", "先创建索引和字段类型，再启动同步", script));
    }

    private List<Map<String, Object>> kafkaResourcePlan(Map<String, String> config) {
        String topic = valueOrDefault(firstText(config, "topic"), "canal.topic");
        String script = """
                kafka-topics.sh --bootstrap-server %s \\
                  --create \\
                  --topic %s \\
                  --partitions %s \\
                  --replication-factor %s
                """.formatted(
                valueOrDefault(firstText(config, "bootstrapServers", "servers"), "127.0.0.1:9092"),
                topic,
                valueOrDefault(firstText(config, "partitions"), "6"),
                valueOrDefault(firstText(config, "replicationFactor"), "1")
        );
        return List.of(planItem("Kafka Topic", "SHELL", "Topic 需要提前创建", script));
    }

    private List<Map<String, Object>> rocketMqResourcePlan(Map<String, String> config) {
        String script = """
                mqadmin updateTopic -n %s \\
                  -c %s \\
                  -t %s
                """.formatted(
                valueOrDefault(firstText(config, "nameServer", "namesrvAddr"), "127.0.0.1:9876"),
                valueOrDefault(firstText(config, "clusterName"), "DefaultCluster"),
                valueOrDefault(firstText(config, "topic"), "canal_topic")
        );
        return List.of(planItem("RocketMQ Topic", "SHELL", "Topic 需要提前创建", script));
    }

    private List<Map<String, Object>> rabbitMqResourcePlan(Map<String, String> config) {
        String script = """
                rabbitmqadmin declare exchange name=%s type=topic durable=true
                rabbitmqadmin declare queue name=%s durable=true
                rabbitmqadmin declare binding source=%s destination=%s routing_key=%s
                """.formatted(
                valueOrDefault(firstText(config, "exchange"), "canal.exchange"),
                valueOrDefault(firstText(config, "topic"), "canal.queue"),
                valueOrDefault(firstText(config, "exchange"), "canal.exchange"),
                valueOrDefault(firstText(config, "topic"), "canal.queue"),
                valueOrDefault(firstText(config, "routingKey", "routing_key"), "canal.#")
        );
        return List.of(planItem("RabbitMQ Exchange/Queue/Binding", "SHELL", "Exchange、Queue 和 Binding 需要提前创建", script));
    }

    private List<Map<String, Object>> pulsarResourcePlan(Map<String, String> config) {
        String topic = valueOrDefault(firstText(config, "topic"), "persistent://public/default/canal_topic");
        String script = """
                pulsar-admin topics create %s
                pulsar-client consume %s -s %s -n 1
                """.formatted(topic, topic, valueOrDefault(firstText(config, "subscriptionName", "consumerGroup"), "canal-web-subscription"));
        return List.of(planItem("Pulsar Topic 和 Subscription", "SHELL", "Topic 需要提前创建，Subscription 首次消费时创建", script));
    }

    private List<Map<String, Object>> hbaseResourcePlan(Map<String, String> config) {
        String table = valueOrDefault(firstText(config, "hbaseTable", "tableName"), "canal:user_profile");
        String namespace = table.contains(":") ? table.split(":", 2)[0] : "default";
        String script = """
                hbase shell
                create_namespace '%s'
                create '%s', '%s'
                """.formatted(namespace, table, valueOrDefault(firstText(config, "family"), "CF"));
        return List.of(planItem("HBase Namespace/Table/Family", "SHELL", "HBase 表和 Column Family 需要提前创建", script));
    }

    private List<Map<String, Object>> tablestoreResourcePlan(Map<String, String> config) {
        String script = """
                # Tablestore 表需要在控制台或 ots-cli 中提前创建
                instance=%s
                table=%s
                primary_key=%s
                write_mode=%s
                """.formatted(
                valueOrDefault(firstText(config, "instanceName"), "canal-instance"),
                valueOrDefault(firstText(config, "tableName", "targetTable"), "user_profile"),
                valueOrDefault(firstText(config, "primaryKey", "targetPk"), "id"),
                valueOrDefault(firstText(config, "writeMode"), "UPSERT")
        );
        return List.of(planItem("Tablestore Table/PrimaryKey", "CHECK", "实例、表和主键需要提前创建", script));
    }

    private static Map<String, Object> planItem(String name, String type, String description, String content) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", name);
        item.put("type", type);
        item.put("description", description);
        item.put("content", content == null ? "" : content.strip());
        return item;
    }

    public DashboardStats dashboard() {
        long datasourceCount = jdbcClient.sql("SELECT COUNT(*) FROM datasource_config").query(Long.class).single();
        long taskCount = jdbcClient.sql("SELECT COUNT(*) FROM sync_task").query(Long.class).single();
        long runningCount = jdbcClient.sql("SELECT COUNT(*) FROM sync_task WHERE task_status = 'RUNNING'").query(Long.class).single();
        long errorCount = jdbcClient.sql("SELECT COUNT(*) FROM sync_task WHERE task_status = 'ERROR'").query(Long.class).single();
        long totalSyncCount = nullableLong("SELECT COALESCE(SUM(total_count), 0) FROM sync_task");
        long failCount = nullableLong("SELECT COALESCE(SUM(fail_count), 0) FROM sync_task");
        long avgDelayMs = nullableLong("SELECT COALESCE(AVG(last_delay_ms), 0) FROM sync_task WHERE task_status = 'RUNNING'");
        return new DashboardStats(datasourceCount, taskCount, runningCount, errorCount, taskLogService.count(),
                totalSyncCount, failCount, avgDelayMs, alertLogService.unacknowledgedCount());
    }

    private SyncTask find(String id) {
        return jdbcClient.sql("""
                SELECT id, task_name, description, data_source_key, sync_sql, target_type, sync_mode,
                       cron_expression, batch_size, task_status, total_count, fail_count, last_delay_ms,
                       last_start_time, last_stop_time, last_schedule_time, full_sync_file, create_time, update_time
                FROM sync_task WHERE id = :id
                """).param("id", id).query(SyncTask.class).optional()
                .orElseThrow(() -> new IllegalArgumentException("未找到同步任务: " + id));
    }

    private boolean exists(String id) {
        return jdbcClient.sql("SELECT COUNT(*) FROM sync_task WHERE id = :id")
                .param("id", id).query(Long.class).single() > 0;
    }

    private List<TaskFieldMapping> fieldMappings(String taskId) {
        return jdbcClient.sql("""
                SELECT id, task_id, source_field, target_field, field_type, primary_key, nullable_field,
                       enabled, default_value, transform_expr, format_pattern, field_options
                FROM task_field_mapping WHERE task_id = :taskId ORDER BY id
                """).param("taskId", taskId).query(TaskFieldMapping.class).list();
    }

    private List<TaskTargetConfig> targetConfig(String taskId) {
        return jdbcClient.sql("""
                SELECT id, task_id, config_key, config_value
                FROM task_target_config WHERE task_id = :taskId ORDER BY id
                """).param("taskId", taskId).query(TaskTargetConfig.class).list();
    }

    private List<FieldMappingRequest> normalizeMappings(List<FieldMappingRequest> mappings) {
        if (mappings == null) {
            return List.of();
        }
        List<FieldMappingRequest> normalized = new ArrayList<>();
        Set<String> targetFields = new HashSet<>();
        int rowNo = 0;
        for (FieldMappingRequest mapping : mappings) {
            rowNo++;
            String sourceField = valueOrDefault(mapping.sourceField(), "").trim();
            String targetField = valueOrDefault(mapping.targetField(), sourceField).trim();
            boolean enabled = mapping.enabled() == null || Boolean.TRUE.equals(mapping.enabled());
            if (enabled && !hasText(sourceField)) {
                throw new IllegalArgumentException("第 " + rowNo + " 行字段映射缺少源字段");
            }
            if (enabled && !hasText(targetField)) {
                throw new IllegalArgumentException("第 " + rowNo + " 行字段映射缺少目标字段");
            }
            if (enabled && hasText(targetField) && !targetFields.add(targetField)) {
                throw new IllegalArgumentException("启用字段映射目标字段重复: " + targetField);
            }
            String fieldOptions = normalizeFieldOptions(mapping.fieldOptions(), rowNo);
            boolean primaryKey = Boolean.TRUE.equals(mapping.primaryKey()) || isPrimaryKeyField(sourceField) || isPrimaryKeyField(targetField);
            boolean nullable = primaryKey ? false : mapping.nullableField() == null || Boolean.TRUE.equals(mapping.nullableField());
            normalized.add(new FieldMappingRequest(
                    sourceField,
                    targetField,
                    normalizeFieldType(mapping.fieldType(), sourceField, targetField),
                    primaryKey,
                    nullable,
                    enabled,
                    valueOrDefault(mapping.defaultValue(), "").trim(),
                    valueOrDefault(mapping.transformExpr(), "").trim(),
                    valueOrDefault(mapping.formatPattern(), "").trim(),
                    fieldOptions
            ));
        }
        return normalized;
    }

    private void replaceMappings(String taskId, List<FieldMappingRequest> mappings) {
        jdbcClient.sql("DELETE FROM task_field_mapping WHERE task_id = :taskId").param("taskId", taskId).update();
        if (mappings == null) {
            return;
        }
        for (FieldMappingRequest mapping : mappings) {
            jdbcClient.sql("""
                    INSERT INTO task_field_mapping
                    (task_id, source_field, target_field, field_type, primary_key, nullable_field,
                     enabled, default_value, transform_expr, format_pattern, field_options)
                    VALUES
                    (:taskId, :sourceField, :targetField, :fieldType, :primaryKey, :nullableField,
                     :enabled, :defaultValue, :transformExpr, :formatPattern, :fieldOptions)
                    """)
                    .param("taskId", taskId)
                    .param("sourceField", mapping.sourceField())
                    .param("targetField", mapping.targetField())
                    .param("fieldType", valueOrDefault(mapping.fieldType(), ""))
                    .param("primaryKey", Boolean.TRUE.equals(mapping.primaryKey()) ? 1 : 0)
                    .param("nullableField", mapping.nullableField() == null || Boolean.TRUE.equals(mapping.nullableField()) ? 1 : 0)
                    .param("enabled", mapping.enabled() == null || Boolean.TRUE.equals(mapping.enabled()) ? 1 : 0)
                    .param("defaultValue", valueOrDefault(mapping.defaultValue(), ""))
                    .param("transformExpr", valueOrDefault(mapping.transformExpr(), ""))
                    .param("formatPattern", valueOrDefault(mapping.formatPattern(), ""))
                    .param("fieldOptions", valueOrDefault(mapping.fieldOptions(), ""))
                    .update();
        }
    }

    private void replaceTargetConfig(String taskId, Map<String, String> targetConfig) {
        jdbcClient.sql("DELETE FROM task_target_config WHERE task_id = :taskId").param("taskId", taskId).update();
        if (targetConfig == null) {
            return;
        }
        targetConfig.forEach((key, value) -> jdbcClient.sql("""
                INSERT INTO task_target_config (task_id, config_key, config_value)
                VALUES (:taskId, :configKey, :configValue)
                """)
                .param("taskId", taskId)
                .param("configKey", key)
                .param("configValue", value == null ? "" : value)
                .update());
    }

    private void updateStatus(String id, String status, boolean starting) {
        String timeColumn = starting ? "last_start_time" : "last_stop_time";
        jdbcClient.sql("UPDATE sync_task SET task_status = :status, last_delay_ms = :delay, " + timeColumn + " = :time, update_time = :time WHERE id = :id")
                .param("id", id)
                .param("status", status)
                .param("delay", "RUNNING".equals(status) ? 0 : 0)
                .param("time", LocalDateTime.now())
                .update();
    }

    private void runFullSnapshot(TaskDetail detail, String reason) {
        var datasource = datasourceService.findByKey(detail.task().dataSourceKey())
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + detail.task().dataSourceKey()));
        String sql = detail.task().syncSql();
        if (!hasText(sql)) {
            throw new IllegalArgumentException("全量同步需要配置 syncSql");
        }
        LocalDateTime start = LocalDateTime.now();
        Path output = fullSyncOutputFile(detail.task().id(), start);
        long count = 0;
        long failures = 0;
        try {
            Files.createDirectories(output.getParent());
            String url = "jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                    .formatted(datasource.host(), datasource.port(), datasource.dbName());
            try (var connection = DriverManager.getConnection(url, datasource.username(), datasource.password());
                 var statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                 var writer = Files.newBufferedWriter(output, StandardCharsets.UTF_8)) {
                statement.setFetchSize(Integer.MIN_VALUE);
                try (ResultSet rs = statement.executeQuery(sql)) {
                    var meta = rs.getMetaData();
                    int columns = meta.getColumnCount();
                    while (rs.next()) {
                        writer.write(fullSyncRowJson(rs, meta, columns));
                        writer.newLine();
                        count++;
                    }
                }
            }
        } catch (Exception ex) {
            failures = 1;
            markFullSyncMetrics(detail.task().id(), count, failures, start, output);
            taskLogService.error(detail.task().id(), "全量同步失败: " + ex.getMessage());
            operationLogService.record("task", "full-sync-error", detail.task().id(), ex.getMessage());
            throw new IllegalStateException("全量同步失败: " + ex.getMessage(), ex);
        }
        markFullSyncMetrics(detail.task().id(), count, failures, start, output);
        String message = "全量同步完成，原因: %s，记录数: %d，文件: %s".formatted(reason, count, output.toAbsolutePath());
        taskLogService.info(detail.task().id(), message);
        operationLogService.record("task", "full-sync", detail.task().id(), message);
    }

    private void markFullSyncMetrics(String taskId, long count, long failures, LocalDateTime start, Path output) {
        long delay = java.time.Duration.between(start, LocalDateTime.now()).toMillis();
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

    private long nullableLong(String sql) {
        Number number = jdbcClient.sql(sql).query(Number.class).single();
        return number == null ? 0 : number.longValue();
    }

    private static Map<String, Object> successTarget(String message) {
        Map<String, Object> result = new HashMap<>();
        result.put("ok", true);
        result.put("message", message);
        return result;
    }

    private List<Map<String, Object>> runtimeFiles(String taskId, Map<String, Object> preview) {
        List<Map<String, Object>> files = new ArrayList<>();
        addRuntimeFile(files, "任务规格", runtimePathCandidates("generated/tasks/" + taskId + ".yml"));
        addRuntimeFile(files, "OuterAdapter", runtimePathCandidates("generated/adapter-plugins/" + taskId + ".outer.yml"));
        addRuntimeFile(files, "Destination", runtimePathCandidates("generated/adapter-plugins/" + taskId + ".destination"));
        String mappingDir = String.valueOf(preview.getOrDefault("mappingDir", ""));
        String mappingFile = String.valueOf(preview.getOrDefault("mappingFile", ""));
        if (hasText(mappingDir) && hasText(mappingFile) && !"logger".equals(mappingDir)) {
            addRuntimeFile(files, "Mapping", mappingPathCandidates(mappingDir, mappingFile));
        }
        return files;
    }

    private List<Path> runtimePathCandidates(String relativeRuntimePath) {
        List<Path> candidates = new ArrayList<>();
        for (Path base : runtimeBaseCandidates()) {
            addRuntimeCandidate(candidates, base.resolve(relativeRuntimePath));
        }
        return candidates;
    }

    private List<Path> mappingPathCandidates(String mappingDir, String mappingFile) {
        List<Path> candidates = new ArrayList<>();
        for (Path base : adapterConfCandidates()) {
            addRuntimeCandidate(candidates, base.resolve(mappingDir).resolve(mappingFile));
        }
        return candidates;
    }

    private List<Path> runtimeBaseCandidates() {
        List<Path> candidates = new ArrayList<>();
        addPathList(candidates, System.getProperty("canal-web.runtime.paths"));
        addPathList(candidates, System.getenv("CANAL_WEB_RUNTIME_PATHS"));
        addPath(candidates, System.getProperty("canal-web.runtime.home"));
        addPath(candidates, System.getenv("CANAL_WEB_RUNTIME_HOME"));
        String canalWebHome = firstText(Map.of(
                "system", configValue(System.getProperty("canal-web.home")),
                "env", configValue(System.getenv("CANAL_WEB_HOME"))
        ), "system", "env");
        if (hasText(canalWebHome)) {
            addPath(candidates, Path.of(canalWebHome).resolve("canal-runtime").toString());
        }
        Path cwd = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        addPath(candidates, projectPath("canal-runtime").toString());
        addPath(candidates, cwd.resolve("canal-runtime").toString());
        addPath(candidates, cwd.resolve("canal-web/canal-runtime").toString());
        addPath(candidates, cwd.getParent() == null ? null : cwd.getParent().resolve("canal-web/canal-runtime").toString());
        return candidates;
    }

    private List<Path> adapterConfCandidates() {
        List<Path> candidates = new ArrayList<>();
        addPathList(candidates, System.getProperty("canal-adapter.conf.paths"));
        addPathList(candidates, System.getenv("CANAL_ADAPTER_CONF_PATHS"));
        String adapterHome = firstText(Map.of(
                "system", configValue(System.getProperty("canal-adapter.home")),
                "env", configValue(System.getenv("CANAL_ADAPTER_HOME"))
        ), "system", "env");
        if (hasText(adapterHome)) {
            addPath(candidates, Path.of(adapterHome).resolve("conf").toString());
        }
        Path cwd = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        addPath(candidates, projectPath("canal-runtime/canal-adapter/conf").toString());
        addPath(candidates, cwd.resolve("canal-runtime/canal-adapter/conf").toString());
        addPath(candidates, cwd.resolve("canal-web/canal-runtime/canal-adapter/conf").toString());
        addPath(candidates, cwd.getParent() == null ? null : cwd.getParent().resolve("canal-web/canal-runtime/canal-adapter/conf").toString());
        String canalWebHome = firstText(Map.of(
                "system", configValue(System.getProperty("canal-web.home")),
                "env", configValue(System.getenv("CANAL_WEB_HOME"))
        ), "system", "env");
        if (hasText(canalWebHome)) {
            addPath(candidates, Path.of(canalWebHome).resolve("canal-runtime/canal-adapter/conf").toString());
        }
        return candidates;
    }

    private Path projectPath(String child) {
        String configured = firstText(Map.of(
                "system", configValue(System.getProperty("canal-web.project-dir")),
                "env", configValue(System.getenv("CANAL_WEB_PROJECT_DIR")),
                "runtime", configValue(canalRuntimeService.getProjectDir())
        ), "system", "env", "runtime");
        Path base = hasText(configured) ? Path.of(configured) : Path.of(System.getProperty("user.dir", "."));
        return base.toAbsolutePath().normalize().resolve(child);
    }

    private void addPathList(List<Path> candidates, String value) {
        if (!hasText(value)) {
            return;
        }
        for (String item : value.split(",")) {
            addPath(candidates, item);
        }
    }

    private void addPath(List<Path> candidates, String value) {
        if (hasText(value)) {
            addRuntimeCandidate(candidates, Path.of(value.trim()));
        }
    }

    private void addRuntimeCandidate(List<Path> candidates, Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        if (candidates.stream().noneMatch(existing -> existing.equals(absolute))) {
            candidates.add(absolute);
        }
    }

    private void addRuntimeFile(List<Map<String, Object>> files, String name, List<Path> paths) {
        List<Map<String, Object>> candidates = paths.stream().map(this::runtimeFileCandidate).toList();
        Map<String, Object> active = candidates.stream()
                .filter(item -> Boolean.TRUE.equals(item.get("exists")))
                .findFirst()
                .orElse(candidates.isEmpty() ? runtimeFileCandidate(Path.of("")) : candidates.get(0));
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", name);
        item.put("path", active.get("path"));
        item.put("activePath", active.get("path"));
        item.put("exists", active.get("exists"));
        item.put("size", active.get("size"));
        item.put("candidates", candidates);
        files.add(item);
    }

    private Map<String, Object> runtimeFileCandidate(Path path) {
        Path absolute = path.toAbsolutePath().normalize();
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("path", absolute.toString());
        boolean exists = Files.isRegularFile(absolute);
        item.put("exists", exists);
        try {
            item.put("size", exists ? Files.size(absolute) : 0);
        } catch (Exception ex) {
            item.put("size", 0);
        }
        return item;
    }

    private Map<String, Object> adapterDestinationStatus(String destination) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("destination", destination);
        result.put("reachable", false);
        result.put("online", false);
        result.put("message", "未检查");
        if (!hasText(destination)) {
            result.put("message", "destination 为空");
            return result;
        }
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:18083/destinations")).GET().build();
            String body = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body();
            result.put("reachable", true);
            result.put("online", body.contains("\"destination\":\"" + destination + "\"") && body.contains("\"status\":\"on\""));
            result.put("message", body);
        } catch (Exception ex) {
            result.put("message", ex.getMessage());
        }
        return result;
    }

    private static String runtimeSuggestion(String status, com.openclaw.canalweb.domain.SyncTaskLog lastError,
                                            Map<String, Object> preview, Map<String, Object> destinationStatus,
                                            List<Map<String, Object>> runtimeFiles) {
        if (lastError != null) {
            return "存在最近错误，优先查看最近错误和 Adapter 日志";
        }
        if (preview.containsKey("error")) {
            return "运行配置生成失败，先处理任务配置或数据源配置";
        }
        boolean missingFile = runtimeFiles != null && runtimeFiles.stream().anyMatch(item -> !Boolean.TRUE.equals(item.get("exists")));
        if ("RUNNING".equalsIgnoreCase(status) && missingFile) {
            return "任务标记为运行中，但 runtime 文件不完整，建议刷新配置或重启任务";
        }
        if ("RUNNING".equalsIgnoreCase(status) && destinationStatus != null && !Boolean.TRUE.equals(destinationStatus.get("online"))) {
            return "任务标记为运行中，但 Adapter destination 未在线，检查 Canal Adapter 状态";
        }
        if ("RUNNING".equalsIgnoreCase(status)) {
            return "任务已运行，观察最近日志、目标端写入和 Canal Adapter 状态";
        }
        return "任务未运行，启动前先执行自检并确认资源准备已完成";
    }

    private static void testJdbc(Map<String, String> config) throws Exception {
        String url = config.get("jdbcUrl");
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("缺少 JDBC URL");
        }
        String driver = config.get("driverClassName");
        if (driver != null && !driver.isBlank()) {
            Class.forName(driver);
        }
        try (var ignored = DriverManager.getConnection(url, config.getOrDefault("username", ""), config.getOrDefault("password", ""))) {
            // Connection success is enough.
        }
    }

    private static void testEndpoint(String endpoints) throws Exception {
        String first = endpoints == null ? "" : endpoints.split(",")[0].trim();
        if (first.isBlank()) {
            throw new IllegalArgumentException("缺少目标地址");
        }
        String host;
        int port;
        if (first.contains("://")) {
            URI uri = URI.create(first);
            host = uri.getHost();
            port = uri.getPort();
            if (port < 0) {
                port = "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
            }
        } else {
            String[] parts = first.split(":");
            host = parts[0];
            port = parts.length > 1 ? intValue(parts[1], 80) : 80;
        }
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("目标地址格式不正确: " + first);
        }
        testHostPort(host, port);
    }

    private static void testHostPort(String host, int port) throws Exception {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 3000);
        }
    }

    public static boolean requiresFullSync(String syncMode) {
        String mode = normalizeSyncMode(syncMode);
        return "FULL".equals(mode) || "FULL_INCREMENTAL".equals(mode);
    }

    private static boolean requiresIncremental(String syncMode) {
        String mode = normalizeSyncMode(syncMode);
        return "INCREMENTAL".equals(mode) || "FULL_INCREMENTAL".equals(mode);
    }

    private static boolean isSupportedSyncMode(String syncMode) {
        return Set.of("INCREMENTAL", "FULL", "FULL_INCREMENTAL").contains(normalizeSyncMode(syncMode));
    }

    private static String normalizeSyncMode(String syncMode) {
        String mode = syncMode == null ? "" : syncMode.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return isSupportedRawMode(mode) ? mode : "INCREMENTAL";
    }

    private static String normalizeTargetType(String targetType) {
        String type = targetType == null ? "" : targetType.trim().toUpperCase(java.util.Locale.ROOT).replace('-', '_');
        return switch (type) {
            case "ELASTICSEARCH" -> "ES";
            case "POSTGRESQL" -> "PGSQL";
            case "RDB" -> "MYSQL";
            case "PULSARMQ" -> "PULSAR";
            case "" -> "LOGGER";
            default -> type;
        };
    }

    private static boolean isSupportedRawMode(String mode) {
        return "INCREMENTAL".equals(mode) || "FULL".equals(mode) || "FULL_INCREMENTAL".equals(mode);
    }

    private static Path fullSyncOutputFile(String taskId, LocalDateTime start) {
        String fileName = "%s-%s.jsonl".formatted(taskId, start.toString().replace(":", "").replace(".", ""));
        return Path.of("canal-runtime", "generated", "full-sync", fileName);
    }

    private static String fullSyncRowJson(ResultSet rs, java.sql.ResultSetMetaData meta, int columns) throws Exception {
        StringBuilder builder = new StringBuilder("{");
        for (int index = 1; index <= columns; index++) {
            if (index > 1) {
                builder.append(',');
            }
            builder.append('"').append(jsonEscape(meta.getColumnLabel(index))).append('"').append(':');
            Object value = rs.getObject(index);
            if (value == null) {
                builder.append("null");
            } else if (value instanceof Number || value instanceof Boolean) {
                builder.append(value);
            } else {
                builder.append('"').append(jsonEscape(String.valueOf(value))).append('"');
            }
        }
        return builder.append('}').toString();
    }

    private static String jsonEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private static int intValue(String value, int defaultValue) {
        try {
            return value == null || value.isBlank() ? defaultValue : Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            return defaultValue;
        }
    }

    private static void addCheck(List<Map<String, Object>> checks, String name, boolean ok, String message, String detail) {
        Map<String, Object> check = new LinkedHashMap<>();
        check.put("name", name);
        check.put("ok", ok);
        check.put("severity", ok ? "INFO" : checkSeverity(name));
        check.put("message", message == null ? "" : message);
        check.put("detail", detail == null ? "" : detail);
        checks.add(check);
    }

    private static String checkSeverity(String name) {
        if (name == null) {
            return "WARN";
        }
        if (name.contains("调度") || name.contains("消费组") || name.contains("Value 字段") || name.contains("Mapping JSON")) {
            return "WARN";
        }
        return "BLOCKER";
    }

    private static long severityCount(List<Map<String, Object>> checks, String severity) {
        return checks.stream()
                .filter(item -> severity.equals(item.get("severity")))
                .count();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String value(Object value) {
        return value == null || String.valueOf(value).isBlank() ? "-" : String.valueOf(value);
    }

    private static String valueOrDefault(String value, String defaultValue) {
        return hasText(value) ? value : defaultValue;
    }

    private static String configValue(String value) {
        return hasText(value) ? value.trim() : "";
    }

    private static String sqlSummary(String sql) {
        if (!hasText(sql)) {
            return "未配置";
        }
        String compact = sql.replaceAll("\\s+", " ").trim();
        return compact.length() > 120 ? compact.substring(0, 120) + "..." : compact;
    }

    private static Map<String, String> targetConfigMap(List<TaskTargetConfig> configs) {
        Map<String, String> result = new HashMap<>();
        for (TaskTargetConfig config : configs) {
            result.put(config.configKey(), config.configValue());
        }
        return result;
    }

    private static void requireText(Map<String, String> config, String key, String message) {
        if (!hasText(config.get(key))) {
            throw new IllegalArgumentException(message);
        }
    }

    private static String firstText(Map<String, String> config, String... keys) {
        for (String key : keys) {
            String value = config.get(key);
            if (hasText(value)) {
                return value.trim();
            }
        }
        return "";
    }

    private static String upper(String value) {
        return value == null ? "" : value.trim().toUpperCase(java.util.Locale.ROOT);
    }

    private static boolean nonNegativeInteger(String value) {
        if (!hasText(value)) {
            return true;
        }
        try {
            return Integer.parseInt(value) >= 0;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private static String valueOrZero(String value) {
        return hasText(value) ? value : "0";
    }

    private static Set<String> sourceFields(List<TaskFieldMapping> mappings) {
        Set<String> fields = new HashSet<>();
        if (mappings == null) {
            return fields;
        }
        for (TaskFieldMapping mapping : mappings) {
            if (hasText(mapping.sourceField())) {
                fields.add(mapping.sourceField());
            }
        }
        return fields;
    }

    private static boolean templateFieldsExist(String template, Set<String> sourceFields) {
        if (!hasText(template)) {
            return false;
        }
        Matcher matcher = TEMPLATE_FIELD_PATTERN.matcher(template);
        boolean found = false;
        while (matcher.find()) {
            found = true;
            if (!sourceFields.contains(matcher.group(1))) {
                return false;
            }
        }
        return found;
    }

    private static boolean rowKeyFieldsExist(String rowKey, Set<String> sourceFields) {
        if (!hasText(rowKey)) {
            return false;
        }
        for (String field : rowKey.split(",")) {
            String normalized = field.trim();
            if (normalized.isBlank() || !sourceFields.contains(normalized)) {
                return false;
            }
        }
        return true;
    }

    private static List<String> duplicateFields(List<String> fields) {
        Set<String> seen = new HashSet<>();
        Set<String> duplicates = new LinkedHashSet<>();
        for (String field : fields) {
            if (!hasText(field)) {
                continue;
            }
            if (!seen.add(field)) {
                duplicates.add(field);
            }
        }
        return new ArrayList<>(duplicates);
    }

    private static boolean validJsonObject(String value) {
        if (!hasText(value)) {
            return true;
        }
        try {
            Object parsed = OBJECT_MAPPER.readValue(value, Object.class);
            return parsed instanceof Map<?, ?>;
        } catch (Exception ex) {
            return false;
        }
    }

    private static String normalizeFieldOptions(String value, int rowNo) {
        if (!hasText(value)) {
            return "";
        }
        try {
            Object parsed = OBJECT_MAPPER.readValue(value, Object.class);
            if (!(parsed instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("第 " + rowNo + " 行字段属性参数必须是 JSON Object");
            }
            return OBJECT_MAPPER.writeValueAsString(parsed);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("第 " + rowNo + " 行字段属性参数不是合法 JSON: " + ex.getMessage(), ex);
        }
    }

    private static String normalizeFieldType(String fieldType, String sourceField, String targetField) {
        String normalized = upper(fieldType);
        if (normalized.isBlank()) {
            normalized = inferFieldType(hasText(sourceField) ? sourceField : targetField);
        }
        return switch (normalized) {
            case "VARCHAR", "CHAR", "TEXT" -> "STRING";
            case "INTEGER", "TINYINT", "SMALLINT" -> "INT";
            case "BIGINT" -> "LONG";
            case "TIMESTAMP" -> "DATETIME";
            case "BOOL" -> "BOOLEAN";
            default -> normalized;
        };
    }

    private static String inferFieldType(String field) {
        String lower = field == null ? "" : field.toLowerCase(java.util.Locale.ROOT);
        if (lower.isBlank()) {
            return "";
        }
        if ("id".equals(lower) || lower.endsWith("_id")) {
            return "LONG";
        }
        if (lower.contains("time") || lower.endsWith("_at") || lower.contains("date")) {
            return "DATETIME";
        }
        if (lower.contains("amount") || lower.contains("price") || lower.contains("rate")) {
            return "DECIMAL";
        }
        if (lower.contains("count") || lower.contains("num") || lower.contains("status")) {
            return "INT";
        }
        if (lower.startsWith("is_") || lower.startsWith("has_")) {
            return "BOOLEAN";
        }
        return "STRING";
    }

    private static boolean isPrimaryKeyField(String field) {
        String lower = field == null ? "" : field.toLowerCase(java.util.Locale.ROOT);
        return "id".equals(lower) || lower.endsWith("_id");
    }

    private static String optionText(TaskFieldMapping mapping, String key) {
        if (mapping == null || !hasText(mapping.fieldOptions()) || !hasText(key)) {
            return "";
        }
        try {
            Object value = OBJECT_MAPPER.readValue(mapping.fieldOptions(), Map.class).get(key);
            return value == null ? "" : String.valueOf(value);
        } catch (Exception ex) {
            return "";
        }
    }

    private static String templateFieldMessage(String template) {
        if (!hasText(template)) {
            return "未配置模板";
        }
        List<String> fields = new ArrayList<>();
        Matcher matcher = TEMPLATE_FIELD_PATTERN.matcher(template);
        while (matcher.find()) {
            fields.add(matcher.group(1));
        }
        return fields.isEmpty() ? "未发现 {字段名}" : "引用字段 " + fields;
    }

    private static boolean validMappingJson(String mapping) {
        if (!hasText(mapping)) {
            return false;
        }
        try {
            OBJECT_MAPPER.readTree(mapping);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static CommandResult redisPing(Map<String, String> config) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(executable("redis-cli", "canal-web.redis-cli", "CANAL_WEB_REDIS_CLI",
                "/opt/homebrew/bin/redis-cli", "/usr/local/bin/redis-cli", "/opt/local/bin/redis-cli", "/usr/bin/redis-cli", "/bin/redis-cli"));
        command.add("-h");
        command.add(valueOrDefault(firstText(config, "host"), "127.0.0.1"));
        command.add("-p");
        command.add(valueOrDefault(firstText(config, "port"), "6379"));
        command.add("-n");
        command.add(valueOrDefault(firstText(config, "database"), "0"));
        String password = firstText(config, "password");
        if (hasText(password)) {
            command.add("-a");
            command.add(password);
        }
        command.add("PING");
        return runCommand(command);
    }

    private static CommandResult kafkaTopics(Map<String, String> config) throws Exception {
        String command = executable("kafka-topics", "canal-web.kafka-topics", "CANAL_WEB_KAFKA_TOPICS",
                "/opt/homebrew/bin/kafka-topics",
                "/opt/homebrew/opt/kafka/bin/kafka-topics",
                "/usr/local/bin/kafka-topics",
                "/usr/local/opt/kafka/bin/kafka-topics",
                "/opt/local/bin/kafka-topics",
                "/usr/bin/kafka-topics",
                "/opt/kafka/bin/kafka-topics",
                "/usr/local/kafka/bin/kafka-topics");
        return runCommand(List.of(command, "--bootstrap-server",
                valueOrDefault(firstText(config, "bootstrapServers", "servers"), "127.0.0.1:9092"), "--list"), 20);
    }

    private static boolean topicExists(String output, String topic) {
        if (!hasText(output) || !hasText(topic)) {
            return false;
        }
        return output.lines().map(String::trim).anyMatch(topic::equals);
    }

    private static CommandResult runCommand(List<String> command) throws Exception {
        return runCommand(command, 5);
    }

    private static CommandResult runCommand(List<String> command, int timeoutSeconds) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        String javaHome = javaHomeForCli();
        if (hasText(javaHome)) {
            builder.environment().put("JAVA_HOME", javaHome);
        }
        builder.environment().put("PATH", cliPath(builder.environment().get("PATH")));
        Process process = builder.start();
        boolean finished = process.waitFor(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            return new CommandResult(124, "命令执行超时: " + String.join(" ", command));
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        return new CommandResult(process.exitValue(), output);
    }

    private static String executable(String fallback, String systemProperty, String envName, String... candidates) {
        String configured = firstText(Map.of(
                "system", configValue(System.getProperty(systemProperty)),
                "env", configValue(System.getenv(envName))
        ), "system", "env");
        if (hasText(configured)) {
            return configured;
        }
        for (String candidate : candidates) {
            if (Files.isExecutable(Path.of(candidate))) {
                return candidate;
            }
        }
        return fallback;
    }

    private static String cliPath(String current) {
        String configured = firstText(Map.of(
                "system", configValue(System.getProperty("canal-web.cli.path")),
                "env", configValue(System.getenv("CANAL_WEB_CLI_PATH"))
        ), "system", "env");
        String base = hasText(configured) ? configured : DEFAULT_CLI_PATH;
        return hasText(current) ? base + ":" + current : base;
    }

    private static String javaHomeForCli() {
        String configured = firstText(Map.of(
                "system", configValue(System.getProperty("canal-web.java-home")),
                "env", configValue(System.getenv("CANAL_WEB_JAVA_HOME"))
        ), "system", "env");
        if (hasText(configured)) {
            return configured;
        }
        List<String> candidates = new ArrayList<>(List.of(
                "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
                "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home",
                "/usr/lib/jvm/java-17-openjdk",
                "/usr/lib/jvm/java-17-openjdk-amd64",
                "/usr/lib/jvm/temurin-17-jdk",
                "/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
                "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
        ));
        String javaHome = System.getenv("JAVA_HOME");
        if (hasText(javaHome)) {
            candidates.add(javaHome);
        }
        return candidates.stream()
                .filter(path -> Files.isExecutable(Path.of(path, "bin/java")))
                .findFirst()
                .orElse("");
    }

    private Map<String, String> sourceColumnTypes(TaskDetail detail, Map<String, String> config) {
        String table = valueOrDefault(firstText(config, "sourceTable"), sourceTableName(detail.task().syncSql()));
        var datasource = datasourceService.findByKey(detail.task().dataSourceKey()).orElse(null);
        if (datasource == null || !hasText(table)) {
            return Map.of();
        }
        String normalizedTable = table.contains(".") ? table.substring(table.indexOf('.') + 1) : table;
        try (Connection connection = DriverManager.getConnection(jdbcUrl(datasource), datasource.username(), datasource.password());
             ResultSet rs = connection.getMetaData().getColumns(datasource.dbName(), null, normalizedTable, "%")) {
            Map<String, String> types = new LinkedHashMap<>();
            while (rs.next()) {
                types.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
            }
            return types;
        } catch (Exception ex) {
            return Map.of();
        }
    }

    private List<Map<String, Object>> fieldTypeChecks(TaskDetail detail, Map<String, String> config) {
        List<Map<String, Object>> checks = new ArrayList<>();
        Map<String, String> sourceTypes = sourceColumnTypes(detail, config);
        if (sourceTypes.isEmpty() || detail.fieldMappings().isEmpty()) {
            addCheck(checks, "源字段类型识别", false, "未识别到源字段类型", "确认 sourceTable 或 sync_sql 能定位到真实源表");
            return checks;
        }
        List<String> missing = detail.fieldMappings().stream()
                .map(TaskFieldMapping::sourceField)
                .filter(field -> hasText(field) && !sourceTypes.containsKey(field))
                .toList();
        addCheck(checks, "源字段覆盖", missing.isEmpty(), missing.isEmpty() ? "源字段均存在" : "缺少 " + missing,
                "字段映射的源字段必须能从源表 metadata 中找到");
        addCheck(checks, "源字段类型", true, sourceTypes.toString(), "资源准备脚本会按源字段类型推断目标类型");
        return checks;
    }

    private static String jdbcUrl(com.openclaw.canalweb.domain.DatasourceConfig datasource) {
        return "jdbc:mysql://%s:%d/%s?connectTimeout=5000&socketTimeout=10000&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                .formatted(datasource.host(), datasource.port(), datasource.dbName());
    }

    private static String sourceTableName(String sql) {
        if (!hasText(sql)) {
            return "";
        }
        String lower = sql.toLowerCase(java.util.Locale.ROOT);
        int from = lower.indexOf(" from ");
        if (from < 0) {
            return "";
        }
        String tail = sql.substring(from + 6).trim();
        return tail.isBlank() ? "" : tail.split("\\s+")[0].replace("`", "");
    }

    private static String sqlColumns(List<TaskFieldMapping> mappings, Map<String, String> sourceTypes, String pk, boolean postgres) {
        List<TaskFieldMapping> safeMappings = enabledMappings(mappings);
        if (safeMappings.isEmpty()) {
            safeMappings = List.of(defaultMapping());
        }
        List<String> lines = new ArrayList<>();
        for (TaskFieldMapping mapping : safeMappings) {
            String field = hasText(mapping.targetField()) ? mapping.targetField() : mapping.sourceField();
            String type = sqlType(field, sourceTypes.get(mapping.sourceField()), mapping, postgres);
            boolean primary = Boolean.TRUE.equals(mapping.primaryKey()) || field.equals(pk) || (!hasText(pk) && "id".equals(field));
            String suffix = primary ? " PRIMARY KEY" : "";
            if (!primary && Boolean.FALSE.equals(mapping.nullableField())) {
                suffix += " NOT NULL";
            }
            lines.add("  " + quoteIdentifier(field, postgres) + " " + type + suffix);
        }
        return String.join(",\n", lines);
    }

    private static String sqlType(String field, String sourceType, TaskFieldMapping mapping, boolean postgres) {
        String explicit = optionText(mapping, "rdbType");
        if (hasText(explicit)) {
            return explicit;
        }
        String mapped = mappedFieldTypeSql(mapping == null ? "" : mapping.fieldType(), postgres);
        if (hasText(mapped)) {
            return mapped;
        }
        String source = sourceType == null ? "" : sourceType.toUpperCase(java.util.Locale.ROOT);
        if (source.contains("BIGINT")) return "bigint";
        if (source.contains("INT")) return postgres ? "integer" : "int";
        if (source.contains("DECIMAL") || source.contains("NUMERIC")) return postgres ? "numeric(20,6)" : "decimal(20,6)";
        if (source.contains("DOUBLE")) return postgres ? "double precision" : "double";
        if (source.contains("FLOAT")) return "float";
        if (source.contains("DATETIME") || source.contains("TIMESTAMP")) return postgres ? "timestamp" : "datetime";
        if (source.equals("DATE")) return "date";
        if (source.contains("TEXT") || source.contains("JSON")) return postgres ? "text" : "text";
        String lower = field == null ? "" : field.toLowerCase(java.util.Locale.ROOT);
        if ("id".equals(lower) || lower.endsWith("_id")) {
            return postgres ? "bigint" : "bigint";
        }
        if (lower.contains("status") || lower.contains("count") || lower.contains("num")) {
            return postgres ? "integer" : "int";
        }
        if (lower.contains("time") || lower.endsWith("_at") || lower.contains("date")) {
            return postgres ? "timestamp" : "datetime";
        }
        return postgres ? "varchar(255)" : "varchar(255)";
    }

    private static String mappedFieldTypeSql(String fieldType, boolean postgres) {
        return switch (upper(fieldType)) {
            case "STRING" -> postgres ? "varchar(255)" : "varchar(255)";
            case "INT", "INTEGER" -> postgres ? "integer" : "int";
            case "LONG", "BIGINT" -> "bigint";
            case "DECIMAL", "NUMERIC" -> postgres ? "numeric(20,6)" : "decimal(20,6)";
            case "DATE" -> "date";
            case "DATETIME", "TIMESTAMP" -> postgres ? "timestamp" : "datetime";
            case "BOOLEAN", "BOOL" -> postgres ? "boolean" : "tinyint(1)";
            case "JSON", "TEXT" -> postgres ? "text" : "text";
            case "BINARY" -> postgres ? "bytea" : "blob";
            default -> "";
        };
    }

    private static String quoteIdentifier(String field, boolean postgres) {
        String safe = valueOrDefault(field, "field").replace("`", "").replace("\"", "");
        return postgres ? safe : "`" + safe + "`";
    }

    private static String generatedEsMapping(List<TaskFieldMapping> mappings, Map<String, String> sourceTypes) {
        List<TaskFieldMapping> safeMappings = enabledMappings(mappings);
        if (safeMappings.isEmpty()) {
            safeMappings = List.of(defaultMapping());
        }
        String properties = safeMappings.stream()
                .map(mapping -> {
                    String field = hasText(mapping.targetField()) ? mapping.targetField() : mapping.sourceField();
                    return "      \"" + field + "\": { \"type\": \"" + esType(field, sourceTypes.get(mapping.sourceField()), mapping) + "\" }";
                })
                .reduce((left, right) -> left + ",\n" + right)
                .orElse("      \"id\": { \"type\": \"long\" }");
        return "{\n    \"properties\": {\n" + properties + "\n    }\n  }";
    }

    private static String esType(String field, String sourceType, TaskFieldMapping mapping) {
        String explicit = optionText(mapping, "esType");
        if (hasText(explicit)) {
            return explicit;
        }
        String mapped = mappedFieldTypeEs(mapping == null ? "" : mapping.fieldType());
        if (hasText(mapped)) {
            return mapped;
        }
        String source = sourceType == null ? "" : sourceType.toUpperCase(java.util.Locale.ROOT);
        if (source.contains("BIGINT")) return "long";
        if (source.contains("INT")) return "integer";
        if (source.contains("DECIMAL") || source.contains("NUMERIC") || source.contains("DOUBLE") || source.contains("FLOAT")) return "double";
        if (source.contains("DATE") || source.contains("TIME")) return "date";
        if (source.contains("TEXT") || source.contains("JSON")) return "text";
        String lower = field == null ? "" : field.toLowerCase(java.util.Locale.ROOT);
        if ("id".equals(lower) || lower.endsWith("_id")) {
            return "long";
        }
        if (lower.contains("status") || lower.contains("count") || lower.contains("num")) {
            return "integer";
        }
        if (lower.contains("time") || lower.endsWith("_at") || lower.contains("date")) {
            return "date";
        }
        return "keyword";
    }

    private static String mappedFieldTypeEs(String fieldType) {
        return switch (upper(fieldType)) {
            case "STRING" -> "keyword";
            case "INT", "INTEGER" -> "integer";
            case "LONG", "BIGINT" -> "long";
            case "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT" -> "double";
            case "DATE", "DATETIME", "TIMESTAMP" -> "date";
            case "BOOLEAN", "BOOL" -> "boolean";
            case "JSON" -> "object";
            case "TEXT" -> "text";
            default -> "";
        };
    }

    private static Connection openJdbc(Map<String, String> config) throws Exception {
        String driver = config.get("driverClassName");
        if (hasText(driver)) {
            Class.forName(driver);
        }
        return DriverManager.getConnection(config.get("jdbcUrl"), config.getOrDefault("username", ""),
                config.getOrDefault("password", ""));
    }

    private static Set<String> tableColumns(Connection connection, String database, String table) throws Exception {
        String normalizedTable = table == null ? "" : table.replace("`", "").trim();
        String schema = hasText(database) ? database.replace("`", "").trim() : null;
        if (normalizedTable.contains(".")) {
            String[] parts = normalizedTable.split("\\.", 2);
            schema = parts[0];
            normalizedTable = parts[1];
        }
        Set<String> columns = new HashSet<>();
        var meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(connection.getCatalog(), schema, normalizedTable, "%")) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        if (columns.isEmpty() && hasText(schema)) {
            try (ResultSet rs = meta.getColumns(null, schema, normalizedTable, "%")) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME"));
                }
            }
        }
        return columns;
    }

    private static Set<String> missingTargetColumns(List<TaskFieldMapping> mappings, Set<String> columns) {
        Set<String> missing = new HashSet<>();
        if (mappings == null) {
            return missing;
        }
        for (TaskFieldMapping mapping : enabledMappings(mappings)) {
            if (hasText(mapping.targetField()) && !columns.contains(mapping.targetField())) {
                missing.add(mapping.targetField());
            }
        }
        return missing;
    }

    private static List<TaskFieldMapping> enabledMappings(List<TaskFieldMapping> mappings) {
        if (mappings == null) {
            return List.of();
        }
        return mappings.stream()
                .filter(item -> item.enabled() == null || Boolean.TRUE.equals(item.enabled()))
                .toList();
    }

    private static TaskFieldMapping defaultMapping() {
        return new TaskFieldMapping(null, "", "id", "id", "", true, false, true, "", "", "", "");
    }

    private static String qualifiedName(String database, String table) {
        if (hasText(database) && hasText(table) && !table.contains(".")) {
            return database + "." + table;
        }
        return value(table);
    }

    private static int esHeadStatus(String hosts, String index) throws Exception {
        String first = hosts == null ? "127.0.0.1:9200" : hosts.split(",")[0].trim();
        String base = first.startsWith("http://") || first.startsWith("https://") ? first : "http://" + first;
        URI uri = URI.create(base.replaceAll("/+$", "") + "/" + index);
        HttpRequest request = HttpRequest.newBuilder(uri).method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private Map<String, ?> taskParams(String id, TaskSaveRequest request) {
        return Map.of(
                "id", id,
                "taskName", request.taskName(),
                "description", request.description() == null ? "" : request.description(),
                "dataSourceKey", request.dataSourceKey(),
                "syncSql", request.syncSql() == null ? "" : request.syncSql(),
                "targetType", normalizeTargetType(request.targetType()),
                "syncMode", normalizeSyncMode(request.syncMode()),
                "cronExpression", request.cronExpression() == null ? "" : request.cronExpression(),
                "batchSize", request.batchSize()
        );
    }

    private String nextCloneName(String sourceName) {
        String baseName = (sourceName == null || sourceName.isBlank() ? "未命名任务" : sourceName) + " 副本";
        boolean exists = jdbcClient.sql("SELECT COUNT(*) FROM sync_task WHERE task_name = :name")
                .param("name", baseName)
                .query(Long.class)
                .single() > 0;
        if (!exists) {
            return baseName;
        }
        for (int i = 2; i < 1000; i++) {
            String candidate = baseName + " " + i;
            boolean candidateExists = jdbcClient.sql("SELECT COUNT(*) FROM sync_task WHERE task_name = :name")
                    .param("name", candidate)
                    .query(Long.class)
                    .single() > 0;
            if (!candidateExists) {
                return candidate;
            }
        }
        return baseName + " " + System.currentTimeMillis();
    }

    private static String stripTrailingSemicolon(String sql) {
        String value = sql.trim();
        while (value.endsWith(";")) {
            value = value.substring(0, value.length() - 1).trim();
        }
        return value;
    }
}
