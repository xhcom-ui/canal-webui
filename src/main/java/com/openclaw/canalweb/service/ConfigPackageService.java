package com.openclaw.canalweb.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openclaw.canalweb.dto.CanalRuntimeSettingRequest;
import com.openclaw.canalweb.dto.DatasourceSaveRequest;
import com.openclaw.canalweb.dto.TaskSaveRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@Service
public class ConfigPackageService {
    private final DatasourceService datasourceService;
    private final SyncTaskService syncTaskService;
    private final CanalRuntimeSettingService canalRuntimeSettingService;
    private final CanalRuntimeService canalRuntimeService;
    private final OperationLogService operationLogService;
    private final ObjectMapper objectMapper;

    public ConfigPackageService(DatasourceService datasourceService, SyncTaskService syncTaskService,
                                CanalRuntimeSettingService canalRuntimeSettingService, CanalRuntimeService canalRuntimeService,
                                OperationLogService operationLogService, ObjectMapper objectMapper) {
        this.datasourceService = datasourceService;
        this.syncTaskService = syncTaskService;
        this.canalRuntimeSettingService = canalRuntimeSettingService;
        this.canalRuntimeService = canalRuntimeService;
        this.operationLogService = operationLogService;
        this.objectMapper = objectMapper;
    }

    public Map<String, Object> exportPackage() {
        List<DatasourceSaveRequest> datasources = datasourceService.list().stream()
                .map(DatasourceService::toSaveRequest)
                .toList();
        List<TaskSaveRequest> tasks = syncTaskService.list().stream()
                .map(task -> syncTaskService.exportRequest(task.id()))
                .toList();
        CanalRuntimeSettingRequest runtime = CanalRuntimeSettingService.toRequest(canalRuntimeSettingService.get());
        return Map.of(
                "schemaVersion", 1,
                "exportTime", LocalDateTime.now().toString(),
                "runtime", runtime,
                "datasources", datasources,
                "tasks", tasks
        );
    }

    @Transactional
    public Map<String, Object> importPackage(Map<String, Object> request) {
        try {
            CanalRuntimeSettingRequest runtime = objectMapper.convertValue(request.get("runtime"), CanalRuntimeSettingRequest.class);
            if (runtime != null) {
                canalRuntimeSettingService.save(runtime);
            }
            List<?> datasourceItems = request.get("datasources") instanceof List<?> list ? list : List.of();
            int datasourceCount = 0;
            for (Object item : datasourceItems) {
                datasourceService.save(objectMapper.convertValue(item, DatasourceSaveRequest.class));
                datasourceCount++;
            }
            List<?> taskItems = request.get("tasks") instanceof List<?> list ? list : List.of();
            int taskCount = 0;
            for (Object item : taskItems) {
                syncTaskService.save(objectMapper.convertValue(item, TaskSaveRequest.class));
                taskCount++;
            }
            canalRuntimeService.refreshRuntime(false);
            operationLogService.record("config-package", "import", "global",
                    "配置包导入完成: datasources=" + datasourceCount + ", tasks=" + taskCount);
            return Map.of("datasourceCount", datasourceCount, "taskCount", taskCount);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("配置包导入失败: " + ex.getMessage(), ex);
        }
    }
}
