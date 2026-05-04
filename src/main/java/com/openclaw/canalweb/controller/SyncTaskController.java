package com.openclaw.canalweb.controller;

import com.openclaw.canalweb.common.Result;
import com.openclaw.canalweb.domain.SyncTask;
import com.openclaw.canalweb.dto.TaskDetail;
import com.openclaw.canalweb.dto.FieldMappingRequest;
import com.openclaw.canalweb.dto.TaskSaveRequest;
import com.openclaw.canalweb.dto.TargetTestRequest;
import com.openclaw.canalweb.service.FullSyncService;
import com.openclaw.canalweb.service.SyncTaskService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/task")
public class SyncTaskController {
    private final SyncTaskService syncTaskService;
    private final FullSyncService fullSyncService;

    public SyncTaskController(SyncTaskService syncTaskService, FullSyncService fullSyncService) {
        this.syncTaskService = syncTaskService;
        this.fullSyncService = fullSyncService;
    }

    @GetMapping("/list")
    public Result<List<SyncTask>> list(@RequestParam(required = false) String status) {
        return Result.success(syncTaskService.list(status));
    }

    @PostMapping("/save")
    public Result<TaskDetail> save(@Valid @RequestBody TaskSaveRequest request) {
        return Result.success(syncTaskService.save(request));
    }

    @PostMapping("/target-test")
    public Result<Map<String, Object>> targetTest(@Valid @RequestBody TargetTestRequest request) {
        return Result.success(syncTaskService.testTarget(request));
    }

    @PostMapping("/sql-preview")
    public Result<Map<String, Object>> previewSql(@Valid @RequestBody TaskSaveRequest request,
                                                  @RequestParam(defaultValue = "20") int limit) {
        return Result.success(syncTaskService.previewSql(request, limit));
    }

    @PostMapping("/field-mapping/validate")
    public Result<Map<String, Object>> validateMappings(@RequestBody List<FieldMappingRequest> mappings) {
        return Result.success(syncTaskService.validateMappings(mappings));
    }

    @PostMapping("/clone/{id}")
    public Result<TaskDetail> cloneTask(@PathVariable String id) {
        return Result.success(syncTaskService.cloneTask(id));
    }

    @PostMapping("/start/{id}")
    public Result<String> start(@PathVariable String id) {
        syncTaskService.start(id);
        return Result.success("任务启动成功");
    }

    @PostMapping("/stop/{id}")
    public Result<String> stop(@PathVariable String id) {
        syncTaskService.stop(id);
        return Result.success("任务停止成功");
    }

    @PostMapping("/full-sync/{id}")
    public Result<String> fullSync(@PathVariable String id) {
        var file = fullSyncService.execute(id, "manual");
        return Result.success(file.toAbsolutePath().toString());
    }

    @GetMapping("/full-sync/preview/{id}")
    public Result<Map<String, Object>> fullSyncPreview(@PathVariable String id,
                                                       @RequestParam(defaultValue = "20") int limit) {
        return Result.success(fullSyncService.preview(id, limit));
    }

    @PostMapping("/batch/start")
    public Result<String> batchStart(@RequestBody List<String> ids) {
        syncTaskService.batchStart(ids);
        return Result.success("批量启动成功");
    }

    @PostMapping("/batch/stop")
    public Result<String> batchStop(@RequestBody List<String> ids) {
        syncTaskService.batchStop(ids);
        return Result.success("批量停止成功");
    }

    @DeleteMapping("/{id}")
    public Result<Void> delete(@PathVariable String id) {
        syncTaskService.delete(id);
        return Result.success();
    }

    @GetMapping("/detail/{id}")
    public Result<TaskDetail> detail(@PathVariable String id) {
        return Result.success(syncTaskService.detail(id));
    }

    @GetMapping("/preview/{id}")
    public Result<Map<String, Object>> preview(@PathVariable String id) {
        return Result.success(syncTaskService.previewConfig(id));
    }

    @GetMapping("/resource-plan/{id}")
    public Result<Map<String, Object>> resourcePlan(@PathVariable String id) {
        return Result.success(syncTaskService.resourcePlan(id));
    }

    @GetMapping("/monitor/{id}")
    public Result<Map<String, Object>> monitor(@PathVariable String id) {
        return Result.success(syncTaskService.monitor(id));
    }

    @GetMapping("/runtime/{id}")
    public Result<Map<String, Object>> runtime(@PathVariable String id) {
        return Result.success(syncTaskService.runtime(id));
    }

    @GetMapping("/diagnostics/{id}")
    public Result<Map<String, Object>> diagnostics(@PathVariable String id) {
        return Result.success(syncTaskService.diagnostics(id));
    }
}
