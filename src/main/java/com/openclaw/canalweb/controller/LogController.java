package com.openclaw.canalweb.controller;

import com.openclaw.canalweb.common.Result;
import com.openclaw.canalweb.domain.AlertLog;
import com.openclaw.canalweb.domain.OperationLog;
import com.openclaw.canalweb.domain.SyncTaskLog;
import com.openclaw.canalweb.service.AlertLogService;
import com.openclaw.canalweb.service.OperationLogService;
import com.openclaw.canalweb.service.TaskLogService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/log")
public class LogController {
    private final TaskLogService taskLogService;
    private final OperationLogService operationLogService;
    private final AlertLogService alertLogService;

    public LogController(TaskLogService taskLogService, OperationLogService operationLogService,
                         AlertLogService alertLogService) {
        this.taskLogService = taskLogService;
        this.operationLogService = operationLogService;
        this.alertLogService = alertLogService;
    }

    @GetMapping("/task/{id}")
    public Result<List<SyncTaskLog>> listByTask(@PathVariable String id,
                                                @RequestParam(defaultValue = "1000") int limit) {
        return Result.success(taskLogService.listByTask(id, limit));
    }

    @GetMapping("/operation")
    public Result<List<OperationLog>> operationLogs(@RequestParam(required = false) String moduleName,
                                                    @RequestParam(required = false) String refId,
                                                    @RequestParam(defaultValue = "1000") int limit) {
        return Result.success(operationLogService.list(moduleName, refId, limit));
    }

    @GetMapping("/alert")
    public Result<List<AlertLog>> alertLogs(@RequestParam(required = false) Integer acknowledged,
                                            @RequestParam(required = false) String moduleName,
                                            @RequestParam(required = false) String refId,
                                            @RequestParam(defaultValue = "1000") int limit) {
        return Result.success(alertLogService.list(acknowledged, moduleName, refId, limit));
    }

    @GetMapping("/alert/stats")
    public Result<java.util.Map<String, Object>> alertStats() {
        return Result.success(alertLogService.stats());
    }

    @PostMapping("/alert/{id}/ack")
    public Result<Void> acknowledge(@PathVariable Long id) {
        alertLogService.acknowledge(id);
        return Result.success();
    }

    @PostMapping("/alert/batch-ack")
    public Result<Integer> acknowledgeBatch(@RequestBody List<Long> ids) {
        return Result.success(alertLogService.acknowledgeBatch(ids));
    }

    @PostMapping("/alert/ack-filter")
    public Result<Integer> acknowledgeByFilter(@RequestBody java.util.Map<String, Object> request) {
        Integer acknowledged = request.get("acknowledged") == null || String.valueOf(request.get("acknowledged")).isBlank()
                ? null : Integer.valueOf(String.valueOf(request.get("acknowledged")));
        String moduleName = request.get("moduleName") == null ? "" : String.valueOf(request.get("moduleName"));
        String refId = request.get("refId") == null ? "" : String.valueOf(request.get("refId"));
        return Result.success(alertLogService.acknowledgeByFilter(acknowledged, moduleName, refId));
    }
}
