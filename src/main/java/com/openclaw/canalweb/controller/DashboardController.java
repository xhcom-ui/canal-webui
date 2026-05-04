package com.openclaw.canalweb.controller;

import com.openclaw.canalweb.common.Result;
import com.openclaw.canalweb.dto.DashboardOverview;
import com.openclaw.canalweb.dto.DashboardStats;
import com.openclaw.canalweb.service.AlertLogService;
import com.openclaw.canalweb.service.OperationLogService;
import com.openclaw.canalweb.service.SyncTaskService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/dashboard")
public class DashboardController {
    private final SyncTaskService syncTaskService;
    private final AlertLogService alertLogService;
    private final OperationLogService operationLogService;

    public DashboardController(SyncTaskService syncTaskService, AlertLogService alertLogService,
                               OperationLogService operationLogService) {
        this.syncTaskService = syncTaskService;
        this.alertLogService = alertLogService;
        this.operationLogService = operationLogService;
    }

    @GetMapping("/stats")
    public Result<DashboardStats> stats() {
        return Result.success(syncTaskService.dashboard());
    }

    @GetMapping("/overview")
    public Result<DashboardOverview> overview() {
        return Result.success(new DashboardOverview(
                syncTaskService.dashboard(),
                syncTaskService.list("ERROR"),
                alertLogService.recentUnacknowledged(8),
                operationLogService.recent(8)
        ));
    }
}
