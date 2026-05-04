package com.openclaw.canalweb.dto;

import com.openclaw.canalweb.domain.AlertLog;
import com.openclaw.canalweb.domain.OperationLog;
import com.openclaw.canalweb.domain.SyncTask;

import java.util.List;

public record DashboardOverview(
        DashboardStats stats,
        List<SyncTask> errorTasks,
        List<AlertLog> unacknowledgedAlerts,
        List<OperationLog> recentOperations
) {
}
