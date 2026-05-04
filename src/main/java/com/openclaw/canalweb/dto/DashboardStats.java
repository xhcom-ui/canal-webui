package com.openclaw.canalweb.dto;

public record DashboardStats(long datasourceCount, long taskCount, long runningCount, long errorCount,
                             long logCount, long totalSyncCount, long failCount, long avgDelayMs,
                             long unacknowledgedAlertCount) {
}
