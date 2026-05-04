package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record SyncTask(
        String id,
        String taskName,
        String description,
        String dataSourceKey,
        String syncSql,
        String targetType,
        String syncMode,
        String cronExpression,
        Integer batchSize,
        String taskStatus,
        Long totalCount,
        Long failCount,
        Long lastDelayMs,
        LocalDateTime lastStartTime,
        LocalDateTime lastStopTime,
        LocalDateTime lastScheduleTime,
        String fullSyncFile,
        LocalDateTime createTime,
        LocalDateTime updateTime
) {
}
