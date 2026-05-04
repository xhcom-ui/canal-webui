package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record SyncTaskLog(Long id, String taskId, String logLevel, String logContent, LocalDateTime createTime) {
}
