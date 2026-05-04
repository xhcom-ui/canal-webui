package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record OperationLog(Long id, String moduleName, String actionName, String refId,
                           String logContent, LocalDateTime createTime) {
}
