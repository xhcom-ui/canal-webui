package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record AlertLog(
        Long id,
        String alertLevel,
        String moduleName,
        String refId,
        String alertTitle,
        String alertContent,
        Integer acknowledged,
        LocalDateTime acknowledgeTime,
        LocalDateTime createTime
) {
}
