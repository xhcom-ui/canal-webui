package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record ConfigVersion(Long id, String configType, String refId, Integer versionNo,
                            String configContent, LocalDateTime createTime) {
}
