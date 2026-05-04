package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record SystemUser(
        Long id,
        String username,
        String password,
        String nickname,
        String roleCode,
        Integer status,
        LocalDateTime createTime
) {
}
