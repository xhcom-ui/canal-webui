package com.openclaw.canalweb.dto;

import jakarta.validation.constraints.NotBlank;

public record UserSaveRequest(
        Long id,
        @NotBlank String username,
        String password,
        @NotBlank String nickname,
        @NotBlank String roleCode,
        Integer status
) {
}
