package com.openclaw.canalweb.dto;

public record CanalAdminPasswordRequest(
        String adminUrl,
        String username,
        String oldPassword,
        String newPassword
) {
}
