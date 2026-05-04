package com.openclaw.canalweb.dto;

public record CanalAdminLoginRequest(
        String adminUrl,
        String username,
        String password
) {
}
