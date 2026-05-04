package com.openclaw.canalweb.dto;

import jakarta.validation.constraints.NotBlank;

import java.util.Map;

public record TargetTestRequest(
        @NotBlank String targetType,
        Map<String, String> targetConfig
) {
}
