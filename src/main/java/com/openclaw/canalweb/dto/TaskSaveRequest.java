package com.openclaw.canalweb.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

public record TaskSaveRequest(
        String id,
        @NotBlank String taskName,
        String description,
        @NotBlank String dataSourceKey,
        String syncSql,
        @NotBlank String targetType,
        @NotBlank String syncMode,
        String cronExpression,
        @NotNull Integer batchSize,
        List<@Valid FieldMappingRequest> fieldMappings,
        Map<String, String> targetConfig
) {
}
