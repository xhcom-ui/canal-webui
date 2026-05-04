package com.openclaw.canalweb.dto;

import jakarta.validation.constraints.NotBlank;

public record FieldMappingRequest(@NotBlank String sourceField, @NotBlank String targetField,
                                  String fieldType, Boolean primaryKey, Boolean nullableField,
                                  Boolean enabled, String defaultValue, String transformExpr,
                                  String formatPattern, String fieldOptions) {
}
