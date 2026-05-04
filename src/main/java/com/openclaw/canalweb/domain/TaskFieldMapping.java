package com.openclaw.canalweb.domain;

public record TaskFieldMapping(Long id, String taskId, String sourceField, String targetField,
                               String fieldType, Boolean primaryKey, Boolean nullableField,
                               Boolean enabled, String defaultValue, String transformExpr,
                               String formatPattern, String fieldOptions) {
}
