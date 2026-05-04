package com.openclaw.canalweb.dto;

import com.openclaw.canalweb.domain.SyncTask;
import com.openclaw.canalweb.domain.TaskFieldMapping;
import com.openclaw.canalweb.domain.TaskTargetConfig;

import java.util.List;

public record TaskDetail(
        SyncTask task,
        List<TaskFieldMapping> fieldMappings,
        List<TaskTargetConfig> targetConfig
) {
}
