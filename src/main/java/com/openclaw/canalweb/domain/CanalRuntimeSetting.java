package com.openclaw.canalweb.domain;

import java.time.LocalDateTime;

public record CanalRuntimeSetting(
        Long id,
        String serverMode,
        String zkServers,
        Integer flatMessage,
        Integer canalBatchSize,
        Integer canalGetTimeout,
        String accessChannel,
        String mqServers,
        String mqUsername,
        String mqPassword,
        String mqTopic,
        String mqPartitionHash,
        String mqDynamicTopic,
        String adminManager,
        String adminUser,
        String adminPassword,
        Integer adminAutoRegister,
        String adminCluster,
        String adminName,
        String projectDir,
        String runtimeRootDir,
        String canalServerHome,
        String canalAdapterHome,
        String dbsyncSourceJar,
        String dbsyncRuntimeJar,
        String generatedPaths,
        String extraProperties,
        LocalDateTime createTime,
        LocalDateTime updateTime
) {
}
