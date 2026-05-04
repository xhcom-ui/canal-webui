package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.ConfigVersion;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
public class ConfigRollbackService {
    private final ConfigVersionService configVersionService;
    private final DatasourceService datasourceService;
    private final SyncTaskService syncTaskService;
    private final CanalRuntimeSettingService canalRuntimeSettingService;
    private final CanalRuntimeService canalRuntimeService;

    public ConfigRollbackService(ConfigVersionService configVersionService, DatasourceService datasourceService,
                                 SyncTaskService syncTaskService, CanalRuntimeSettingService canalRuntimeSettingService,
                                 CanalRuntimeService canalRuntimeService) {
        this.configVersionService = configVersionService;
        this.datasourceService = datasourceService;
        this.syncTaskService = syncTaskService;
        this.canalRuntimeSettingService = canalRuntimeSettingService;
        this.canalRuntimeService = canalRuntimeService;
    }

    @Transactional
    public Map<String, Object> rollback(Long versionId) {
        ConfigVersion version = configVersionService.find(versionId);
        Object restored = switch (version.configType()) {
            case "datasource" -> datasourceService.rollback(version);
            case "task" -> syncTaskService.rollback(version);
            case "canal-runtime" -> canalRuntimeSettingService.rollback(version);
            default -> throw new IllegalArgumentException("暂不支持回滚该配置类型: " + version.configType());
        };
        canalRuntimeService.refreshRuntime(false);
        return Map.of(
                "configType", version.configType(),
                "refId", version.refId(),
                "versionNo", version.versionNo(),
                "restored", restored
        );
    }
}
