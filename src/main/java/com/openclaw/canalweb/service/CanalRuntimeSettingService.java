package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.CanalRuntimeSetting;
import com.openclaw.canalweb.dto.CanalRuntimeSettingRequest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.time.LocalDateTime;
import java.util.Map;

@Service
public class CanalRuntimeSettingService {
    private static final long SETTING_ID = 1L;

    private final JdbcClient jdbcClient;
    private final OperationLogService operationLogService;
    private final ConfigVersionService configVersionService;

    public CanalRuntimeSettingService(DataSource dataSource, OperationLogService operationLogService,
                                      ConfigVersionService configVersionService) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.operationLogService = operationLogService;
        this.configVersionService = configVersionService;
    }

    public CanalRuntimeSetting get() {
        return jdbcClient.sql("""
                SELECT id, server_mode, zk_servers, flat_message, canal_batch_size, canal_get_timeout,
                       access_channel, mq_servers, mq_username, mq_password, mq_topic, mq_partition_hash,
                       mq_dynamic_topic, admin_manager, admin_user, admin_password,
                       admin_auto_register, admin_cluster, admin_name,
                       project_dir, runtime_root_dir, canal_server_home, canal_adapter_home, dbsync_source_jar,
                       dbsync_runtime_jar, generated_paths, extra_properties, create_time, update_time
                FROM canal_runtime_setting WHERE id = :id
                """)
                .param("id", SETTING_ID)
                .query(CanalRuntimeSetting.class)
                .optional()
                .orElse(defaultSetting());
    }

    @Transactional
    public CanalRuntimeSetting save(CanalRuntimeSettingRequest request) {
        CanalRuntimeSetting normalized = normalize(request);
        jdbcClient.sql("""
                INSERT INTO canal_runtime_setting
                (id, server_mode, zk_servers, flat_message, canal_batch_size, canal_get_timeout,
                 access_channel, mq_servers, mq_username, mq_password, mq_topic, mq_partition_hash,
                 mq_dynamic_topic, admin_manager, admin_user, admin_password,
                 admin_auto_register, admin_cluster, admin_name,
                 project_dir, runtime_root_dir, canal_server_home, canal_adapter_home, dbsync_source_jar,
                 dbsync_runtime_jar, generated_paths, extra_properties, update_time)
                VALUES
                (:id, :serverMode, :zkServers, :flatMessage, :canalBatchSize, :canalGetTimeout,
                 :accessChannel, :mqServers, :mqUsername, :mqPassword, :mqTopic, :mqPartitionHash,
                 :mqDynamicTopic, :adminManager, :adminUser, :adminPassword,
                 :adminAutoRegister, :adminCluster, :adminName,
                 :projectDir, :runtimeRootDir, :canalServerHome, :canalAdapterHome, :dbsyncSourceJar,
                 :dbsyncRuntimeJar, :generatedPaths, :extraProperties, :updateTime)
                ON DUPLICATE KEY UPDATE
                  server_mode = VALUES(server_mode),
                  zk_servers = VALUES(zk_servers),
                  flat_message = VALUES(flat_message),
                  canal_batch_size = VALUES(canal_batch_size),
                  canal_get_timeout = VALUES(canal_get_timeout),
                  access_channel = VALUES(access_channel),
                  mq_servers = VALUES(mq_servers),
                  mq_username = VALUES(mq_username),
                  mq_password = VALUES(mq_password),
                  mq_topic = VALUES(mq_topic),
                  mq_partition_hash = VALUES(mq_partition_hash),
                  mq_dynamic_topic = VALUES(mq_dynamic_topic),
                  admin_manager = VALUES(admin_manager),
                  admin_user = VALUES(admin_user),
                  admin_password = VALUES(admin_password),
                  admin_auto_register = VALUES(admin_auto_register),
                  admin_cluster = VALUES(admin_cluster),
                  admin_name = VALUES(admin_name),
                  project_dir = VALUES(project_dir),
                  runtime_root_dir = VALUES(runtime_root_dir),
                  canal_server_home = VALUES(canal_server_home),
                  canal_adapter_home = VALUES(canal_adapter_home),
                  dbsync_source_jar = VALUES(dbsync_source_jar),
                  dbsync_runtime_jar = VALUES(dbsync_runtime_jar),
                  generated_paths = VALUES(generated_paths),
                  extra_properties = VALUES(extra_properties),
                  update_time = VALUES(update_time)
                """)
                .params(params(normalized))
                .param("updateTime", LocalDateTime.now())
                .update();
        CanalRuntimeSetting saved = get();
        configVersionService.snapshot("canal-runtime", "global",
                configVersionService.snapshotPayload("canal-runtime", toRequest(saved)));
        operationLogService.record("canal-runtime", "save-setting", "global",
                "Canal 全局运行配置已保存: serverMode=" + saved.serverMode());
        return saved;
    }

    public CanalRuntimeSetting normalizeForPreview(CanalRuntimeSettingRequest request) {
        return normalize(request);
    }

    @Transactional
    public CanalRuntimeSetting rollback(com.openclaw.canalweb.domain.ConfigVersion version) {
        if (!"canal-runtime".equals(version.configType())) {
            throw new IllegalArgumentException("配置版本类型不是 Canal Runtime");
        }
        CanalRuntimeSettingRequest request = configVersionService.readSnapshotPayload(version, CanalRuntimeSettingRequest.class);
        CanalRuntimeSetting saved = save(request);
        operationLogService.record("canal-runtime", "rollback", "global",
                "Canal 全局运行配置已回滚到版本 v" + version.versionNo());
        return saved;
    }

    private static CanalRuntimeSetting normalize(CanalRuntimeSettingRequest request) {
        return new CanalRuntimeSetting(
                SETTING_ID,
                oneOf(request.serverMode(), "tcp", "tcp", "kafka", "rocketMQ", "rabbitMQ", "pulsarMQ"),
                value(request.zkServers()),
                bool(request.flatMessage(), 1),
                positive(request.canalBatchSize(), 50),
                positive(request.canalGetTimeout(), 100),
                oneOf(request.accessChannel(), "local", "local", "zookeeper"),
                value(request.mqServers()),
                value(request.mqUsername()),
                value(request.mqPassword()),
                value(request.mqTopic()),
                value(request.mqPartitionHash()),
                value(request.mqDynamicTopic()),
                valueOrDefault(request.adminManager(), "127.0.0.1:8089"),
                valueOrDefault(request.adminUser(), "admin"),
                value(request.adminPassword()),
                bool(request.adminAutoRegister(), 0),
                value(request.adminCluster()),
                valueOrDefault(request.adminName(), "canal-web"),
                value(request.projectDir()),
                value(request.runtimeRootDir()),
                value(request.canalServerHome()),
                value(request.canalAdapterHome()),
                value(request.dbsyncSourceJar()),
                value(request.dbsyncRuntimeJar()),
                value(request.generatedPaths()),
                value(request.extraProperties()),
                null,
                null
        );
    }

    private static Map<String, Object> params(CanalRuntimeSetting setting) {
        return Map.ofEntries(
                Map.entry("id", SETTING_ID),
                Map.entry("serverMode", setting.serverMode()),
                Map.entry("zkServers", setting.zkServers()),
                Map.entry("flatMessage", setting.flatMessage()),
                Map.entry("canalBatchSize", setting.canalBatchSize()),
                Map.entry("canalGetTimeout", setting.canalGetTimeout()),
                Map.entry("accessChannel", setting.accessChannel()),
                Map.entry("mqServers", setting.mqServers()),
                Map.entry("mqUsername", setting.mqUsername()),
                Map.entry("mqPassword", setting.mqPassword()),
                Map.entry("mqTopic", setting.mqTopic()),
                Map.entry("mqPartitionHash", setting.mqPartitionHash()),
                Map.entry("mqDynamicTopic", setting.mqDynamicTopic()),
                Map.entry("adminManager", setting.adminManager()),
                Map.entry("adminUser", setting.adminUser()),
                Map.entry("adminPassword", setting.adminPassword()),
                Map.entry("adminAutoRegister", setting.adminAutoRegister()),
                Map.entry("adminCluster", setting.adminCluster()),
                Map.entry("adminName", setting.adminName()),
                Map.entry("projectDir", setting.projectDir()),
                Map.entry("runtimeRootDir", setting.runtimeRootDir()),
                Map.entry("canalServerHome", setting.canalServerHome()),
                Map.entry("canalAdapterHome", setting.canalAdapterHome()),
                Map.entry("dbsyncSourceJar", setting.dbsyncSourceJar()),
                Map.entry("dbsyncRuntimeJar", setting.dbsyncRuntimeJar()),
                Map.entry("generatedPaths", setting.generatedPaths()),
                Map.entry("extraProperties", setting.extraProperties())
        );
    }

    private static CanalRuntimeSetting defaultSetting() {
        return new CanalRuntimeSetting(SETTING_ID, "tcp", "", 1, 50, 100, "local",
                "", "", "", "", "", "", "127.0.0.1:8089", "admin", "", 0, "", "canal-web",
                "", "", "", "", "", "", "", "", null, null);
    }

    private static int bool(Integer value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        return value == 1 ? 1 : 0;
    }

    private static int positive(Integer value, int defaultValue) {
        return value == null || value <= 0 ? defaultValue : value;
    }

    private static String value(String value) {
        return value == null ? "" : value.trim();
    }

    private static String valueOrDefault(String value, String defaultValue) {
        String normalized = value(value);
        return normalized.isBlank() ? defaultValue : normalized;
    }

    private static String oneOf(String value, String defaultValue, String... values) {
        String normalized = value(value);
        for (String allowed : values) {
            if (allowed.equals(normalized)) {
                return normalized;
            }
        }
        return defaultValue;
    }

    static CanalRuntimeSettingRequest toRequest(CanalRuntimeSetting setting) {
        return new CanalRuntimeSettingRequest(setting.serverMode(), setting.zkServers(), setting.flatMessage(),
                setting.canalBatchSize(), setting.canalGetTimeout(), setting.accessChannel(), setting.mqServers(),
                setting.mqUsername(), setting.mqPassword(), setting.mqTopic(), setting.mqPartitionHash(),
                setting.mqDynamicTopic(), setting.adminManager(), setting.adminUser(), setting.adminPassword(),
                setting.adminAutoRegister(), setting.adminCluster(), setting.adminName(), setting.projectDir(), setting.runtimeRootDir(), setting.canalServerHome(),
                setting.canalAdapterHome(), setting.dbsyncSourceJar(), setting.dbsyncRuntimeJar(),
                setting.generatedPaths(), setting.extraProperties());
    }
}
