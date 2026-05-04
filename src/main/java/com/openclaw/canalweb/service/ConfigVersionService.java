package com.openclaw.canalweb.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openclaw.canalweb.domain.ConfigVersion;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

@Service
public class ConfigVersionService {
    private final JdbcClient jdbcClient;
    private final ObjectMapper objectMapper;

    public ConfigVersionService(DataSource dataSource, ObjectMapper objectMapper) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.objectMapper = objectMapper;
    }

    public void snapshot(String configType, String refId, String content) {
        Integer version = jdbcClient.sql("""
                SELECT COALESCE(MAX(version_no), 0) + 1 FROM config_version
                WHERE config_type = :configType AND ref_id = :refId
                """)
                .param("configType", configType)
                .param("refId", refId)
                .query(Integer.class)
                .single();
        jdbcClient.sql("""
                INSERT INTO config_version (config_type, ref_id, version_no, config_content)
                VALUES (:configType, :refId, :versionNo, :content)
                """)
                .param("configType", configType)
                .param("refId", refId)
                .param("versionNo", version)
                .param("content", content == null ? "" : content)
                .update();
    }

    public List<ConfigVersion> list(String configType, String refId) {
        return list(configType, refId, 100);
    }

    public List<ConfigVersion> list(String configType, String refId, int limit) {
        StringBuilder sql = new StringBuilder("""
                SELECT id, config_type, ref_id, version_no, config_content, create_time
                FROM config_version WHERE 1 = 1
                """);
        if (configType != null && !configType.isBlank()) {
            sql.append(" AND config_type = :configType");
        }
        if (refId != null && !refId.isBlank()) {
            sql.append(" AND ref_id = :refId");
        }
        sql.append(" ORDER BY id DESC LIMIT :limit");
        var spec = jdbcClient.sql(sql.toString());
        if (configType != null && !configType.isBlank()) {
            spec = spec.param("configType", configType);
        }
        if (refId != null && !refId.isBlank()) {
            spec = spec.param("refId", refId);
        }
        spec = spec.param("limit", boundedLimit(limit));
        return spec.query(ConfigVersion.class).list();
    }

    public ConfigVersion find(Long id) {
        return jdbcClient.sql("""
                SELECT id, config_type, ref_id, version_no, config_content, create_time
                FROM config_version WHERE id = :id
                """)
                .param("id", id)
                .query(ConfigVersion.class)
                .optional()
                .orElseThrow(() -> new IllegalArgumentException("未找到配置版本: " + id));
    }

    public String snapshotPayload(String configType, Object payload) {
        try {
            return objectMapper.writeValueAsString(Map.of(
                    "schemaVersion", 1,
                    "configType", configType,
                    "payload", payload
            ));
        } catch (Exception ex) {
            throw new IllegalStateException("生成配置快照失败: " + ex.getMessage(), ex);
        }
    }

    public <T> T readSnapshotPayload(ConfigVersion version, Class<T> payloadType) {
        try {
            JsonNode root = objectMapper.readTree(version.configContent());
            JsonNode payload = root.get("payload");
            if (payload == null || payload.isNull()) {
                throw new IllegalArgumentException("该版本不是可回滚的结构化快照");
            }
            return objectMapper.treeToValue(payload, payloadType);
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("该版本不是可回滚的结构化快照");
        }
    }

    private int boundedLimit(int limit) {
        return Math.max(1, Math.min(limit, 5000));
    }
}
