package com.openclaw.canal.adapter.es;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class CustomEsOuterAdapter implements OuterAdapter {
    private static final Logger log = LoggerFactory.getLogger(CustomEsOuterAdapter.class);

    private String destination;
    private String groupId;
    private String sourceDatabase;
    private String sourceTable;
    private String hosts;
    private String auth;
    private String index;
    private String documentIdField;
    private String deletePolicy;
    private Map<String, String> columns = new LinkedHashMap<>();

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        Map<String, String> properties = configuration.getProperties() == null ? Map.of() : configuration.getProperties();
        this.destination = value(properties.get("destination"), envProperties.getProperty("canal.conf.canalAdapters[0].instance", ""));
        this.groupId = value(properties.get("groupId"), "g1");
        this.sourceDatabase = value(properties.get("sourceDatabase"), "");
        this.sourceTable = value(properties.get("sourceTable"), "");
        this.hosts = normalizeHost(value(configuration.getHosts(), properties.get("hosts")));
        this.auth = value(properties.get("security.auth"), "");
        this.index = value(properties.get("index"), "");
        this.documentIdField = normalizeDocumentId(value(properties.get("documentId"), properties.get("_id")));
        this.deletePolicy = value(properties.get("deletePolicy"), "DELETE_DOCUMENT").toUpperCase(Locale.ROOT);
        this.columns = loadColumns(properties.get("mappingFile"));
        if (index.isBlank() || documentIdField.isBlank()) {
            throw new IllegalStateException("custom-es requires index and documentId");
        }
        request("GET", "/", null, false);
        log.info("custom es adapter initialized, hosts={}, index={}, documentId={}", hosts, index, documentIdField);
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        for (Dml dml : dmls) {
            if (!matches(dml) || Boolean.TRUE.equals(dml.getIsDdl())) {
                continue;
            }
            for (Map<String, Object> row : rows(dml)) {
                String id = String.valueOf(row.get(documentIdField));
                if (id == null || id.isBlank() || "null".equals(id)) {
                    continue;
                }
                if ("DELETE".equalsIgnoreCase(dml.getType())) {
                    deleteDocument(id);
                } else {
                    indexDocument(id, mappedRow(row));
                }
            }
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult result = new EtlResult();
        result.setSucceeded(true);
        result.setResultMessage("custom es adapter does not implement full ETL");
        return result;
    }

    @Override
    public Map<String, Object> count(String task) {
        return Map.of("count", 0);
    }

    private boolean matches(Dml dml) {
        return dml != null
                && (destination.isBlank() || destination.equals(dml.getDestination()))
                && (groupId.isBlank() || groupId.equals(dml.getGroupId()))
                && (sourceDatabase.isBlank() || sourceDatabase.equalsIgnoreCase(dml.getDatabase()))
                && (sourceTable.isBlank() || sourceTable.equalsIgnoreCase(dml.getTable()));
    }

    private List<Map<String, Object>> rows(Dml dml) {
        if ("DELETE".equalsIgnoreCase(dml.getType()) && dml.getOld() != null && !dml.getOld().isEmpty()) {
            return dml.getOld();
        }
        if (dml.getData() != null && !dml.getData().isEmpty()) {
            return dml.getData();
        }
        return Collections.emptyList();
    }

    private void indexDocument(String id, Map<String, Object> row) {
        String path = "/" + encode(index) + "/_doc/" + encode(id);
        request("PUT", path, JSON.toJSONString(row), true);
    }

    private void deleteDocument(String id) {
        if ("KEEP_DOCUMENT".equals(deletePolicy) || "NONE".equals(deletePolicy)) {
            return;
        }
        String path = "/" + encode(index) + "/_doc/" + encode(id);
        request("DELETE", path, null, false);
    }

    private Map<String, Object> mappedRow(Map<String, Object> row) {
        if (columns.isEmpty()) {
            return row;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        columns.forEach((source, target) -> result.put(target, row.get(source)));
        return result;
    }

    private String request(String method, String path, String body, boolean requireSuccess) {
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(hosts + path).toURL().openConnection();
            connection.setRequestMethod(method);
            connection.setConnectTimeout((int) Duration.ofSeconds(10).toMillis());
            connection.setReadTimeout((int) Duration.ofSeconds(30).toMillis());
            connection.setRequestProperty("Accept", "application/json");
            if (!auth.isBlank()) {
                connection.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(auth.getBytes(StandardCharsets.UTF_8)));
            }
            if (body != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                try (OutputStream out = connection.getOutputStream()) {
                    out.write(payload);
                }
            }
            int status = connection.getResponseCode();
            String response = new String((status >= 400 ? connection.getErrorStream() : connection.getInputStream()).readAllBytes(), StandardCharsets.UTF_8);
            if (requireSuccess && (status < 200 || status >= 300)) {
                throw new IllegalStateException("Elasticsearch request failed, status=" + status + ", response=" + response);
            }
            return response;
        } catch (IOException ex) {
            throw new IllegalStateException("Elasticsearch request failed: " + ex.getMessage(), ex);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> loadColumns(String mappingFile) {
        if (mappingFile == null || mappingFile.isBlank()) {
            return Map.of();
        }
        Path path = Path.of(mappingFile);
        if (!Files.exists(path)) {
            return Map.of();
        }
        try {
            Map<String, Object> yaml = new Yaml().load(Files.newInputStream(path));
            Object columns = yaml == null ? null : yaml.get("columns");
            if (!(columns instanceof List<?> list)) {
                return Map.of();
            }
            Map<String, String> result = new LinkedHashMap<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> map) {
                    map.forEach((source, target) -> result.put(String.valueOf(source), String.valueOf(target)));
                } else if (item != null) {
                    result.put(String.valueOf(item), String.valueOf(item));
                }
            }
            return result;
        } catch (Exception ex) {
            log.warn("failed to load es mapping file: {}", mappingFile, ex);
            return Map.of();
        }
    }

    private static String normalizeHost(String host) {
        String value = value(host, "http://127.0.0.1:9200");
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            return "http://" + value;
        }
        return value.replaceAll("/+$", "");
    }

    private static String normalizeDocumentId(String documentId) {
        String value = value(documentId, "id").trim();
        if (value.startsWith("{") && value.endsWith("}") && value.length() > 2) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String value(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
