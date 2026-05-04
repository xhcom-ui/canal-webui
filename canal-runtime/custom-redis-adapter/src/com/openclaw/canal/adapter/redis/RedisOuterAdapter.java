package com.openclaw.canal.adapter.redis;

import com.alibaba.fastjson2.JSON;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RedisOuterAdapter implements OuterAdapter {
    private static final Logger log = LoggerFactory.getLogger(RedisOuterAdapter.class);
    private static final Pattern TEMPLATE_FIELD_PATTERN = Pattern.compile("\\{([A-Za-z0-9_]+)}");

    private String destination;
    private String groupId;
    private String sourceDatabase;
    private String sourceTable;
    private String host;
    private int port;
    private String password;
    private int database;
    private String keyPattern;
    private String valueType;
    private int ttlSeconds;
    private String deletePolicy;
    private Map<String, String> columns = new LinkedHashMap<>();

    @Override
    public void init(OuterAdapterConfig configuration, Properties envProperties) {
        Map<String, String> properties = configuration.getProperties() == null ? Map.of() : configuration.getProperties();
        this.destination = value(properties.get("destination"), envProperties.getProperty("canal.conf.canalAdapters[0].instance", ""));
        this.groupId = value(properties.get("groupId"), "g1");
        this.sourceDatabase = value(properties.get("sourceDatabase"), "");
        this.sourceTable = value(properties.get("sourceTable"), "");
        this.host = value(properties.get("host"), "127.0.0.1");
        this.port = parseInt(properties.get("port"), 6379);
        this.password = value(properties.get("password"), "");
        this.database = parseInt(properties.get("database"), 0);
        this.keyPattern = value(properties.get("keyPattern"), "{id}");
        this.valueType = value(properties.get("valueType"), "HASH").toUpperCase(Locale.ROOT);
        this.ttlSeconds = parseInt(value(properties.get("ttlSeconds"), properties.get("expireSeconds")), 0);
        this.deletePolicy = value(properties.get("deletePolicy"), "DELETE_KEY").toUpperCase(Locale.ROOT);
        this.columns = loadColumns(properties.get("mappingFile"));
        ping();
        log.info("custom redis adapter initialized, host={}, database={}, keyPattern={}, valueType={}",
                host, database, keyPattern, valueType);
    }

    @Override
    public void sync(List<Dml> dmls) {
        if (dmls == null || dmls.isEmpty()) {
            return;
        }
        try (RedisConnection redis = new RedisConnection(host, port, password, database)) {
            for (Dml dml : dmls) {
                if (!matches(dml) || Boolean.TRUE.equals(dml.getIsDdl())) {
                    continue;
                }
                List<Map<String, Object>> rows = rows(dml);
                for (Map<String, Object> row : rows) {
                    String key = renderKey(row);
                    if (key.isBlank()) {
                        continue;
                    }
                    if ("DELETE".equalsIgnoreCase(dml.getType())) {
                        handleDelete(redis, key);
                    } else {
                        writeRow(redis, key, row);
                    }
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Redis sync failed: " + ex.getMessage(), ex);
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult result = new EtlResult();
        result.setSucceeded(true);
        result.setResultMessage("custom redis adapter does not implement full ETL");
        return result;
    }

    @Override
    public Map<String, Object> count(String task) {
        return Map.of("count", 0);
    }

    private boolean matches(Dml dml) {
        if (dml == null) {
            return false;
        }
        return (destination.isBlank() || destination.equals(dml.getDestination()))
                && (groupId.isBlank() || groupId.equals(dml.getGroupId()))
                && (sourceDatabase.isBlank() || sourceDatabase.equalsIgnoreCase(dml.getDatabase()))
                && (sourceTable.isBlank() || sourceTable.equalsIgnoreCase(dml.getTable()));
    }

    private List<Map<String, Object>> rows(Dml dml) {
        if (dml.getData() != null && !dml.getData().isEmpty()) {
            return dml.getData();
        }
        if (dml.getOld() != null && !dml.getOld().isEmpty()) {
            return dml.getOld();
        }
        return Collections.emptyList();
    }

    private void handleDelete(RedisConnection redis, String key) throws IOException {
        if ("KEEP_KEY".equals(deletePolicy) || "NONE".equals(deletePolicy)) {
            return;
        }
        redis.command("DEL", key);
    }

    private void writeRow(RedisConnection redis, String key, Map<String, Object> row) throws IOException {
        if ("STRING".equals(valueType) || "JSON".equals(valueType)) {
            redis.command("SET", key, JSON.toJSONString(mappedRow(row)));
        } else {
            List<String> args = new ArrayList<>();
            args.add(key);
            mappedRow(row).forEach((field, value) -> {
                args.add(field);
                args.add(value == null ? "" : String.valueOf(value));
            });
            if (args.size() > 1) {
                redis.command("HSET", args.toArray(String[]::new));
            }
        }
        if (ttlSeconds > 0) {
            redis.command("EXPIRE", key, String.valueOf(ttlSeconds));
        }
    }

    private Map<String, Object> mappedRow(Map<String, Object> row) {
        if (columns.isEmpty()) {
            return row;
        }
        Map<String, Object> result = new LinkedHashMap<>();
        columns.forEach((source, target) -> result.put(target, row.get(source)));
        return result;
    }

    private String renderKey(Map<String, Object> row) {
        Matcher matcher = TEMPLATE_FIELD_PATTERN.matcher(keyPattern);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            Object value = row.get(matcher.group(1));
            matcher.appendReplacement(result, Matcher.quoteReplacement(value == null ? "" : String.valueOf(value)));
        }
        matcher.appendTail(result);
        return result.toString();
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
            log.warn("failed to load redis mapping file: {}", mappingFile, ex);
            return Map.of();
        }
    }

    private void ping() {
        try (RedisConnection redis = new RedisConnection(host, port, password, database)) {
            redis.command("PING");
        } catch (IOException ex) {
            throw new IllegalStateException("Redis connection failed: " + ex.getMessage(), ex);
        }
    }

    private static int parseInt(String value, int defaultValue) {
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return Integer.parseInt(value.trim());
    }

    private static String value(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static final class RedisConnection implements AutoCloseable {
        private final Socket socket;
        private final BufferedReader reader;
        private final BufferedWriter writer;

        private RedisConnection(String host, int port, String password, int database) throws IOException {
            socket = new Socket(host, port);
            socket.setSoTimeout((int) Duration.ofSeconds(10).toMillis());
            reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            if (password != null && !password.isBlank()) {
                command("AUTH", password);
            }
            command("SELECT", String.valueOf(database));
        }

        private Object command(String command, String... args) throws IOException {
            writer.write("*" + (args.length + 1) + "\r\n");
            writeBulk(command);
            for (String arg : args) {
                writeBulk(Objects.toString(arg, ""));
            }
            writer.flush();
            return readReply();
        }

        private void writeBulk(String value) throws IOException {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            writer.write("$" + bytes.length + "\r\n");
            writer.write(value);
            writer.write("\r\n");
        }

        private Object readReply() throws IOException {
            int type = reader.read();
            if (type == -1) {
                throw new IOException("Redis closed connection");
            }
            String line = reader.readLine();
            return switch (type) {
                case '+' -> line;
                case '-' -> throw new IOException(line);
                case ':' -> Long.parseLong(line);
                case '$' -> readBulkReply(Integer.parseInt(line));
                case '*' -> readArrayReply(Integer.parseInt(line));
                default -> throw new IOException("Unsupported Redis reply type: " + (char) type);
            };
        }

        private String readBulkReply(int length) throws IOException {
            if (length < 0) {
                return null;
            }
            char[] chars = new char[length];
            int offset = 0;
            while (offset < length) {
                int read = reader.read(chars, offset, length - offset);
                if (read < 0) {
                    throw new IOException("Redis closed bulk reply");
                }
                offset += read;
            }
            reader.readLine();
            return new String(chars);
        }

        private List<Object> readArrayReply(int length) throws IOException {
            List<Object> result = new ArrayList<>();
            for (int i = 0; i < length; i++) {
                result.add(readReply());
            }
            return result;
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }
}
