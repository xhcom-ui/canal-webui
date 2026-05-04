package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.DatasourceConfig;
import com.openclaw.canalweb.dto.DatasourceSaveRequest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class DatasourceService {
    private static final String SELECT_COLUMNS = """
            id, data_source_key, host, port, username, password, db_name,
            canal_destination, filter_regex, filter_black_regex, binlog_file, binlog_position,
            binlog_timestamp, gtid, gtid_enabled, server_id,
            field_filter, field_black_filter, filter_dml_insert, filter_dml_update, filter_dml_delete,
            filter_query_dml, filter_query_dcl, filter_query_ddl, filter_rows, filter_table_error,
            filter_transaction_entry, ddl_isolation, tsdb_enable, tsdb_url, tsdb_username, tsdb_password,
            tsdb_snapshot_interval, tsdb_snapshot_expire, standby_address, standby_journal_name,
            standby_position, standby_timestamp, standby_gtid, rds_accesskey, rds_secretkey,
            rds_instance_id, ssl_mode, extra_properties, status, create_time, update_time
            """;

    private final JdbcClient jdbcClient;
    private final OperationLogService operationLogService;
    private final ConfigVersionService configVersionService;

    public DatasourceService(DataSource dataSource, OperationLogService operationLogService,
                             ConfigVersionService configVersionService) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.operationLogService = operationLogService;
        this.configVersionService = configVersionService;
    }

    public List<DatasourceConfig> list() {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + " FROM datasource_config ORDER BY id DESC")
                .query(DatasourceConfig.class).list();
    }

    public Optional<DatasourceConfig> findByKey(String key) {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + " FROM datasource_config WHERE data_source_key = :key")
                .param("key", key).query(DatasourceConfig.class).optional();
    }

    @Transactional
    public DatasourceConfig save(DatasourceSaveRequest request) {
        DatasourceSaveRequest normalizedRequest = normalizeRestoreId(request);
        if (normalizedRequest.id() == null) {
            jdbcClient.sql("""
                    INSERT INTO datasource_config
                    (data_source_key, host, port, username, password, db_name, canal_destination,
                     filter_regex, filter_black_regex, binlog_file, binlog_position, binlog_timestamp,
                     gtid, gtid_enabled, server_id, field_filter, field_black_filter,
                     filter_dml_insert, filter_dml_update, filter_dml_delete, filter_query_dml,
                     filter_query_dcl, filter_query_ddl, filter_rows, filter_table_error,
                     filter_transaction_entry, ddl_isolation, tsdb_enable, tsdb_url, tsdb_username,
                     tsdb_password, tsdb_snapshot_interval, tsdb_snapshot_expire, standby_address,
                     standby_journal_name, standby_position, standby_timestamp, standby_gtid,
                     rds_accesskey, rds_secretkey, rds_instance_id, ssl_mode, extra_properties,
                     status, update_time)
                    VALUES
                    (:key, :host, :port, :username, :password, :dbName, :destination,
                     :filterRegex, :filterBlackRegex, :binlogFile, :binlogPosition, :binlogTimestamp,
                     :gtid, :gtidEnabled, :serverId, :fieldFilter, :fieldBlackFilter,
                     :filterDmlInsert, :filterDmlUpdate, :filterDmlDelete, :filterQueryDml,
                     :filterQueryDcl, :filterQueryDdl, :filterRows, :filterTableError,
                     :filterTransactionEntry, :ddlIsolation, :tsdbEnable, :tsdbUrl, :tsdbUsername,
                     :tsdbPassword, :tsdbSnapshotInterval, :tsdbSnapshotExpire, :standbyAddress,
                     :standbyJournalName, :standbyPosition, :standbyTimestamp, :standbyGtid,
                     :rdsAccesskey, :rdsSecretkey, :rdsInstanceId, :sslMode, :extraProperties,
                     :status, :updateTime)
                    """)
                    .params(params(normalizedRequest))
                    .param("updateTime", LocalDateTime.now())
                    .update();
        } else {
            jdbcClient.sql("""
                    UPDATE datasource_config
                    SET data_source_key = :key, host = :host, port = :port, username = :username,
                        password = :password, db_name = :dbName, canal_destination = :destination,
                        filter_regex = :filterRegex, filter_black_regex = :filterBlackRegex,
                        binlog_file = :binlogFile, binlog_position = :binlogPosition,
                        binlog_timestamp = :binlogTimestamp, gtid = :gtid,
                        gtid_enabled = :gtidEnabled, server_id = :serverId,
                        field_filter = :fieldFilter, field_black_filter = :fieldBlackFilter,
                        filter_dml_insert = :filterDmlInsert, filter_dml_update = :filterDmlUpdate,
                        filter_dml_delete = :filterDmlDelete, filter_query_dml = :filterQueryDml,
                        filter_query_dcl = :filterQueryDcl, filter_query_ddl = :filterQueryDdl,
                        filter_rows = :filterRows, filter_table_error = :filterTableError,
                        filter_transaction_entry = :filterTransactionEntry, ddl_isolation = :ddlIsolation,
                        tsdb_enable = :tsdbEnable, tsdb_url = :tsdbUrl, tsdb_username = :tsdbUsername,
                        tsdb_password = :tsdbPassword, tsdb_snapshot_interval = :tsdbSnapshotInterval,
                        tsdb_snapshot_expire = :tsdbSnapshotExpire, standby_address = :standbyAddress,
                        standby_journal_name = :standbyJournalName, standby_position = :standbyPosition,
                        standby_timestamp = :standbyTimestamp, standby_gtid = :standbyGtid,
                        rds_accesskey = :rdsAccesskey, rds_secretkey = :rdsSecretkey,
                        rds_instance_id = :rdsInstanceId, ssl_mode = :sslMode,
                        extra_properties = :extraProperties,
                        status = :status, update_time = :updateTime
                    WHERE id = :id
                    """)
                    .param("id", normalizedRequest.id())
                    .params(params(normalizedRequest))
                    .param("updateTime", LocalDateTime.now())
                    .update();
        }
        DatasourceConfig saved = findByKey(normalizedRequest.dataSourceKey()).orElseThrow();
        configVersionService.snapshot("datasource", saved.dataSourceKey(), datasourceSnapshotPayload(saved));
        operationLogService.record("datasource", normalizedRequest.id() == null ? "create" : "update",
                saved.dataSourceKey(), "数据源配置已保存: " + saved.host() + ":" + saved.port() + "/" + saved.dbName());
        return saved;
    }

    @Transactional
    public DatasourceConfig rollback(com.openclaw.canalweb.domain.ConfigVersion version) {
        if (!"datasource".equals(version.configType())) {
            throw new IllegalArgumentException("配置版本类型不是数据源");
        }
        DatasourceSaveRequest request = configVersionService.readSnapshotPayload(version, DatasourceSaveRequest.class);
        DatasourceConfig saved = save(request);
        operationLogService.record("datasource", "rollback", saved.dataSourceKey(),
                "数据源已回滚到版本 v" + version.versionNo());
        return saved;
    }

    @Transactional
    public DatasourceConfig cloneDatasource(Long id) {
        DatasourceConfig source = jdbcClient.sql("SELECT " + SELECT_COLUMNS + " FROM datasource_config WHERE id = :id")
                .param("id", id)
                .query(DatasourceConfig.class)
                .optional()
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + id));
        String key = nextCloneValue(source.dataSourceKey(), "data_source_key");
        String destination = nextCloneValue(source.canalDestination(), "canal_destination");
        DatasourceSaveRequest cloneRequest = new DatasourceSaveRequest(null, key, source.host(), source.port(),
                source.username(), source.password(), source.dbName(), destination, source.filterRegex(),
                source.filterBlackRegex(), source.binlogFile(), source.binlogPosition(), source.binlogTimestamp(),
                source.gtid(), source.gtidEnabled(), source.serverId(), source.fieldFilter(), source.fieldBlackFilter(),
                source.filterDmlInsert(), source.filterDmlUpdate(), source.filterDmlDelete(), source.filterQueryDml(),
                source.filterQueryDcl(), source.filterQueryDdl(), source.filterRows(), source.filterTableError(),
                source.filterTransactionEntry(), source.ddlIsolation(), source.tsdbEnable(), source.tsdbUrl(),
                source.tsdbUsername(), source.tsdbPassword(), source.tsdbSnapshotInterval(), source.tsdbSnapshotExpire(),
                source.standbyAddress(), source.standbyJournalName(), source.standbyPosition(),
                source.standbyTimestamp(), source.standbyGtid(), source.rdsAccesskey(), source.rdsSecretkey(),
                source.rdsInstanceId(), source.sslMode(), source.extraProperties(), 0);
        DatasourceConfig cloned = save(cloneRequest);
        operationLogService.record("datasource", "clone", cloned.dataSourceKey(),
                "数据源已从 " + source.dataSourceKey() + " 复制，默认禁用");
        return cloned;
    }

    @Transactional
    public void enable(Long id, boolean enabled) {
        jdbcClient.sql("UPDATE datasource_config SET status = :status, update_time = :time WHERE id = :id")
                .param("id", id)
                .param("status", enabled ? 1 : 0)
                .param("time", LocalDateTime.now())
                .update();
        operationLogService.record("datasource", enabled ? "enable" : "disable", String.valueOf(id),
                enabled ? "数据源已启用" : "数据源已禁用");
    }

    public void testConnection(DatasourceSaveRequest request) {
        String url = "jdbc:mysql://%s:%d/%s?connectTimeout=3000&socketTimeout=3000&useSSL=false"
                .formatted(request.host(), request.port(), request.dbName());
        try (var ignored = DriverManager.getConnection(url, request.username(), request.password())) {
            // Connection success is enough for the API contract.
        } catch (Exception ex) {
            throw new IllegalArgumentException("数据源连接失败: " + ex.getMessage(), ex);
        }
    }

    public Map<String, String> currentBinlogPosition(DatasourceSaveRequest request) {
        String url = "jdbc:mysql://%s:%d/%s?connectTimeout=5000&socketTimeout=5000&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                .formatted(request.host(), request.port(), request.dbName());
        try (var connection = DriverManager.getConnection(url, request.username(), request.password())) {
            Map<String, String> status = binlogStatus(connection.createStatement());
            if (status.isEmpty()) {
                throw new IllegalArgumentException("未读取到 Binlog 位点，请确认已开启 log_bin 且当前账号具备 REPLICATION CLIENT 权限");
            }
            return status;
        } catch (IllegalArgumentException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalArgumentException("读取 Binlog 位点失败: " + ex.getMessage(), ex);
        }
    }

    public List<Map<String, Object>> tables(String key) {
        DatasourceConfig config = findByKey(key)
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + key));
        String url = jdbcUrl(config);
        try (var connection = DriverManager.getConnection(url, config.username(), config.password());
             ResultSet rs = connection.getMetaData().getTables(config.dbName(), null, "%", new String[]{"TABLE"})) {
            List<Map<String, Object>> tables = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("tableName", rs.getString("TABLE_NAME"));
                item.put("remarks", rs.getString("REMARKS"));
                tables.add(item);
            }
            return tables;
        } catch (Exception ex) {
            throw new IllegalArgumentException("读取数据源表失败: " + ex.getMessage(), ex);
        }
    }

    public List<Map<String, Object>> columns(String key, String tableName) {
        DatasourceConfig config = findByKey(key)
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + key));
        String url = jdbcUrl(config);
        try (var connection = DriverManager.getConnection(url, config.username(), config.password());
             ResultSet rs = connection.getMetaData().getColumns(config.dbName(), null, tableName, "%")) {
            List<Map<String, Object>> columns = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("columnName", rs.getString("COLUMN_NAME"));
                item.put("typeName", rs.getString("TYPE_NAME"));
                item.put("nullable", rs.getInt("NULLABLE"));
                item.put("remarks", rs.getString("REMARKS"));
                item.put("ordinalPosition", rs.getInt("ORDINAL_POSITION"));
                columns.add(item);
            }
            return columns;
        } catch (Exception ex) {
            throw new IllegalArgumentException("读取数据源字段失败: " + ex.getMessage(), ex);
        }
    }

    public Map<String, Object> diagnostics(String key) {
        DatasourceConfig config = findByKey(key)
                .orElseThrow(() -> new IllegalArgumentException("未找到数据源: " + key));
        List<Map<String, Object>> checks = new ArrayList<>();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("dataSourceKey", config.dataSourceKey());
        result.put("host", config.host());
        result.put("port", config.port());
        result.put("dbName", config.dbName());
        result.put("canalDestination", config.canalDestination());
        String url = jdbcUrl(config);
        try (var connection = DriverManager.getConnection(url, config.username(), config.password())) {
            addCheck(checks, "连接源库", true, "连接成功", url);
            String logBin = variable(connection.createStatement(), "log_bin");
            addCheck(checks, "binlog 开关", "ON".equalsIgnoreCase(logBin), "log_bin=" + value(logBin), "需要 ON 才能被 Canal 订阅");
            String binlogFormat = variable(connection.createStatement(), "binlog_format");
            addCheck(checks, "binlog_format", "ROW".equalsIgnoreCase(binlogFormat), "binlog_format=" + value(binlogFormat), "建议 ROW");
            String serverId = variable(connection.createStatement(), "server_id");
            addCheck(checks, "MySQL server_id", serverId != null && !"0".equals(serverId), "server_id=" + value(serverId), "MySQL 主库自身 server_id");
            addCheck(checks, "Canal slaveId", config.serverId() == null || !String.valueOf(config.serverId()).equals(serverId),
                    "canal slaveId=" + value(config.serverId()), "Canal slaveId 不能和 MySQL server_id 相同");
            String gtidMode = variable(connection.createStatement(), "gtid_mode");
            if (config.gtidEnabled() != null && config.gtidEnabled() == 1) {
                addCheck(checks, "GTID 模式", "ON".equalsIgnoreCase(gtidMode), "gtid_mode=" + value(gtidMode), "启用 GTID 时 MySQL gtid_mode 需要 ON");
            } else {
                addCheck(checks, "GTID 模式", true, "gtid_mode=" + value(gtidMode), "当前数据源未启用 GTID");
            }
            Map<String, String> position = binlogStatus(connection.createStatement());
            result.put("binlogStatus", position);
            addCheck(checks, "binlog 位点", !position.isEmpty(),
                    position.isEmpty() ? "未读取到位点" : position.toString(), "用于确认当前主库可提供 binlog 位点");
            List<Map<String, Object>> matchedTables = matchedTables(connection.createStatement(), config);
            result.put("matchedTables", matchedTables);
            addCheck(checks, "订阅表匹配", !matchedTables.isEmpty(),
                    "匹配 " + matchedTables.size() + " 张表", config.filterRegex());
            List<String> grants = grants(connection.createStatement());
            result.put("grants", grants);
            boolean hasReplication = grants.stream().map(String::toUpperCase)
                    .anyMatch(item -> item.contains("REPLICATION SLAVE") || item.contains("REPLICATION CLIENT") || item.contains("ALL PRIVILEGES"));
            addCheck(checks, "复制权限", hasReplication, hasReplication ? "检测到复制相关权限" : "未从 SHOW GRANTS 中看到复制权限",
                    "Canal 通常需要 REPLICATION SLAVE、REPLICATION CLIENT");
        } catch (Exception ex) {
            addCheck(checks, "连接源库", false, ex.getMessage(), url);
        }
        long okCount = checks.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        result.put("checks", checks);
        result.put("okCount", okCount);
        result.put("total", checks.size());
        result.put("ok", okCount == checks.size());
        return result;
    }

    public List<DatasourceConfig> enabledList() {
        return jdbcClient.sql("SELECT " + SELECT_COLUMNS + " FROM datasource_config WHERE status = 1 ORDER BY id")
                .query(DatasourceConfig.class).list();
    }

    private static java.util.Map<String, Object> params(DatasourceSaveRequest request) {
        var params = new java.util.HashMap<String, Object>();
        params.put("key", request.dataSourceKey());
        params.put("host", request.host());
        params.put("port", request.port());
        params.put("username", request.username());
        params.put("password", request.password());
        params.put("dbName", request.dbName());
        params.put("destination", request.canalDestination());
        params.put("filterRegex", valueOrDefault(request.filterRegex(), request.dbName() + "\\\\..*"));
        params.put("filterBlackRegex", valueOrDefault(request.filterBlackRegex(), "mysql\\\\.slave_.*"));
        params.put("binlogFile", valueOrDefault(request.binlogFile(), ""));
        params.put("binlogPosition", request.binlogPosition());
        params.put("binlogTimestamp", request.binlogTimestamp());
        params.put("gtid", valueOrDefault(request.gtid(), ""));
        params.put("gtidEnabled", request.gtidEnabled() == null ? 0 : request.gtidEnabled());
        params.put("serverId", request.serverId());
        params.put("fieldFilter", valueOrDefault(request.fieldFilter(), ""));
        params.put("fieldBlackFilter", valueOrDefault(request.fieldBlackFilter(), ""));
        params.put("filterDmlInsert", flag(request.filterDmlInsert()));
        params.put("filterDmlUpdate", flag(request.filterDmlUpdate()));
        params.put("filterDmlDelete", flag(request.filterDmlDelete()));
        params.put("filterQueryDml", flag(request.filterQueryDml()));
        params.put("filterQueryDcl", flag(request.filterQueryDcl()));
        params.put("filterQueryDdl", flag(request.filterQueryDdl()));
        params.put("filterRows", flag(request.filterRows()));
        params.put("filterTableError", flag(request.filterTableError()));
        params.put("filterTransactionEntry", flag(request.filterTransactionEntry()));
        params.put("ddlIsolation", flag(request.ddlIsolation()));
        params.put("tsdbEnable", flag(request.tsdbEnable()));
        params.put("tsdbUrl", valueOrDefault(request.tsdbUrl(), ""));
        params.put("tsdbUsername", valueOrDefault(request.tsdbUsername(), ""));
        params.put("tsdbPassword", valueOrDefault(request.tsdbPassword(), ""));
        params.put("tsdbSnapshotInterval", request.tsdbSnapshotInterval() == null ? 24 : request.tsdbSnapshotInterval());
        params.put("tsdbSnapshotExpire", request.tsdbSnapshotExpire() == null ? 360 : request.tsdbSnapshotExpire());
        params.put("standbyAddress", valueOrDefault(request.standbyAddress(), ""));
        params.put("standbyJournalName", valueOrDefault(request.standbyJournalName(), ""));
        params.put("standbyPosition", request.standbyPosition());
        params.put("standbyTimestamp", request.standbyTimestamp());
        params.put("standbyGtid", valueOrDefault(request.standbyGtid(), ""));
        params.put("rdsAccesskey", valueOrDefault(request.rdsAccesskey(), ""));
        params.put("rdsSecretkey", valueOrDefault(request.rdsSecretkey(), ""));
        params.put("rdsInstanceId", valueOrDefault(request.rdsInstanceId(), ""));
        params.put("sslMode", valueOrDefault(request.sslMode(), "DISABLED"));
        params.put("extraProperties", valueOrDefault(request.extraProperties(), ""));
        params.put("status", request.status() == null ? 1 : request.status());
        return params;
    }

    private static int flag(Integer value) {
        return value != null && value == 1 ? 1 : 0;
    }

    private static String jdbcUrl(DatasourceConfig config) {
        return "jdbc:mysql://%s:%d/%s?connectTimeout=5000&socketTimeout=5000&useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true"
                .formatted(config.host(), config.port(), config.dbName());
    }

    private static String valueOrDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static void addCheck(List<Map<String, Object>> checks, String name, boolean ok, String message, String detail) {
        Map<String, Object> check = new LinkedHashMap<>();
        check.put("name", name);
        check.put("ok", ok);
        check.put("message", message == null ? "" : message);
        check.put("detail", detail == null ? "" : detail);
        checks.add(check);
    }

    private static String variable(Statement statement, String name) {
        try (statement; ResultSet rs = statement.executeQuery("SHOW VARIABLES LIKE '" + name + "'")) {
            return rs.next() ? rs.getString(2) : "";
        } catch (Exception ex) {
            return "";
        }
    }

    private static Map<String, String> binlogStatus(Statement statement) {
        try (statement) {
            try (ResultSet rs = statement.executeQuery("SHOW BINARY LOG STATUS")) {
                if (rs.next()) {
                    return Map.of("file", rs.getString("File"), "position", String.valueOf(rs.getLong("Position")));
                }
            } catch (Exception ignored) {
                try (ResultSet rs = statement.executeQuery("SHOW MASTER STATUS")) {
                    if (rs.next()) {
                        return Map.of("file", rs.getString("File"), "position", String.valueOf(rs.getLong("Position")));
                    }
                }
            }
        } catch (Exception ignored) {
            // Missing privileges or disabled binlog are reported as an empty status.
        }
        return Map.of();
    }

    private static List<Map<String, Object>> matchedTables(Statement statement, DatasourceConfig config) {
        List<Map<String, Object>> tables = new ArrayList<>();
        String regex = valueOrDefault(config.filterRegex(), config.dbName() + "\\..*");
        String blackRegex = valueOrDefault(config.filterBlackRegex(), "");
        try (statement; ResultSet rs = statement.executeQuery("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_schema = '%s' AND table_type = 'BASE TABLE'
                ORDER BY table_name
                LIMIT 200
                """.formatted(config.dbName().replace("'", "''")))) {
            while (rs.next()) {
                String fullName = rs.getString("table_schema") + "." + rs.getString("table_name");
                boolean white = fullName.matches(regex);
                boolean black = !blackRegex.isBlank() && fullName.matches(blackRegex);
                if (white && !black) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("tableName", fullName);
                    tables.add(item);
                }
            }
        } catch (Exception ignored) {
            // The caller still gets the check result based on the collected list.
        }
        return tables;
    }

    private static List<String> grants(Statement statement) {
        List<String> grants = new ArrayList<>();
        try (statement; ResultSet rs = statement.executeQuery("SHOW GRANTS FOR CURRENT_USER")) {
            while (rs.next()) {
                grants.add(rs.getString(1));
            }
        } catch (Exception ignored) {
            // Some managed databases restrict SHOW GRANTS.
        }
        return grants;
    }

    private static String value(Object value) {
        return value == null || String.valueOf(value).isBlank() ? "-" : String.valueOf(value);
    }

    private String nextCloneValue(String source, String column) {
        String base = (source == null || source.isBlank() ? "datasource" : source) + "_copy";
        if (!existsValue(column, base)) {
            return base;
        }
        for (int i = 2; i < 1000; i++) {
            String candidate = base + "_" + i;
            if (!existsValue(column, candidate)) {
                return candidate;
            }
        }
        return base + "_" + System.currentTimeMillis();
    }

    private boolean existsValue(String column, String value) {
        if (!"data_source_key".equals(column) && !"canal_destination".equals(column)) {
            throw new IllegalArgumentException("不支持的唯一字段: " + column);
        }
        return jdbcClient.sql("SELECT COUNT(*) FROM datasource_config WHERE " + column + " = :value")
                .param("value", value)
                .query(Long.class)
                .single() > 0;
    }

    private String datasourceSnapshotPayload(DatasourceConfig config) {
        return configVersionService.snapshotPayload("datasource", toSaveRequest(config));
    }

    private DatasourceSaveRequest normalizeRestoreId(DatasourceSaveRequest request) {
        if (request.id() == null) {
            return request;
        }
        boolean idExists = jdbcClient.sql("SELECT COUNT(*) FROM datasource_config WHERE id = :id")
                .param("id", request.id()).query(Long.class).single() > 0;
        if (idExists) {
            return request;
        }
        Long existingId = findByKey(request.dataSourceKey()).map(DatasourceConfig::id).orElse(null);
        return new DatasourceSaveRequest(existingId, request.dataSourceKey(), request.host(), request.port(),
                request.username(), request.password(), request.dbName(), request.canalDestination(),
                request.filterRegex(), request.filterBlackRegex(), request.binlogFile(), request.binlogPosition(),
                request.binlogTimestamp(), request.gtid(), request.gtidEnabled(), request.serverId(),
                request.fieldFilter(), request.fieldBlackFilter(), request.filterDmlInsert(), request.filterDmlUpdate(),
                request.filterDmlDelete(), request.filterQueryDml(), request.filterQueryDcl(), request.filterQueryDdl(),
                request.filterRows(), request.filterTableError(), request.filterTransactionEntry(), request.ddlIsolation(),
                request.tsdbEnable(), request.tsdbUrl(), request.tsdbUsername(), request.tsdbPassword(),
                request.tsdbSnapshotInterval(), request.tsdbSnapshotExpire(), request.standbyAddress(),
                request.standbyJournalName(), request.standbyPosition(), request.standbyTimestamp(),
                request.standbyGtid(), request.rdsAccesskey(), request.rdsSecretkey(), request.rdsInstanceId(),
                request.sslMode(), request.extraProperties(), request.status());
    }

    static DatasourceSaveRequest toSaveRequest(DatasourceConfig config) {
        return new DatasourceSaveRequest(config.id(), config.dataSourceKey(), config.host(), config.port(),
                config.username(), config.password(), config.dbName(), config.canalDestination(), config.filterRegex(),
                config.filterBlackRegex(), config.binlogFile(), config.binlogPosition(), config.binlogTimestamp(),
                config.gtid(), config.gtidEnabled(), config.serverId(), config.fieldFilter(), config.fieldBlackFilter(),
                config.filterDmlInsert(), config.filterDmlUpdate(), config.filterDmlDelete(), config.filterQueryDml(),
                config.filterQueryDcl(), config.filterQueryDdl(), config.filterRows(), config.filterTableError(),
                config.filterTransactionEntry(), config.ddlIsolation(), config.tsdbEnable(), config.tsdbUrl(),
                config.tsdbUsername(), config.tsdbPassword(), config.tsdbSnapshotInterval(), config.tsdbSnapshotExpire(),
                config.standbyAddress(), config.standbyJournalName(), config.standbyPosition(),
                config.standbyTimestamp(), config.standbyGtid(), config.rdsAccesskey(), config.rdsSecretkey(),
                config.rdsInstanceId(), config.sslMode(), config.extraProperties(), config.status());
    }
}
