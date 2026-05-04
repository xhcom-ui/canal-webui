package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.DatasourceConfig;
import com.openclaw.canalweb.domain.CanalRuntimeSetting;
import com.openclaw.canalweb.dto.CanalAdminLoginRequest;
import com.openclaw.canalweb.dto.CanalAdminPasswordRequest;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@ConfigurationProperties(prefix = "canal-web.runtime")
public class CanalRuntimeService {
    private final DatasourceService datasourceService;
    private final CanalRuntimeSettingService settingService;
    private final JdbcClient jdbcClient;
    private String projectDir = "";
    private String rootDir = "./canal-runtime";
    private String javaCommand = "java";
    private String javaOpts = "-server -Xms128m -Xmx512m";
    private boolean autoStart = true;
    private boolean restartOnChange = true;
    private RuntimeComponent canalServer = new RuntimeComponent("./canal-runtime/canal-server");
    private AdapterComponent canalAdapter = new AdapterComponent("./canal-runtime/canal-adapter");

    public CanalRuntimeService(DatasourceService datasourceService, CanalRuntimeSettingService settingService,
                               DataSource dataSource) {
        this.datasourceService = datasourceService;
        this.settingService = settingService;
        this.jdbcClient = JdbcClient.create(dataSource);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        refreshRuntime(autoStart);
    }

    @PreDestroy
    public void onShutdown() {
        stopAll();
    }

    public synchronized void refreshRuntime(boolean startAfterWrite) {
        List<DatasourceConfig> datasources = datasourceService.enabledList();
        writeCanalServerConfig(datasources);
        writeCanalAdapterConfig(datasources);
        if (startAfterWrite && autoStart) {
            restartAll();
        }
    }

    public synchronized void restartAll() {
        if (restartOnChange) {
            stopAll();
        }
        startAll();
    }

    public synchronized void startAll() {
        startComponent(canalServer);
        startComponent(canalAdapter);
    }

    public synchronized void stopAll() {
        stopComponent(canalAdapter);
        stopComponent(canalServer);
    }

    public synchronized void restartServer() {
        stopComponent(canalServer);
        startComponent(canalServer);
    }

    public synchronized void restartAdapter() {
        stopComponent(canalAdapter);
        startComponent(canalAdapter);
    }

    public synchronized void startServer() {
        startComponent(canalServer);
    }

    public synchronized void stopServer() {
        stopComponent(canalServer);
    }

    public synchronized void startAdapter() {
        startComponent(canalAdapter);
    }

    public synchronized void stopAdapter() {
        stopComponent(canalAdapter);
    }

    public Map<String, Object> status() {
        return Map.of(
                "rootDir", runtimeRoot().toString(),
                "canalServer", componentStatus(canalServer, "canal.pid"),
                "canalAdapter", componentStatus(canalAdapter, "adapter.pid")
        );
    }

    public Map<String, Object> configView() {
        Map<String, Object> view = new LinkedHashMap<>();
        view.put("paths", runtimePaths());
        view.put("canalProperties", readText(serverHome().resolve("conf/canal.properties"), 80_000));
        view.put("adapterApplication", readText(adapterHome().resolve("conf/application.yml"), 80_000));
        view.put("instances", instanceConfigs());
        view.put("taskSpecs", generatedTaskSpecs());
        return view;
    }

    public Map<String, Object> configConsistency() {
        List<DatasourceConfig> datasources = datasourceService.enabledList();
        List<Map<String, Object>> items = new ArrayList<>();
        items.add(compareConfig("canal.properties",
                serverHome().resolve("conf/canal.properties"),
                buildCanalProperties(datasources)));
        items.add(compareConfig("adapter application.yml",
                adapterHome().resolve("conf/application.yml"),
                buildAdapterApplication(datasources)));
        for (DatasourceConfig datasource : datasources) {
            items.add(compareConfig("instance " + datasource.canalDestination(),
                    serverHome().resolve(Path.of("conf", datasource.canalDestination(), "instance.properties")),
                    buildInstanceProperties(datasource)));
        }
        long okCount = items.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        return Map.of(
                "ok", okCount == items.size(),
                "okCount", okCount,
                "total", items.size(),
                "items", items
        );
    }

    public Map<String, Object> staleRuntimeFiles() {
        List<DatasourceConfig> enabledDatasources = datasourceService.enabledList();
        var enabledDestinations = enabledDatasources.stream()
                .map(DatasourceConfig::canalDestination)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        var taskIds = jdbcClient.sql("SELECT id FROM sync_task")
                .query(String.class)
                .list()
                .stream()
                .collect(Collectors.toSet());
        List<Map<String, Object>> items = new ArrayList<>();
        Path conf = serverHome().resolve("conf");
        if (Files.isDirectory(conf)) {
            try (var paths = Files.list(conf)) {
                paths.filter(Files::isDirectory)
                        .filter(path -> Files.exists(path.resolve("instance.properties")))
                        .filter(path -> !enabledDestinations.contains(path.getFileName().toString()))
                        .forEach(path -> items.add(staleItem("instance", path.getFileName().toString(), path)));
            } catch (IOException ignored) {
                // Ignore scan errors; diagnostics will still report what was found.
            }
        }
        for (Path pluginDir : generatedAdapterPluginDirs()) {
            if (!Files.isDirectory(pluginDir)) {
                continue;
            }
            try (var paths = Files.list(pluginDir)) {
                paths.filter(path -> path.getFileName().toString().endsWith(".outer.yml"))
                        .filter(path -> isStalePlugin(path, taskIds, enabledDestinations))
                        .forEach(path -> items.add(staleItem("adapter-plugin", taskIdFromPlugin(path), path)));
            } catch (IOException ignored) {
                // Ignore scan errors.
            }
        }
        return Map.of("count", items.size(), "items", items);
    }

    public Map<String, Object> cleanStaleRuntimeFiles() {
        Map<String, Object> stale = staleRuntimeFiles();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) stale.getOrDefault("items", List.of());
        int cleaned = 0;
        for (Map<String, Object> item : items) {
            String type = String.valueOf(item.get("type"));
            Path path = Path.of(String.valueOf(item.get("path")));
            try {
                if ("instance".equals(type)) {
                    deleteDirectory(path);
                    cleaned++;
                } else if ("adapter-plugin".equals(type)) {
                    String taskId = String.valueOf(item.get("name"));
                    Files.deleteIfExists(path);
                    Files.deleteIfExists(path.resolveSibling(taskId + ".destination"));
                    deleteMappingFile(adapterHome().resolve("conf"), taskId + ".yml");
                    cleaned++;
                }
            } catch (IOException ex) {
                throw new IllegalStateException("清理失效运行配置失败: " + path + ", " + ex.getMessage(), ex);
            }
        }
        refreshRuntime(false);
        return Map.of("cleaned", cleaned);
    }

    public Map<String, Object> logsView() {
        Map<String, Object> logs = new LinkedHashMap<>();
        logs.put("canalServer", logFiles(serverHome().resolve("logs")));
        logs.put("canalAdapter", logFiles(adapterHome().resolve("logs")));
        return logs;
    }

    public Map<String, Object> metricsView() {
        Map<String, Object> view = new LinkedHashMap<>();
        String endpoint = "http://127.0.0.1:" + canalServer.metricsPort + "/metrics";
        view.put("endpoint", endpoint);
        if (!isAlive(canalServer, canalServer.pidName())) {
            view.put("available", false);
            view.put("message", "Canal Server 未运行，启动后可读取 Prometheus 指标");
            view.put("samples", List.of());
            view.put("raw", "");
            return view;
        }
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(3))
                    .GET()
                    .build();
            String body = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString()).body();
            view.put("available", true);
            view.put("message", "指标读取成功");
            view.put("samples", metricSamples(body));
            view.put("raw", body.length() <= 80_000 ? body : body.substring(0, 80_000));
        } catch (Exception ex) {
            view.put("available", false);
            view.put("message", "读取 Canal 指标失败: " + ex.getMessage());
            view.put("samples", List.of());
            view.put("raw", "");
        }
        return view;
    }

    public Map<String, Object> diagnostics() {
        List<DatasourceConfig> datasources = datasourceService.enabledList();
        List<Map<String, Object>> checks = new ArrayList<>();
        Map<String, Object> adminStatus = adminManagerStatus(settingService.get());
        checks.add(checkDirectory("Runtime 根目录", runtimeRoot()));
        checks.add(checkDirectory("Canal Server 目录", serverHome()));
        checks.add(checkFile("Canal Server 启动脚本", serverHome().resolve("bin/startup.sh")));
        checks.add(checkFile("Canal Server 停止脚本", serverHome().resolve("bin/stop.sh")));
        checks.add(checkFile("Canal Server 全局配置", serverHome().resolve("conf/canal.properties")));
        checks.add(checkFile("dbsync 可用 Jar", dbsyncSourceJar()));
        checks.add(checkFile("dbsync Runtime 依赖", dbsyncRuntimeJar()));
        checks.add(checkDirectory("Canal Adapter 目录", adapterHome()));
        checks.add(checkFile("Canal Adapter 启动脚本", adapterHome().resolve("bin/startup.sh")));
        checks.add(checkFile("Canal Adapter 停止脚本", adapterHome().resolve("bin/stop.sh")));
        checks.add(checkFile("Canal Adapter 配置", adapterHome().resolve("conf/application.yml")));
        checks.add(checkValue("启用数据源", !datasources.isEmpty(), datasources.size() + " 个"));
        for (DatasourceConfig datasource : datasources) {
            checks.add(checkFile("实例配置 " + datasource.canalDestination(),
                    serverHome().resolve(Path.of("conf", datasource.canalDestination(), "instance.properties"))));
        }
        for (Path pluginDir : generatedAdapterPluginDirs()) {
            checks.add(checkDirectory("任务插件目录", pluginDir));
        }
        checks.add(checkValue("Canal Server 进程", isAlive(canalServer, canalServer.pidName()),
                isAlive(canalServer, canalServer.pidName()) ? "运行中" : "未运行"));
        checks.add(checkValue("Canal Adapter 进程", isAlive(canalAdapter, canalAdapter.pidName()),
                isAlive(canalAdapter, canalAdapter.pidName()) ? "运行中" : "未运行"));
        checks.add(checkValue("Canal Admin Manager",
                !Boolean.TRUE.equals(adminStatus.get("autoRegister")) || Boolean.TRUE.equals(adminStatus.get("reachable")),
                String.valueOf(adminStatus.get("message"))));

        long okCount = checks.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        return Map.of(
                "ok", okCount == checks.size(),
                "okCount", okCount,
                "total", checks.size(),
                "checks", checks
        );
    }

    public Map<String, Object> adminManagerStatus(CanalRuntimeSetting setting) {
        String manager = valueOrDefault(setting.adminManager(), "127.0.0.1:8089");
        Map<String, Object> result = new LinkedHashMap<>();
        List<Map<String, Object>> checks = new ArrayList<>();
        result.put("manager", manager);
        result.put("user", valueOrDefault(setting.adminUser(), "admin"));
        result.put("autoRegister", setting.adminAutoRegister() != null && setting.adminAutoRegister() == 1);
        result.put("cluster", value(setting.adminCluster()));
        result.put("name", valueOrDefault(setting.adminName(), "canal-web"));
        result.put("serverAdminPort", canalServer.adminPort);
        result.put("expectedNode", valueOrDefault(setting.adminName(), "canal-web") + " / " + canalServer.adminPort);
        if (setting.adminAutoRegister() == null || setting.adminAutoRegister() != 1) {
            result.put("reachable", false);
            result.put("skipped", true);
            result.put("message", "未启用 Canal Admin 自动注册，跳过 Manager 连通性检查");
            checks.add(adminCheck("自动注册", true, "未启用", "开启后会校验 Canal Admin Manager、Canal Server admin 端口和注册记录"));
            result.put("checks", checks);
            result.put("ok", true);
            return result;
        }
        HostPort hostPort = parseHostPort(manager);
        result.put("host", hostPort.host());
        result.put("port", hostPort.port());
        boolean managerReachable = false;
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(hostPort.host(), hostPort.port()), 2000);
            managerReachable = true;
            result.put("reachable", true);
            result.put("message", "Canal Admin Manager 可连接: " + manager);
        } catch (Exception ex) {
            result.put("reachable", false);
            result.put("message", "Canal Admin Manager 不可连接: " + manager + ", " + ex.getMessage());
        }
        checks.add(adminCheck("Admin Manager 端口", managerReachable, manager,
                managerReachable ? "8089 可连接" : "请先启动 canal-admin"));

        Map<String, Object> httpCheck = adminHttpCheck(hostPort, setting);
        checks.add(httpCheck);

        boolean serverAdminReachable = portReachable("127.0.0.1", canalServer.adminPort, 2000);
        checks.add(adminCheck("Canal Server Admin 端口", serverAdminReachable,
                "127.0.0.1:" + canalServer.adminPort,
                serverAdminReachable ? "Canal Server admin 端口可连接" : "Canal Server 未开放 admin 端口或未重启"));

        Map<String, Object> nodeCheck = canalAdminNodeCheck(setting);
        checks.add(nodeCheck);

        long okCount = checks.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();
        result.put("checks", checks);
        result.put("ok", okCount == checks.size());
        result.put("okCount", okCount);
        result.put("total", checks.size());
        return result;
    }

    public Map<String, Object> loginCanalAdmin(CanalAdminLoginRequest request) {
        String adminUrl = valueOrDefault(request.adminUrl(), valueOrDefault(settingService.get().adminManager(), "127.0.0.1:8089"));
        URI loginUri = URI.create(normalizeHttpUrl(adminUrl) + "/api/v1/user/login");
        String username = valueOrDefault(request.username(), "admin");
        String password = valueOrDefault(request.password(), "123456");
        String payload = "{\"username\":\"" + jsonEscape(username) + "\",\"password\":\"" + jsonEscape(password) + "\"}";
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(loginUri)
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
                    .build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("statusCode", response.statusCode());
            result.put("body", response.body());
            result.put("ok", response.statusCode() >= 200 && response.statusCode() < 300 && response.body().contains("\"code\":20000"));
            result.put("token", extractJsonString(response.body(), "token"));
            result.put("needPasswordChange", extractJsonString(response.body(), "needPasswordChange"));
            result.put("message", extractJsonString(response.body(), "message"));
            if (!Boolean.TRUE.equals(result.get("ok"))) {
                throw new IllegalArgumentException(valueOrDefault(String.valueOf(result.get("message")), "Canal Admin 登录失败"));
            }
            return result;
        } catch (IOException ex) {
            throw new IllegalArgumentException("Canal Admin 登录请求失败: " + ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Canal Admin 登录请求被中断", ex);
        }
    }

    public Map<String, Object> updateCanalAdminPassword(CanalAdminPasswordRequest request) {
        String adminUrl = valueOrDefault(request.adminUrl(), valueOrDefault(settingService.get().adminManager(), "127.0.0.1:8089"));
        String username = valueOrDefault(request.username(), "admin");
        String oldPassword = valueOrDefault(request.oldPassword(), "123456");
        String newPassword = valueOrDefault(request.newPassword(), "admin123");
        Map<String, Object> login = loginCanalAdmin(new CanalAdminLoginRequest(adminUrl, username, oldPassword));
        String token = value(String.valueOf(login.get("token")));
        if (token.isBlank()) {
            throw new IllegalArgumentException("Canal Admin 登录未返回 token，无法修改密码");
        }
        String payload = "{\"username\":\"" + jsonEscape(username)
                + "\",\"oldPassword\":\"" + jsonEscape(oldPassword)
                + "\",\"password\":\"" + jsonEscape(newPassword) + "\"}";
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                            URI.create(normalizeHttpUrl(adminUrl) + "/api/v1/user"))
                    .timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json")
                    .header("X-Token", token)
                    .PUT(HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8))
                    .build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("statusCode", response.statusCode());
            result.put("body", response.body());
            result.put("ok", response.statusCode() >= 200 && response.statusCode() < 300
                    && response.body().contains("\"code\":20000") && response.body().contains("\"success\""));
            result.put("token", token);
            result.put("message", extractJsonString(response.body(), "message"));
            if (!Boolean.TRUE.equals(result.get("ok"))) {
                throw new IllegalArgumentException(valueOrDefault(String.valueOf(result.get("message")), "Canal Admin 修改密码失败"));
            }
            return result;
        } catch (IOException ex) {
            throw new IllegalArgumentException("Canal Admin 修改密码请求失败: " + ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Canal Admin 修改密码请求被中断", ex);
        }
    }

    private Map<String, Object> adminHttpCheck(HostPort hostPort, CanalRuntimeSetting setting) {
        String base = "http://" + hostPort.host() + ":" + hostPort.port();
        String url = base + "/api/v1/config/server_polling?ip=127.0.0.1&port=" + canalServer.adminPort
                + "&md5=canal-web-check&register=0&cluster=&name=" + valueOrDefault(setting.adminName(), "canal-web");
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(3))
                    .header("user", valueOrDefault(setting.adminUser(), "admin"))
                    .header("passwd", valueOrDefault(setting.adminPassword(), ""))
                    .GET()
                    .build();
            HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            boolean ok = response.statusCode() >= 200 && response.statusCode() < 500
                    && (response.body().contains("\"code\":20000") || response.body().contains("\"code\":0"));
            return adminCheck("Admin HTTP/API", ok, "HTTP " + response.statusCode(),
                    ok ? "server_polling 鉴权通过" : truncate(response.body(), 120));
        } catch (Exception ex) {
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/"))
                        .timeout(Duration.ofSeconds(3))
                        .GET()
                        .build();
                HttpResponse<String> response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
                boolean ok = response.statusCode() >= 200 && response.statusCode() < 500;
                return adminCheck("Admin HTTP/API", ok, "HTTP " + response.statusCode(),
                        ok ? "Canal Admin HTTP 已响应" : truncate(response.body(), 120));
            } catch (Exception inner) {
                return adminCheck("Admin HTTP/API", false, base, inner.getMessage());
            }
        }
    }

    private Map<String, Object> canalAdminNodeCheck(CanalRuntimeSetting setting) {
        try {
            List<Map<String, Object>> rows = jdbcClient.sql("""
                    SELECT name, ip, admin_port, tcp_port, metric_port, status
                    FROM canal_manager.canal_node_server
                    WHERE admin_port = :adminPort
                    ORDER BY modified_time DESC
                    LIMIT 5
                    """)
                    .param("adminPort", canalServer.adminPort)
                    .query((rs, rowNum) -> {
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("name", rs.getString("name"));
                        row.put("ip", rs.getString("ip"));
                        row.put("adminPort", rs.getInt("admin_port"));
                        row.put("tcpPort", rs.getInt("tcp_port"));
                        row.put("metricPort", rs.getInt("metric_port"));
                        row.put("status", rs.getString("status"));
                        return row;
                    })
                    .list();
            String expectedName = valueOrDefault(setting.adminName(), "canal-web");
            boolean ok = rows.stream().anyMatch(row -> expectedName.equals(row.get("name"))
                    || "127.0.0.1".equals(row.get("ip"))
                    || "localhost".equals(row.get("ip")));
            Map<String, Object> check = adminCheck("Admin 注册记录", ok,
                    rows.isEmpty() ? "未找到 canal_node_server" : rows.toString(),
                    ok ? "已发现 Canal Server 注册记录" : "Canal Admin 已连接数据库，但未发现当前 Server 注册记录");
            check.put("nodes", rows);
            return check;
        } catch (Exception ex) {
            return adminCheck("Admin 注册记录", false, "canal_manager.canal_node_server",
                    "无法查询注册表: " + ex.getMessage());
        }
    }

    private boolean portReachable(String host, int port, int timeoutMs) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private Map<String, Object> adminCheck(String name, boolean ok, String message, String detail) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", name);
        item.put("ok", ok);
        item.put("message", message == null ? "" : message);
        item.put("detail", detail == null ? "" : detail);
        return item;
    }

    public List<Map<String, Object>> capabilities() {
        return List.of(
                capability("runtime", "运行时管理", "Canal Server/Adapter 启停、刷新配置、状态检测", "已接入",
                        "Canal 运维 / 运行状态", "canal.port, canal.metrics.pull.port, adapter server.port",
                        "继续增加单实例重启、进程环境变量和健康检查策略"),
                capability("runtime-paths", "运行时管理", "运行根目录、Server/Adapter 目录、dbsync Jar 和生成目录可页面化编辑", "已接入",
                        "Canal 运维 / 运行路径配置", "runtimeRootDir, canalServerHome, canalAdapterHome, dbsyncSourceJar, dbsyncRuntimeJar, generatedPaths",
                        "已接入顶部卡片快捷编辑、全局配置编辑和状态/自检/配置一致性联动"),
                capability("instance-basic", "实例基础配置", "destination、主库地址、账号、binlog 位点、GTID、表白名单/黑名单", "已接入",
                        "数据源管理", "canal.instance.master.*, canal.instance.gtidon, canal.instance.filter.*",
                        "补充字段过滤、DML/DDL 过滤、备用库和 SSL 配置"),
                capability("incremental", "增量同步", "基于 Canal Server 读取 binlog，Adapter 按 destination 消费", "已接入",
                        "同步任务", "canal.destinations, canalAdapters", "按目标端生成真实插件映射文件"),
                capability("full-sync", "全量同步", "按任务 SQL 拉取源库数据并输出 JSONL 快照，支持手动和 Cron 调度", "已接入",
                        "同步任务", "sync_sql, cron_expression", "增加全量导入到目标端和断点续传"),
                capability("sync-mode", "同步模式", "任务级支持增量、全量、全量+增量三种模式，并按模式控制全量快照和 Adapter 运行", "已接入",
                        "同步任务 / 同步模式", "sync_mode, cron_expression",
                        "已支持 INCREMENTAL、FULL、FULL_INCREMENTAL；下一步补失败重试和断点续跑"),
                capability("field-mapping", "字段映射", "源字段、目标字段、类型、主键、可空、默认值、转换、格式和扩展属性", "已接入",
                        "同步任务 / 字段映射", "task_field_mapping.*, fieldOptions",
                        "已接入属性模板、导入导出、后端校验和资源脚本消费"),
                capability("resource-plan", "资源准备", "按目标端生成 Redis key/value、RDB 建库建表、ES mapping、MQ Topic 等准备项", "已接入",
                        "同步任务 / 资源准备", "targetConfig, fieldMappings, sourceTypes",
                        "下一步补一键执行资源创建和执行结果回写"),
                capability("target-verify", "目标端自检", "启动前校验目标连接、必填配置、字段映射和资源脚本完整性", "已接入",
                        "同步任务 / 启动自检", "targetConfig, resource checks, fieldMappings",
                        "已接入目标端连接和资源维度校验；下一步补目标端写入样例验证"),
                capability("audit", "审计与版本", "操作日志、任务日志、数据源/任务/Adapter 配置版本快照", "已接入",
                        "日志中心 / 系统管理", "operation_log, config_version", "增加配置回滚和差异对比"),
                capability("metrics", "Prometheus 指标", "读取 Canal metrics 端口并在前端展示原始指标和样例", "已接入",
                        "Canal 运维 / 指标", "canal.metrics.pull.port", "解析延迟、TPS、堆积量并接入大盘"),
                capability("mq-mode", "MQ ServerMode", "Kafka、RocketMQ、RabbitMQ、PulsarMQ 投递模式", "部分接入",
                        "Canal 运维 / MQ 与 HA", "canal.serverMode, kafka.*, rocketmq.*, rabbitmq.*, pulsarmq.*",
                        "已页面化 MQ 参数并生成 canal.properties；下一步补端到端投递验证"),
                capability("ha", "Zookeeper HA", "ZK 协调、HA 位点、集群模式", "部分接入",
                        "Canal 运维 / MQ 与 HA", "canal.zkServers, canal.zookeeper.flush.period",
                        "已接入 ZK 地址和 accessChannel；下一步补故障切换演练"),
                capability("admin-compatible", "Canal Admin 兼容", "原生 canal-admin manager/register 能力", "部分接入",
                        "Canal 运维 / Admin 兼容", "canal.admin.manager, canal.admin.register.*",
                        "已接入 manager/register 配置生成；下一步补 canal-admin 注册联调"),
                capability("tsdb", "表结构 TSDB", "DDL 结构快照、元数据回溯，支持 H2/MySQL TSDB", "已接入",
                        "数据源管理 / 高级配置", "canal.instance.tsdb.*", "已页面化 TSDB 开关、MySQL TSDB 连接和快照周期"),
                capability("parser-filter", "解析与过滤", "字段过滤、DML/DDL/query 过滤、事务 entry 过滤、DDL 隔离", "已接入",
                        "数据源管理 / 高级配置", "canal.instance.filter.*, canal.instance.get.ddl.isolation",
                        "已接入常用过滤开关和 extra properties 高级编辑"),
                capability("standby", "备用库与云 RDS", "standby master、RDS OSS binlog、SSL/TLS 连接", "已接入",
                        "数据源管理 / 高级配置", "canal.instance.standby.*, canal.instance.rds.*, sslMode",
                        "已接入主备、RDS OSS binlog 和 SSL/TLS 配置分组"),
                capability("adapter-plugins", "Adapter 插件", "logger、rdb、es6/es7/es8、hbase、tablestore、clickhouse 等", "已接入",
                        "同步任务 / 目标端配置", "outerAdapters, conf/*/*.yml", "已覆盖 logger/rdb/es/hbase/clickhouse/tablestore 以及 MQ contract"),
                capability("redis-custom", "Redis 目标端", "通过自定义 Redis Adapter 写入 HASH/STRING，支持 key 模板、TTL 和删除策略", "已接入",
                        "同步任务 / 目标端配置", "conf/custom-redis/*.yml, plugin/client-adapter.custom-redis-*.jar", "继续补充 Redis Cluster/Sentinel 和批量管道写入"),
                capability("rdb-target", "目标端", "MySQL/PostgreSQL 等 RDB 目标库、目标表、主键和写入模式配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=RDB, targetConfig.rdb.*, fieldOptions.rdbType",
                        "已接入建库建表示例和字段类型消费；下一步补自动执行 DDL"),
                capability("es-target", "目标端", "Elasticsearch 索引、mapping、document_id 和写入模式配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=ES, targetConfig.es.*, fieldOptions.esType",
                        "已接入 mapping 生成和文档 ID 配置；下一步补索引模板与 alias 切换"),
                capability("hbase-target", "目标端", "HBase 表、rowKey、列族和列映射配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=HBASE, targetConfig.hbase.*, fieldOptions.hbaseFamily",
                        "已接入配置与资源准备说明；下一步补 HBase 端到端写入验证"),
                capability("pgsql-target", "目标端", "PostgreSQL 目标库表、schema、主键和 upsert 写入配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=RDB, targetConfig.rdb.type=postgresql, fieldOptions.rdbType",
                        "已接入 PostgreSQL 连接与建表示例；下一步补 schema 自动创建"),
                capability("mq-routing", "动态 Topic/分区", "按库表路由 Topic、动态分区、hash 字段", "已接入",
                        "同步任务 / 目标端配置", "canal.mq.topic, canal.mq.dynamicTopic, canal.mq.partitionHash",
                        "已接入全局和任务级 topic/dynamicTopic/partitionHash 配置"),
                capability("kafka-target", "目标端", "Kafka Topic、message key、消息格式和 consumer group 准备项", "已接入",
                        "同步任务 / 目标端配置", "targetType=KAFKA, targetConfig.kafka.*, canal.mq.*",
                        "已接入 Topic 创建示例和消息契约；下一步补 broker 端到端投递验证"),
                capability("rocketmq-target", "目标端", "RocketMQ Topic、tag、message key 和消费组配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=ROCKETMQ, targetConfig.rocketmq.*, rocketmq.*",
                        "已接入资源准备和消息契约；下一步补 nameserver 投递验证"),
                capability("rabbitmq-target", "目标端", "RabbitMQ exchange、routing key、queue 和消费组语义配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=RABBITMQ, targetConfig.rabbitmq.*, rabbitmq.*",
                        "已接入资源准备和路由配置；下一步补 exchange/queue 自动创建"),
                capability("pulsar-target", "目标端", "Pulsar Topic、subscription、message key 和消息格式配置", "已接入",
                        "同步任务 / 目标端配置", "targetType=PULSAR, targetConfig.pulsar.*, pulsarmq.*",
                        "已接入资源准备和消息契约；下一步补 tenant/namespace 自动检查")
        );
    }

    public Path saveTaskAdapterSpec(String taskId, String config) {
        try {
            Path dir = runtimeRoot().resolve("generated/tasks");
            Files.createDirectories(dir);
            Path file = dir.resolve(taskId + ".yml");
            Files.writeString(file, config, StandardCharsets.UTF_8);
            return file;
        } catch (IOException ex) {
            throw new IllegalStateException("写入 Canal 任务插件配置失败: " + ex.getMessage(), ex);
        }
    }

    public void saveTaskAdapterRuntimeFiles(String taskId, CanalAdapterService.AdapterRuntimeFiles files) {
        try {
            Path generatedDir = runtimeRoot().resolve("generated/adapter-plugins");
            Files.createDirectories(generatedDir);
            Files.writeString(generatedDir.resolve(taskId + ".outer.yml"), files.outerAdapterBlock(), StandardCharsets.UTF_8);
            Files.writeString(generatedDir.resolve(taskId + ".destination"), files.destination(), StandardCharsets.UTF_8);
            if (!"logger".equals(files.mappingDir())) {
                Path mappingDir = adapterHome().resolve(Path.of("conf", files.mappingDir()));
                Files.createDirectories(mappingDir);
                Files.writeString(mappingDir.resolve(files.mappingFile()), files.mappingContent(), StandardCharsets.UTF_8);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("写入 Canal Adapter 插件配置失败: " + ex.getMessage(), ex);
        }
    }

    public void deleteTaskRuntimeFiles(String taskId) {
        try {
            Files.deleteIfExists(runtimeRoot().resolve(Path.of("generated", "tasks", taskId + ".yml")));
            Files.deleteIfExists(runtimeRoot().resolve(Path.of("generated", "adapter-plugins", taskId + ".outer.yml")));
            Files.deleteIfExists(runtimeRoot().resolve(Path.of("generated", "adapter-plugins", taskId + ".destination")));
            deleteMappingFile(adapterHome().resolve("conf"), taskId + ".yml");
        } catch (IOException ex) {
            throw new IllegalStateException("删除 Canal 任务运行配置失败: " + ex.getMessage(), ex);
        }
    }

    private void writeCanalServerConfig(List<DatasourceConfig> datasources) {
        Path home = serverHome();
        Path conf = home.resolve("conf");
        try {
            Files.createDirectories(conf);
            Files.writeString(conf.resolve("canal.properties"), buildCanalProperties(datasources), StandardCharsets.UTF_8);
            for (DatasourceConfig datasource : datasources) {
                Path instanceDir = conf.resolve(datasource.canalDestination());
                Files.createDirectories(instanceDir);
                Files.writeString(instanceDir.resolve("instance.properties"), buildInstanceProperties(datasource),
                        StandardCharsets.UTF_8);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("生成 Canal Server 配置失败: " + ex.getMessage(), ex);
        }
    }

    private void writeCanalAdapterConfig(List<DatasourceConfig> datasources) {
        Path conf = adapterHome().resolve("conf");
        try {
            Files.createDirectories(conf);
            Files.writeString(conf.resolve("application.yml"), buildAdapterApplication(datasources), StandardCharsets.UTF_8);
        } catch (IOException ex) {
            throw new IllegalStateException("生成 Canal Adapter 配置失败: " + ex.getMessage(), ex);
        }
    }

    private String buildCanalProperties(List<DatasourceConfig> datasources) {
        CanalRuntimeSetting setting = settingService.get();
        String destinations = datasources.stream()
                .map(DatasourceConfig::canalDestination)
                .filter(Objects::nonNull)
                .filter(value -> !value.isBlank())
                .distinct()
                .collect(Collectors.joining(","));
        return """
                #################################################
                # Generated by canal-web. Do not edit manually.
                #################################################
                canal.ip =
                canal.register.ip = 127.0.0.1
                canal.port = %d
                canal.metrics.pull.port = %d
                canal.admin.port = %d
                canal.admin.manager = %s
                canal.admin.user = %s
                canal.admin.passwd = %s
                canal.admin.register.auto = %s
                canal.admin.register.cluster = %s
                canal.admin.register.name = %s
                canal.zkServers = %s
                canal.zookeeper.flush.period = 1000
                canal.withoutNetty = false
                canal.serverMode = %s
                canal.file.data.dir = ${canal.conf.dir}
                canal.file.flush.period = 1000
                canal.instance.memory.buffer.size = 16384
                canal.instance.memory.buffer.memunit = 1024
                canal.instance.memory.batch.mode = MEMSIZE
                canal.instance.memory.rawEntry = true
                canal.instance.detecting.enable = false
                canal.instance.detecting.sql = select 1
                canal.instance.detecting.interval.time = 3
                canal.instance.detecting.retry.threshold = 3
                canal.instance.detecting.heartbeatHaEnable = false
                canal.instance.transaction.size = 1024
                canal.instance.fallbackIntervalInSeconds = 60
                canal.instance.network.receiveBufferSize = 16384
                canal.instance.network.sendBufferSize = 16384
                canal.instance.network.soTimeout = 30
                canal.instance.filter.druid.ddl = true
                canal.instance.filter.query.dcl = false
                canal.instance.filter.query.dml = false
                canal.instance.filter.query.ddl = false
                canal.instance.filter.table.error = false
                canal.instance.filter.rows = false
                canal.instance.filter.transaction.entry = false
                canal.instance.filter.dml.insert = false
                canal.instance.filter.dml.update = false
                canal.instance.filter.dml.delete = false
                canal.instance.binlog.format = ROW,STATEMENT,MIXED
                canal.instance.binlog.image = FULL,MINIMAL,NOBLOB
                canal.instance.get.ddl.isolation = false
                canal.instance.parser.parallel = true
                canal.instance.parser.parallelBufferSize = 256
                canal.instance.tsdb.enable = false
                canal.destinations = %s
                canal.conf.dir = ../conf
                canal.auto.scan = true
                canal.auto.scan.interval = 5
                canal.auto.reset.latest.pos.mode = false
                canal.instance.global.mode = spring
                canal.instance.global.lazy = false
                canal.instance.global.manager.address =
                canal.instance.global.spring.xml = classpath:spring/file-instance.xml
                canal.mq.flatMessage = %s
                canal.mq.canalBatchSize = %d
                canal.mq.canalGetTimeout = %d
                canal.mq.accessChannel = %s
                kafka.bootstrap.servers = %s
                kafka.acks = all
                kafka.compression.type = none
                kafka.batch.size = 16384
                kafka.linger.ms = 1
                kafka.max.request.size = 1048576
                kafka.buffer.memory = 33554432
                kafka.max.in.flight.requests.per.connection = 1
                kafka.retries = 0
                kafka.sasl.jaas.config = %s
                rocketmq.namesrv.addr = %s
                rocketmq.producer.group = canal-web-producer
                rabbitmq.host = %s
                rabbitmq.username = %s
                rabbitmq.password = %s
                pulsarmq.serverUrl = %s
                canal.mq.topic = %s
                canal.mq.partitionHash = %s
                canal.mq.dynamicTopic = %s
                %s
                """.formatted(
                canalServer.port,
                canalServer.metricsPort,
                canalServer.adminPort,
                value(setting.adminManager()),
                valueOrDefault(setting.adminUser(), "admin"),
                value(setting.adminPassword()),
                setting.adminAutoRegister() != null && setting.adminAutoRegister() == 1,
                value(setting.adminCluster()),
                value(setting.adminName()),
                value(setting.zkServers()),
                setting.serverMode(),
                destinations,
                setting.flatMessage() == null || setting.flatMessage() == 1,
                setting.canalBatchSize() == null ? 50 : setting.canalBatchSize(),
                setting.canalGetTimeout() == null ? 100 : setting.canalGetTimeout(),
                valueOrDefault(setting.accessChannel(), "local"),
                value(setting.mqServers()),
                buildKafkaSasl(setting),
                value(setting.mqServers()),
                value(setting.mqServers()),
                value(setting.mqUsername()),
                value(setting.mqPassword()),
                value(setting.mqServers()),
                value(setting.mqTopic()),
                value(setting.mqPartitionHash()),
                value(setting.mqDynamicTopic()),
                value(setting.extraProperties())
        );
    }

    private static String buildKafkaSasl(CanalRuntimeSetting setting) {
        if (setting.mqUsername() == null || setting.mqUsername().isBlank()) {
            return "";
        }
        return "org.apache.kafka.common.security.scram.ScramLoginModule required username=\\\"%s\\\" password=\\\"%s\\\";"
                .formatted(setting.mqUsername(), value(setting.mqPassword()));
    }

    private String buildInstanceProperties(DatasourceConfig datasource) {
        return """
                #################################################
                # Generated by canal-web. Destination: %s
                #################################################
                %s
                canal.instance.gtidon=%s
                canal.instance.rds.accesskey=%s
                canal.instance.rds.secretkey=%s
                canal.instance.rds.instanceId=%s
                canal.instance.master.address=%s:%d
                canal.instance.master.journal.name=%s
                canal.instance.master.position=%s
                canal.instance.master.timestamp=%s
                canal.instance.master.gtid=%s
                canal.instance.master.sslMode=%s
                canal.instance.standby.address=%s
                canal.instance.standby.journal.name=%s
                canal.instance.standby.position=%s
                canal.instance.standby.timestamp=%s
                canal.instance.standby.gtid=%s
                canal.instance.multi.stream.on=false
                canal.instance.tsdb.enable=%s
                canal.instance.tsdb.url=%s
                canal.instance.tsdb.dbUsername=%s
                canal.instance.tsdb.dbPassword=%s
                canal.instance.tsdb.snapshot.interval=%d
                canal.instance.tsdb.snapshot.expire=%d
                canal.instance.dbUsername=%s
                canal.instance.dbPassword=%s
                canal.instance.connectionCharset=UTF-8
                canal.instance.enableDruid=false
                canal.instance.filter.regex=%s
                canal.instance.filter.black.regex=%s
                canal.instance.filter.field=%s
                canal.instance.filter.black.field=%s
                canal.instance.filter.query.dml=%s
                canal.instance.filter.query.dcl=%s
                canal.instance.filter.query.ddl=%s
                canal.instance.filter.rows=%s
                canal.instance.filter.table.error=%s
                canal.instance.filter.transaction.entry=%s
                canal.instance.filter.dml.insert=%s
                canal.instance.filter.dml.update=%s
                canal.instance.filter.dml.delete=%s
                canal.instance.get.ddl.isolation=%s
                canal.mq.topic=%s
                canal.mq.partition=0
                %s
                """.formatted(
                datasource.canalDestination(),
                datasource.serverId() == null ? "" : "canal.instance.mysql.slaveId=" + datasource.serverId(),
                datasource.gtidEnabled() != null && datasource.gtidEnabled() == 1,
                value(datasource.rdsAccesskey()),
                value(datasource.rdsSecretkey()),
                value(datasource.rdsInstanceId()),
                datasource.host(),
                datasource.port(),
                value(datasource.binlogFile()),
                datasource.binlogPosition() == null ? "" : datasource.binlogPosition(),
                datasource.binlogTimestamp() == null ? "" : datasource.binlogTimestamp(),
                value(datasource.gtid()),
                valueOrDefault(datasource.sslMode(), "DISABLED"),
                value(datasource.standbyAddress()),
                value(datasource.standbyJournalName()),
                datasource.standbyPosition() == null ? "" : datasource.standbyPosition(),
                datasource.standbyTimestamp() == null ? "" : datasource.standbyTimestamp(),
                value(datasource.standbyGtid()),
                isEnabled(datasource.tsdbEnable()),
                value(datasource.tsdbUrl()),
                value(datasource.tsdbUsername()),
                value(datasource.tsdbPassword()),
                datasource.tsdbSnapshotInterval() == null ? 24 : datasource.tsdbSnapshotInterval(),
                datasource.tsdbSnapshotExpire() == null ? 360 : datasource.tsdbSnapshotExpire(),
                datasource.username(),
                datasource.password(),
                valueOrDefault(datasource.filterRegex(), datasource.dbName() + "\\\\..*"),
                valueOrDefault(datasource.filterBlackRegex(), "mysql\\\\.slave_.*"),
                value(datasource.fieldFilter()),
                value(datasource.fieldBlackFilter()),
                isEnabled(datasource.filterQueryDml()),
                isEnabled(datasource.filterQueryDcl()),
                isEnabled(datasource.filterQueryDdl()),
                isEnabled(datasource.filterRows()),
                isEnabled(datasource.filterTableError()),
                isEnabled(datasource.filterTransactionEntry()),
                isEnabled(datasource.filterDmlInsert()),
                isEnabled(datasource.filterDmlUpdate()),
                isEnabled(datasource.filterDmlDelete()),
                isEnabled(datasource.ddlIsolation()),
                datasource.canalDestination(),
                value(datasource.extraProperties())
        );
    }

    private static boolean isEnabled(Integer value) {
        return value != null && value == 1;
    }

    private String buildAdapterApplication(List<DatasourceConfig> datasources) {
        String srcDataSources = buildAdapterSrcDataSources(datasources);
        String adapters = datasources.isEmpty() ? "  []" : datasources.stream()
                .map(ds -> """
                  - instance: %s
                    groups:
                    - groupId: g1
                      outerAdapters:
                %s
                """.formatted(ds.canalDestination(), outerAdaptersForDestination(ds.canalDestination())))
                .collect(Collectors.joining());
        return """
                # Generated by canal-web. Do not edit manually.
                server:
                  port: %d
                spring:
                  jackson:
                    date-format: yyyy-MM-dd HH:mm:ss
                    time-zone: GMT+8
                    default-property-inclusion: non_null
                canal.conf:
                  mode: %s
                  flatMessage: true
                  zookeeperHosts:
                  syncBatchSize: 1000
                  retries: -1
                  timeout:
                  accessKey:
                  secretKey:
                  consumerProperties:
                    canal.tcp.server.host: %s:%d
                    canal.tcp.zookeeper.hosts:
                    canal.tcp.batch.size: 500
                    canal.tcp.username:
                    canal.tcp.password:
                  srcDataSources:
                %s
                  canalAdapters:
                %s
                """.formatted(canalAdapter.getPort(), canalAdapter.mode, canalAdapter.canalHost, canalAdapter.canalPort,
                srcDataSources, adapters);
    }

    private String buildAdapterSrcDataSources(List<DatasourceConfig> datasources) {
        if (datasources.isEmpty()) {
            return "    {}\n";
        }
        return datasources.stream()
                .map(ds -> """
                    %s:
                      url: jdbc:mysql://%s:%d/%s?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai
                      username: %s
                      password: %s
                """.formatted(ds.dataSourceKey(), ds.host(), ds.port(), ds.dbName(), ds.username(), ds.password()))
                .collect(Collectors.joining());
    }

    private String outerAdaptersForDestination(String destination) {
        List<String> blocks = new ArrayList<>();
        for (Path dir : generatedAdapterPluginDirs()) {
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (var paths = Files.list(dir)) {
                blocks.addAll(paths
                        .filter(path -> path.getFileName().toString().endsWith(".outer.yml"))
                        .filter(path -> destination.equals(readText(path.resolveSibling(path.getFileName().toString().replace(".outer.yml", ".destination")), 512).trim()))
                        .sorted()
                        .map(path -> readText(path, 20_000))
                        .filter(content -> content != null && !content.isBlank())
                        .toList());
            } catch (IOException ignored) {
                // Try next generated directory.
            }
        }
        String joined = blocks.stream().distinct().collect(Collectors.joining());
        return joined.isBlank() ? "                      - name: logger\n" : joined;
    }

    private void startComponent(RuntimeComponent component) {
        if (!component.enabled || isAlive(component, component.pidName())) {
            return;
        }
        deleteStalePid(component, component.pidName());
        runScript(component, "startup.sh");
    }

    private static Map<String, Object> checkDirectory(String name, Path path) {
        boolean ok = Files.isDirectory(path);
        return diagnostic(name, ok, path.toAbsolutePath().normalize().toString(), ok ? "目录存在" : "目录不存在");
    }

    private static Map<String, Object> checkFile(String name, Path path) {
        boolean ok = Files.isRegularFile(path);
        return diagnostic(name, ok, path.toAbsolutePath().normalize().toString(), ok ? "文件存在" : "文件不存在");
    }

    private static Map<String, Object> checkValue(String name, boolean ok, String message) {
        return diagnostic(name, ok, "", message);
    }

    private static Map<String, Object> diagnostic(String name, boolean ok, String path, String message) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", name);
        item.put("ok", ok);
        item.put("path", path);
        item.put("message", message);
        return item;
    }

    private static Map<String, Object> compareConfig(String name, Path path, String expected) {
        String actual = readText(path, 400_000);
        String normalizedActual = normalizeConfig(actual);
        String normalizedExpected = normalizeConfig(expected);
        boolean exists = Files.isRegularFile(path);
        boolean same = exists && normalizedActual.equals(normalizedExpected);
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("name", name);
        item.put("path", path.toAbsolutePath().normalize().toString());
        item.put("exists", exists);
        item.put("ok", same);
        item.put("message", !exists ? "文件不存在" : same ? "配置一致" : "配置不一致，请刷新 Canal 配置");
        item.put("actualSize", actual.length());
        item.put("expectedSize", expected == null ? 0 : expected.length());
        item.put("actualPreview", actual.length() <= 20_000 ? actual : actual.substring(0, 20_000));
        item.put("expectedPreview", expected == null ? "" : expected.length() <= 20_000 ? expected : expected.substring(0, 20_000));
        return item;
    }

    private static Map<String, Object> staleItem(String type, String name, Path path) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("type", type);
        item.put("name", name);
        item.put("path", path.toAbsolutePath().normalize().toString());
        item.put("message", "instance".equals(type) ? "未启用数据源残留实例目录" : "无效任务或无效 destination 的 Adapter 插件");
        return item;
    }

    private boolean isStalePlugin(Path outerFile, java.util.Set<String> taskIds, java.util.Set<String> enabledDestinations) {
        String taskId = taskIdFromPlugin(outerFile);
        String destination = readText(outerFile.resolveSibling(taskId + ".destination"), 512).trim();
        return !taskIds.contains(taskId) || !enabledDestinations.contains(destination);
    }

    private static String taskIdFromPlugin(Path outerFile) {
        String name = outerFile.getFileName().toString();
        return name.endsWith(".outer.yml") ? name.substring(0, name.length() - ".outer.yml".length()) : name;
    }

    private static void deleteDirectory(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        try (var paths = Files.walk(dir)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static String normalizeConfig(String content) {
        return content == null ? "" : content.replace("\r\n", "\n").trim();
    }

    private void stopComponent(RuntimeComponent component) {
        if (!component.enabled) {
            return;
        }
        runScript(component, "stop.sh");
    }

    private void runScript(RuntimeComponent component, String scriptName) {
        Path script = componentHome(component).resolve(Path.of("bin", scriptName));
        if (!Files.exists(script)) {
            throw new IllegalStateException("Canal 运行脚本不存在: " + script.toAbsolutePath());
        }
        try {
            ProcessBuilder builder = new ProcessBuilder("bash", script.toAbsolutePath().toString());
            builder.directory(script.getParent().toFile());
            builder.environment().put("JAVA", javaCommand);
            builder.environment().put("JAVA_OPTS", javaOpts);
            builder.redirectErrorStream(true);
            Process process = builder.start();
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                throw new IllegalStateException("执行 Canal 脚本失败: " + script + "\n" + output);
            }
        } catch (IOException ex) {
            throw new IllegalStateException("执行 Canal 脚本失败: " + ex.getMessage(), ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("执行 Canal 脚本被中断", ex);
        }
    }

    private Map<String, Object> componentStatus(RuntimeComponent component, String pidName) {
        Path home = componentHome(component);
        return Map.of(
                "enabled", component.enabled,
                "home", home.toString(),
                "pidFile", home.resolve("bin").resolve(pidName).toString(),
                "running", isAlive(component, pidName)
        );
    }

    private List<Map<String, String>> instanceConfigs() {
        Path conf = serverHome().resolve("conf");
        if (!Files.isDirectory(conf)) {
            return List.of();
        }
        try (var paths = Files.list(conf)) {
            return paths
                    .filter(Files::isDirectory)
                    .filter(path -> Files.exists(path.resolve("instance.properties")))
                    .sorted()
                    .map(path -> Map.of(
                            "destination", path.getFileName().toString(),
                            "path", path.resolve("instance.properties").toAbsolutePath().normalize().toString(),
                            "content", readText(path.resolve("instance.properties"), 80_000)
                    ))
                    .toList();
        } catch (IOException ex) {
            return List.of();
        }
    }

    private List<Map<String, String>> generatedTaskSpecs() {
        List<Map<String, String>> specs = new ArrayList<>();
        for (Path dir : generatedTaskDirs()) {
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (var paths = Files.list(dir)) {
                specs.addAll(paths
                        .filter(path -> path.getFileName().toString().endsWith(".yml"))
                        .sorted()
                        .map(path -> Map.of(
                                "name", path.getFileName().toString(),
                                "path", path.toAbsolutePath().normalize().toString(),
                                "content", readText(path, 80_000)
                        ))
                        .toList());
            } catch (IOException ignored) {
                // Try next generated directory.
            }
        }
        return specs;
    }

    private static void deleteMappingFile(Path conf, String fileName) throws IOException {
        if (!Files.isDirectory(conf)) {
            return;
        }
        try (var paths = Files.walk(conf, 3)) {
            for (Path path : paths.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().equals(fileName)).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private Map<String, Object> runtimePaths() {
        return Map.of(
                "projectDir", projectPath("").toString(),
                "rootDir", runtimeRoot().toString(),
                "configuredRootDir", configuredPath(rootDir).toString(),
                "canalServerHome", serverHome().toString(),
                "configuredCanalServerHome", configuredPath(canalServer.home).toString(),
                "canalAdapterHome", adapterHome().toString(),
                "configuredCanalAdapterHome", configuredPath(canalAdapter.getHome()).toString(),
                "generatedTaskDirs", generatedTaskDirs().stream().map(Path::toString).toList(),
                "generatedAdapterPluginDirs", generatedAdapterPluginDirs().stream().map(Path::toString).toList()
        );
    }

    private Path runtimeRoot() {
        return firstDirectory(runtimeRootCandidates(), true);
    }

    private Path serverHome() {
        return firstDirectory(serverHomeCandidates(), true);
    }

    private Path adapterHome() {
        return firstDirectory(adapterHomeCandidates(), true);
    }

    private Path componentHome(RuntimeComponent component) {
        if (component == canalServer) {
            return serverHome();
        }
        if (component == canalAdapter) {
            return adapterHome();
        }
        return Path.of(component.home).toAbsolutePath().normalize();
    }

    private List<Path> runtimeRootCandidates() {
        List<Path> candidates = new ArrayList<>();
        addPathList(candidates, System.getProperty("canal-web.runtime.root-dirs"));
        addPathList(candidates, System.getenv("CANAL_WEB_RUNTIME_ROOT_DIRS"));
        addPath(candidates, System.getProperty("canal-web.runtime.root-dir"));
        addPath(candidates, System.getenv("CANAL_WEB_RUNTIME_ROOT_DIR"));
        addPath(candidates, settingService.get().runtimeRootDir());
        addPath(candidates, projectPath("canal-runtime").toString());
        addPath(candidates, rootDir);
        addPath(candidates, Path.of(System.getProperty("user.dir", ".")).resolve("canal-runtime").toString());
        return candidates;
    }

    private List<Path> serverHomeCandidates() {
        List<Path> candidates = new ArrayList<>();
        addPathList(candidates, System.getProperty("canal-web.runtime.server-homes"));
        addPathList(candidates, System.getenv("CANAL_SERVER_HOME_PATHS"));
        addPath(candidates, System.getProperty("canal-web.runtime.server-home"));
        addPath(candidates, System.getenv("CANAL_SERVER_HOME"));
        addPath(candidates, runtimeRoot().resolve("canal-server").toString());
        addPath(candidates, projectPath("canal-runtime/canal-server").toString());
        addPath(candidates, canalServer.home);
        addPath(candidates, Path.of(System.getProperty("user.dir", ".")).resolve("canal-runtime/canal-server").toString());
        addPath(candidates, settingService.get().canalServerHome());
        return candidates;
    }

    private List<Path> adapterHomeCandidates() {
        List<Path> candidates = new ArrayList<>();
        addPathList(candidates, System.getProperty("canal-web.runtime.adapter-homes"));
        addPathList(candidates, System.getenv("CANAL_ADAPTER_HOME_PATHS"));
        addPath(candidates, System.getProperty("canal-web.runtime.adapter-home"));
        addPath(candidates, System.getenv("CANAL_ADAPTER_HOME"));
        addPath(candidates, runtimeRoot().resolve("canal-adapter").toString());
        addPath(candidates, projectPath("canal-runtime/canal-adapter").toString());
        addPath(candidates, canalAdapter.getHome());
        addPath(candidates, Path.of(System.getProperty("user.dir", ".")).resolve("canal-runtime/canal-adapter").toString());
        addPath(candidates, settingService.get().canalAdapterHome());
        return candidates;
    }

    private List<Path> generatedTaskDirs() {
        List<Path> candidates = generatedBaseDirs("tasks");
        return existingOrFirst(candidates);
    }

    private List<Path> generatedAdapterPluginDirs() {
        List<Path> candidates = generatedBaseDirs("adapter-plugins");
        return existingOrFirst(candidates);
    }

    private List<Path> generatedBaseDirs(String child) {
        List<Path> candidates = new ArrayList<>();
        addPathList(candidates, System.getProperty("canal-web.runtime.generated.paths"));
        addPathList(candidates, System.getenv("CANAL_WEB_RUNTIME_GENERATED_PATHS"));
        addPathList(candidates, settingService.get().generatedPaths());
        List<Path> normalized = new ArrayList<>();
        for (Path candidate : candidates) {
            addPath(normalized, candidate.resolve(child).toString());
        }
        addPath(normalized, runtimeRoot().resolve("generated").resolve(child).toString());
        addPath(normalized, projectPath("canal-runtime/generated").resolve(child).toString());
        return normalized;
    }

    private Path dbsyncSourceJar() {
        List<Path> candidates = new ArrayList<>();
        addPath(candidates, System.getProperty("canal-web.runtime.dbsync-source-jar"));
        addPath(candidates, System.getenv("CANAL_DBSYNC_SOURCE_JAR"));
        addPath(candidates, settingService.get().dbsyncSourceJar());
        addPath(candidates, dbsyncRuntimeJar().toString());
        addPath(candidates, serverHome().resolve("lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar").toString());
        addPath(candidates, runtimeRoot().resolve("canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar").toString());
        addPath(candidates, projectPath("canal-runtime/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar").toString());
        addPath(candidates, Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize()
                .resolve("canal-runtime/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar").toString());
        return firstFile(candidates);
    }

    private Path dbsyncRuntimeJar() {
        List<Path> candidates = new ArrayList<>();
        addPath(candidates, System.getProperty("canal-web.runtime.dbsync-runtime-jar"));
        addPath(candidates, System.getenv("CANAL_DBSYNC_RUNTIME_JAR"));
        addPath(candidates, settingService.get().dbsyncRuntimeJar());
        addPath(candidates, serverHome().resolve("lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar").toString());
        return firstFile(candidates);
    }

    private Path firstFile(List<Path> candidates) {
        return candidates.stream()
                .filter(Files::isRegularFile)
                .findFirst()
                .orElse(candidates.isEmpty() ? Path.of(".").toAbsolutePath().normalize() : candidates.get(0));
    }

    private List<Path> existingOrFirst(List<Path> candidates) {
        List<Path> existing = candidates.stream().filter(Files::isDirectory).toList();
        if (!existing.isEmpty()) {
            return existing;
        }
        return candidates.isEmpty() ? List.of() : List.of(candidates.get(0));
    }

    private Path firstDirectory(List<Path> candidates, boolean allowFirst) {
        return candidates.stream()
                .filter(Files::isDirectory)
                .findFirst()
                .orElse(allowFirst && !candidates.isEmpty() ? candidates.get(0) : Path.of(".").toAbsolutePath().normalize());
    }

    private void addPathList(List<Path> candidates, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        for (String item : value.split(",")) {
            addPath(candidates, item);
        }
    }

    private Path projectPath(String child) {
        String configured = valueOrDefault(System.getProperty("canal-web.project-dir"),
                valueOrDefault(System.getenv("CANAL_WEB_PROJECT_DIR"),
                        valueOrDefault(settingService.get().projectDir(), projectDir)));
        Path base = configured.isBlank()
                ? Path.of(System.getProperty("user.dir", "."))
                : Path.of(configured);
        return base.toAbsolutePath().normalize().resolve(child);
    }

    private Path configuredPath(String value) {
        if (value == null || value.isBlank()) {
            return projectPath("");
        }
        Path path = Path.of(value.trim());
        if (path.isAbsolute()) {
            return path.normalize();
        }
        return projectPath("").resolve(path).normalize();
    }

    private void addPath(List<Path> candidates, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        Path path = Path.of(value.trim()).toAbsolutePath().normalize();
        if (candidates.stream().noneMatch(path::equals)) {
            candidates.add(path);
        }
    }

    private static Map<String, Object> capability(String key, String category, String name, String status,
                                                  String entry, String configKeys, String nextAction) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("key", key);
        item.put("category", category);
        item.put("name", name);
        item.put("status", status);
        item.put("entry", entry);
        item.put("configKeys", configKeys);
        item.put("nextAction", nextAction);
        return item;
    }

    private static List<Map<String, String>> metricSamples(String body) {
        if (body == null || body.isBlank()) {
            return List.of();
        }
        List<Map<String, String>> samples = new ArrayList<>();
        for (String line : body.split("\\R")) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            int split = line.lastIndexOf(' ');
            if (split <= 0 || split == line.length() - 1) {
                continue;
            }
            samples.add(Map.of("metric", line.substring(0, split), "value", line.substring(split + 1)));
            if (samples.size() >= 20) {
                break;
            }
        }
        return samples;
    }

    private List<Map<String, String>> logFiles(Path root) {
        if (!Files.isDirectory(root)) {
            return List.of();
        }
        try (var paths = Files.walk(root, 4)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".log"))
                    .sorted(Comparator.comparing(path -> path.toAbsolutePath().toString()))
                    .map(path -> Map.of(
                            "name", root.relativize(path).toString(),
                            "path", path.toAbsolutePath().normalize().toString(),
                            "content", tail(path, 20_000)
                    ))
                    .toList();
        } catch (IOException ex) {
            return List.of();
        }
    }

    private static String readText(Path path, int maxChars) {
        if (!Files.exists(path)) {
            return "";
        }
        try {
            String content = Files.readString(path, StandardCharsets.UTF_8);
            return content.length() <= maxChars ? content : content.substring(content.length() - maxChars);
        } catch (IOException ex) {
            return "";
        }
    }

    private static String tail(Path path, int maxChars) {
        return readText(path, maxChars);
    }

    private boolean isAlive(RuntimeComponent component, String pidName) {
        Path pidFile = componentHome(component).resolve(Path.of("bin", pidName));
        if (Files.exists(pidFile)) {
            try {
                String pid = Files.readString(pidFile).trim();
                if (!pid.isBlank() && ProcessHandle.of(Long.parseLong(pid)).map(ProcessHandle::isAlive).orElse(false)) {
                    return true;
                }
            } catch (Exception ignored) {
                // Fall back to port probing below. Canal scripts can leave a stale pid file after a failed duplicate start.
            }
        }
        return isPortListening(component.getPort());
    }

    private boolean isPortListening(int port) {
        if (port <= 0) {
            return false;
        }
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 300);
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    private void deleteStalePid(RuntimeComponent component, String pidName) {
        Path pidFile = componentHome(component).resolve(Path.of("bin", pidName));
        if (!Files.exists(pidFile) || isAlive(component, pidName)) {
            return;
        }
        try {
            Files.deleteIfExists(pidFile);
        } catch (IOException ex) {
            throw new IllegalStateException("清理 Canal pid 文件失败: " + pidFile.toAbsolutePath(), ex);
        }
    }

    private static String value(String value) {
        return value == null ? "" : value;
    }

    private static String valueOrDefault(String value, String defaultValue) {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String truncate(String value, int maxLength) {
        if (value == null || value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength) + "...";
    }

    private static HostPort parseHostPort(String value) {
        String normalized = valueOrDefault(value, "127.0.0.1:8089");
        if (normalized.startsWith("http://")) {
            normalized = normalized.substring("http://".length());
        } else if (normalized.startsWith("https://")) {
            normalized = normalized.substring("https://".length());
        }
        int slashIndex = normalized.indexOf('/');
        if (slashIndex >= 0) {
            normalized = normalized.substring(0, slashIndex);
        }
        String host = normalized;
        int port = 8089;
        int colonIndex = normalized.lastIndexOf(':');
        if (colonIndex > 0 && colonIndex < normalized.length() - 1) {
            host = normalized.substring(0, colonIndex);
            try {
                port = Integer.parseInt(normalized.substring(colonIndex + 1));
            } catch (NumberFormatException ignored) {
                port = 8089;
            }
        }
        return new HostPort(valueOrDefault(host, "127.0.0.1"), port);
    }

    private static String normalizeHttpUrl(String value) {
        String normalized = valueOrDefault(value, "127.0.0.1:8089");
        if (normalized.startsWith("http://") || normalized.startsWith("https://")) {
            return normalized.endsWith("/") ? normalized.substring(0, normalized.length() - 1) : normalized;
        }
        return "http://" + normalized;
    }

    private static String jsonEscape(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String extractJsonString(String body, String key) {
        if (body == null || key == null) {
            return "";
        }
        java.util.regex.Matcher matcher = java.util.regex.Pattern
                .compile("\"" + java.util.regex.Pattern.quote(key) + "\"\\s*:\\s*\"([^\"]*)\"")
                .matcher(body);
        return matcher.find() ? matcher.group(1) : "";
    }

    private record HostPort(String host, int port) {
    }

    public String getRootDir() {
        return rootDir;
    }

    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    public String getJavaCommand() {
        return javaCommand;
    }

    public void setJavaCommand(String javaCommand) {
        this.javaCommand = javaCommand;
    }

    public String getJavaOpts() {
        return javaOpts;
    }

    public void setJavaOpts(String javaOpts) {
        this.javaOpts = javaOpts;
    }

    public boolean isAutoStart() {
        return autoStart;
    }

    public void setAutoStart(boolean autoStart) {
        this.autoStart = autoStart;
    }

    public boolean isRestartOnChange() {
        return restartOnChange;
    }

    public void setRestartOnChange(boolean restartOnChange) {
        this.restartOnChange = restartOnChange;
    }

    public RuntimeComponent getCanalServer() {
        return canalServer;
    }

    public String getProjectDir() {
        return projectPath("").toString();
    }

    public void setProjectDir(String projectDir) {
        this.projectDir = projectDir;
    }

    public void setCanalServer(RuntimeComponent canalServer) {
        this.canalServer = canalServer;
    }

    public AdapterComponent getCanalAdapter() {
        return canalAdapter;
    }

    public void setCanalAdapter(AdapterComponent canalAdapter) {
        this.canalAdapter = canalAdapter;
    }

    public static class RuntimeComponent {
        private boolean enabled = true;
        private String home;
        private int port = 11111;
        private int metricsPort = 11112;
        private int adminPort = 11110;

        public RuntimeComponent() {
        }

        public RuntimeComponent(String home) {
            this.home = home;
        }

        String pidName() {
            return "canal.pid";
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getHome() {
            return home;
        }

        public void setHome(String home) {
            this.home = home;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getMetricsPort() {
            return metricsPort;
        }

        public void setMetricsPort(int metricsPort) {
            this.metricsPort = metricsPort;
        }

        public int getAdminPort() {
            return adminPort;
        }

        public void setAdminPort(int adminPort) {
            this.adminPort = adminPort;
        }
    }

    public static class AdapterComponent extends RuntimeComponent {
        private String mode = "tcp";
        private String canalHost = "127.0.0.1";
        private int canalPort = 11111;

        public AdapterComponent() {
        }

        public AdapterComponent(String home) {
            super(home);
            setPort(18083);
        }

        @Override
        String pidName() {
            return "adapter.pid";
        }

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }

        public String getCanalHost() {
            return canalHost;
        }

        public void setCanalHost(String canalHost) {
            this.canalHost = canalHost;
        }

        public int getCanalPort() {
            return canalPort;
        }

        public void setCanalPort(int canalPort) {
            this.canalPort = canalPort;
        }
    }
}
