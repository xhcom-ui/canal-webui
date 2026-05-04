package com.openclaw.canalweb.service;

import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Service
public class LocalStackService {
    private static final Duration PORT_TIMEOUT = Duration.ofMillis(800);
    private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(2);
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(12);
    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(90);

    public Map<String, Object> status() {
        List<Map<String, Object>> services = List.of(
                portService("canal-web", "Canal Web", "127.0.0.1", 18082),
                portService("canal-server", "Canal Server", "127.0.0.1", 11111),
                httpService("canal-adapter", "Canal Adapter", "http://127.0.0.1:18083/destinations", "status"),
                portService("mysql", "MySQL", "127.0.0.1", 3306),
                portService("redis", "Redis DB 6", "127.0.0.1", 6379),
                httpService("elasticsearch", "Elasticsearch", "http://127.0.0.1:9200", "cluster_name"),
                portService("pgsql", "PostgreSQL", "127.0.0.1", 5432),
                portService("kafka", "Kafka", "127.0.0.1", 9092),
                portService("rocketmq", "RocketMQ", "127.0.0.1", 9876),
                portService("rabbitmq", "RabbitMQ", "127.0.0.1", 5672),
                portService("pulsar", "Pulsar", "127.0.0.1", 6650)
        );
        long running = services.stream().filter(item -> Boolean.TRUE.equals(item.get("ok"))).count();

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("ok", running == services.size());
        result.put("runningCount", running);
        result.put("total", services.size());
        result.put("services", services);
        result.put("launchAgents", command(List.of("bash", "-lc", "launchctl list | grep 'com.openclaw.canal' || true"), COMMAND_TIMEOUT));
        result.put("kafkaService", command(List.of("bash", "-lc", "brew services list | grep kafka || true"), COMMAND_TIMEOUT));
        result.put("script", localStackScript().map(Path::toString).orElse(""));
        return result;
    }

    public Map<String, Object> verify(String verifyId) {
        return runLocalStack("verify", verifyId, VERIFY_TIMEOUT);
    }

    public Map<String, Object> start() {
        return runLocalStack("start", null, Duration.ofSeconds(45));
    }

    public Map<String, Object> stop() {
        return runLocalStack("stop", null, Duration.ofSeconds(45));
    }

    public Map<String, Object> restart() {
        return runLocalStack("restart", null, Duration.ofSeconds(70));
    }

    private Map<String, Object> runLocalStack(String action, String verifyId, Duration timeout) {
        Path script = localStackScript().orElseThrow(() -> new IllegalArgumentException("未找到 canal-web/scripts/local-stack.sh"));
        List<String> command = List.of("bash", script.toAbsolutePath().toString(), action);
        Map<String, String> env = new LinkedHashMap<>();
        if (verifyId != null && !verifyId.isBlank()) {
            env.put("VERIFY_ID", verifyId.trim());
        }
        CommandResult commandResult = command(command, timeout, env);
        Map<String, Object> result = commandResult.toMap();
        result.put("action", action);
        result.put("script", script.toString());
        result.put("ok", commandResult.exitCode == 0 && !commandResult.timedOut);
        return result;
    }

    private Optional<Path> localStackScript() {
        List<String> candidates = new ArrayList<>();
        addCandidate(candidates, System.getProperty("canal-web.local-stack.script"));
        addCandidate(candidates, System.getenv("CANAL_WEB_LOCAL_STACK_SCRIPT"));
        addCandidateList(candidates, System.getProperty("canal-web.local-stack.script.paths"));
        addCandidateList(candidates, System.getenv("CANAL_WEB_LOCAL_STACK_SCRIPT_PATHS"));
        addCandidate(candidates, projectPath("scripts/local-stack.sh"));
        addCandidate(candidates, System.getenv("CANAL_WEB_HOME") == null ? null : System.getenv("CANAL_WEB_HOME") + "/scripts/local-stack.sh");

        Path cwd = Path.of(System.getProperty("user.dir", ".")).toAbsolutePath().normalize();
        candidates.add(cwd.resolve("scripts/local-stack.sh").toString());
        candidates.add(cwd.resolve("canal-web/scripts/local-stack.sh").toString());
        if (cwd.getParent() != null) {
            candidates.add(cwd.getParent().resolve("canal-web/scripts/local-stack.sh").toString());
        }

        return candidates.stream()
                .map(Path::of)
                .filter(Files::isRegularFile)
                .findFirst();
    }

    private String projectPath(String child) {
        String configured = firstText(System.getProperty("canal-web.project-dir"), System.getenv("CANAL_WEB_PROJECT_DIR"));
        if (configured.isBlank()) {
            return "";
        }
        return Path.of(configured).toAbsolutePath().normalize().resolve(child).toString();
    }

    private void addCandidate(List<String> candidates, String value) {
        if (value != null && !value.isBlank()) {
            candidates.add(value);
        }
    }

    private void addCandidateList(List<String> candidates, String value) {
        if (value == null || value.isBlank()) {
            return;
        }
        for (String item : value.split(",")) {
            addCandidate(candidates, item.trim());
        }
    }

    private Map<String, Object> portService(String key, String name, String host, int port) {
        boolean ok = canConnect(host, port);
        return service(key, name, host + ":" + port, ok, ok ? "端口可连接" : "端口不可连接");
    }

    private Map<String, Object> httpService(String key, String name, String endpoint, String expectedText) {
        String message;
        boolean ok = false;
        try {
            HttpURLConnection connection = (HttpURLConnection) URI.create(endpoint).toURL().openConnection();
            connection.setConnectTimeout((int) HTTP_TIMEOUT.toMillis());
            connection.setReadTimeout((int) HTTP_TIMEOUT.toMillis());
            connection.setRequestMethod("GET");
            int status = connection.getResponseCode();
            String body = new String(connection.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            ok = status >= 200 && status < 300 && (expectedText == null || body.contains(expectedText));
            message = ok ? "HTTP 正常" : "HTTP " + status + " 响应不符合预期";
        } catch (Exception ex) {
            message = ex.getMessage();
        }
        String target = endpoint.replace("http://", "").replace("https://", "");
        return service(key, name, target, ok, message);
    }

    private Map<String, Object> service(String key, String name, String target, boolean ok, String message) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("key", key);
        item.put("name", name);
        item.put("target", target);
        item.put("ok", ok);
        item.put("status", ok ? "UP" : "DOWN");
        item.put("message", message == null ? "" : message);
        return item;
    }

    private boolean canConnect(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), (int) PORT_TIMEOUT.toMillis());
            return true;
        } catch (IOException ex) {
            return false;
        }
    }

    private CommandResult command(List<String> command, Duration timeout) {
        return command(command, timeout, Map.of());
    }

    private CommandResult command(List<String> command, Duration timeout, Map<String, String> env) {
        long started = System.currentTimeMillis();
        try {
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(false);
            builder.environment().put("PATH", defaultPath(builder.environment().get("PATH")));
            builder.environment().putAll(env);
            Process process = builder.start();
            boolean finished = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
                process.waitFor(2, TimeUnit.SECONDS);
            }
            String stdout = read(process.getInputStream().readAllBytes());
            String stderr = read(process.getErrorStream().readAllBytes());
            return new CommandResult(finished ? process.exitValue() : -1, !finished, stdout, stderr,
                    System.currentTimeMillis() - started);
        } catch (Exception ex) {
            return new CommandResult(-1, false, "", ex.getMessage(), System.currentTimeMillis() - started);
        }
    }

    private String read(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8).trim();
    }

    private String defaultPath(String current) {
        String configured = firstText(System.getProperty("canal-web.local-stack.path"), System.getenv("CANAL_WEB_LOCAL_STACK_PATH"));
        String base = configured == null || configured.isBlank()
                ? "/opt/anaconda3/bin:/usr/local/mysql/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/local/sbin:/opt/local/bin:/opt/local/sbin:/usr/bin:/bin:/usr/sbin:/sbin"
                : configured;
        return current == null || current.isBlank() ? base : base + ":" + current;
    }

    private String firstText(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    private record CommandResult(int exitCode, boolean timedOut, String stdout, String stderr, long durationMs) {
        Map<String, Object> toMap() {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("exitCode", exitCode);
            result.put("timedOut", timedOut);
            result.put("stdout", stdout);
            result.put("stderr", stderr);
            result.put("durationMs", durationMs);
            return result;
        }
    }
}
