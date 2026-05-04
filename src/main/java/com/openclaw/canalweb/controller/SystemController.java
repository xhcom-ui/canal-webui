package com.openclaw.canalweb.controller;

import com.openclaw.canalweb.common.Result;
import com.openclaw.canalweb.domain.CanalRuntimeSetting;
import com.openclaw.canalweb.domain.ConfigVersion;
import com.openclaw.canalweb.dto.CanalAdminLoginRequest;
import com.openclaw.canalweb.dto.CanalAdminPasswordRequest;
import com.openclaw.canalweb.dto.CanalRuntimeSettingRequest;
import com.openclaw.canalweb.dto.UserSaveRequest;
import com.openclaw.canalweb.dto.UserInfo;
import com.openclaw.canalweb.service.CanalRuntimeSettingService;
import com.openclaw.canalweb.service.CanalRuntimeService;
import com.openclaw.canalweb.service.ConfigPackageService;
import com.openclaw.canalweb.service.ConfigRollbackService;
import com.openclaw.canalweb.service.ConfigVersionService;
import com.openclaw.canalweb.service.LocalStackService;
import com.openclaw.canalweb.service.SystemUserService;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/system")
public class SystemController {
    private final SystemUserService systemUserService;
    private final CanalRuntimeService canalRuntimeService;
    private final CanalRuntimeSettingService canalRuntimeSettingService;
    private final ConfigVersionService configVersionService;
    private final ConfigRollbackService configRollbackService;
    private final ConfigPackageService configPackageService;
    private final LocalStackService localStackService;

    public SystemController(SystemUserService systemUserService, CanalRuntimeService canalRuntimeService,
                            CanalRuntimeSettingService canalRuntimeSettingService,
                            ConfigVersionService configVersionService,
                            ConfigRollbackService configRollbackService,
                            ConfigPackageService configPackageService,
                            LocalStackService localStackService) {
        this.systemUserService = systemUserService;
        this.canalRuntimeService = canalRuntimeService;
        this.canalRuntimeSettingService = canalRuntimeSettingService;
        this.configVersionService = configVersionService;
        this.configRollbackService = configRollbackService;
        this.configPackageService = configPackageService;
        this.localStackService = localStackService;
    }

    @GetMapping("/user/list")
    public Result<List<UserInfo>> listUsers() {
        return Result.success(systemUserService.listUsers());
    }

    @PostMapping("/user/save")
    public Result<UserInfo> saveUser(@RequestBody UserSaveRequest request) {
        return Result.success(systemUserService.save(request));
    }

    @PostMapping("/user/enable")
    public Result<Void> enableUser(@RequestBody Map<String, Object> request) {
        Long id = Long.valueOf(String.valueOf(request.get("id")));
        boolean enabled = Boolean.parseBoolean(String.valueOf(request.get("enabled")));
        systemUserService.enable(id, enabled);
        return Result.success();
    }

    @PostMapping("/user/reset-password")
    public Result<Void> resetPassword(@RequestBody Map<String, Object> request) {
        Long id = Long.valueOf(String.valueOf(request.get("id")));
        String password = String.valueOf(request.getOrDefault("password", "123456"));
        systemUserService.resetPassword(id, password);
        return Result.success();
    }

    @GetMapping("/config/version/list")
    public Result<List<ConfigVersion>> configVersions(@RequestParam(required = false) String configType,
                                                      @RequestParam(required = false) String refId,
                                                      @RequestParam(defaultValue = "1000") int limit) {
        return Result.success(configVersionService.list(configType, refId, limit));
    }

    @PostMapping("/config/version/{id}/rollback")
    public Result<Map<String, Object>> rollbackConfigVersion(@org.springframework.web.bind.annotation.PathVariable Long id) {
        return Result.success(configRollbackService.rollback(id));
    }

    @GetMapping("/config/package/export")
    public Result<Map<String, Object>> exportConfigPackage() {
        return Result.success(configPackageService.exportPackage());
    }

    @PostMapping("/config/package/import")
    public Result<Map<String, Object>> importConfigPackage(@RequestBody Map<String, Object> request) {
        return Result.success(configPackageService.importPackage(request));
    }

    @GetMapping("/local-stack/status")
    public Result<Map<String, Object>> localStackStatus() {
        return Result.success(localStackService.status());
    }

    @PostMapping("/local-stack/verify")
    public Result<Map<String, Object>> verifyLocalStack(@RequestBody(required = false) Map<String, Object> request) {
        String verifyId = request == null ? null : String.valueOf(request.getOrDefault("verifyId", ""));
        return Result.success(localStackService.verify(verifyId));
    }

    @PostMapping("/local-stack/start")
    public Result<Map<String, Object>> startLocalStack() {
        return Result.success(localStackService.start());
    }

    @PostMapping("/local-stack/stop")
    public Result<Map<String, Object>> stopLocalStack() {
        return Result.success(localStackService.stop());
    }

    @PostMapping("/local-stack/restart")
    public Result<Map<String, Object>> restartLocalStack() {
        return Result.success(localStackService.restart());
    }

    @GetMapping("/canal/status")
    public Result<Map<String, Object>> canalStatus() {
        return Result.success(canalRuntimeService.status());
    }

    @GetMapping("/canal/config")
    public Result<Map<String, Object>> canalConfig() {
        return Result.success(canalRuntimeService.configView());
    }

    @GetMapping("/canal/config/consistency")
    public Result<Map<String, Object>> canalConfigConsistency() {
        return Result.success(canalRuntimeService.configConsistency());
    }

    @GetMapping("/canal/stale-files")
    public Result<Map<String, Object>> canalStaleFiles() {
        return Result.success(canalRuntimeService.staleRuntimeFiles());
    }

    @PostMapping("/canal/stale-files/clean")
    public Result<Map<String, Object>> cleanCanalStaleFiles() {
        return Result.success(canalRuntimeService.cleanStaleRuntimeFiles());
    }

    @GetMapping("/canal/logs")
    public Result<Map<String, Object>> canalLogs() {
        return Result.success(canalRuntimeService.logsView());
    }

    @GetMapping("/canal/metrics")
    public Result<Map<String, Object>> canalMetrics() {
        return Result.success(canalRuntimeService.metricsView());
    }

    @GetMapping("/canal/capabilities")
    public Result<List<Map<String, Object>>> canalCapabilities() {
        return Result.success(canalRuntimeService.capabilities());
    }

    @GetMapping("/canal/diagnostics")
    public Result<Map<String, Object>> canalDiagnostics() {
        return Result.success(canalRuntimeService.diagnostics());
    }

    @GetMapping("/canal/setting")
    public Result<CanalRuntimeSetting> canalSetting() {
        return Result.success(canalRuntimeSettingService.get());
    }

    @PostMapping("/canal/setting")
    public Result<CanalRuntimeSetting> saveCanalSetting(@RequestBody CanalRuntimeSettingRequest request) {
        CanalRuntimeSetting saved = canalRuntimeSettingService.save(request);
        canalRuntimeService.refreshRuntime(false);
        return Result.success(saved);
    }

    @PostMapping("/canal/admin/test")
    public Result<Map<String, Object>> testCanalAdmin(@RequestBody CanalRuntimeSettingRequest request) {
        CanalRuntimeSetting setting = canalRuntimeSettingService.normalizeForPreview(request);
        return Result.success(canalRuntimeService.adminManagerStatus(setting));
    }

    @PostMapping("/canal/admin/login")
    public Result<Map<String, Object>> loginCanalAdmin(@RequestBody CanalAdminLoginRequest request) {
        return Result.success(canalRuntimeService.loginCanalAdmin(request));
    }

    @PostMapping("/canal/admin/password")
    public Result<Map<String, Object>> updateCanalAdminPassword(@RequestBody CanalAdminPasswordRequest request) {
        return Result.success(canalRuntimeService.updateCanalAdminPassword(request));
    }

    @PostMapping("/canal/refresh")
    public Result<Void> refreshCanal() {
        canalRuntimeService.refreshRuntime(true);
        return Result.success();
    }

    @PostMapping("/canal/start")
    public Result<Void> startCanal() {
        canalRuntimeService.startAll();
        return Result.success();
    }

    @PostMapping("/canal/stop")
    public Result<Void> stopCanal() {
        canalRuntimeService.stopAll();
        return Result.success();
    }

    @PostMapping("/canal/server/start")
    public Result<Void> startCanalServer() {
        canalRuntimeService.startServer();
        return Result.success();
    }

    @PostMapping("/canal/server/stop")
    public Result<Void> stopCanalServer() {
        canalRuntimeService.stopServer();
        return Result.success();
    }

    @PostMapping("/canal/server/restart")
    public Result<Void> restartCanalServer() {
        canalRuntimeService.restartServer();
        return Result.success();
    }

    @PostMapping("/canal/adapter/start")
    public Result<Void> startCanalAdapter() {
        canalRuntimeService.startAdapter();
        return Result.success();
    }

    @PostMapping("/canal/adapter/stop")
    public Result<Void> stopCanalAdapter() {
        canalRuntimeService.stopAdapter();
        return Result.success();
    }

    @PostMapping("/canal/adapter/restart")
    public Result<Void> restartCanalAdapter() {
        canalRuntimeService.restartAdapter();
        return Result.success();
    }
}
