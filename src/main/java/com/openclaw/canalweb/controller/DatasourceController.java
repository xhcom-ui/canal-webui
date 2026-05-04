package com.openclaw.canalweb.controller;

import com.openclaw.canalweb.common.Result;
import com.openclaw.canalweb.domain.DatasourceConfig;
import com.openclaw.canalweb.dto.DatasourceSaveRequest;
import com.openclaw.canalweb.service.CanalRuntimeService;
import com.openclaw.canalweb.service.DatasourceService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/datasource")
public class DatasourceController {
    private final DatasourceService datasourceService;
    private final CanalRuntimeService canalRuntimeService;

    public DatasourceController(DatasourceService datasourceService, CanalRuntimeService canalRuntimeService) {
        this.datasourceService = datasourceService;
        this.canalRuntimeService = canalRuntimeService;
    }

    @GetMapping("/list")
    public Result<List<DatasourceConfig>> list() {
        return Result.success(datasourceService.list());
    }

    @PostMapping("/save")
    public Result<DatasourceConfig> save(@Valid @RequestBody DatasourceSaveRequest request) {
        DatasourceConfig saved = datasourceService.save(request);
        canalRuntimeService.refreshRuntime(false);
        return Result.success(saved);
    }

    @PostMapping("/test")
    public Result<String> test(@Valid @RequestBody DatasourceSaveRequest request) {
        datasourceService.testConnection(request);
        return Result.success("连接成功");
    }

    @PostMapping("/binlog-position")
    public Result<Map<String, String>> binlogPosition(@Valid @RequestBody DatasourceSaveRequest request) {
        return Result.success(datasourceService.currentBinlogPosition(request));
    }

    @PostMapping("/clone/{id}")
    public Result<DatasourceConfig> cloneDatasource(@PathVariable Long id) {
        DatasourceConfig cloned = datasourceService.cloneDatasource(id);
        canalRuntimeService.refreshRuntime(false);
        return Result.success(cloned);
    }

    @GetMapping("/{key}/tables")
    public Result<List<Map<String, Object>>> tables(@PathVariable String key) {
        return Result.success(datasourceService.tables(key));
    }

    @GetMapping("/{key}/columns")
    public Result<List<Map<String, Object>>> columns(@PathVariable String key, @RequestParam String tableName) {
        return Result.success(datasourceService.columns(key, tableName));
    }

    @GetMapping("/{key}/diagnostics")
    public Result<Map<String, Object>> diagnostics(@PathVariable String key) {
        return Result.success(datasourceService.diagnostics(key));
    }

    @PostMapping("/enable")
    public Result<Void> enable(@RequestBody Map<String, Object> request) {
        Long id = Long.valueOf(String.valueOf(request.get("id")));
        boolean enabled = Boolean.parseBoolean(String.valueOf(request.get("enabled")));
        datasourceService.enable(id, enabled);
        canalRuntimeService.refreshRuntime(true);
        return Result.success();
    }
}
