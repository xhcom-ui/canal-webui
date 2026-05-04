package com.openclaw.canalweb.controller;

import cn.dev33.satoken.stp.StpUtil;
import com.openclaw.canalweb.common.Result;
import com.openclaw.canalweb.dto.LoginRequest;
import com.openclaw.canalweb.dto.LoginResponse;
import com.openclaw.canalweb.dto.UserInfo;
import com.openclaw.canalweb.service.SystemUserService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {
    private final SystemUserService systemUserService;

    public AuthController(SystemUserService systemUserService) {
        this.systemUserService = systemUserService;
    }

    @PostMapping("/login")
    public Result<LoginResponse> login(@Valid @RequestBody LoginRequest request) {
        var user = systemUserService.findByUsername(request.username())
                .filter(item -> item.status() == 1)
                .filter(item -> item.password().equals(request.password()))
                .orElseThrow(() -> new IllegalArgumentException("用户名或密码错误"));
        StpUtil.login(user.id());
        return Result.success(new LoginResponse(
                StpUtil.getTokenName(),
                StpUtil.getTokenValue(),
                systemUserService.toUserInfo(user)
        ));
    }

    @GetMapping("/captcha")
    public Result<Map<String, Object>> captcha() {
        return Result.success(Map.of(
                "enabled", false,
                "captchaRequired", false
        ));
    }

    @GetMapping("/userinfo")
    public Result<UserInfo> userinfo() {
        StpUtil.checkLogin();
        Long userId = Long.valueOf(StpUtil.getLoginIdAsString());
        var user = systemUserService.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("用户不存在"));
        return Result.success(systemUserService.toUserInfo(user));
    }

    @PostMapping("/logout")
    public Result<Void> logout() {
        StpUtil.logout();
        return Result.success();
    }
}
