package com.openclaw.canalweb.config;

import cn.dev33.satoken.stp.StpUtil;
import com.openclaw.canalweb.service.SystemUserService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class AuthInterceptor implements HandlerInterceptor {
    private final SystemUserService systemUserService;

    public AuthInterceptor(SystemUserService systemUserService) {
        this.systemUserService = systemUserService;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        String path = request.getRequestURI();
        if (path.startsWith("/api/auth/")) {
            return true;
        }
        StpUtil.checkLogin();
        String role = currentRole();
        if ("ADMIN".equals(role)) {
            return true;
        }
        if (path.startsWith("/api/system/user")) {
            throw new IllegalArgumentException("仅管理员可管理用户");
        }
        String method = request.getMethod();
        if ("GET".equalsIgnoreCase(method)) {
            return true;
        }
        if ("VIEWER".equals(role)) {
            throw new IllegalArgumentException("只读角色无权执行写操作");
        }
        if (path.startsWith("/api/system/config")) {
            throw new IllegalArgumentException("仅管理员可管理系统配置");
        }
        if (path.startsWith("/api/system/local-stack/") && !path.startsWith("/api/system/local-stack/verify")) {
            throw new IllegalArgumentException("仅管理员可启停本机链路");
        }
        if (path.startsWith("/api/system/canal/setting")) {
            throw new IllegalArgumentException("仅管理员可修改 Canal 全局配置");
        }
        if (path.startsWith("/api/system/canal/admin/password")) {
            throw new IllegalArgumentException("仅管理员可修改 Canal Admin 密码");
        }
        return true;
    }

    private String currentRole() {
        Long userId = Long.valueOf(StpUtil.getLoginIdAsString());
        return systemUserService.findById(userId)
                .filter(user -> user.status() == 1)
                .map(user -> user.roleCode() == null ? "VIEWER" : user.roleCode())
                .orElseThrow(() -> new IllegalArgumentException("用户不存在或已禁用"));
    }
}
