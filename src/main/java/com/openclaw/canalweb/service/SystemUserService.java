package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.SystemUser;
import com.openclaw.canalweb.dto.UserInfo;
import com.openclaw.canalweb.dto.UserSaveRequest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.util.List;
import java.util.Optional;

@Service
public class SystemUserService {
    private final JdbcClient jdbcClient;
    private final OperationLogService operationLogService;

    public SystemUserService(DataSource dataSource, OperationLogService operationLogService) {
        this.jdbcClient = JdbcClient.create(dataSource);
        this.operationLogService = operationLogService;
    }

    public Optional<SystemUser> findByUsername(String username) {
        return jdbcClient.sql("""
                SELECT id, username, password, nickname, role_code, status, create_time
                FROM app_user WHERE username = :username
                """).param("username", username).query(SystemUser.class).optional();
    }

    public Optional<SystemUser> findById(Long id) {
        return jdbcClient.sql("""
                SELECT id, username, password, nickname, role_code, status, create_time
                FROM app_user WHERE id = :id
                """).param("id", id).query(SystemUser.class).optional();
    }

    public List<UserInfo> listUsers() {
        return jdbcClient.sql("""
                SELECT id, username, nickname, role_code, status
                FROM app_user ORDER BY id
                """).query(UserInfo.class).list();
    }

    public UserInfo toUserInfo(SystemUser user) {
        return new UserInfo(user.id(), user.username(), user.nickname(), user.roleCode(), user.status());
    }

    @Transactional
    public UserInfo save(UserSaveRequest request) {
        String roleCode = normalizeRole(request.roleCode());
        int status = request.status() == null ? 1 : (request.status() == 1 ? 1 : 0);
        if (request.id() == null) {
            String password = request.password() == null || request.password().isBlank() ? "123456" : request.password();
            jdbcClient.sql("""
                    INSERT INTO app_user (username, password, nickname, role_code, status)
                    VALUES (:username, :password, :nickname, :roleCode, :status)
                    """)
                    .param("username", request.username())
                    .param("password", password)
                    .param("nickname", request.nickname())
                    .param("roleCode", roleCode)
                    .param("status", status)
                    .update();
            operationLogService.record("system-user", "create", request.username(), "用户已创建");
        } else {
            jdbcClient.sql("""
                    UPDATE app_user
                    SET username = :username, nickname = :nickname, role_code = :roleCode, status = :status
                    WHERE id = :id
                    """)
                    .param("id", request.id())
                    .param("username", request.username())
                    .param("nickname", request.nickname())
                    .param("roleCode", roleCode)
                    .param("status", status)
                    .update();
            if (request.password() != null && !request.password().isBlank()) {
                resetPassword(request.id(), request.password());
            }
            operationLogService.record("system-user", "update", String.valueOf(request.id()), "用户已更新");
        }
        return findByUsername(request.username()).map(this::toUserInfo).orElseThrow();
    }

    @Transactional
    public void enable(Long id, boolean enabled) {
        jdbcClient.sql("UPDATE app_user SET status = :status WHERE id = :id")
                .param("id", id)
                .param("status", enabled ? 1 : 0)
                .update();
        operationLogService.record("system-user", enabled ? "enable" : "disable", String.valueOf(id),
                enabled ? "用户已启用" : "用户已禁用");
    }

    @Transactional
    public void resetPassword(Long id, String password) {
        jdbcClient.sql("UPDATE app_user SET password = :password WHERE id = :id")
                .param("id", id)
                .param("password", password == null || password.isBlank() ? "123456" : password)
                .update();
        operationLogService.record("system-user", "reset-password", String.valueOf(id), "用户密码已重置");
    }

    private static String normalizeRole(String roleCode) {
        if ("OPERATOR".equals(roleCode) || "VIEWER".equals(roleCode)) {
            return roleCode;
        }
        return "ADMIN";
    }
}
