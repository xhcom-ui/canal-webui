package com.openclaw.canalweb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:mysql://${MYSQL_HOST:127.0.0.1}:${MYSQL_PORT:3306}/${MYSQL_DATABASE:canal_web}?useUnicode=true&characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai&allowPublicKeyRetrieval=true",
        "spring.datasource.username=${MYSQL_USERNAME:root}",
        "spring.datasource.password=${MYSQL_PASSWORD:root}",
        "spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver",
        "server.port=0"
})
@Disabled("Requires a running MySQL instance configured by MYSQL_HOST/MYSQL_PORT/MYSQL_DATABASE.")
class CanalWebApplicationTests {
    @Test
    void contextLoads() {
    }
}
