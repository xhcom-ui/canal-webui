package com.openclaw.canalweb;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CanalWebApplication {
    public static void main(String[] args) {
        SpringApplication.run(CanalWebApplication.class, args);
    }
}
