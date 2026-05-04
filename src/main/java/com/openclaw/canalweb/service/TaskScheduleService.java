package com.openclaw.canalweb.service;

import com.openclaw.canalweb.domain.SyncTask;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.support.CronExpression;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class TaskScheduleService {
    private final SyncTaskService syncTaskService;
    private final FullSyncService fullSyncService;
    private final TaskLogService taskLogService;

    public TaskScheduleService(SyncTaskService syncTaskService, FullSyncService fullSyncService,
                               TaskLogService taskLogService) {
        this.syncTaskService = syncTaskService;
        this.fullSyncService = fullSyncService;
        this.taskLogService = taskLogService;
    }

    @Scheduled(fixedDelayString = "${canal-web.scheduler.scan-interval-ms:30000}")
    public void scanFullSyncCron() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime since = now.minusMinutes(2);
        for (String taskId : fullSyncService.dueFullTaskIds(since)) {
            try {
                SyncTask task = syncTaskService.detail(taskId).task();
                if (!isDue(task, now)) {
                    continue;
                }
                fullSyncService.markScheduled(taskId);
                fullSyncService.execute(taskId, "cron");
            } catch (Exception ex) {
                taskLogService.error(taskId, "Cron 调度执行失败: " + ex.getMessage());
            }
        }
    }

    private static boolean isDue(SyncTask task, LocalDateTime now) {
        if (task.cronExpression() == null || task.cronExpression().isBlank()) {
            return false;
        }
        CronExpression cron = CronExpression.parse(task.cronExpression());
        LocalDateTime baseline = task.lastScheduleTime() == null ? now.minusMinutes(2) : task.lastScheduleTime();
        LocalDateTime next = cron.next(baseline);
        return next != null && !next.isAfter(now);
    }
}
