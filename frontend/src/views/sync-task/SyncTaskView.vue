<template>
  <div>
    <div class="page-head">
      <h1 class="page-title">同步任务</h1>
      <div class="toolbar">
        <a-select v-model:value="statusFilter" style="width:140px" :options="statusOptions" @change="loadTasks" />
        <a-button v-if="store.canOperate" @click="batchStart" :disabled="!selectedRowKeys.length"><Play :size="14" />批量启动</a-button>
        <a-button v-if="store.canOperate" @click="batchStop" :disabled="!selectedRowKeys.length"><Pause :size="14" />批量停止</a-button>
        <a-button v-if="store.canOperate" type="primary" @click="openCreate"><Plus :size="15" />新建任务</a-button>
      </div>
    </div>
    <div class="panel">
      <a-table
        :data-source="store.tasks"
        :columns="columns"
        row-key="id"
        :pagination="tablePagination(10)"
        :row-selection="{ selectedRowKeys, onChange: keys => selectedRowKeys = keys }"
      >
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'taskStatus'" :color="statusColor(record.taskStatus)">{{ record.taskStatus }}</a-tag>
          <a-space v-if="column.key === 'actions'" class="task-actions">
            <a-button v-if="store.canOperate" size="small" :loading="isActionLoading('edit', record.id)" @click="edit(record)"><Pencil :size="14" />编辑</a-button>
            <a-button v-if="store.canOperate" size="small" :loading="isActionLoading('clone', record.id)" @click="cloneTask(record)"><Copy :size="14" />复制</a-button>
            <a-button size="small" type="primary" v-if="store.canOperate && record.taskStatus !== 'RUNNING'" :loading="isActionLoading('start', record.id)" @click="start(record.id)"><Play :size="14" />启动</a-button>
            <a-button size="small" v-else-if="store.canOperate" :loading="isActionLoading('stop', record.id)" @click="stop(record.id)"><Pause :size="14" />停止</a-button>
            <a-tooltip :title="fullSyncTip(record)">
              <a-button v-if="store.canOperate" size="small" :disabled="!canRunFullSync(record)" :loading="isActionLoading('full', record.id)" @click="fullSync(record.id)"><DatabaseZap :size="14" />全量</a-button>
            </a-tooltip>
            <a-button size="small" :loading="isActionLoading('diagnostics', record.id)" @click="openDiagnostics(record.id)"><ShieldCheck :size="14" />自检</a-button>
            <a-button size="small" :loading="isActionLoading('detail', record.id)" @click="openDetail(record.id)"><FileSearch :size="14" />详情</a-button>
            <a-button size="small" :loading="isActionLoading('monitor', record.id)" @click="loadMonitor(record.id)"><Activity :size="14" />监控</a-button>
            <a-button v-if="store.canOperate" size="small" danger :loading="isActionLoading('delete', record.id)" @click="removeTask(record)"><Trash2 :size="14" />删除</a-button>
          </a-space>
        </template>
      </a-table>
    </div>

    <a-drawer v-model:open="drawerOpen" :title="form.id ? '编辑同步任务' : '新建同步任务'" width="780px">
      <a-form layout="vertical" :model="form">
        <a-row :gutter="12">
          <a-col :span="12"><a-form-item label="任务名称"><a-input v-model:value="form.taskName" /></a-form-item></a-col>
          <a-col :span="12">
            <a-form-item label="数据源">
              <a-select v-model:value="form.dataSourceKey" :options="datasourceOptions" @change="loadSourceTables" />
            </a-form-item>
          </a-col>
          <a-col :span="24"><a-form-item label="描述"><a-input v-model:value="form.description" /></a-form-item></a-col>
        </a-row>
        <a-row :gutter="12">
          <a-col :span="12">
            <a-form-item label="源表">
              <a-select
                v-model:value="selectedTable"
                show-search
                :options="tableOptions"
                :loading="tableLoading"
                @focus="loadSourceTables"
                @change="loadSourceColumns"
              />
            </a-form-item>
          </a-col>
          <a-col :span="12">
            <a-form-item label="表结构操作">
              <div class="toolbar">
                <a-button @click="applyTableSql" :disabled="!selectedTable">生成 SQL</a-button>
                <a-button @click="applyFieldMappings" :disabled="!sourceColumns.length">生成字段映射</a-button>
              </div>
            </a-form-item>
          </a-col>
        </a-row>
        <a-form-item label="同步 SQL"><SqlEditor v-model:value="form.syncSql" /></a-form-item>
        <div class="toolbar target-test-actions">
          <a-button @click="previewSql"><TableProperties :size="15" />预览 SQL</a-button>
          <a-button @click="applyPreviewFieldMappings" :disabled="!sqlPreview.columns?.length"><ListPlus :size="15" />按预览字段生成映射</a-button>
          <span class="muted-path" v-if="sqlPreview.columns?.length">字段 {{ sqlPreview.columns.length }} 个，样例 {{ sqlPreview.rows.length }} 行</span>
        </div>
        <a-collapse v-if="sqlPreview.columns?.length" class="preview-collapse">
          <a-collapse-panel key="sqlPreview" header="SQL 样例数据">
            <a-table
              :data-source="sqlPreview.rows"
              :columns="sqlPreviewTableColumns"
              :pagination="tablePagination(10)"
              size="small"
              row-key="__rowKey"
              :scroll="{ x: 'max-content' }"
            />
            <a-divider orientation="left">字段类型</a-divider>
            <a-table :data-source="sqlPreview.columns" :columns="sqlPreviewColumnColumns" row-key="name" :pagination="false" size="small" />
          </a-collapse-panel>
        </a-collapse>
        <a-form-item label="目标端类型">
          <a-radio-group v-model:value="form.targetType" button-style="solid">
            <a-radio-button value="LOGGER">Logger</a-radio-button>
            <a-radio-button value="KAFKA">Kafka</a-radio-button>
            <a-radio-button value="ROCKETMQ">RocketMQ</a-radio-button>
            <a-radio-button value="RABBITMQ">RabbitMQ</a-radio-button>
            <a-radio-button value="PULSAR">Pulsar</a-radio-button>
            <a-radio-button value="MYSQL">MySQL/RDB</a-radio-button>
            <a-radio-button value="ES">Elasticsearch</a-radio-button>
            <a-radio-button value="REDIS">Redis</a-radio-button>
            <a-radio-button value="HBASE">HBase</a-radio-button>
            <a-radio-button value="TABLESTORE">Tablestore</a-radio-button>
            <a-radio-button value="PGSQL">PostgreSQL</a-radio-button>
            <a-radio-button value="CLICKHOUSE">ClickHouse</a-radio-button>
          </a-radio-group>
        </a-form-item>
        <TargetConfig v-model="form.targetConfig" :type="form.targetType" />
        <div class="toolbar target-test-actions">
          <a-button @click="testTarget"><PlugZap :size="15" />测试目标端</a-button>
        </div>
        <SyncStrategy :model-value="form" @update:modelValue="updateForm" />
        <a-form-item label="字段映射">
          <FieldMapping v-model="form.fieldMappings" :target-type="form.targetType" />
          <div class="toolbar target-test-actions field-mapping-actions">
            <a-button @click="validateFieldMappings" :disabled="!form.fieldMappings.length"><ShieldCheck :size="15" />校验字段映射</a-button>
            <span class="muted-path" v-if="fieldMappingValidation.total">
              通过 {{ fieldMappingValidation.okCount }}/{{ fieldMappingValidation.total }}，阻断 {{ fieldMappingValidation.blockerCount || 0 }}
            </span>
          </div>
        </a-form-item>
        <div class="toolbar" style="justify-content:flex-end">
          <a-button @click="drawerOpen = false">取消</a-button>
          <a-button type="primary" @click="submit"><Save :size="15" />保存</a-button>
          <a-button type="primary" @click="submitAndStart"><Play :size="15" />保存并启动</a-button>
        </div>
      </a-form>
    </a-drawer>

    <a-modal v-model:open="monitorOpen" title="任务监控" :footer="null" width="680px">
      <div class="toolbar task-panel-actions">
        <a-button size="small" :loading="isActionLoading('monitor', monitor.taskId)" @click="loadMonitor(monitor.taskId)"><Activity :size="14" />刷新监控</a-button>
        <a-button v-if="store.canOperate && monitor.status !== 'RUNNING'" size="small" type="primary" :loading="isActionLoading('start', monitor.taskId)" @click="start(monitor.taskId, { refreshMonitor: true })"><Play :size="14" />启动</a-button>
        <a-button v-if="store.canOperate && monitor.status === 'RUNNING'" size="small" :loading="isActionLoading('stop', monitor.taskId)" @click="stop(monitor.taskId, { refreshMonitor: true })"><Pause :size="14" />停止</a-button>
        <a-button size="small" @click="openDetail(monitor.taskId)"><FileSearch :size="14" />打开详情</a-button>
      </div>
      <a-descriptions bordered :column="1" size="small">
        <a-descriptions-item label="任务 ID">{{ monitor.taskId }}</a-descriptions-item>
        <a-descriptions-item label="状态">{{ monitor.status }}</a-descriptions-item>
        <a-descriptions-item label="日志数">{{ monitor.logCount }}</a-descriptions-item>
        <a-descriptions-item label="异常数">{{ monitor.errorCount }}</a-descriptions-item>
        <a-descriptions-item label="同步总数">{{ monitor.totalCount }}</a-descriptions-item>
        <a-descriptions-item label="失败总数">{{ monitor.failCount }}</a-descriptions-item>
        <a-descriptions-item label="延迟">{{ monitor.delayMs }} ms</a-descriptions-item>
        <a-descriptions-item label="最后启动">{{ monitor.lastStartTime || '-' }}</a-descriptions-item>
        <a-descriptions-item label="最后停止">{{ monitor.lastStopTime || '-' }}</a-descriptions-item>
        <a-descriptions-item label="最后调度">{{ monitor.lastScheduleTime || '-' }}</a-descriptions-item>
        <a-descriptions-item label="全量文件">{{ monitor.fullSyncFile || '-' }}</a-descriptions-item>
      </a-descriptions>
    </a-modal>

    <a-drawer v-model:open="diagnosticsOpen" title="任务启动自检" width="820px">
      <div class="toolbar task-panel-actions">
        <a-button size="small" :loading="isActionLoading('diagnostics', diagnostics.taskId)" @click="openDiagnostics(diagnostics.taskId)"><ShieldCheck :size="14" />重新自检</a-button>
        <a-button
          v-if="store.canOperate && diagnostics.taskId && !diagnostics.blockerCount"
          size="small"
          type="primary"
          :loading="isActionLoading('start', diagnostics.taskId)"
          @click="start(diagnostics.taskId, { closeDiagnostics: true })"
        >
          <Play :size="14" />通过并启动
        </a-button>
        <a-button v-if="diagnostics.taskId" size="small" @click="openDetail(diagnostics.taskId)"><FileSearch :size="14" />打开详情</a-button>
      </div>
      <a-alert
        :type="diagnostics.blockerCount ? 'error' : (diagnostics.warnCount ? 'warning' : 'success')"
        :message="diagnosticMessage(diagnostics)"
        show-icon
      />
      <a-descriptions bordered :column="1" size="small" style="margin:12px 0">
        <a-descriptions-item label="任务">{{ diagnostics.taskName || diagnostics.taskId }}</a-descriptions-item>
        <a-descriptions-item label="模式">{{ diagnostics.syncMode }}</a-descriptions-item>
        <a-descriptions-item label="目标端">{{ diagnostics.targetType }}</a-descriptions-item>
      </a-descriptions>
      <a-table :data-source="diagnostics.checks || []" :columns="diagnosticColumns" row-key="name" :pagination="tablePagination(10)" size="small">
        <template #bodyCell="{ column, record }">
          <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : 'red'">{{ record.ok ? '通过' : '异常' }}</a-tag>
          <a-tag v-if="column.key === 'severity'" :color="severityColor(record.severity)">{{ severityLabel(record.severity) }}</a-tag>
        </template>
      </a-table>
      <a-divider orientation="left">生成配置摘要</a-divider>
      <a-collapse>
        <a-collapse-panel key="outerAdapter" header="Adapter outerAdapters">
          <pre class="code-block">{{ diagnostics.preview?.outerAdapter || '暂无配置' }}</pre>
        </a-collapse-panel>
        <a-collapse-panel key="mapping" header="Mapping">
          <pre class="code-block">{{ diagnostics.preview?.mappingContent || '暂无配置' }}</pre>
        </a-collapse-panel>
      </a-collapse>
    </a-drawer>

    <a-drawer v-model:open="detailOpen" title="任务详情" width="920px">
      <div class="toolbar task-panel-actions">
        <a-button size="small" :loading="isActionLoading('detail', detail.task?.id)" @click="refreshDetail()"><Activity :size="14" />刷新详情</a-button>
        <a-button v-if="store.canOperate && detail.task?.taskStatus !== 'RUNNING'" size="small" type="primary" :loading="isActionLoading('start', detail.task?.id)" @click="start(detail.task?.id, { refreshDetail: true })"><Play :size="14" />启动</a-button>
        <a-button v-if="store.canOperate && detail.task?.taskStatus === 'RUNNING'" size="small" :loading="isActionLoading('stop', detail.task?.id)" @click="stop(detail.task?.id, { refreshDetail: true })"><Pause :size="14" />停止</a-button>
        <a-tooltip :title="fullSyncTip(detail.task)">
          <a-button v-if="store.canOperate" size="small" :disabled="!canRunFullSync(detail.task)" :loading="isActionLoading('full', detail.task?.id)" @click="fullSync(detail.task?.id, { refreshDetail: true, switchTab: 'fullSync' })"><DatabaseZap :size="14" />执行全量</a-button>
        </a-tooltip>
        <a-button v-if="detail.task?.id" size="small" :loading="isActionLoading('diagnostics', detail.task?.id)" @click="openDiagnostics(detail.task.id)"><ShieldCheck :size="14" />启动自检</a-button>
      </div>
      <a-tabs v-model:activeKey="detailTab">
        <a-tab-pane key="base" tab="基础信息">
          <a-descriptions bordered :column="1" size="small" v-if="detail.task">
            <a-descriptions-item label="任务 ID">{{ detail.task.id }}</a-descriptions-item>
            <a-descriptions-item label="任务名称">{{ detail.task.taskName }}</a-descriptions-item>
            <a-descriptions-item label="数据源">{{ detail.task.dataSourceKey }}</a-descriptions-item>
            <a-descriptions-item label="目标端">{{ detail.task.targetType }}</a-descriptions-item>
            <a-descriptions-item label="模式">{{ detail.task.syncMode }}</a-descriptions-item>
            <a-descriptions-item label="Cron">{{ detail.task.cronExpression || '-' }}</a-descriptions-item>
            <a-descriptions-item label="状态">
              <a-tag :color="statusColor(detail.task.taskStatus)">{{ detail.task.taskStatus }}</a-tag>
            </a-descriptions-item>
            <a-descriptions-item label="同步 SQL"><pre class="code-block">{{ detail.task.syncSql }}</pre></a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>
        <a-tab-pane key="monitor" tab="监控">
          <a-descriptions bordered :column="1" size="small">
            <a-descriptions-item label="同步总数">{{ detailMonitor.totalCount ?? 0 }}</a-descriptions-item>
            <a-descriptions-item label="失败总数">{{ detailMonitor.failCount ?? 0 }}</a-descriptions-item>
            <a-descriptions-item label="延迟">{{ detailMonitor.delayMs ?? 0 }} ms</a-descriptions-item>
            <a-descriptions-item label="最后启动">{{ detailMonitor.lastStartTime || '-' }}</a-descriptions-item>
            <a-descriptions-item label="最后停止">{{ detailMonitor.lastStopTime || '-' }}</a-descriptions-item>
            <a-descriptions-item label="最后调度">{{ detailMonitor.lastScheduleTime || '-' }}</a-descriptions-item>
            <a-descriptions-item label="全量文件">{{ detailMonitor.fullSyncFile || '-' }}</a-descriptions-item>
          </a-descriptions>
        </a-tab-pane>
        <a-tab-pane key="runtime" tab="运行态">
          <a-alert :type="detailRuntime.lastError ? 'error' : 'info'" :message="detailRuntime.suggestion || '暂无运行态建议'" show-icon style="margin-bottom:12px" />
          <a-descriptions bordered :column="2" size="small" style="margin-bottom:12px">
            <a-descriptions-item label="状态">{{ detailRuntime.status || '-' }}</a-descriptions-item>
            <a-descriptions-item label="目标端">{{ detailRuntime.targetType || '-' }}</a-descriptions-item>
            <a-descriptions-item label="Destination">{{ detailRuntime.destination || '-' }}</a-descriptions-item>
            <a-descriptions-item label="Adapter Key">{{ detailRuntime.adapterKey || '-' }}</a-descriptions-item>
            <a-descriptions-item label="Mapping 目录">{{ detailRuntime.mappingDir || '-' }}</a-descriptions-item>
            <a-descriptions-item label="Mapping 文件">{{ detailRuntime.mappingFile || '-' }}</a-descriptions-item>
            <a-descriptions-item label="日志数">{{ detailRuntime.logCount ?? 0 }}</a-descriptions-item>
            <a-descriptions-item label="错误数">{{ detailRuntime.errorCount ?? 0 }}</a-descriptions-item>
            <a-descriptions-item label="Adapter 可达">
              <a-tag :color="detailRuntime.destinationStatus?.reachable ? 'green' : 'red'">{{ detailRuntime.destinationStatus?.reachable ? '可达' : '不可达' }}</a-tag>
            </a-descriptions-item>
            <a-descriptions-item label="Destination 在线">
              <a-tag :color="detailRuntime.destinationStatus?.online ? 'green' : 'orange'">{{ detailRuntime.destinationStatus?.online ? '在线' : '未在线' }}</a-tag>
            </a-descriptions-item>
          </a-descriptions>
          <a-collapse>
            <a-collapse-panel key="runtimeFiles" header="Runtime 文件">
              <a-table :data-source="detailRuntime.runtimeFiles || []" :columns="runtimeFileColumns" row-key="path" :pagination="tablePagination(10)" size="small">
                <template #bodyCell="{ column, record }">
                  <a-tag v-if="column.key === 'exists'" :color="record.exists ? 'green' : 'red'">{{ record.exists ? '存在' : '缺失' }}</a-tag>
                  <span v-else-if="column.key === 'path'">{{ record.activePath || record.path }}</span>
                </template>
              </a-table>
            </a-collapse-panel>
            <a-collapse-panel key="lastError" header="最近错误">
              <pre class="code-block">{{ detailRuntime.lastError?.logContent || '暂无错误' }}</pre>
            </a-collapse-panel>
            <a-collapse-panel key="recentLogs" header="最近日志">
              <a-table :data-source="detailRuntime.recentLogs || []" :columns="logColumns" row-key="id" :pagination="tablePagination(10)" size="small">
                <template #bodyCell="{ column, record }">
                  <a-tag v-if="column.key === 'logLevel'" :color="record.logLevel === 'ERROR' ? 'red' : 'blue'">{{ record.logLevel }}</a-tag>
                </template>
              </a-table>
            </a-collapse-panel>
          </a-collapse>
        </a-tab-pane>
        <a-tab-pane key="diagnostics" tab="启动自检">
          <a-alert
            :type="detailDiagnostics.blockerCount ? 'error' : (detailDiagnostics.warnCount ? 'warning' : 'success')"
            :message="diagnosticMessage(detailDiagnostics)"
            show-icon
          />
          <a-table :data-source="detailDiagnostics.checks || []" :columns="diagnosticColumns" row-key="name" :pagination="tablePagination(10)" size="small" style="margin-top:12px">
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : 'red'">{{ record.ok ? '通过' : '异常' }}</a-tag>
              <a-tag v-if="column.key === 'severity'" :color="severityColor(record.severity)">{{ severityLabel(record.severity) }}</a-tag>
            </template>
          </a-table>
        </a-tab-pane>
        <a-tab-pane key="preview" tab="生成配置">
          <a-collapse>
            <a-collapse-panel key="taskSpec" header="任务规格">
              <pre class="code-block">{{ detailPreview.taskSpec || '暂无配置' }}</pre>
            </a-collapse-panel>
            <a-collapse-panel key="outerAdapter" header="Adapter outerAdapters">
              <pre class="code-block">{{ detailPreview.outerAdapter || '暂无配置' }}</pre>
            </a-collapse-panel>
            <a-collapse-panel key="mapping" :header="`${detailPreview.mappingDir || '-'} / ${detailPreview.mappingFile || '-'}`">
              <pre class="code-block">{{ detailPreview.mappingContent || '暂无配置' }}</pre>
            </a-collapse-panel>
          </a-collapse>
        </a-tab-pane>
        <a-tab-pane key="resourcePlan" tab="资源准备">
          <a-alert
            :type="detailResourcePlan.blockerCount ? 'error' : (detailResourcePlan.warnCount ? 'warning' : 'success')"
            :message="resourcePlanMessage(detailResourcePlan)"
            show-icon
            style="margin-bottom:12px"
          />
          <a-descriptions bordered :column="4" size="small" style="margin-bottom:12px">
            <a-descriptions-item label="目标端">{{ detailResourcePlan.targetType || '-' }}</a-descriptions-item>
            <a-descriptions-item label="资源项">{{ detailResourcePlan.count ?? 0 }}</a-descriptions-item>
            <a-descriptions-item label="已就绪">{{ detailResourcePlan.readyCount ?? 0 }}/{{ detailResourcePlan.checkCount ?? 0 }}</a-descriptions-item>
            <a-descriptions-item label="阻断/警告">{{ detailResourcePlan.blockerCount ?? 0 }}/{{ detailResourcePlan.warnCount ?? 0 }}</a-descriptions-item>
          </a-descriptions>
          <a-table
            v-if="detailResourcePlan.checks?.length"
            :data-source="detailResourcePlan.checks"
            :columns="diagnosticColumns"
            row-key="name"
            :pagination="tablePagination(10)"
            size="small"
            style="margin-bottom:12px"
          >
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'ok'" :color="record.ok ? 'green' : severityColor(record.severity)">
                {{ record.ok ? '通过' : severityLabel(record.severity) }}
              </a-tag>
            </template>
          </a-table>
          <a-descriptions v-if="Object.keys(detailResourcePlan.sourceTypes || {}).length" bordered :column="1" size="small" style="margin-bottom:12px">
            <a-descriptions-item label="源字段类型">
              <pre class="inline-code">{{ JSON.stringify(detailResourcePlan.sourceTypes, null, 2) }}</pre>
            </a-descriptions-item>
          </a-descriptions>
          <a-collapse v-if="detailResourcePlan.resources?.length">
            <a-collapse-panel
              v-for="item in detailResourcePlan.resources"
              :key="item.name"
              :header="`${item.name} / ${item.type}`"
            >
              <div class="muted-path">{{ item.description }}</div>
              <pre class="code-block">{{ item.content || '暂无脚本' }}</pre>
            </a-collapse-panel>
          </a-collapse>
          <a-empty v-else description="当前目标端暂无前置资源脚本" />
        </a-tab-pane>
        <a-tab-pane key="fullSync" tab="全量结果">
          <div class="toolbar" style="margin-bottom:12px">
            <a-button @click="loadFullSyncPreview(detail.task?.id)" :disabled="!detail.task?.id">刷新结果</a-button>
            <span class="muted-path" v-if="fullSyncPreview.file">{{ fullSyncPreview.file }}</span>
          </div>
          <a-alert
            v-if="fullSyncPreview.message"
            :type="fullSyncPreview.exists ? 'success' : 'info'"
            :message="fullSyncPreview.message"
            show-icon
            style="margin-bottom:12px"
          />
          <a-descriptions bordered :column="3" size="small" style="margin-bottom:12px">
            <a-descriptions-item label="文件状态">{{ fullSyncPreview.exists ? '存在' : '暂无' }}</a-descriptions-item>
            <a-descriptions-item label="文件大小">{{ formatBytes(fullSyncPreview.size || 0) }}</a-descriptions-item>
            <a-descriptions-item label="总行数">{{ fullSyncPreview.lineCount || 0 }}</a-descriptions-item>
          </a-descriptions>
          <a-table
            v-if="fullSyncPreviewRows.length"
            :data-source="fullSyncPreviewRows"
            :columns="fullSyncPreviewColumns"
            row-key="__rowKey"
            :pagination="tablePagination(10)"
            size="small"
            :scroll="{ x: 'max-content' }"
          />
          <a-empty v-else-if="fullSyncPreview.exists" description="结果文件为空或暂无可表格化的数据" />
          <a-collapse v-if="fullSyncPreview.rawLines?.length" style="margin-top:12px">
            <a-collapse-panel key="rawLines" header="原始行">
              <pre class="code-block">{{ fullSyncPreview.rawLines.join('\n') }}</pre>
            </a-collapse-panel>
          </a-collapse>
        </a-tab-pane>
        <a-tab-pane key="logs" tab="任务日志">
          <a-table :data-source="detailLogs" :columns="logColumns" row-key="id" :pagination="tablePagination(10)">
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'logLevel'" :color="record.logLevel === 'ERROR' ? 'red' : 'blue'">{{ record.logLevel }}</a-tag>
            </template>
          </a-table>
        </a-tab-pane>
        <a-tab-pane key="versions" tab="配置版本">
          <a-table :data-source="detailVersions" :columns="versionColumns" row-key="id" :pagination="tablePagination(10)">
            <template #bodyCell="{ column, record }">
              <a-tag v-if="column.key === 'rollbackable'" :color="isRollbackable(record) ? 'green' : 'default'">
                {{ isRollbackable(record) ? '可回滚' : '仅查看' }}
              </a-tag>
              <a-button
                v-if="column.key === 'actions' && store.isAdmin && isRollbackable(record)"
                size="small"
                @click="rollbackVersion(record)"
              >
                回滚
              </a-button>
            </template>
            <template #expandedRowRender="{ record }">
              <pre class="code-block">{{ record.configContent }}</pre>
            </template>
          </a-table>
        </a-tab-pane>
      </a-tabs>
    </a-drawer>
  </div>
</template>

<script setup>
import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { message, Modal } from 'ant-design-vue'
import { Activity, Copy, DatabaseZap, FileSearch, ListPlus, Pause, Pencil, Play, PlugZap, Plus, Save, ShieldCheck, TableProperties, Trash2 } from 'lucide-vue-next'
import FieldMapping from '../../components/FieldMapping.vue'
import SqlEditor from '../../components/SqlEditor.vue'
import SyncStrategy from '../../components/SyncStrategy.vue'
import TargetConfig from '../../components/TargetConfig.vue'
import { datasourceApi, logApi, systemApi, taskApi } from '../../api'
import { useAppStore } from '../../stores/useAppStore'
import { tablePagination } from '../../utils/pagination'
import { normalizeTargetType } from '../../utils/targetType'

const store = useAppStore()
const drawerOpen = ref(false)
const monitorOpen = ref(false)
const diagnosticsOpen = ref(false)
const detailOpen = ref(false)
const detailTab = ref('base')
const statusFilter = ref('')
const selectedRowKeys = ref([])
const monitor = reactive({})
const diagnostics = ref({})
const detail = reactive({})
const detailMonitor = reactive({})
const detailDiagnostics = reactive({})
const detailPreview = reactive({})
const detailResourcePlan = reactive({})
const detailRuntime = reactive({})
const fullSyncPreview = ref({})
const fieldMappingValidation = ref({})
const detailLogs = ref([])
const detailVersions = ref([])
const sqlPreview = ref({})
const actionLoading = ref({})
const form = reactive(defaultForm())
const selectedTable = ref('')
const sourceTables = ref([])
const sourceColumns = ref([])
const tableLoading = ref(false)
let refreshTimer = null
const datasourceOptions = computed(() => store.datasources.map(item => ({ label: `${item.dataSourceKey} / ${item.dbName}`, value: item.dataSourceKey })))
const tableOptions = computed(() => sourceTables.value.map(item => ({
  label: item.remarks ? `${item.tableName} / ${item.remarks}` : item.tableName,
  value: item.tableName
})))
const columns = [
  { title: '任务名称', dataIndex: 'taskName' },
  { title: '数据源', dataIndex: 'dataSourceKey' },
  { title: '目标端', dataIndex: 'targetType' },
  { title: '模式', dataIndex: 'syncMode' },
  { title: '同步数', dataIndex: 'totalCount' },
  { title: '失败数', dataIndex: 'failCount' },
  { title: '状态', key: 'taskStatus' },
  { title: '操作', key: 'actions', width: 500 }
]
const logColumns = [
  { title: '级别', key: 'logLevel', width: 100 },
  { title: '内容', dataIndex: 'logContent' },
  { title: '时间', dataIndex: 'createTime', width: 210 }
]
const runtimeFileColumns = [
  { title: '文件', dataIndex: 'name', width: 120 },
  { title: '状态', key: 'exists', width: 90 },
  { title: '大小', dataIndex: 'size', width: 90 },
  { title: '路径', key: 'path' }
]
const versionColumns = [
  { title: '类型', dataIndex: 'configType', width: 140 },
  { title: '版本', dataIndex: 'versionNo', width: 100 },
  { title: '回滚', key: 'rollbackable', width: 100 },
  { title: '时间', dataIndex: 'createTime', width: 210 },
  { title: '操作', key: 'actions', width: 90 }
]
const diagnosticColumns = [
  { title: '检查项', dataIndex: 'name', width: 150 },
  { title: '结果', key: 'ok', width: 80 },
  { title: '级别', key: 'severity', width: 90 },
  { title: '说明', dataIndex: 'message' },
  { title: '处理建议', dataIndex: 'detail' }
]
const sqlPreviewColumnColumns = [
  { title: '字段', dataIndex: 'name' },
  { title: '类型', dataIndex: 'type', width: 140 }
]
const sqlPreviewTableColumns = computed(() => (sqlPreview.value.columns || []).map(column => ({
  title: column.name,
  dataIndex: column.name,
  width: 160,
  customRender: ({ text }) => text == null ? '' : String(text)
})))
const fullSyncPreviewRows = computed(() => (fullSyncPreview.value.rows || []).map((row, index) => ({ __rowKey: index, ...row })))
const fullSyncPreviewColumns = computed(() => {
  const keys = Array.from(new Set(fullSyncPreviewRows.value.flatMap(row => Object.keys(row).filter(key => key !== '__rowKey'))))
  return keys.map(key => ({
    title: key,
    dataIndex: key,
    width: 160,
    customRender: ({ text }) => text == null ? '' : String(text)
  }))
})
const statusOptions = [
  { label: '全部状态', value: '' },
  { label: '运行中', value: 'RUNNING' },
  { label: '已停止', value: 'STOPPED' },
  { label: '异常', value: 'ERROR' }
]

function actionKey(action, id) {
  return `${action}:${id || ''}`
}

function isActionLoading(action, id) {
  return Boolean(actionLoading.value[actionKey(action, id)])
}

async function withActionLoading(action, id, fn) {
  const key = actionKey(action, id)
  actionLoading.value = { ...actionLoading.value, [key]: true }
  try {
    return await fn()
  } finally {
    const next = { ...actionLoading.value }
    delete next[key]
    actionLoading.value = next
  }
}

function canRunFullSync(task) {
  return ['FULL', 'FULL_INCREMENTAL'].includes((task?.syncMode || '').toUpperCase())
}

function fullSyncTip(task) {
  if (!task) return '先选择任务'
  return canRunFullSync(task) ? '按同步 SQL 执行全量快照' : '仅 FULL 或 FULL_INCREMENTAL 模式可执行全量'
}

function defaultForm() {
  return {
    id: '', taskName: '', description: '', dataSourceKey: '', syncSql: 'SELECT * FROM your_table',
    targetType: 'LOGGER', syncMode: 'INCREMENTAL', cronExpression: '', batchSize: 1000,
    fieldMappings: [defaultFieldMapping('id', 'id', true)],
    targetConfig: { adapterKey: 'logger' }
  }
}

function assign(data) {
  Object.assign(form, defaultForm(), data)
  form.targetType = normalizeTargetType(form.targetType)
  selectedTable.value = ''
  sourceColumns.value = []
  sqlPreview.value = {}
  fieldMappingValidation.value = {}
}

function updateForm(next) {
  Object.assign(form, next)
}

function openCreate() {
  assign({ dataSourceKey: store.datasources[0]?.dataSourceKey || '' })
  drawerOpen.value = true
  loadSourceTables()
}

async function edit(record) {
  await withActionLoading('edit', record.id, async () => {
    const detail = await taskApi.detail(record.id)
    const config = Object.fromEntries((detail.targetConfig || []).map(item => [item.configKey, item.configValue]))
    assign({ ...detail.task, targetConfig: config, fieldMappings: detail.fieldMappings || [] })
    drawerOpen.value = true
    loadSourceTables()
  })
}

async function cloneTask(record) {
  await withActionLoading('clone', record.id, async () => {
    const cloned = await taskApi.clone(record.id)
    await store.refreshAll()
    message.success(`已复制任务：${cloned?.task?.taskName || '新任务'}`)
  })
}

async function submit() {
  await saveTaskOnly()
  drawerOpen.value = false
  await store.refreshAll()
  message.success('任务已保存')
}

async function testTarget() {
  const result = await taskApi.targetTest({
    targetType: form.targetType,
    targetConfig: form.targetConfig
  })
  message.success(result?.message || '目标端连接检查通过')
}

async function previewSql() {
  const result = await taskApi.sqlPreview(form, 20)
  sqlPreview.value = {
    columns: result.columns || [],
    rows: (result.rows || []).map((row, index) => ({ __rowKey: index, ...row })),
    limit: result.limit
  }
  message.success('SQL 预览完成')
}

async function validateFieldMappings() {
  const result = await taskApi.validateFieldMappings(form.fieldMappings)
  fieldMappingValidation.value = result || {}
  if (result?.mappings) {
    form.fieldMappings = result.mappings
  }
  if (result?.blockerCount > 0) {
    message.error(`字段映射存在 ${result.blockerCount} 个阻断项`)
    return
  }
  message.success('字段映射校验通过，已回填规范化结果')
}

function applyPreviewFieldMappings() {
  if (!sqlPreview.value.columns?.length) return
  form.fieldMappings = sqlPreview.value.columns.map(column => ({
    sourceField: column.name,
    targetField: column.name,
    fieldType: column.type || '',
    primaryKey: column.name === 'id',
    nullableField: column.name !== 'id',
    enabled: true,
    defaultValue: '',
    transformExpr: '',
    formatPattern: '',
    fieldOptions: ''
  }))
  if (form.targetConfig) {
    form.targetConfig.sourceTable = form.targetConfig.sourceTable || selectedTable.value || ''
  }
  message.success('已按 SQL 预览字段生成映射')
}

async function saveTaskOnly() {
  const saved = await taskApi.save(form)
  if (saved?.task?.id) {
    form.id = saved.task.id
  }
  return saved
}

async function submitAndStart() {
  const saved = await saveTaskOnly()
  const id = saved?.task?.id || form.id
  await taskApi.start(id)
  drawerOpen.value = false
  await store.refreshAll()
  message.success('任务已保存并启动')
}

async function loadSourceTables() {
  if (!form.dataSourceKey) return
  tableLoading.value = true
  try {
    sourceTables.value = await datasourceApi.tables(form.dataSourceKey)
  } finally {
    tableLoading.value = false
  }
}

async function loadSourceColumns() {
  sourceColumns.value = []
  if (!form.dataSourceKey || !selectedTable.value) return
  sourceColumns.value = await datasourceApi.columns(form.dataSourceKey, selectedTable.value)
}

async function applyTableSql() {
  if (!selectedTable.value) return
  form.syncSql = `SELECT * FROM ${selectedTable.value}`
  if (!sourceColumns.value.length) {
    await loadSourceColumns()
  }
  message.success('同步 SQL 已生成')
}

async function applyFieldMappings() {
  if (!sourceColumns.value.length) {
    await loadSourceColumns()
  }
  form.fieldMappings = sourceColumns.value.map(item => ({
    sourceField: item.columnName,
    targetField: item.columnName,
    fieldType: item.columnType || item.typeName || '',
    primaryKey: item.columnName === 'id',
    nullableField: item.columnName !== 'id',
    enabled: true,
    defaultValue: '',
    transformExpr: '',
    formatPattern: '',
    fieldOptions: ''
  }))
  if (form.targetConfig) {
    form.targetConfig.sourceTable = selectedTable.value
  }
  message.success('字段映射已生成')
}

async function start(id, options = {}) {
  if (!id) return
  await withActionLoading('start', id, async () => {
    const result = await taskApi.diagnostics(id)
    if (result.blockerCount > 0) {
      diagnostics.value = result
      diagnosticsOpen.value = true
      message.error(`存在 ${result.blockerCount} 个阻断项，已打开自检详情`)
      return
    }
    await taskApi.start(id)
    if (options.closeDiagnostics) diagnosticsOpen.value = false
    await store.refreshAll()
    await refreshOpenPanels(id, options)
    message.success(result.warnCount > 0 ? `任务已启动，仍有 ${result.warnCount} 个风险项` : '任务已启动')
  })
}

async function stop(id, options = {}) {
  if (!id) return
  await withActionLoading('stop', id, async () => {
    await taskApi.stop(id)
    await store.refreshAll()
    await refreshOpenPanels(id, options)
    message.success('任务已停止')
  })
}

async function removeTask(record) {
  Modal.confirm({
    title: '删除同步任务',
    content: `确认删除任务「${record.taskName}」？删除后会清理对应 Canal 运行配置。`,
    async onOk() {
      await withActionLoading('delete', record.id, async () => {
        await taskApi.remove(record.id)
        await store.refreshAll()
        if (detail.task?.id === record.id) detailOpen.value = false
        if (monitor.taskId === record.id) monitorOpen.value = false
        message.success('任务已删除')
      })
    }
  })
}

async function fullSync(id, options = {}) {
  if (!id) return
  const task = store.tasks.find(item => item.id === id) || detail.task
  if (!canRunFullSync(task)) {
    message.warning('当前任务不是 FULL 或 FULL_INCREMENTAL 模式，不能执行全量')
    return
  }
  Modal.confirm({
    title: '执行全量同步',
    content: '全量同步会按 syncSql 读取源库并生成快照文件，确认执行？',
    async onOk() {
      await withActionLoading('full', id, async () => {
        const file = await taskApi.fullSync(id)
        await store.refreshAll()
        await refreshOpenPanels(id, options)
        if (options.switchTab) detailTab.value = options.switchTab
        message.success(`全量同步完成：${file}`)
      })
    }
  })
}

async function batchStart() {
  await taskApi.batchStart(selectedRowKeys.value)
  selectedRowKeys.value = []
  await store.refreshAll()
  message.success('批量启动完成')
}

async function batchStop() {
  await taskApi.batchStop(selectedRowKeys.value)
  selectedRowKeys.value = []
  await store.refreshAll()
  message.success('批量停止完成')
}

async function loadTasks() {
  store.tasks = await taskApi.list(statusFilter.value)
  await store.refreshStats()
}

async function loadMonitor(id) {
  if (!id) return
  await withActionLoading('monitor', id, async () => {
    clearReactive(monitor)
    Object.assign(monitor, await taskApi.monitor(id))
    monitorOpen.value = true
  })
}

async function openDiagnostics(id) {
  if (!id) return
  await withActionLoading('diagnostics', id, async () => {
    diagnostics.value = await taskApi.diagnostics(id)
    diagnosticsOpen.value = true
  })
}

async function openDetail(id) {
  if (!id) return
  await withActionLoading('detail', id, async () => {
    detailTab.value = 'base'
    await loadDetailData(id)
    detailOpen.value = true
  })
}

async function refreshDetail() {
  const id = detail.task?.id
  if (!id) return
  await withActionLoading('detail', id, async () => {
    await loadDetailData(id)
    message.success('详情已刷新')
  })
}

async function loadDetailData(id) {
  clearReactive(detail)
  clearReactive(detailMonitor)
  clearReactive(detailRuntime)
  clearReactive(detailDiagnostics)
  clearReactive(detailPreview)
  clearReactive(detailResourcePlan)
  Object.assign(detail, await taskApi.detail(id))
  Object.assign(detailMonitor, await taskApi.monitor(id))
  Object.assign(detailRuntime, await taskApi.runtime(id))
  Object.assign(detailDiagnostics, await taskApi.diagnostics(id))
  Object.assign(detailPreview, await taskApi.preview(id))
  Object.assign(detailResourcePlan, await taskApi.resourcePlan(id))
  fullSyncPreview.value = await taskApi.fullSyncPreview(id)
  detailLogs.value = await logApi.task(id)
  detailVersions.value = await systemApi.configVersions({ refId: id })
}

async function refreshOpenPanels(id, options = {}) {
  if ((monitorOpen.value && monitor.taskId === id) || options.refreshMonitor) {
    clearReactive(monitor)
    Object.assign(monitor, await taskApi.monitor(id))
  }
  if ((detailOpen.value && detail.task?.id === id) || options.refreshDetail) {
    await loadDetailData(id)
  }
  if (diagnosticsOpen.value && diagnostics.value?.taskId === id && !options.closeDiagnostics) {
    diagnostics.value = await taskApi.diagnostics(id)
  }
}

async function loadFullSyncPreview(id) {
  if (!id) return
  fullSyncPreview.value = await taskApi.fullSyncPreview(id)
}

function isRollbackable(record) {
  const content = record?.configContent || ''
  return content.trim().startsWith('{') && record.configType === 'task'
}

function rollbackVersion(record) {
  Modal.confirm({
    title: '回滚任务配置',
    content: `确认将当前任务回滚到 v${record.versionNo}？回滚后会刷新 Canal 运行配置。`,
    async onOk() {
      await systemApi.rollbackConfigVersion(record.id)
      Object.assign(detail, await taskApi.detail(detail.task.id))
      Object.assign(detailPreview, await taskApi.preview(detail.task.id))
      detailVersions.value = await systemApi.configVersions({ refId: detail.task.id })
      await store.refreshAll()
      message.success('任务配置已回滚')
    }
  })
}

watch(detailTab, async tab => {
  const id = detail.task?.id
  if (!detailOpen.value || !id) return
  if (tab === 'monitor') Object.assign(detailMonitor, await taskApi.monitor(id))
  if (tab === 'runtime') Object.assign(detailRuntime, await taskApi.runtime(id))
  if (tab === 'diagnostics') Object.assign(detailDiagnostics, await taskApi.diagnostics(id))
  if (tab === 'preview') Object.assign(detailPreview, await taskApi.preview(id))
  if (tab === 'resourcePlan') Object.assign(detailResourcePlan, await taskApi.resourcePlan(id))
  if (tab === 'fullSync') await loadFullSyncPreview(id)
  if (tab === 'logs') detailLogs.value = await logApi.task(id)
  if (tab === 'versions') detailVersions.value = await systemApi.configVersions({ refId: id })
})

onMounted(() => {
  refreshTimer = window.setInterval(() => {
    if (!drawerOpen.value && !detailOpen.value) loadTasks().catch(() => {})
  }, 15000)
})

onUnmounted(() => {
  if (refreshTimer) window.clearInterval(refreshTimer)
})

function statusColor(status) {
  return status === 'RUNNING' ? 'green' : status === 'ERROR' ? 'red' : 'default'
}

function defaultFieldMapping(sourceField = '', targetField = '', primaryKey = false) {
  return {
    sourceField,
    targetField,
    fieldType: '',
    primaryKey,
    nullableField: !primaryKey,
    enabled: true,
    defaultValue: '',
    transformExpr: '',
    formatPattern: '',
    fieldOptions: ''
  }
}

function diagnosticMessage(data) {
  return `通过 ${data.okCount || 0}/${data.total || 0} 项检查，阻断 ${data.blockerCount || 0}，风险 ${data.warnCount || 0}`
}

function resourcePlanMessage(data) {
  const targetType = data?.targetType || '-'
  const count = data?.count ?? 0
  if (data?.blockerCount) return `目标端 ${targetType} 需要准备 ${count} 项资源，当前存在 ${data.blockerCount} 个阻断项`
  if (data?.warnCount) return `目标端 ${targetType} 需要准备 ${count} 项资源，当前存在 ${data.warnCount} 个警告项`
  return `目标端 ${targetType} 需要准备 ${count} 项资源，当前检查通过`
}

function severityColor(severity) {
  return severity === 'BLOCKER' ? 'red' : severity === 'WARN' ? 'orange' : 'blue'
}

function severityLabel(severity) {
  return severity === 'BLOCKER' ? '阻断' : severity === 'WARN' ? '风险' : '信息'
}

function formatBytes(size) {
  if (!size) return '0 B'
  if (size < 1024) return `${size} B`
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`
  return `${(size / 1024 / 1024).toFixed(1)} MB`
}

function clearReactive(target) {
  Object.keys(target).forEach(key => delete target[key])
}
</script>
