Canal 数据同步管理平台（CanalSync-Admin）

## 当前实现

本目录已落地一套可运行的 CanalSync-Admin 基础版本：
底层能力来源：https://github.com/alibaba/canal

- 后端：Spring Boot 3，提供数据源、同步任务、任务启停、监控、告警、日志接口。
- 前端：Vue 3 + Vite + Ant Design Vue，提供大盘、数据源、同步任务、Canal 运维、日志、系统管理页面。
- 存储：默认使用 MySQL，启动时自动初始化表结构和默认账号。
- Canal：项目内置 `canal-runtime/canal-server` 和 `canal-runtime/canal-adapter`，启动 canal-web 时会按数据源配置生成 Canal Server/Adapter 配置并拉起进程。
- Canal Admin：提供嵌入式 Canal Admin 控制台入口，支持页面内登录、修改默认密码、刷新和新窗口打开；Canal 运维页支持 Admin Manager 注册联调。
- 数据源接入：支持连接测试、数据源复制和页面化自检，检查 binlog、binlog_format、server_id、GTID、位点、订阅表匹配和复制权限。
- 任务管理：支持状态筛选、任务复制、同步 SQL 预览、启动前自检、单任务启停、批量启停、手动全量同步、Cron 全量调度、任务监控指标、配置版本快照。
- 字段映射：支持字段类型、主键、是否可空、默认值、转换表达式、格式化模板和字段级扩展参数，可用于 RDB DDL、ES mapping、Redis value、MQ message key、HBase family 等目标端生成。
- 测试模拟：提供本机目标端测试菜单，可直接调用后端目标端连通性测试接口，覆盖 MySQL、Redis、Elasticsearch、PostgreSQL、Kafka、RabbitMQ、RocketMQ、Pulsar。
- 日志审计：支持任务日志、操作日志、告警日志筛选、告警批量确认、配置版本查看与结构化版本回滚。
- 监控大盘：集中展示运行指标、未确认告警、异常任务、任务运行概览和最近操作。
- 全量同步：FULL 任务会按 `sync_sql` 从源 MySQL 拉取数据，落地到 `canal-runtime/generated/full-sync/*.jsonl`，并更新同步条数、失败数和延迟指标；任务详情页可直接预览全量结果文件。
- 页面化运维：Canal 运维页可查看 Canal 状态、启停/重启、预览生成配置、配置一致性、失效配置清理、查看 Server/Adapter 日志、指标、自检、能力覆盖与扩展点。
- 权限角色：`ADMIN` 可管理用户和全局配置，`OPERATOR` 可执行数据源/任务/Canal 运维操作，`VIEWER` 只读查看。
- 配置迁移：系统管理页支持导出/导入配置包，包含 Canal 全局配置、数据源和同步任务。

## 本地运行

<img width="1832" height="1009" alt="e73dd10d5367774e0acf6add582517d3" src="https://github.com/user-attachments/assets/0856b10a-0674-4f39-88d7-60aa68a6cfdc" />
<img width="2560" height="1319" alt="ab807766725b9bc5e9492a733910df41" src="https://github.com/user-attachments/assets/8da1181d-0ce4-4111-ab2e-4772c8a7acaa" />
<img width="1833" height="1007" alt="9187cc84784abf7f10a97a1d2aefc8da" src="https://github.com/user-attachments/assets/7613daec-5742-40c7-aea3-3dc53c66c4cd" />
<img width="1826" height="1004" alt="5c1af65e337ba2bafd3e2038021dade6" src="https://github.com/user-attachments/assets/66bf5413-9aa3-4541-bc28-9cec8ecc690e" />
<img width="1833" height="1009" alt="62fa8d4844dc53557a38eac378b16451" src="https://github.com/user-attachments/assets/4adf4b23-b25b-436a-a9fa-b900fee0bf7b" />




启动后端：

```bash
cd canal-web
mysql -uroot -proot -e "CREATE DATABASE IF NOT EXISTS canal_web DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home mvn package -DskipTests
java -jar target/canal-web-0.1.0-SNAPSHOT.jar
```

默认 MySQL 连接配置：

```bash
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_DATABASE=canal_web
MYSQL_USERNAME=root
MYSQL_PASSWORD=12345678
```

如果本机 MySQL 账号密码不同，启动前覆盖对应环境变量即可。

启动前端：

```bash
cd frontend
npm install
npm run dev -- --port 5182 --host 0.0.0.0
```

访问地址：`http://localhost:5182/`

默认开发账号：`admin / admin123`

## 页面菜单与功能颗粒度

当前前端按实际运维流程拆成以下菜单：

| 菜单 | 主要能力 | 关键操作 |
| --- | --- | --- |
| 监控大盘 | 查看任务总数、运行数、异常数、同步统计、未确认告警和最近操作 | 刷新大盘、跳转异常任务 |
| 数据源管理 | 管理 MySQL 源库和 Canal destination | 新增/编辑、连接测试、获取 binlog 位点、保存并自检、复制、启停 |
| 同步任务 | 管理 FULL、INCREMENTAL、FULL_INCREMENTAL 任务 | SQL 预览、字段映射生成/校验、目标端测试、启动自检、资源准备、启停、批量启停、全量同步、运行态查看 |
| 测试模拟 | 验证本机目标端连接能力 | 单个目标端测试、全部测试、本机服务状态刷新、复制测试 payload |
| Canal 运维 | 管理 Canal Runtime、Admin 注册和配置一致性 | 启停/重启 Server、启停/重启 Adapter、刷新配置并重启、编辑路径、运行自检、日志/指标查看、清理失效配置 |
| Canal Admin | 嵌入式打开 Canal Admin 原生控制台 | 设置地址、账号密码登录、修改默认密码、刷新 iframe、新窗口打开 |
| 日志中心 | 查询任务日志、操作日志、告警日志 | 条件筛选、单条确认、批量确认、按筛选确认 |
| 系统管理 | 管理用户、配置版本、本机链路和配置包 | 用户启停/重置密码、版本回滚、配置包导入导出、本机服务启停/验证 |

登录过期处理规则：

- 无 token 或 token 失效时直接回到登录页。
- `/api/auth/**` 登录相关接口不需要 token。
- 后端鉴权由 `AuthInterceptor` 兜底，前端隐藏按钮只是交互优化。

## 功能状态矩阵

| 能力 | 当前状态 | 说明 |
| --- | --- | --- |
| MySQL 源库接入 | 已接入 | 支持连接测试、binlog 位点、表/字段读取、复制权限和订阅表自检 |
| Canal Server 配置生成 | 已接入 | 生成 `canal.properties` 和每个 destination 的 `instance.properties` |
| Canal Adapter 配置生成 | 已接入 | 生成 `application.yml` 和任务 mapping 预览/运行态检查 |
| MySQL/RDB 目标端 | 已接入 | 支持目标连接测试、资源准备 DDL、字段映射、启动自检 |
| PostgreSQL 目标端 | 已接入 | 支持 JDBC 连接测试、schema/table/主键配置、资源准备 DDL |
| Redis 目标端 | 已接入 | 支持 DB、key 模板、HASH/JSON/STRING 约定、TTL、删除策略、自检项 |
| Elasticsearch 目标端 | 已接入 | 支持索引、mapping、document id、写入模式、资源准备和连接测试 |
| Kafka 目标端 | 已接入 | 支持 broker、topic、message key、消息格式、consumer group 约定和 Topic 检测 |
| RabbitMQ 目标端 | 部分接入 | 已有目标端测试和配置约定；Canal Adapter 原生投递需结合运行插件或外部消费程序落地 |
| RocketMQ 目标端 | 部分接入 | 已有目标端测试、资源准备命令和配置生成约定；本机需启动 NameServer/Broker 后验证 |
| Pulsar 目标端 | 部分接入 | 已有目标端测试、Topic/Subscription 配置约定；本机需启动 Pulsar 后验证 |
| HBase 目标端 | 部分接入 | 已有资源准备脚本、RowKey/Family 配置约定；本机需启动 HBase/ZK 后验证 |
| Tablestore/ClickHouse | 待接入 | README 中保留扩展方向，尚未作为完整页面闭环验证 |

判断“跑通”的标准：

1. 本机服务端口可连接，测试模拟页对应目标端返回成功。
2. 同步任务保存成功，字段映射校验通过。
3. 资源准备页无阻断项，目标库表、索引、Topic、key 模板等已明确。
4. 启动自检无 `BLOCKER`。
5. 任务运行态显示 runtime 文件存在，Adapter destination 在线。
6. 写入源表后，目标端能查到对应数据。

## 本机后台验证

当前本机示例已将 Canal Web、Canal Server、Canal Adapter 和 Kafka 固化为后台服务：

```bash
launchctl list | grep 'com.openclaw.canal'
brew services list | grep kafka
```

服务与端口：

```text
canal-web      18082  com.openclaw.canal-web-local
canal-server   11111  com.openclaw.canal-server-local
canal-adapter  18083  com.openclaw.canal-adapter-local
kafka           9092  homebrew.mxcl.kafka
redis           6379
elasticsearch   9200
postgresql      5432
mysql           3306
```

macOS 对 `Desktop` 路径可能有隐私访问限制。如果用 LaunchAgent 运行，可以把 jar 或脚本放到用户可访问目录；但运行时配置仍建议通过 `CANAL_WEB_PROJECT_DIR` 指向项目目录。当前机器示例后台运行目录为：

```text
~/.local/canal-web-local
~/.local/canal-server-local
~/.local/canal-adapter-local
```

路径可按环境调整，不再要求固定在当前机器目录。默认以项目目录为基准读取 `canal-runtime`：

- 项目目录：`CANAL_WEB_PROJECT_DIR` 或 `-Dcanal-web.project-dir`
- Runtime：`{projectDir}/canal-runtime`
- Canal Server：`{projectDir}/canal-runtime/canal-server`
- Canal Adapter：`{projectDir}/canal-runtime/canal-adapter`
- 生成文件：`{projectDir}/canal-runtime/generated`
- CLI：`/opt/homebrew/bin`、`/usr/local/bin`、`/opt/local/bin`、`/usr/bin` 等；Redis/Kafka/JDK 也内置 Homebrew、MacPorts、Linux 常见路径

```bash
# 项目根目录，换电脑后改这一项即可
export CANAL_WEB_PROJECT_DIR="$PWD"

# Runtime 文件候选目录，逗号分隔，可追加多个；不配置时默认使用 $CANAL_WEB_PROJECT_DIR/canal-runtime
export CANAL_WEB_RUNTIME_PATHS="$CANAL_WEB_PROJECT_DIR/canal-runtime"
export CANAL_WEB_RUNTIME_ROOT_DIR="$CANAL_WEB_PROJECT_DIR/canal-runtime"

# Adapter conf 候选目录，逗号分隔
export CANAL_ADAPTER_CONF_PATHS="$CANAL_WEB_PROJECT_DIR/canal-runtime/canal-adapter/conf"
export CANAL_SERVER_HOME="$CANAL_WEB_PROJECT_DIR/canal-runtime/canal-server"
export CANAL_ADAPTER_HOME="$CANAL_WEB_PROJECT_DIR/canal-runtime/canal-adapter"

# 本机运维脚本候选路径，逗号分隔
export CANAL_WEB_LOCAL_STACK_SCRIPT_PATHS="$PWD/canal-web/scripts/local-stack.sh"

# Redis/Kafka CLI 和 Java Home，可用于 LaunchAgent PATH 不完整的场景
export CANAL_WEB_REDIS_CLI=/opt/homebrew/bin/redis-cli
export CANAL_WEB_KAFKA_TOPICS=/opt/homebrew/bin/kafka-topics
export CANAL_WEB_JAVA_HOME=/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home
export CANAL_WEB_CLI_PATH=/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin
```

对应 JVM 参数也可使用：

```bash
-Dcanal-web.project-dir=/data/canal-web
-Dcanal-web.runtime.paths=/data/canal-web/canal-runtime,/backup/canal-runtime
-Dcanal-web.runtime.root-dir=/data/canal-web/canal-runtime
-Dcanal-web.runtime.server-home=/data/canal-server
-Dcanal-web.runtime.adapter-home=/data/canal-adapter
-Dcanal-adapter.conf.paths=/data/canal-adapter/conf
-Dcanal-web.local-stack.script=/data/canal-web/scripts/local-stack.sh
-Dcanal-web.redis-cli=/usr/bin/redis-cli
-Dcanal-web.kafka-topics=/usr/bin/kafka-topics
-Dcanal-web.java-home=/usr/lib/jvm/java-17
```

一键验证本机链路：

```bash
cd /path/to/canal-webui
canal-web/scripts/verify-local-stack.sh 113
# 或使用统一运维脚本
canal-web/scripts/local-stack.sh verify
```

脚本会写入源表 `canal_sync_verify.user_profile`，并检查：

- Canal Web 登录接口
- Canal Adapter 状态接口
- Kafka broker
- MySQL 目标表 `canal_sync_target.user_profile_copy`
- PostgreSQL 目标表 `public.user_profile_pg_copy`
- Redis DB 6 的 `user_profile:{id}` HASH
- Elasticsearch `user_profile_index/_doc/{id}`

停止或重启后台服务：

```bash
canal-web/scripts/local-stack.sh status
canal-web/scripts/local-stack.sh stop
canal-web/scripts/local-stack.sh start
canal-web/scripts/local-stack.sh restart
```

## 内置 Canal Runtime

`canal-runtime/` 已放入可运行的 Canal 组件：

```text
canal-runtime/
├── canal-server/   # Canal deployer，负责读取 MySQL binlog
└── canal-adapter/  # Canal adapter，按插件方式挂载 logger/rdb/es/clickhouse 等 adapter
```

运行配置统一由 `canal-web` 管理：

- 数据源配置会生成 `canal-runtime/canal-server/conf/canal.properties`
- 每个启用的数据源会生成 `canal-runtime/canal-server/conf/{destination}/instance.properties`
- Canal Adapter 会生成 `canal-runtime/canal-adapter/conf/application.yml`
- 任务启动时会刷新 runtime，并把任务插件规格写到 `canal-runtime/generated/tasks/{taskId}.yml`

Canal 运维页的“路径配置”可以按本机部署实际情况修改，保存后会立即参与自检、状态、配置一致性、刷新配置和任务插件目录解析：

- `runtimeRootDir`：Runtime 根目录，留空时读取 `{projectDir}/canal-runtime`
- `canalServerHome`：Canal Server 安装目录，留空时读取 `{projectDir}/canal-runtime/canal-server`
- `canalAdapterHome`：Canal Adapter 安装目录，留空时读取 `{projectDir}/canal-runtime/canal-adapter`
- `dbsyncSourceJar`：dbsync 可用 Jar，留空时读取 `{projectDir}/canal-runtime/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar`
- `dbsyncRuntimeJar`：Server runtime 依赖，留空时读取 `{projectDir}/canal-runtime/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar`
- `generatedPaths`：生成文件根目录，多个目录用逗号分隔；系统会在每个根目录下查找 `tasks/` 和 `adapter-plugins/`

路径也可以通过 JVM 参数或环境变量覆盖：

```bash
-Dcanal-web.project-dir=/data/canal-web
-Dcanal-web.runtime.root-dir=/data/canal-web/canal-runtime
-Dcanal-web.runtime.server-home=/data/canal-server
-Dcanal-web.runtime.adapter-home=/data/canal-adapter
-Dcanal-web.runtime.generated.paths=/data/canal-web/canal-runtime/generated,/backup/canal-runtime/generated
-Dcanal-web.runtime.dbsync-source-jar=/path/canal.parse.dbsync-1.1.9-SNAPSHOT.jar
-Dcanal-web.runtime.dbsync-runtime-jar=/data/canal-server/lib/canal.parse.dbsync-1.1.9-SNAPSHOT.jar
```

数据源中的 binlog 字段说明：

- `订阅表正则`：写入 `canal.instance.filter.regex`，默认 `{dbName}\\..*`
- `黑名单正则`：写入 `canal.instance.filter.black.regex`
- `Binlog 文件`、`Binlog Position`、`Binlog Timestamp`、`GTID`：写入 Canal 实例起始位点
- `Server ID`：写入 `canal.instance.mysql.slaveId`
- `启用 GTID`：写入 `canal.instance.gtidon`

数据源自检接口：

```text
GET /api/datasource/{key}/diagnostics
```

自检会只读连接源库并返回检查项：

- MySQL 连接是否成功
- `log_bin` 是否开启
- `binlog_format` 是否为 `ROW`
- MySQL `server_id` 与 Canal `slaveId` 是否冲突
- GTID 配置是否匹配
- 当前 binlog 文件和位点
- 订阅表正则实际匹配到的表
- 当前用户是否具备复制相关权限

数据源复制接口：

```text
POST /api/datasource/clone/{id}
```

复制会保留源库连接、过滤规则、TSDB、主备和云 RDS 配置，自动生成新的数据源标识和 Canal Destination，并默认禁用，避免复制后立刻参与 Canal 实例生成。

系统管理页提供 Canal Runtime 的状态查看、启动、停止、刷新配置并重启操作。对应接口：

```text
GET  /api/system/canal/status
GET  /api/system/canal/config
GET  /api/system/canal/config/consistency
GET  /api/system/canal/stale-files
POST /api/system/canal/stale-files/clean
GET  /api/system/canal/logs
POST /api/system/canal/refresh
POST /api/system/canal/start
POST /api/system/canal/stop
```

配置一致性会按当前数据库配置重新生成期望的 `canal.properties`、`application.yml` 和各实例 `instance.properties`，与 runtime 目录中的实际文件比对。若不一致，业务人员可直接在 Canal 运维页执行“刷新配置并重启”。

失效配置清理会扫描未启用数据源残留的实例目录，以及无效任务或无效 destination 对应的 Adapter 插件文件。清理前可在页面查看列表，确认后由系统删除并刷新 runtime 配置。

## 配置版本与回滚

数据源、同步任务、Canal 全局运行配置在保存时会写入 `config_version` 快照。新版本快照使用结构化 JSON，前端会标记为“可回滚”；早期文本摘要版本仍可展开查看，但不会误触发回滚。

管理员可在两个入口回滚：

- 系统管理 / 配置版本：支持数据源、任务、Canal Runtime 全局配置回滚。
- 同步任务 / 详情 / 配置版本：支持当前任务配置回滚。

回滚接口：

```text
GET  /api/system/config/version/list
POST /api/system/config/version/{id}/rollback
```

回滚后 canal-web 会刷新运行配置，业务人员不需要手工修改 `canal.properties`、`instance.properties` 或 Adapter YAML。

## 配置包导入导出

系统管理页提供配置包 JSON 的导出和导入：

```text
GET  /api/system/config/package/export
POST /api/system/config/package/import
```

配置包包含：

- Canal 全局运行配置
- 数据源配置
- 同步任务、字段映射和目标端配置

导入时会按数据源标识和任务 ID 覆盖已有配置，导入完成后自动刷新 Canal 运行配置。该能力适合测试环境到生产环境迁移、手动备份、误操作后的离线恢复。

## 角色权限

后端接口和前端入口已按角色对齐：

- `ADMIN`：用户管理、Canal 全局配置、配置回滚、所有运维操作。
- `OPERATOR`：数据源维护、任务维护、任务启停、Canal 启停/重启、告警确认。
- `VIEWER`：监控大盘、数据源、任务详情、日志、Canal 状态和生成配置只读查看。

后端以 `AuthInterceptor` 作为最终权限兜底，前端仅负责隐藏或禁用不允许的操作入口。

## 全量同步与 Cron 调度

同步任务支持两类运行方式：

- `INCREMENTAL`：由 Canal Server 读取 binlog，Canal Adapter 按 destination 消费增量数据。
- `FULL`：由 canal-web 按任务 `sync_sql` 直接读取源 MySQL，输出 JSONL 快照文件并记录指标。
- `FULL_INCREMENTAL`：启动时先执行一次 FULL 快照，再写入 Adapter 运行配置并启动 INCREMENTAL 增量链路。

手动全量同步接口：

```text
POST /api/task/full-sync/{id}
GET  /api/task/full-sync/preview/{id}?limit=20
```

全量结果预览会读取最近一次生成的 JSONL 快照文件，返回文件路径、文件大小、总行数和前 N 行样例数据。读取范围限制在 `canal-runtime/generated/full-sync/` 目录内，业务人员可在同步任务详情的“全量结果”页签查看，不需要登录服务器打开文件。

任务运行态接口：

```text
GET /api/task/runtime/{id}
```

运行态会汇总任务状态、目标端类型、Canal destination、Adapter Key、mapping 目录和文件、runtime 文件存在性、Adapter destination 在线状态、日志数、错误数、最近日志、最近错误和处理建议。前端在任务详情的“运行态”页签展示，用于判断任务是否只是状态为 `RUNNING`，还是确实具备对应的 Adapter 配置、runtime 文件和运行日志。

任务启动前自检接口：

```text
GET /api/task/diagnostics/{id}
```

自检会检查任务名称、同步 SQL、批大小、数据源存在和启用状态、字段映射完整性、目标端连通性、Adapter 配置生成结果。检查项会按级别返回：

- `BLOCKER`：阻断项，必须修复，否则后端拒绝启动任务。
- `WARN`：风险项，允许启动，但页面会提示继续观察或补齐配置。
- `INFO`：信息项，表示检查通过或仅作说明。

前端在任务列表和任务详情中都提供“自检”入口；点击启动时会先执行自检，存在阻断项时自动打开自检详情。

目标端资源准备接口：

```text
GET /api/task/resource-plan/{id}
```

资源准备会按目标端生成可审阅的前置资源脚本或命令：

- Redis：key/value/TTL/delete 策略检查命令
- MySQL/PostgreSQL：建库建表 SQL
- Elasticsearch：索引和 mapping 创建请求
- Kafka/RocketMQ/Pulsar：Topic 创建命令
- HBase：namespace、table、Column Family 创建命令

资源准备会尽量根据 `sourceTable` 或 `sync_sql` 定位源表 metadata，识别源字段类型，并用于推断 MySQL/PostgreSQL 建表字段类型和 Elasticsearch mapping 类型；无法识别时回退到字段名规则。接口同时返回资源检查统计和检查明细，例如目标表/索引/Topic 是否存在、字段映射是否覆盖、主键或 message key 是否有效。本机环境下还会对 Redis 执行只读 `PING`，对 Kafka 执行 Topic 列表探测，用于确认服务和 Topic 已就绪。前端在同步任务详情的“资源准备”页签展示源字段类型、资源脚本和当前就绪状态，方便启动前交给 DBA/运维审核执行。

任务复制接口：

```text
POST /api/task/clone/{id}
```

复制会保留源任务的数据源、同步 SQL、字段映射、目标端配置和调度策略，生成一个默认 `STOPPED` 的新任务，适合快速创建同类同步任务后再微调。

同步 SQL 预览接口：

```text
POST /api/task/sql-preview?limit=20
```

预览仅允许 `SELECT`/`WITH` 查询，会自动限制返回行数，返回字段名、字段类型和样例数据。前端在任务编辑抽屉里提供“预览 SQL”按钮，并可按预览字段一键生成字段映射，便于保存前确认 SQL 和字段映射。

## 目标端前置资源准备与配置流程

同步任务不是只填写源库 SQL 和目标类型即可启动。不同目标端在启动前必须先明确或创建对应的目标资源，任务保存和启动自检也要围绕这些资源做校验，避免 Canal Adapter 已经消费 binlog 但目标端写入失败。

通用流程：

1. 选择源数据源，编写 `sync_sql`，通过 SQL 预览确认源字段。
2. 选择目标端类型：`REDIS`、`MYSQL`/`RDB`、`ES`、`KAFKA`、`ROCKETMQ`、`RABBITMQ`、`PULSAR`、`HBASE`、`TABLESTORE` 等。
3. 按目标端填写资源配置，例如 Redis key 模板、MySQL 库表、ES 索引和 mapping、Kafka/RocketMQ/Pulsar Topic、HBase 表和 Column Family。
4. 维护字段映射，确认源字段、目标字段、主键/唯一键、写入模式。
5. 执行任务自检，检查目标端连通性、资源是否存在、字段映射是否完整。
6. 自检通过后再启动任务；启动时系统生成 Canal Adapter 配置并刷新 runtime。

字段映射支持字段级属性参数：

- `fieldType`：字段类型，例如 `STRING`、`LONG`、`DECIMAL`、`DATETIME`、`JSON`
- `primaryKey`：是否主键或业务 key，可用于 RDB 主键、HBase RowKey、MQ message key 约定
- `nullableField`：是否允许为空，资源准备生成 RDB DDL 时会用于 `NOT NULL`
- `enabled`：是否启用该字段；禁用字段不会进入生成的 Adapter 配置
- `defaultValue`：目标端默认值或补值约定
- `transformExpr`：字段转换表达式，例如 `trim(name)`、`date_format(updated_at)`
- `formatPattern`：日期、数字、字符串格式化模板
- `fieldOptions`：扩展 JSON 参数，例如 `{"messageKey":true,"hbaseFamily":"CF","redisValueField":true}`

启动配置预览会把 `fieldOptions` 展开为结构化 YAML，用于对齐 Redis/HBase/MQ/ES/RDB 等目标端的字段级行为。资源准备脚本会优先使用字段级参数：

- `rdbType`：覆盖 MySQL/PostgreSQL 建表字段类型，例如 `varchar(64)`、`numeric(20,6)`
- `esType`：覆盖 Elasticsearch mapping 类型，例如 `keyword`、`text`、`date`
- `messageKey`：标记字段可作为 MQ message key 约定
- `redisValueField`：标记字段参与 Redis value 生成
- `hbaseFamily`：覆盖 HBase Column Family
- `mask`：标记脱敏策略，例如 `PHONE`、`EMAIL`、`ID_CARD`

### 同步到 Redis

Redis 目标端需要在任务配置中明确 key 和 value 的生成规则。Redis 本身不需要提前建表，但必须提前约定 key 命名、数据结构和过期策略。

需要配置：

- `redis.host`、`redis.port`、`redis.database`、`redis.password`
- `key_pattern`：Redis key 模板，例如 `user:{id}`、`order:{order_no}`
- `value_type`：写入结构，例如 `STRING`、`HASH`、`JSON`
- `value_mapping`：value 字段来源，例如把 `id/name/status/update_time` 写入 JSON 或 HASH
- `ttl_seconds`：可选，写入后的过期时间；不配置表示不过期
- `delete_policy`：源库删除时的处理方式，例如删除 key、标记删除、忽略

示例：

```text
目标类型：REDIS
key_pattern=user:{id}
value_type=HASH
value_mapping=id,name,mobile,status,updated_at
ttl_seconds=86400
delete_policy=DELETE_KEY
```

启动前自检应检查：

- Redis 连接是否成功
- `key_pattern` 中引用的字段是否都能从 `sync_sql` 或字段映射中找到
- `value_mapping` 是否为空
- TTL 是否为合法数字
- 当前账号是否有写入、删除相关权限

### 同步到 MySQL / RDB

MySQL 目标端需要提前创建目标库和目标表。系统可以提供建表 SQL 预览或辅助生成，但生产环境通常应由 DBA 审核后执行。

需要配置：

- `target.jdbc.url`、`target.username`、`target.password`
- `target_database`：目标库名，例如 `report_center`
- `target_table`：目标表名，例如 `user_profile`
- `primary_key`：目标表主键或唯一键字段
- `write_mode`：写入模式，例如 `INSERT`、`UPSERT`、`UPDATE`
- `field_mapping`：源字段到目标字段的映射关系

目标库表示例：

```sql
CREATE DATABASE IF NOT EXISTS `report_center`
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `report_center`.`user_profile` (
  `id` bigint NOT NULL,
  `name` varchar(128) DEFAULT NULL,
  `mobile` varchar(32) DEFAULT NULL,
  `status` tinyint DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

启动前自检应检查：

- 目标 MySQL 连接是否成功
- 目标库和目标表是否存在
- 目标表字段是否覆盖字段映射中的目标字段
- 主键或唯一键是否存在，`UPSERT`/`UPDATE` 模式必须配置主键
- 字段类型是否存在明显不兼容，例如字符串写入数字字段

### 同步到 PostgreSQL

PostgreSQL 目标端同样属于 RDB 写入场景，需要提前创建目标 schema、目标表和主键/唯一键。页面中的 `PGSQL` 目标端会保存 JDBC、schema/table、主键和批量参数，启动前自检会复用 RDB 检查逻辑确认表结构。

需要配置：

- `driverClassName`：`org.postgresql.Driver`
- `jdbcUrl`：例如 `jdbc:postgresql://localhost:5432/postgres`
- `username`、`password`
- `targetSchema` 或 `targetDatabase`：目标 schema，例如 `public`
- `targetTable`：目标表名，例如 `user_profile_copy`
- `targetPk` 或 `primaryKey`：目标主键或唯一键字段
- `writeMode`：写入模式，例如 `INSERT`、`UPSERT`、`UPDATE`

目标表示例：

```sql
CREATE SCHEMA IF NOT EXISTS public;

CREATE TABLE IF NOT EXISTS public.user_profile_copy (
  id bigint PRIMARY KEY,
  name varchar(128),
  mobile varchar(32),
  status integer,
  updated_at timestamp
);
```

启动前自检应检查：

- PostgreSQL 连接是否成功
- 目标 schema 和目标表是否存在
- 目标表字段是否覆盖字段映射中的目标字段
- 主键或唯一键字段是否存在
- 写入模式是否为 `INSERT`、`UPSERT` 或 `UPDATE`

### 同步到 Elasticsearch

ES 目标端需要提前创建索引和 mapping。索引字段类型不建议完全依赖动态 mapping，否则后续字段类型变更容易导致写入失败或查询不符合预期。

需要配置：

- `es.hosts`、`es.username`、`es.password`
- `index_name`：索引名，例如 `user_profile_v1`
- `document_id`：文档 ID 模板，例如 `{id}`
- `mapping`：字段类型、分词器、日期格式等
- `write_mode`：写入模式，例如 `INDEX`、`UPDATE`、`UPSERT`
- `delete_policy`：源库删除时删除文档、标记删除或忽略

索引和 mapping 示例：

```json
PUT /user_profile_v1
{
  "mappings": {
    "properties": {
      "id": { "type": "long" },
      "name": { "type": "keyword" },
      "mobile": { "type": "keyword" },
      "status": { "type": "integer" },
      "updated_at": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time" }
    }
  }
}
```

启动前自检应检查：

- ES 集群连接是否成功
- 索引是否存在
- mapping 是否包含字段映射中的目标字段
- `document_id` 引用字段是否存在
- 日期、数字、keyword/text 等类型是否与源字段基本匹配

### 同步到 Kafka

Kafka 目标端需要提前创建 Topic，并明确消息 key、消息 value 格式和消费组约定。Canal 写入 Kafka 时通常关注 producer 配置；下游业务消费时还必须约定 group。

需要配置：

- `bootstrap.servers`
- `topic`：目标 Topic，例如 `canal.user_profile`
- `message_key`：消息 key 模板，例如 `{id}`、`{order_no}`
- `message_format`：消息格式，例如 `JSON`、`CANAL_JSON`
- `partition_key`：可选，用于控制同一业务主键进入同一分区
- `consumer_group`：下游消费组约定，例如 `user-profile-sync-group`
- `acks`、`retries`、`batch.size` 等 producer 参数

Topic 创建示例：

```bash
kafka-topics.sh --bootstrap-server 127.0.0.1:9092 \
  --create \
  --topic canal.user_profile \
  --partitions 6 \
  --replication-factor 1
```

下游消费组约定示例：

```text
topic=canal.user_profile
consumer_group=user-profile-sync-group
message_key={id}
message_format=JSON
```

启动前自检应检查：

- Kafka broker 是否可连接
- Topic 是否存在，分区数是否符合预期
- `message_key` 或 `partition_key` 引用字段是否存在
- 消息格式是否已选择
- 下游消费组名称是否已登记，避免多个业务共用同一 group 导致消费互相影响

### 同步到 RocketMQ

RocketMQ 目标端按 MQ 投递契约管理，需要提前创建 Topic，并明确 NameServer、Producer Group、消息 key 和下游消费组约定。

需要配置：

- `nameServer` 或 `namesrvAddr`：例如 `127.0.0.1:9876`
- `producerGroup`：例如 `canal-web-producer`
- `topic`：目标 Topic，例如 `canal_user_profile`
- `messageKey`：消息 key 模板，例如 `{id}`
- `messageFormat`：`JSON` 或 `CANAL_JSON`
- `consumerGroup`：下游消费组约定

Topic 创建示例：

```bash
mqadmin updateTopic -n 127.0.0.1:9876 \
  -c DefaultCluster \
  -t canal_user_profile
```

启动前自检应检查：

- RocketMQ NameServer 是否可连接
- Topic、Producer Group 是否已配置
- `messageKey` 或 `partitionKey` 引用字段是否存在
- 消息格式是否已选择
- 下游消费组是否已登记

### 同步到 Pulsar

Pulsar 目标端按 MQ 投递契约管理，需要提前创建 Topic，并明确 Service URL、Subscription、消息 key 和消息格式。

需要配置：

- `serviceUrl` 或 `serverUrl`：例如 `pulsar://127.0.0.1:6650`
- `topic`：例如 `persistent://public/default/user_profile`
- `subscriptionName`：例如 `canal-web-subscription`
- `messageKey`：消息 key 模板，例如 `{id}`
- `messageFormat`：`JSON` 或 `CANAL_JSON`

Topic 创建示例：

```bash
pulsar-admin topics create persistent://public/default/user_profile
```

启动前自检应检查：

- Pulsar Broker 是否可连接
- Topic、Subscription 是否已配置
- `messageKey` 或 `partitionKey` 引用字段是否存在
- 消息格式是否已选择

### 同步到 HBase

HBase 目标端使用 Canal Adapter 的 `hbase` mapping，需要提前创建 namespace、表和 Column Family，并明确 RowKey 规则。

需要配置：

- `zkQuorum`、`zkClientPort`、`zkParent`
- `mode`：`STRING`、`NATIVE` 或 `PHOENIX`
- `hbaseTable`：例如 `canal:user_profile`
- `family`：Column Family，例如 `CF`
- `rowKey`：例如 `id` 或 `id,type`
- 字段映射：源字段到 HBase qualifier 的映射

建表示例：

```bash
hbase shell
create_namespace 'canal'
create 'canal:user_profile', 'CF'
```

启动前自检应检查：

- HBase ZK Quorum 是否可连接
- HBase 表名和 Column Family 是否已配置
- RowKey 引用字段是否存在于字段映射
- 字段映射是否完整

Cron 调度：

- 仅扫描 `task_status = RUNNING` 且 `sync_mode` 为 `FULL` 或 `FULL_INCREMENTAL` 且配置了 `cron_expression` 的任务。
- 默认每 30 秒扫描一次，可通过 `canal-web.scheduler.scan-interval-ms` 调整。
- Cron 表达式使用 Spring Cron 格式，包含秒字段，例如 `0 */5 * * * *`。

## 目标端配置 key 约定

同步任务的 `targetConfig` 使用 key/value 保存。页面会按目标端展示表单，后端也会兼容常见别名。建议优先使用以下 key，便于资源准备、预览、自检和运行态统一识别。

| 目标端 | 推荐 key | 说明 |
| --- | --- | --- |
| Redis | `host`、`port`、`password`、`database`、`keyPattern`、`valueType`、`valueMapping`、`ttlSeconds`、`deletePolicy` | `keyPattern` 支持 `{id}` 这类字段引用 |
| MySQL/RDB | `jdbcUrl`、`username`、`password`、`targetDatabase`、`targetTable`、`targetPk`、`writeMode`、`batchSize` | `writeMode` 建议使用 `INSERT`、`UPSERT`、`UPDATE` |
| PostgreSQL | `driverClassName`、`jdbcUrl`、`username`、`password`、`targetSchema`、`targetTable`、`targetPk`、`writeMode` | `targetSchema` 默认可用 `public` |
| Elasticsearch | `hosts`、`username`、`password`、`indexName`、`documentId`、`writeMode`、`deletePolicy` | `documentId` 支持 `{id}` 模板 |
| Kafka | `bootstrapServers`、`topic`、`messageKey`、`messageFormat`、`partitionKey`、`consumerGroup` | `messageFormat` 建议 `JSON` 或 `CANAL_JSON` |
| RabbitMQ | `host`、`port`、`username`、`password`、`virtualHost`、`exchange`、`routingKey`、`queue`、`messageKey`、`messageFormat` | 用于目标端连通性和外部消费契约 |
| RocketMQ | `namesrvAddr`、`producerGroup`、`topic`、`messageKey`、`messageFormat`、`consumerGroup` | 本机验证需要 RocketMQ NameServer/Broker |
| Pulsar | `serviceUrl`、`topic`、`subscriptionName`、`messageKey`、`messageFormat` | Topic 建议使用完整 persistent 名称 |
| HBase | `zkQuorum`、`zkClientPort`、`zkParent`、`mode`、`hbaseTable`、`family`、`rowKey` | `rowKey` 支持单字段或逗号分隔多字段 |

字段映射 `fieldOptions` 推荐 key：

```json
{
  "rdbType": "varchar(128)",
  "esType": "keyword",
  "messageKey": true,
  "redisValueField": true,
  "hbaseFamily": "CF",
  "mask": "PHONE"
}
```

## API 清单

认证：

```text
POST /api/auth/login
GET  /api/auth/captcha
GET  /api/auth/userinfo
POST /api/auth/logout
```

监控大盘：

```text
GET /api/dashboard/stats
GET /api/dashboard/overview
```

数据源：

```text
GET  /api/datasource/list
POST /api/datasource/save
POST /api/datasource/test
POST /api/datasource/binlog-position
POST /api/datasource/clone/{id}
POST /api/datasource/enable
GET  /api/datasource/{key}/tables
GET  /api/datasource/{key}/columns?tableName=xxx
GET  /api/datasource/{key}/diagnostics
```

同步任务：

```text
GET    /api/task/list
POST   /api/task/save
DELETE /api/task/{id}
GET    /api/task/detail/{id}
POST   /api/task/target-test
POST   /api/task/sql-preview?limit=20
POST   /api/task/field-mapping/validate
POST   /api/task/clone/{id}
POST   /api/task/start/{id}
POST   /api/task/stop/{id}
POST   /api/task/batch/start
POST   /api/task/batch/stop
POST   /api/task/full-sync/{id}
GET    /api/task/full-sync/preview/{id}?limit=20
GET    /api/task/preview/{id}
GET    /api/task/resource-plan/{id}
GET    /api/task/monitor/{id}
GET    /api/task/runtime/{id}
GET    /api/task/diagnostics/{id}
```

Canal Runtime 和本机链路：

```text
GET  /api/system/local-stack/status
POST /api/system/local-stack/verify
POST /api/system/local-stack/start
POST /api/system/local-stack/stop
POST /api/system/local-stack/restart

GET  /api/system/canal/status
GET  /api/system/canal/config
GET  /api/system/canal/config/consistency
GET  /api/system/canal/stale-files
POST /api/system/canal/stale-files/clean
GET  /api/system/canal/logs
GET  /api/system/canal/metrics
GET  /api/system/canal/capabilities
GET  /api/system/canal/diagnostics
GET  /api/system/canal/setting
POST /api/system/canal/setting
POST /api/system/canal/admin/test
POST /api/system/canal/admin/login
POST /api/system/canal/admin/password
POST /api/system/canal/refresh
POST /api/system/canal/start
POST /api/system/canal/stop
POST /api/system/canal/server/start
POST /api/system/canal/server/stop
POST /api/system/canal/server/restart
POST /api/system/canal/adapter/start
POST /api/system/canal/adapter/stop
POST /api/system/canal/adapter/restart
```

日志、用户和配置：

```text
GET  /api/log/task/{id}
GET  /api/log/operation
GET  /api/log/alert
GET  /api/log/alert/stats
POST /api/log/alert/{id}/ack
POST /api/log/alert/batch-ack
POST /api/log/alert/ack-filter

GET  /api/system/user/list
POST /api/system/user/save
POST /api/system/user/enable
POST /api/system/user/reset-password
GET  /api/system/config/version/list
POST /api/system/config/version/{id}/rollback
GET  /api/system/config/package/export
POST /api/system/config/package/import
```

## 换机部署清单

迁移到新电脑或新服务器时，不要写死旧机器路径。按下面顺序处理：

1. 安装基础依赖：JDK 17、MySQL 8、Node 18+、Redis、PostgreSQL、Elasticsearch、Kafka；RocketMQ/Pulsar/HBase/RabbitMQ 按需要安装。
2. 创建 `canal_web` 库，并确认运行账号可建表、改表、读写。
3. 设置项目目录：

```bash
export CANAL_WEB_PROJECT_DIR=/data/canal-web
```

4. 如果 Canal Runtime 没放在项目内，设置实际路径：

```bash
export CANAL_WEB_RUNTIME_ROOT_DIR=/data/canal-runtime
export CANAL_SERVER_HOME=/data/canal-server
export CANAL_ADAPTER_HOME=/data/canal-adapter
export CANAL_WEB_RUNTIME_PATHS=/data/canal-runtime
export CANAL_ADAPTER_CONF_PATHS=/data/canal-adapter/conf
```

5. 如果用 LaunchAgent、systemd 或容器运行，显式设置 CLI 路径：

```bash
export CANAL_WEB_CLI_PATH=/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin
export CANAL_WEB_REDIS_CLI=/opt/homebrew/bin/redis-cli
export CANAL_WEB_KAFKA_TOPICS=/opt/homebrew/bin/kafka-topics
export CANAL_WEB_JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home
```

6. 启动后进入 Canal 运维页，打开“编辑路径”，检查项目目录、Runtime 根目录、Server Home、Adapter Home、generated 路径。
7. 执行 Canal 运维页“运行自检”和“配置一致性”，确认没有路径、权限和缺文件问题。
8. 使用系统管理页导入配置包，或手工新建数据源和任务。
9. 进入测试模拟页，先跑 MySQL、Redis、ES、PGSQL、Kafka，再按需要跑 MQ、Pulsar、HBase。
10. 对每个同步任务执行“资源准备”和“启动自检”，确认无阻断项后启动。

## 本机联调验证命令

使用 curl 验证登录和核心接口：

```bash
TOKEN=$(curl -fsS -X POST http://127.0.0.1:18082/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin123"}' \
  | sed -n 's/.*"tokenValue":"\([^"]*\)".*/\1/p')

curl -fsS -H "Authorization: $TOKEN" http://127.0.0.1:18082/api/dashboard/overview
curl -fsS -H "Authorization: $TOKEN" http://127.0.0.1:18082/api/task/list
curl -fsS -H "Authorization: $TOKEN" http://127.0.0.1:18082/api/system/canal/setting
curl -fsS -H "Authorization: $TOKEN" http://127.0.0.1:18082/api/system/local-stack/status
```

测试模拟页对应后端接口：

```bash
curl -fsS -X POST http://127.0.0.1:18082/api/task/target-test \
  -H "Authorization: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"targetType":"REDIS","targetConfig":{"host":"localhost","port":"6379","database":"6"}}'
```

Canal Admin 联调：

```bash
curl -fsS -X POST http://127.0.0.1:18082/api/system/canal/admin/login \
  -H "Authorization: $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"adminUrl":"http://127.0.0.1:8089","username":"admin","password":"admin123"}'
```

## 常见问题排查

`401 Unauthorized` 或页面提示“未登录或登录已过期”：

- 确认浏览器重新登录 `admin / admin123`。
- 确认接口使用了 `Authorization` header，值为登录返回的 `tokenValue`。
- `/api/auth/login` 和 `/api/auth/captcha` 不需要 token；如果这两个接口也返回 401，检查后端是否使用了最新包。

登录成功后没有跳转：

- 前端以 `store.token` 判断是否进入主页面，登录接口必须返回 `data.tokenValue`。
- 打开浏览器控制台确认 `/api/auth/login` 返回 `code: 0`。
- 清理旧的 `localStorage` 中 `canal-web-token`、`canal-web-user` 后重试。

Canal Admin iframe 打不开：

- 先确认 `http://127.0.0.1:8089` 可直接访问。
- 在 Canal Admin 菜单填写 Admin 地址、账号、密码后点击登录。
- 如果 Canal Admin 仍提示默认密码，先在嵌入页使用“修改密码”改为新密码。

本机状态接口超时：

- `/api/system/local-stack/status` 会检查多个本机服务，未启动 RocketMQ、Pulsar、RabbitMQ、HBase 时可能返回 `ok=false`，但不影响 MySQL/Redis/ES/PGSQL/Kafka 已跑通的链路。
- LaunchAgent 环境 PATH 较短时，设置 `CANAL_WEB_CLI_PATH`、`CANAL_WEB_REDIS_CLI`、`CANAL_WEB_KAFKA_TOPICS`。

MySQL 增量同步无数据：

- 源 MySQL 必须开启 `log_bin`。
- `binlog_format` 必须为 `ROW`。
- 源库 `server_id` 不能与 Canal `slaveId` 冲突。
- 数据源订阅表正则必须能匹配实际表名，例如 `canal_sync_verify\\..*`。
- 任务启动后检查任务运行态中的 runtime 文件和 Adapter destination 状态。

Redis/ES/PGSQL/Kafka 目标端连接成功但没有数据：

- 先在任务详情查看“资源准备”，确认目标资源已创建。
- 查看“启动自检”，确认没有 `BLOCKER`。
- 查看“运行态”，确认 Mapping 文件存在，Adapter destination 在线。
- 查看日志中心任务日志和 Canal Adapter 日志，定位写入异常。
