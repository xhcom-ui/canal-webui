CREATE TABLE IF NOT EXISTS datasource_config (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  data_source_key VARCHAR(64) NOT NULL UNIQUE,
  host VARCHAR(128) NOT NULL,
  port INT NOT NULL,
  username VARCHAR(64) NOT NULL,
  password VARCHAR(128) NOT NULL,
  db_name VARCHAR(64) NOT NULL,
  canal_destination VARCHAR(64) NOT NULL,
  filter_regex VARCHAR(512) DEFAULT '',
  filter_black_regex VARCHAR(512) DEFAULT 'mysql\\.slave_.*',
  binlog_file VARCHAR(128) DEFAULT '',
  binlog_position BIGINT DEFAULT NULL,
  binlog_timestamp BIGINT DEFAULT NULL,
  gtid VARCHAR(512) DEFAULT '',
  gtid_enabled TINYINT DEFAULT 0,
  server_id BIGINT DEFAULT NULL,
  field_filter VARCHAR(512) DEFAULT '',
  field_black_filter VARCHAR(512) DEFAULT '',
  filter_dml_insert TINYINT DEFAULT 0,
  filter_dml_update TINYINT DEFAULT 0,
  filter_dml_delete TINYINT DEFAULT 0,
  filter_query_dml TINYINT DEFAULT 0,
  filter_query_dcl TINYINT DEFAULT 0,
  filter_query_ddl TINYINT DEFAULT 0,
  filter_rows TINYINT DEFAULT 0,
  filter_table_error TINYINT DEFAULT 0,
  filter_transaction_entry TINYINT DEFAULT 0,
  ddl_isolation TINYINT DEFAULT 0,
  tsdb_enable TINYINT DEFAULT 0,
  tsdb_url VARCHAR(512) DEFAULT '',
  tsdb_username VARCHAR(128) DEFAULT '',
  tsdb_password VARCHAR(128) DEFAULT '',
  tsdb_snapshot_interval INT DEFAULT 24,
  tsdb_snapshot_expire INT DEFAULT 360,
  standby_address VARCHAR(128) DEFAULT '',
  standby_journal_name VARCHAR(128) DEFAULT '',
  standby_position BIGINT DEFAULT NULL,
  standby_timestamp BIGINT DEFAULT NULL,
  standby_gtid VARCHAR(512) DEFAULT '',
  rds_accesskey VARCHAR(256) DEFAULT '',
  rds_secretkey VARCHAR(256) DEFAULT '',
  rds_instance_id VARCHAR(128) DEFAULT '',
  ssl_mode VARCHAR(32) DEFAULT 'DISABLED',
  extra_properties TEXT,
  status TINYINT DEFAULT 1,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE datasource_config ADD COLUMN filter_regex VARCHAR(512) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN filter_black_regex VARCHAR(512) DEFAULT 'mysql\\.slave_.*';
ALTER TABLE datasource_config ADD COLUMN binlog_file VARCHAR(128) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN binlog_position BIGINT DEFAULT NULL;
ALTER TABLE datasource_config ADD COLUMN binlog_timestamp BIGINT DEFAULT NULL;
ALTER TABLE datasource_config ADD COLUMN gtid VARCHAR(512) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN gtid_enabled TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN server_id BIGINT DEFAULT NULL;
ALTER TABLE datasource_config ADD COLUMN field_filter VARCHAR(512) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN field_black_filter VARCHAR(512) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN filter_dml_insert TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_dml_update TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_dml_delete TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_query_dml TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_query_dcl TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_query_ddl TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_rows TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_table_error TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN filter_transaction_entry TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN ddl_isolation TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN tsdb_enable TINYINT DEFAULT 0;
ALTER TABLE datasource_config ADD COLUMN tsdb_url VARCHAR(512) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN tsdb_username VARCHAR(128) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN tsdb_password VARCHAR(128) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN tsdb_snapshot_interval INT DEFAULT 24;
ALTER TABLE datasource_config ADD COLUMN tsdb_snapshot_expire INT DEFAULT 360;
ALTER TABLE datasource_config ADD COLUMN standby_address VARCHAR(128) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN standby_journal_name VARCHAR(128) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN standby_position BIGINT DEFAULT NULL;
ALTER TABLE datasource_config ADD COLUMN standby_timestamp BIGINT DEFAULT NULL;
ALTER TABLE datasource_config ADD COLUMN standby_gtid VARCHAR(512) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN rds_accesskey VARCHAR(256) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN rds_secretkey VARCHAR(256) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN rds_instance_id VARCHAR(128) DEFAULT '';
ALTER TABLE datasource_config ADD COLUMN ssl_mode VARCHAR(32) DEFAULT 'DISABLED';
ALTER TABLE datasource_config ADD COLUMN extra_properties TEXT;

CREATE TABLE IF NOT EXISTS sync_task (
  id VARCHAR(32) PRIMARY KEY,
  task_name VARCHAR(64) NOT NULL,
  description VARCHAR(255) DEFAULT '',
  data_source_key VARCHAR(64) NOT NULL,
  sync_sql TEXT,
  target_type VARCHAR(32) NOT NULL,
  sync_mode VARCHAR(16) NOT NULL,
  cron_expression VARCHAR(64) DEFAULT '',
  batch_size INT DEFAULT 1000,
  task_status VARCHAR(16) DEFAULT 'STOPPED',
  total_count BIGINT DEFAULT 0,
  fail_count BIGINT DEFAULT 0,
  last_delay_ms BIGINT DEFAULT 0,
  last_start_time DATETIME DEFAULT NULL,
  last_stop_time DATETIME DEFAULT NULL,
  last_schedule_time DATETIME DEFAULT NULL,
  full_sync_file VARCHAR(512) DEFAULT '',
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE sync_task ADD COLUMN total_count BIGINT DEFAULT 0;
ALTER TABLE sync_task ADD COLUMN fail_count BIGINT DEFAULT 0;
ALTER TABLE sync_task ADD COLUMN last_delay_ms BIGINT DEFAULT 0;
ALTER TABLE sync_task ADD COLUMN last_start_time DATETIME DEFAULT NULL;
ALTER TABLE sync_task ADD COLUMN last_stop_time DATETIME DEFAULT NULL;
ALTER TABLE sync_task ADD COLUMN last_schedule_time DATETIME DEFAULT NULL;
ALTER TABLE sync_task ADD COLUMN full_sync_file VARCHAR(512) DEFAULT '';

CREATE TABLE IF NOT EXISTS task_field_mapping (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  task_id VARCHAR(32) NOT NULL,
  source_field VARCHAR(64) NOT NULL,
  target_field VARCHAR(64) NOT NULL,
  field_type VARCHAR(64) DEFAULT '',
  primary_key TINYINT DEFAULT 0,
  nullable_field TINYINT DEFAULT 1,
  enabled TINYINT DEFAULT 1,
  default_value VARCHAR(255) DEFAULT '',
  transform_expr VARCHAR(512) DEFAULT '',
  format_pattern VARCHAR(128) DEFAULT '',
  field_options TEXT,
  INDEX idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE task_field_mapping ADD COLUMN field_type VARCHAR(64) DEFAULT '';
ALTER TABLE task_field_mapping ADD COLUMN primary_key TINYINT DEFAULT 0;
ALTER TABLE task_field_mapping ADD COLUMN nullable_field TINYINT DEFAULT 1;
ALTER TABLE task_field_mapping ADD COLUMN enabled TINYINT DEFAULT 1;
ALTER TABLE task_field_mapping ADD COLUMN default_value VARCHAR(255) DEFAULT '';
ALTER TABLE task_field_mapping ADD COLUMN transform_expr VARCHAR(512) DEFAULT '';
ALTER TABLE task_field_mapping ADD COLUMN format_pattern VARCHAR(128) DEFAULT '';
ALTER TABLE task_field_mapping ADD COLUMN field_options TEXT;

CREATE TABLE IF NOT EXISTS task_target_config (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  task_id VARCHAR(32) NOT NULL,
  config_key VARCHAR(64) NOT NULL,
  config_value TEXT,
  INDEX idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE task_target_config MODIFY COLUMN config_value TEXT;

CREATE TABLE IF NOT EXISTS sync_task_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  task_id VARCHAR(32) NOT NULL,
  log_level VARCHAR(16) DEFAULT 'INFO',
  log_content TEXT,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_task_id (task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS app_user (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(64) NOT NULL UNIQUE,
  password VARCHAR(128) NOT NULL,
  nickname VARCHAR(64) NOT NULL,
  role_code VARCHAR(32) NOT NULL,
  status TINYINT DEFAULT 1,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS operation_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  module_name VARCHAR(64) NOT NULL,
  action_name VARCHAR(64) NOT NULL,
  ref_id VARCHAR(64) DEFAULT '',
  log_content TEXT,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_module_ref (module_name, ref_id),
  INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS alert_log (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  alert_level VARCHAR(16) DEFAULT 'ERROR',
  module_name VARCHAR(64) NOT NULL,
  ref_id VARCHAR(64) DEFAULT '',
  alert_title VARCHAR(128) NOT NULL,
  alert_content TEXT,
  acknowledged TINYINT DEFAULT 0,
  acknowledge_time DATETIME DEFAULT NULL,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_alert_status (acknowledged, create_time),
  INDEX idx_alert_ref (module_name, ref_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE alert_log ADD COLUMN alert_level VARCHAR(16) DEFAULT 'ERROR';
ALTER TABLE alert_log ADD COLUMN module_name VARCHAR(64) NOT NULL;
ALTER TABLE alert_log ADD COLUMN ref_id VARCHAR(64) DEFAULT '';
ALTER TABLE alert_log ADD COLUMN alert_title VARCHAR(128) NOT NULL;
ALTER TABLE alert_log ADD COLUMN alert_content TEXT;
ALTER TABLE alert_log ADD COLUMN acknowledged TINYINT DEFAULT 0;
ALTER TABLE alert_log ADD COLUMN acknowledge_time DATETIME DEFAULT NULL;

CREATE TABLE IF NOT EXISTS config_version (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  config_type VARCHAR(64) NOT NULL,
  ref_id VARCHAR(64) NOT NULL,
  version_no INT NOT NULL,
  config_content MEDIUMTEXT,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_config_version (config_type, ref_id, version_no),
  INDEX idx_config_ref (config_type, ref_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS canal_runtime_setting (
  id BIGINT PRIMARY KEY,
  server_mode VARCHAR(32) DEFAULT 'tcp',
  zk_servers VARCHAR(512) DEFAULT '',
  flat_message TINYINT DEFAULT 1,
  canal_batch_size INT DEFAULT 50,
  canal_get_timeout INT DEFAULT 100,
  access_channel VARCHAR(32) DEFAULT 'local',
  mq_servers VARCHAR(512) DEFAULT '',
  mq_username VARCHAR(128) DEFAULT '',
  mq_password VARCHAR(128) DEFAULT '',
  mq_topic VARCHAR(128) DEFAULT '',
  mq_partition_hash VARCHAR(256) DEFAULT '',
  mq_dynamic_topic VARCHAR(512) DEFAULT '',
  admin_manager VARCHAR(128) DEFAULT '',
  admin_user VARCHAR(128) DEFAULT 'admin',
  admin_password VARCHAR(256) DEFAULT '',
  admin_auto_register TINYINT DEFAULT 0,
  admin_cluster VARCHAR(128) DEFAULT '',
  admin_name VARCHAR(128) DEFAULT '',
  project_dir VARCHAR(512) DEFAULT '',
  runtime_root_dir VARCHAR(512) DEFAULT '',
  canal_server_home VARCHAR(512) DEFAULT '',
  canal_adapter_home VARCHAR(512) DEFAULT '',
  dbsync_source_jar VARCHAR(512) DEFAULT '',
  dbsync_runtime_jar VARCHAR(512) DEFAULT '',
  generated_paths TEXT,
  extra_properties TEXT,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

ALTER TABLE canal_runtime_setting ADD COLUMN server_mode VARCHAR(32) DEFAULT 'tcp';
ALTER TABLE canal_runtime_setting ADD COLUMN zk_servers VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN flat_message TINYINT DEFAULT 1;
ALTER TABLE canal_runtime_setting ADD COLUMN canal_batch_size INT DEFAULT 50;
ALTER TABLE canal_runtime_setting ADD COLUMN canal_get_timeout INT DEFAULT 100;
ALTER TABLE canal_runtime_setting ADD COLUMN access_channel VARCHAR(32) DEFAULT 'local';
ALTER TABLE canal_runtime_setting ADD COLUMN mq_servers VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN mq_username VARCHAR(128) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN mq_password VARCHAR(128) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN mq_topic VARCHAR(128) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN mq_partition_hash VARCHAR(256) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN mq_dynamic_topic VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN admin_manager VARCHAR(128) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN admin_user VARCHAR(128) DEFAULT 'admin';
ALTER TABLE canal_runtime_setting ADD COLUMN admin_password VARCHAR(256) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN admin_auto_register TINYINT DEFAULT 0;
ALTER TABLE canal_runtime_setting ADD COLUMN admin_cluster VARCHAR(128) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN admin_name VARCHAR(128) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN project_dir VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN runtime_root_dir VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN canal_server_home VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN canal_adapter_home VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN dbsync_source_jar VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN dbsync_runtime_jar VARCHAR(512) DEFAULT '';
ALTER TABLE canal_runtime_setting ADD COLUMN generated_paths TEXT;
ALTER TABLE canal_runtime_setting ADD COLUMN extra_properties TEXT;

INSERT INTO canal_runtime_setting
(id, server_mode, zk_servers, flat_message, canal_batch_size, canal_get_timeout, access_channel,
 mq_servers, mq_username, mq_password, mq_topic, mq_partition_hash, mq_dynamic_topic,
 admin_manager, admin_user, admin_password, admin_auto_register, admin_cluster, admin_name, project_dir, runtime_root_dir, canal_server_home,
 canal_adapter_home, dbsync_source_jar, dbsync_runtime_jar, generated_paths, extra_properties)
VALUES
(1, 'tcp', '', 1, 50, 100, 'local', '', '', '', '', '', '', '127.0.0.1:8089', 'admin', '', 0, 'default', 'canal-web', '', '', '', '', '', '', '', '')
ON DUPLICATE KEY UPDATE id = id;

INSERT INTO app_user (username, password, nickname, role_code, status)
VALUES ('admin', 'admin123', 'Administrator', 'ADMIN', 1)
ON DUPLICATE KEY UPDATE
  username = username;
