DROP table IF EXISTS cdc_conf;
CREATE TABLE IF NOT EXISTS cdc_conf (
  instance_host string NOT NULL DEFAULT '' COMMENT '实例IP',
  db_name string NOT NULL DEFAULT '' COMMENT '数据库名称',
  server_id string NULL COMMENT 'CDC监听端口',
  source_table string NOT NULL DEFAULT '' COMMENT '源表名',
  ods_table_prefix string NOT NULL DEFAULT '' COMMENT 'ODS表前缀',
  source_uid string NOT NULL DEFAULT '' COMMENT 'source uid',
  filter_uid string NOT NULL DEFAULT '' COMMENT 'filter uid',
  flatmap_uid string NOT NULL DEFAULT '' COMMENT 'flatmap uid',
  sink_uid string NOT NULL DEFAULT '' COMMENT 'sink uid'
) ENGINE=OLAP
DUPLICATE KEY(instance_host, db_name, server_id, source_table)
COMMENT "CDC配置表"
DISTRIBUTED BY HASH(instance_host) BUCKETS 3
PROPERTIES (
"replication_num" = "3",
"in_memory" = "false",
"storage_format" = "DEFAULT"
);


INSERT INTO dim_cdc_conf VALUES('xxx', 'xxx', 'xxx', 'xxx', 'xxx', uuid(), uuid(), uuid(), uuid());