MB = 1024 * 1024
GB = 1024 * 1024 * 1024

LogHomeDir = "/home/hadoop/cdh-data/log"


remote_log_for_yarn_jobhistory = '/var/log/hadoop-yarn'

default_service_conf = {}
### Zookeeper config
zk_server_conf = {
    #'dataLogDir': '%s/zookeeper' % LogHomeDir,
    'zk_server_log_dir': '%s/zookeeper' % LogHomeDir,
    'zookeeper_server_java_heapsize': 128 * MB, #bytes
    #'dataDir': '%s/zookeeper' % LogHomeDir,
    # 'zookeeper_config_safety_valve': """ """
}

### HDFS config
hdfs_service_config = {
    'dfs_replication': 2,
}
dn_config = {
    'dfs_datanode_handler_count': 4,
    'datanode_java_heapsize': 1 * GB,
    'datanode_log_dir': '%s/hadoop-hdfs'  % LogHomeDir,
    #'dfs_data_dir_list': '/home1/hadoop/hadoop_data,/home2/hadoop/hadoop_data,/home3/hadoop/hadoop_data',
    'dfs_datanode_drop_cache_behind_reads': True,
    'dfs_datanode_drop_cache_behind_writes': True,
    'dfs_datanode_du_reserved': 50 * GB,
    'dfs_datanode_failed_volumes_tolerated': 1,
    'dfs_datanode_max_xcievers': 4096,
    # 'datanode_config_safety_valve': """ """
}
failover_config = {
    'failover_controller_log_dir': '%s/hadoop-hdfs'  % LogHomeDir,
}
client_config = {
    'dfs_client_use_trash': True,
    # 'hdfs_client_env_safety_valve': """JAVA_HOME=/usr/java/jdk1.7.0_25/"""
}
journalnode_config = {
    #'dfs_journalnode_edits_dir': '/home/hadoop/journal_edits',
    'dfs_journalnode_http_port': 50480,
    'journalNode_java_heapsize': 512 * MB,
    'journalnode_log_dir': '%s/hadoop-hdfs' % LogHomeDir,
    # 'jn_config_safety_valve': """ """,
}
namenode_config = {
    # 'dfs_name_dir_list': '/home/hadoop/hadoop_name',
    'fs_trash_interval': 60,  # Minutes
    'dfs_http_port': 50070,
    'dfs_namenode_handler_count': 55,
    'dfs_namenode_service_handler_count': 55,
    'dfs_namenode_servicerpc_address': 8022,
    'namenode_java_heapsize': 2 * GB,
    'namenode_log_dir': '%s/hadoop-hdfs' % LogHomeDir,
    'namenode_config_safety_valve':
"""<property>
    <name>fs.trash.checkpoint.interval</name>
    <value>60</value>
</property>
<property>
    <name>hadoop.http.staticuser.user</name>
    <value>hdfs</value>
</property>
"""
}
secondarynamenode_config = {
    'secondarynamenode_log_dir': '%s/hadoop-hdfs' % LogHomeDir,
    'dfs_secondary_http_port': 50090,
    'secondary_namenode_java_heapsize': 2 * GB, #bytes
    # 'secondarynamenode_java_opts': '',
    # 'fs_checkpoint_dir_list': '%s/snn' % LogHomeDir,
    # 'secondarynamenode_config_safety_valve': """"""

}

### yarn config
yarn_service_config = {
    'yarn_log_aggregation_enable': True,
    'yarn_service_mapred_safety_valve':
"""<property>
    <name>mapreduce.reduce.shuffle.input.buffer.percent</name>
    <value>0.5</value>
</property>
"""
}
jobhistory_conf = {
    'mapreduce_jobhistory_webapp_address': 50888,
    'mr2_jobhistory_java_heapsize': 1 * GB,
    'mr2_jobhistory_log_dir': '%s/hadoop-mapreduce' % LogHomeDir,
    'yarn_app_mapreduce_am_staging_dir': '/user',
    'jobhistory_config_safety_valve':
"""<property>
    <name>yarn.nodemanager.remote-app-log-dir</name>
    <value>%s</value>
</property>
""" % remote_log_for_yarn_jobhistory,

    'jobhistory_mapred_safety_valve':
"""<property>
    <name>mapreduce.jobhistory.cleaner.enable</name>
    <value>true</value>
</property>
""",
    #'mr2_jobhistory_java_opts': '',
}
node_manager_config = {
    'node_manager_java_heapsize': 2 * GB,
    # 'yarn_nodemanager_local_dirs': '/home1/hadoop/yarn_nm,/home2/hadoop/yarn_nm,/home3/hadoop/yarn_nm',
    'node_manager_log_dir': '%s/hadoop-yarn' % LogHomeDir,
    'yarn_nodemanager_log_dirs': '%s/hadoop-yarn-container' % LogHomeDir,
    'yarn_nodemanager_resource_memory_mb': 10 * GB / MB,
    'yarn_nodemanager_vmem_pmem_ratio': 8.1,
    'yarn_nodemanager_webapp_address': 50842,
    'yarn_nodemanager_remote_app_log_dir': remote_log_for_yarn_jobhistory,
    'yarn_nodemanager_log_retain_seconds': 604800,
    # 'nodemanager_mapred_safety_valve': """ """,
    # 'nodemanager_config_safety_valve': """ """

}
resource_manager_config = {
    'resource_manager_java_heapsize': 2 * GB,
    'resource_manager_log_dir': '%s/hadoop-yarn' % LogHomeDir,
    'yarn_resourcemanager_webapp_address': 50088,
    'yarn_resourcemanager_scheduler_class': 'org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler',
    'resourcemanager_fair_scheduler_configuration':
"""<?xml version="1.0"?>
<allocations>
    <queue name="pool_high">
        <minResources>4096</minResources>
        <weight>6.0</weight>
        <maxRunningApps>5</maxRunningApps>
    </queue>
    <queue name="default">
        <weight>3.0</weight>
        <maxRunningApps>10</maxRunningApps>
    </queue>
    <queue name="pool_low">
        <weight>1.0</weight>
        <maxRunningApps>10</maxRunningApps>
    </queue>
    <user name="stat">
        <maxRunningApps>9</maxRunningApps>
        <weight>3.0</weight>
    </user>
    <userMaxAppsDefault>10</userMaxAppsDefault>
    <defaultMinSharePreemptionTimeout>600</defaultMinSharePreemptionTimeout>
    <fairSharePreemptionTimeout>600</fairSharePreemptionTimeout>
    <queueMaxAppsDefault>30</queueMaxAppsDefault>
    <defaultQueueSchedulingMode>fair</defaultQueueSchedulingMode>
</allocations>
""",
    # 'resource_manager_java_opts': '',
    # 'resourcemanager_config_safety_valve': """""",
    # 'resourcemanager_mapred_safety_valve': """"""
}
yarn_client_config = {
    'io_sort_factor': 20,
    'io_sort_mb': 200,
    'mapred_reduce_slowstart_completed_maps': 0.90,
    'mapred_reduce_tasks': 56,
    'mapred_submit_replication': 2,
    # 'mapreduce_client_env_safety_valve':"""JAVA_HOME=/usr/java/jdk1.7.0_25/""",
    'mapreduce_map_java_opts':"""-XX:+UseConcMarkSweepGC -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 -Duser.language=zh -Duser.region=CN -server""",
}

### hive config
hive_config = {
    'hive_log_dir': '%s/hive' % LogHomeDir,
    'hive_metastore_max_threads': 100000,
    'hive_metastore_min_threads': 200,
    'hive_metastore_java_heapsize': 128 * MB, #bytes
    # 'hive_metastore_java_opts': '',
    # 'hive_metastore_config_safety_valve': """ """
}


### hbase config
master_config = { # hbase
    'hbase_master_handler_count': 25,
    'hbase_master_info_port': 50610,
    'hbase_master_logcleaner_ttl': 60000, # millisecond
    'hbase_master_log_dir': '%s/hbase' % LogHomeDir,
    'hbase_master_java_heapsize': 2 * GB,
    # 'hbase_master_java_opts': '',
    # 'hbase_master_config_safety_valve': """ """
}
region_server_config = { # region server conf
    'hbase_hregion_memstore_flush_size': 128 * MB, #bytes
    'hbase_regionserver_handler_count': 10,
    'hbase_regionserver_java_heapsize': 512 * MB, #bytes
    'hbase_regionserver_info_port': 50630,
    'hbase_regionserver_log_dir': '%s/hbase' % LogHomeDir,
    'hbase_regionserver_lease_period': 60000, # millisecond
    # 'hbase_regionserver_java_opts': '',
    # 'hbase_regionserver_config_safety_valve': """ """
}

###  hue config
hue_config = { # hue server conf
    'hue_http_port': 8888,
    'hue_server_log_dir': '%s/hue' % LogHomeDir,
    # 'hue_server_hue_safety_valve': """ """

}
beeswax_config = { # beeswax server conf
    'beeswax_server_conn_timeout': 120, # seconds
    'metastore_conn_timeout': 10, # seconds
    'beeswax_log_dir': '%s/hue' % LogHomeDir,
    'beeswax_server_heapsize': 128 * MB, #bytes
    # 'beeswax_hive_conf_safety_valve': """ """
}

# impala config
impala_conf = { # impala conf
    'impalad_memory_limit': 4 * GB, #bytes
    'log_dir': '%s/impalad' % LogHomeDir,
    # 'impala_hive_conf_safety_valve': """ """,
    # 'impala_hdfs_site_conf_safety_valve': """ """
}
statestore_config = { # impala state store
    'log_dir': '%s/statestore' % LogHomeDir,
    'state_store_num_server_worker_threads': 4
}

# Oozie config
oozie_config = { # Oozie server conf
    'oozie_log_dir': '%s/log/oozie' % LogHomeDir,
    'oozie_java_heapsize': 512 * MB, #bytes
    'oozie_data_dir': '%s/oozie-data' % LogHomeDir,
    # 'oozie_config_safety_valve': """ """
}

agent_conf = { # flume conf
    'agent_java_heapsize': 256 * MB, #bytes
    'agent_home_dir': '%s/flum' % LogHomeDir
}
