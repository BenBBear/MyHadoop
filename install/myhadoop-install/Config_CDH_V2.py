import getopt
import inspect
import logging
import sys
import textwrap

from cm_api.api_client import ApiResource

from cm_conf.confs import *
from utils import *
from CDH_configs_V2 import *


def get_cm_host():
    if not CM_HOST.strip():
        return socket.gethostname()
    else:
        return CM_HOST.strip()

def get_clusters():
    api = ApiResource(get_cm_host(), username=CM_USERNAME, password=CM_USER_PASSWORD, version=2)
    return api.get_all_clusters()

def update_services_conf():
    clusters = get_clusters()

    for cluster in clusters:
        services = cluster.get_all_services()
        for service in services:
            if str(service.type) == 'HDFS':
                service.update_config(
                    svc_config=hdfs_service_config,
                    DATANODE=dn_config,
                    FAILOVERCONTROLLER=failover_config,
                    GATEWAY=client_config,
                    JOURNALNODE=journalnode_config,
                    NAMENODE=namenode_config,
                    SECONDARYNAMENODE=secondarynamenode_config
                )
            elif str(service.type) == 'ZOOKEEPER':
                service.update_config(
                    svc_config=default_service_conf,
                    SERVER=zk_server_conf
                )
            elif str(service.type) == 'YARN':
                service.update_config(
                    svc_config=yarn_service_config,
                    JOBHISTORY=jobhistory_conf,
                    NODEMANAGER=node_manager_config,
                    RESOURCEMANAGER=resource_manager_config,
                    GATEWAY=yarn_client_config
                )
            elif str(service.type) == 'HIVE':
                service.update_config(
                    svc_config=default_service_conf,
                    HIVEMETASTORE=hive_config
                )
            elif str(service.type) == 'HBASE':
                service.update_config(
                    svc_config=default_service_conf,
                    MASTER=master_config,
                    REGIONSERVER=region_server_config
                )
            elif str(service.type) == 'OOZIE':
                service.update_config(
                    svc_config=default_service_conf,
                    OOZIE_SERVER=oozie_config
                )
            elif str(service.type) == 'HUE':
                service.update_config(
                    svc_config=default_service_conf,
                    HUE_SERVER=hue_config,
                    BEESWAX_SERVER=beeswax_config
                )
            elif str(service.type) == 'IMPALA':
                service.update_config(
                    svc_config=default_service_conf,
                    IMPALAD=impala_conf,
                    STATESTORE=statestore_config
                )
            elif str(service.type) == 'FLUME':
                service.update_config(
                    svc_config=default_service_conf,
                    AGENT=agent_conf
                )

if __name__ == '__main__':
    update_services_conf()
