# CM卸载

1.  停CDH服务（界面操作，不详解）
2.  停CM服务

        service cloudera-scm-agent stop
        service cloudera-scm-server stop
        service cloudera-scm-server-db stop
        yum remove 'cloudera-manager-*' -y
        yum clean all

3.  删除安装的软件

        rm -Rf /usr/share/cmf /var/lib/cloudera* /var/cache/yum/cloudera* /var/log/cloudera* /var/run/cloudera*
        rm -Rf /var/lib/flume-ng /var/lib/hadoop* /var/lib/hue /var/lib/oozie /var/lib/solr /var/lib/sqoop*

4.  删除数据目录
    
        #如果你修改了默认配置，你自己删除它吧
        rm -Rf /dfs /mapred /yarn
        rm /tmp/.scm_prepare_node.lock
		
service cloudera-scm-agent stop; service cloudera-scm-server stop; service cloudera-scm-server-db stop; yum remove 'cloudera-manager-*' -y; yum clean all; rm -Rf /usr/share/cmf /var/lib/cloudera* /var/cache/yum/cloudera* /var/log/cloudera* /var/run/cloudera*; rm -Rf /var/lib/flume-ng /var/lib/hadoop* /var/lib/hue /var/lib/oozie /var/lib/solr /var/lib/sqoop*; rm -Rf /dfs /mapred /yarn; rm /tmp/.scm_prepare_node.lock