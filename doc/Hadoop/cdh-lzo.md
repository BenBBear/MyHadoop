# CDH LZO

主要参考：<http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM4Ent/4.5.3/Cloudera-Manager-Enterprise-Edition-Installation-Guide/cmeeig_install_LZO_Compression.html>

    CDH的LZO是单独作为组件的，作为Parcel发布。

Parcel是：<http://archive.cloudera.com/gplextras/parcels/latest/>

主要操作步骤：

1.  在管理界面配置上此Parcel
2.  下载、分发、激活 
3.  修改配置和重新启动需要LZO功能的服务

## MR or YARN

在YARN environment safety valve（环境变量设置）中增加：

    HADOOP_CLASSPATH=/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/*
    JAVA_LIBRARY_PATH=/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/native

在YARN Client environment safety valve（客户端的环境变量设置）中增加：
    
    HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/*
    JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/native

在YARN的Gateway的YARN 客户端配置安全阀（yarn-site.xml）中增加：

    <property>
    <name>yarn.application.classpath</name>
    <value>$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$YARN_HOME/*,$YARN_HOME/lib/*,/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/*,/opt/cloudera/parcels/CDH/lib/hbase/*,/opt/cloudera/parcels/CDH/lib/hbase/lib/*</value>
    </property>

另外，你可能需要安装lzo，如果你的机器上没有的话。（因为Parcel中提供的是hadoop-lzo，不包括lzo本身）

    wget http://www.oberhumer.com/opensource/lzo/download/lzo-2.06.tar.gz
    tar -zxvf lzo-2.06.tar.gz
    cd lzo-2.06
    ./configure
    make
    make install

这种方法安装的LZO是在/usr/lib中，你或许需要放到/usr/lib64目录中。


## HIVE

    不需要配置
