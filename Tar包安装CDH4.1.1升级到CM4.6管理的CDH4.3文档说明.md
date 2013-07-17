# CDH4.1.1升级到CDH4.3.0(由Tar包安装转为CM安装管理)

### 1. 说明     
因为CDH4 Beta 2或更新的版本升级不需要升级HDFS元数据，所以CDH4.1.1升级到CDH4.3.0，HDFS的元数据不用升级，CDH4.3.0可以直接使用CDH4.1.1的元数据启动完成HDFS的升级。       
CM管理的CDH会为各服务创建不同的用户与组进行服务的启动（对于组可以统一配置为hadoop），在由Tar包安装的CDH转为CM管理的CDH所需要的做的就是在CM上安装CDH服务后在管理界面修改各服务数据目录的指向及对已有的数据目录的所属权限进行修改（把对应的数据目录所属权限修改为启动相应服务所对应的用户与组），在转换过程中最需要注意的就是权限问题。

### 2. 准备工作     
 
- 安装CM        
- 添加所需升级的服务CDH4.3.0（各服务角色与所在的机器与升级前保持一致）
- 初始化运行CDH4.3.0
- 关闭集群CDH4.3.0
- 关闭集群CDH4.1.1
- 启动集群CDH4.1.1
- 关闭集群CDH4.1.1（为了升级简单，把所有editlog都merge完，升级启动只需加载fsimage文件）

### 3. 升级         
这里主要给出ZoonKeeper、HDFS、Yarn、Hbase、Hive的升级。

#### 3.1 升级ZoonKeeper     

- 在CM界面修改CDH4.3.0中的ZoonKeeper的配置参数，主要是配置端口、JVM参数与数据目录（配置与CDH4.1.1相同）。需要配置的目录有：dataDir、dataLogDir及ZooKeeper日志目录。        
- ZoonKeeper启动运行的用户与组都为zoonkeeper，需要修改配置的目录的所属也为zoonkeeper。不然会因权限问题出错。        
- 启动ZoonKeeper        
- 完成升级      

#### 3.2 升级HDFS

- 在NameNode机器备份元数据      
        
        cd /home/zouhc/hadoop_name
        tar -cvf /root/nn_backup_data.tar .

- 在CM界面修改CDH4.3.0中的HDFS的配置参数，主要配置端口、JVM参数与数据目录（配置与CDH4.1.1相同）。需要配置的目录有：dfs.datanode.data.dir (/home/hadoop/hadoop_data)、dfs.namenode.name.dir (/home/hadoop/hadoop_name)、dfs.journalnode.edits.dir (/home/hadoop/hadoop_journal/edits)及HDFS的日志目录。        
- HDFS启动运行的用户与组都为hdfs，需要修改配置的目录的所属也为hdfs。不然会因权限问题出错。        
- 启动HDFS      
- 完成升级      

#### 3.3 升级Yarn       

- 在CM界面修改CDH4.3.0中的Yarn的配置参数，主要配置端口、JVM参数与数据目录（配置与CDH4.1.1相同）。需要配置的本地目录有：yarn.nodemanager.local-dirs (/home/hadoop/yarn_nm/local-dir)、yarn.nodemanager.log-dirs (/home/hadoop/yarn_nm/log-dir)及Yarn的日志目录。需要配置的HDFS上的目录有：yarn.app.mapreduce.am.staging-dir (/user)和mapreduce.jobhistory.intermediate-done-dir、mapreduce.jobhistory.done-dir。在CM中mapreduce.jobhistory.intermediate-done-dir、mapreduce.jobhistory.done-dir没有给出配置，需要在JobHistory Server的配置页的高级项中以xml片段的方式进行配置,如:

           <property>
            <name>mapreduce.jobhistory.intermediate-done-dir</name>
             <value>/home/hadoop/yarn_nm/jobhistory/intermediate-done-dir</value>
           </property>

           <property>
             <name>mapreduce.jobhistory.done-dir</name>
             <value>/home/hadoop/yarn_nm/jobhistory/done</value>
           </property>

- Yarn启动运行的用户为yarn，组为hadoop，需要修改配置的本地目录的所属也为yarn:hadoop。不然会因权限问题出错。        
- JobHistory Server启动运行的用户为mapred，组为hadoop，需要修改配置的HDFS目录的所属也为mapred:hadoop。不然会因权限问题出错。        
- 启动Yarn及JobHistory Server      
- 完成升级      

#### 3.4 升级Hbase      

- 在CM界面修改CDH4.3.0中的Hbase的配置参数，主要配置端口、JVM参数与数据目录（配置与CDH4.1.1相同）。需要配置的目录有：hbase.rootdir、Hadoop 度量输出目录及HDFS的日志目录。        
- HDFS启动运行的用户与组都为hbase，需要修改配置的目录的所属也为hbase。不然会因权限问题出错。        
- 启动Hbase      
- 完成升级      

#### 3.5 升级Hive

- 在CM界面修改CDH4.3.0中的Hive的配置参数，主要配置端口、Hive Metastore数据库、与数据目录（配置与CDH4.1.1相同）。需要配置的数据目录有：hive.metastore.warehouse.dir及Hive的日志目录。        
- 升级Hive Metastore数据库Schema(CDH4.1.1使用的Hive版本为0.9.0, CDH4.3.0使用的Hive版本为0.10.0)    
    - 备份数据库      
    - 运行/opt/cloudera/parcels/CDH/lib/hive/scripts/metastore/upgrade/下的升级脚本。如数据库使用的是Mysql则：
            
            cd /opt/cloudera/parcels/CDH/lib/hive/scripts/metastore/upgrade/mysql
            mysql> source upgrade-0.9.0-to-0.10.0.mysql.sql
- Hive启动运行的用户与组都为hive，需要修改配置的目录的所属也为Hive。不然会因权限问题出错。        
- 启动Hive      
- 完成升级      
