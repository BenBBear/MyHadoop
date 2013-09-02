# Hadoop安装
	
Hadoop经典安装。

    此安装是Tar包单用户安装，而非官方的yum系统安装。
    此安装方式在数台机器上安装，方便学习和研究。
    
    安装版本：Hadoop CDH4.*
    安装特性：安装Yarn，启用HA，启用QJM。

假设在5台机器上安装，安装后角色如下：

    hadoop1 NN，DN，RM，NM，ZK，JN，ZKFC
    hadoop2 NN，DN，NM，ZK，JN，ZKFC
    hadoop1 DN，NM，ZK，JN
    hadoop1 DN，NM，ZK，JN
    hadoop1 DN，NM，ZK，JN

    注：NN=NameNode，DN=DataNode，ZK=ZookeeperNode，JN=JournalNode，
        ZKFC=Zookeeper Failover Controller，RM=Resouse Manager，NM=NodeManager

安装之后的目录如下：
    
    $home/local/jdk         -- 程序目录
    $home/local/hadoop      -- 程序目录
    $home/local/zookeeper   -- 程序目录
    $home/download          -- 下载目录
    $home/apps              -- hadoop等安装目录
    $home/meta              -- 元数据目录
    $home/data              -- 数据目录
    $home/yarn              -- yarn本地目录和日志目录
    $home/temp

## 安装条件

* 支持的操作系统
    
    常用UNIX系操作系统都支持。 [点击支持的操作系统列表](http://www.cloudera.com/content/cloudera-content/cloudera-docs/CDH4/latest/CDH4-Requirements-and-Supported-Versions/cdhrsv_topic_1.html)

* JDK版本

    * JDK1.6.0_8及更新版本
    * JDK1.7.0_15及更新版本

* 机器
    
    * 内存：5G+
    * 磁盘：200G+
    
    *注：此外，全部机器最好有相同的操作系统，相同的目录结构，方便安装*
   
## 安装

Tar包安装，相关Tar包在[http://archive.cloudera.com/cdh4/cdh/4]()页面查找下载，注意是tar.gz后缀。

### 准备

假设你在5台机器上安装，安装用户为hadoop。
    
*   干净的用户  
    
    用户最好是新建的，以减少环境影响。多台机器上应该建立相同的用户名并且有相同的密码（纯粹为了方便管理）。
    
*   主机域名解析文件（/etc/hosts）
    
    必须的一步，假设我们的域名定义如下（修改需要root权限）：  
  
        192.168.10.1    hadoop1
        192.168.10.2    hadoop2
        192.168.10.3    hadoop3
        192.168.10.4    hadoop4
        192.168.10.5    hadoop5
    
    *注：需要在全部机器上统一修改*

*   免密码登陆
    
    单机免密码：

        # 在hadoop1上
        ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
        cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
        chmod 755 ~/.ssh
        chmod 600 ~/.ssh/authorized_keys
        ssh localhost

    多机免密码
    
    仅需要NN机器到DN机器免密码就可以了。为了方便，可以如下操作（取巧了）：

        # 在hadoop1机器执行，把.ssh目录整个拷贝到其它机器
        scp -P 22 -r ~/.ssh $USER@192.168.10.2:$HOME
        
    *注：此方法让所有机器共用一个密钥文件*

*  时间同步

    在root用户的定时任务中增加：

        */30 * * * * /usr/sbin/ntpdate cn.pool.ntp.org>/dev/null 2>&1;/sbin/hwclock -w >/dev/null 2>&1
        注：每30分钟更新一下时间，并修改硬件时钟

*   必要的目录

    mkdir -p ~/download ~/local ~/apps

### 安装JDK

常规安装方式，下载，解压，安装，配置。

下载需要选择适合自己系统的版本。下载[JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)，选择tar包版本，例如：[jdk-7u25-linux-x64.tar.gz](http://download.oracle.com/otn-pub/java/jdk/7u25-b15/jdk-7u25-linux-x64.tar.gz)

    # 在hadoop1上
    # 下载
    cd ~/download
    wget http://download.oracle.com/otn-pub/java/jdk/7u25-b15/jdk-7u25-linux-x64.tar.gz
    # 解压
    tar -xzvf jdk*.tar.gz
    # 安装
    mv jdk-7u25-linux-x64 ~/apps/
    ln -sf $HOME/apps/jdk* $HOME/local/jdk
    # 配置
    vi ~/.bash_profile  # 增加如下内容
        # Java environment
        export JAVA_HOME=$HOME/local/jdk
        export CLASSPATH=$JAVA_HOME/lib
        export PATH=$JAVA_HOME/bin:$PATH
    # 重载环境
    source ~/.bash_profile

    # 在其它机器上做相同的操作，或者把安装好的软件拷贝过去，注意软链不能直接拷贝。

### 安装Zookeeper

    Hadoop的HA功能需要Zookeeper

普通安装方式。

以安装4.2.1版本系列为例。

    # 在hadoop1上
    # 下载
    cd ~/download
    wget http://archive.cloudera.com/cdh4/cdh/4/zookeeper-3.4.5-cdh4.2.1.tar.gz
    # 解压
    tar -xzvf zookeeper*.tar.gz
    # 安装
    mv zookeeper-3.4.5-cdh4.2.1 ~/apps
    ln -sf $HOME/apps/zookeeper* $HOME/local/zookeeper
    # 配置
    cd ~/local/zookeeper/conf
    vi zoo.cfg
        # 编辑文件内容如下
        tickTime=2000
        initLimit=10
        syncLimit=2
        clientPort=50181                                  #可选客户端连接端口    
        maxClientCnxns=200
        dataDir=/home/hadoop/local/zookeeper/data         #可选数据存储目录
        dataLogDir=/home/hadoop/local/zookeeper/datalog   #可选数据日志（类似binlog）存储目录
        server.1=hadoop1:50288:50388      #机器编号和域名:选举端口:leader端口
        server.2=hadoop2:50288:50388
        server.3=hadoop3:50288:50388
        server.4=hadoop4:50288:50388
        server.5=hadoop5:50288:50388
    cd ~/local/zookeeper
    sh bin/zkServer-initialize.sh --myid=1
    vi ~/.bash_profile
        # 编辑文件，增加如下内容
        export ZK_HOME=$HOME/local/zookeeper
        export ZK_BIN=$ZK_HOME/bin
        export ZK_CONF_DIR=$ZK_HOME/conf
        export PATH=$ZK_BIN:$PATH
    # 重载环境
    source ~/.bash_profile

    # 在其它机器上做相同的操作，或者把安装好的软件拷贝过去，注意软链不能直接拷贝。
    # 在配置部分，运行zk初始化时，myid每台机器各不相同，分别是1,2,3,4,5。

启动

    # 在每台机器上执行
    cd ~/local/zookeeper
    bin/zkServer.sh start       #启动
    bin/zkServer.sh status      #查看状态
    # 如果启动失败，请查看当前目录下的zookeeper.out日志文件

### 安装Hadoop

以安装4.2.1版本系列为例。

    在hadoop1上
    # 下载
    cd ~/download
    wget http://archive.cloudera.com/cdh4/cdh/4/hadoop-2.0.0-cdh4.2.1.tar.gz
    # 解压
    tar -xzvf hadoop-2.0.0-cdh4.2.1.tar.gz
    # 安装
    mv hadoop-2.0.0-cdh4.2.1 ~/apps
    ln -sf $HOME/apps/hadoop-2.0.0-cdh4.2.1.tar.gz $HOME/local/hadoop

#### 配置

在`~/local/hadoop/etc/hadoop`目录。

    cd ~/local/hadoop/etc/hadoop

*   配置hadoop-env.sh  
        
        # 在最前位置增加
        shopt -s expand_aliases;
        . $HOME/.bash_profile
        
        # 注释掉原有的JAVA_HOME
        # export JAVA_HOME=

        # 如果你的SSH端口不是标准的22，可以修改这个
        export HADOOP_SSH_OPTS="-p 22"

*   配置core-site.xml

    先拷贝一个默认值文件
        
        cp $HOME/local/hadoop/src/hadoop-common-project/hadoop-common/src/main/resources/core-default.xml .
    
    新增，或者修改core-site.xml，完整内容如下：

        <?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
        
          <property>
            <name>hadoop.tmp.dir</name>
            <value>${user.home}/temp/hadoop_temp</value>
          </property>
        
          <property>
            <name>fs.defaultFS</name>
            <value>hdfs://mycluster</value>
          </property>
        
          <property>
            <name>ha.zookeeper.quorum</name>
            <value>hadoop1:50181,hadoop2:50181,hadoop3:50181,hadoop4:50181,hadoop5:50181</value>
          </property>
        
          <property>
            <name>fs.trash.interval</name>
            <value>1440</value>
          </property>
        
          <property>
            <name>fs.trash.checkpoint.interval</name>
            <value>60</value>
          </property>
          
          <property>
            <name>hadoop.security.authentication</name>
            <value>simple</value>
          </property>
        
          <property>
            <name>hadoop.security.authorization</name>
            <value>false</value>
          </property>
        
          <property>
            <name>hadoop.http.staticuser.user</name>
            <value>hadoop</value>
          </property>
        
        </configuration>

*   配置hdfs-site.xml
    
    先拷贝默认配置文件

        cp $HOME/local/hadoop/src/hadoop-hdfs-project/hadoop-hdfs/src/main/resources/hdfs-default.xml .

    在创建或修改hdfs-site.xml，完整内容如下：  
    *注意：其中有需要制定ssh端口的地方，请根据实际设置*

        <?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration xmlns:xi="http://www.w3.org/2001/XInclude">
          
          <property>
            <name>dfs.nameservices</name>
            <value>mycluster</value>
          </property>
        
          <property>
            <name>dfs.ha.namenodes.mycluster</name>
            <value>nn1,nn2</value>
          </property>
        
          <property>
            <name>dfs.namenode.rpc-address.mycluster.nn1</name>
            <value>hadoop1:50900</value>
          </property>
        
          <property>
            <name>dfs.namenode.rpc-address.mycluster.nn2</name>
            <value>hadoop2:50900</value>
          </property>
          
          <property>
            <name>dfs.namenode.http-address.mycluster.nn1</name>
            <value>hadoop1:50070</value>
          </property>
        
          <property>
            <name>dfs.namenode.http-address.mycluster.nn2</name>
            <value>hadoop2:50070</value>
          </property>

          <property>
            <name>dfs.datanode.http.address</name>
            <value>0.0.0.0:50075</value>
          </property>

          <property>
            <name>dfs.namenode.shared.edits.dir</name>
            <value>qjournal://hadoop1:50485;hadoop2:50485;hadoop3:50485;hadoop4:50485;hadoop5:50485/mycluster</value>
          </property>
         
          <!-- journal , QJM -->
          <property>
            <name>dfs.namenode.edits.journal-plugin.qjournal</name>
            <value>org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager</value>
          </property>
          
          <property>
            <name>dfs.journalnode.edits.dir</name>
            <value>${user.home}/name/hadoop_journal/edits</value>
          </property>
        
          <property>
            <name>dfs.journalnode.rpc-address</name>
            <value>0.0.0.0:50485</value>
          </property>
        
          <property>
            <name>dfs.journalnode.http-address</name>
            <value>0.0.0.0:50480</value>
          </property>

          <property>
            <name>dfs.client.failover.proxy.provider.mycluster</name>
            <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
          </property>
        
          <property>
            <name>dfs.ha.fencing.methods</name>
            <!-- 注意这里的端口设置 -->
            <value>sshfence(hadoop:22)</value>
          </property>
        
          <property>
            <name>dfs.ha.fencing.ssh.private-key-files</name>
            <value>${user.home}/.ssh/id_rsa</value>
          </property>
        
          <property>
            <name>dfs.ha.automatic-failover.enabled</name>
            <value>true</value>
          </property>
        
          <property>
            <name>dfs.namenode.name.dir</name>
            <value>file://${user.home}/meta/hadoop/name</value>
          </property>
        
          <property>
            <name>dfs.datanode.data.dir</name>
            <value>file://${user.home}/data/hadoop</value>
          </property>
        
          <property>
            <name>dfs.replication</name>
            <value>2</value>
          </property>
        
          <property>
            <name>dfs.namenode.safemode.threshold-pct</name>
            <value>1.0f</value>
          </property>
          
          <property>
            <name>dfs.umaskmode</name>
            <value>027</value>
          </property>
          
          <property>
            <name>dfs.block.size</name>
            <value>134217728</value>
          </property>
          
          <property>
            <name>dfs.block.access.token.enable</name>
            <value>false</value>
          </property>
        
          <property>
            <name>dfs.datanode.data.dir.perm</name>
            <value>700</value>
          </property>
        
          <property>
            <name>dfs.permissions.superusergroup</name>
            <value>hadoop</value>
          </property>
        
          <property>
            <name>dfs.hosts</name>
            <value>etc/hadoop/dfs.include</value>
          </property>
        
          <property>
            <name>dfs.hosts.exclude</name>
            <value>etc/hadoop/dfs.exclude</value>
          </property>
          
          <property>
            <name>dfs.webhdfs.enabled</name>
            <value>true</value>
          </property>
        
          <property>
            <name>dfs.support.append</name>
            <value>true</value>
          </property>
          
          <property>
            <name>dfs.datanode.max.xcievers</name>
            <value>4096</value>
          </property>
        
          <property>
            <name>dfs.balance.bandwidthPerSec</name>
            <value>20000000</value>
          </property>
        
          <property>
            <name>dfs.namenode.num.extra.edits.retained</name>
            <value>2200</value>
          </property>
        
          <property>
            <name>dfs.datanode.du.reserved</name>
            <!-- ~1G -->
            <value>1024000000</value>
          </property>
        
        </configuration>

*   配置yarn-site.xml

    先拷贝默认文件

        cp $HOME/local/hadoop/src/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-common/src/main/resources/yarn-default.xml .

    创建或者修改yarn-site.xml，完整内容如下：

        <?xml version="1.0"?>
        <configuration xmlns:xi="http://www.w3.org/2001/XInclude">
        
            <property>
                <name>yarn.app.mapreduce.am.staging-dir</name>
                <value>/user</value>
            </property>
        
            <property>
                <name>yarn.resourcemanager.webapp.address</name>
                <value>hadoop1:50088</value>
            </property>

            <property>
                <name>yarn.nodemanager.aux-services</name>
                <value>mapreduce.shuffle</value>
            </property> 
        
            <property>
                <name>yarn.log-aggregation-enable</name>
                <value>true</value>
            </property> 
        
            <property>
                <name>yarn.nodemanager.local-dirs</name>
                <value>${user.home}/yarn/local-dir</value>
            </property>
        
            <property>
                <name>yarn.nodemanager.log-dirs</name>
                <value>${user.home}/yarn/log-dir</value>
            </property>
        
            <property>
                <description>hdfs path</description>
                <name>yarn.nodemanager.remote-app-log-dir</name>
                <value>/var/log</value>
            </property>
        
            <property>
        
            <property>
                <name>yarn.nodemanager.webapp.address</name>
                <value>0.0.0.0:50842</value>
            </property>
        
            <property>
                <name>yarn.nodemanager.vmem-pmem-ratio</name>
                <value>8.1</value>
            </property>
        
            <property>
                <name>yarn.resourcemanager.scheduler.class</name>
                 <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
                <!-- <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>  -->
            </property>
        
            <property>
                <name>yarn.scheduler.minimum-allocation-mb</name>
                <value>128</value>
            </property>
        
            <property>
                <name>yarn.scheduler.maximum-allocation-mb</name>
                <value>4096</value>
            </property>
        
            <property>
                <name>yarn.app.mapreduce.am.resource.mb</name>
                <value>640</value>
            </property>
        
            <property>
                <name>yarn.resourcemanager.nodes.include-path</name>
                <value>yarn.include</value>
            </property>
            
            <property>
                <name>yarn.resourcemanager.nodes.exclude-path</name>
                <value>yarn.exclude</value>
            </property>
        
            <property>
                <name>yarn.nodemanager.resource.memory-mb</name>
                <value>4096</value>
            </property>

        </configuration>

*   配置mapred-site.xml

    先拷贝默认配置文件

        cp $HOME/local/hadoop/src/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/resources/mapred-default.xml .

    创建或修改mapred-site.xml文件，完整内容如下：

        <?xml version="1.0"?>
        <?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
        <configuration>
        
            <property>
                <name>mapreduce.framework.name</name>
                <value>yarn</value>
            </property>

            <property>
                <name>mapreduce.jobhistory.webapp.address</name>
                <value>hadoop1:50888</value>
            </property>

            <property>
                <name>mapreduce.jobhistory.intermediate-done-dir</name>
                <value>${yarn.app.mapreduce.am.staging-dir}/${user.name}/history/intermediate-done-dir</value>
            </property>

            <property>
                <name>mapreduce.jobhistory.done-dir</name>
                <value>${yarn.app.mapreduce.am.staging-dir}/${user.name}/history/done</value>
            </property>

            <property>
                <name>mapred.child.java.opts</name>
                <value>-Xmx512m -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:+CMSClassUnloadingEnabled -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 -Duser.language=zh -Duser.region=CN</value>
            </property>

            <property>
                <name>mapreduce.client.submit.file.replication</name>
                <value>3</value>
            </property>

            <property>
                <name>mapreduce.map.speculative</name>
                <value>false</value>
            </property>

            <property>
                <name>mapreduce.reduce.speculative</name>
                <value>false</value>
            </property>

            <property>
                <name>mapreduce.reduce.shuffle.input.buffer.percent</name>
                <value>0.60</value>
            </property>
        
        </configuration>

#### 初始化

    # 在hadoop1执行
    # 启动journal nodes
    cd ~/local/hadoop
    sbin/hadoop-daemons.sh --hostnames "hadoop1 hadoop2 hadoop3 hadoop4 hadoop5" start journalnode
    
    mkdir -p ~/meta/hadoop
    hadoop namenode -format
    
    # 从hadoop1拷贝元数据到hadoop2，保持元数据一致
    ssh -p 22 hadoop2 "mkdir -p $HOME/meta/hadoop"
    scp -P 22 -r ~/meta/hadoop/name $USER@hadoop2:$HOME/meta/hadoop 

    # 检查确保zookeeper成功启动
    hdfs zkfc -formatZK

#### 启动

    # 在hadoop1执行
    cd ~/local/hadoop
    sbin/start-dfs.sh
    
    # 此时，如果成功，可以通过浏览器访问http://hadoop1:50070来看Hdfs系统

    sbin/start-yarn.sh
    # 此时，如果成功，可以通过浏览器访问http://hadoop1:50088来看Yarn系统


---

### 补充说明

现在CDH4版本正在快速开发中，版本之间的变动稍大，上面示例并不完全适合CDH4.0~4.3的全部版本，但大同小异。