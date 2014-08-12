# Hadoop安装

Hadoop经典安装。

    此安装是Tar包单用户安装，而非官方的yum系统安装。
    此安装方式在数台机器上安装，方便学习和研究。
    
    安装版本：Hadoop CDH4.*
    安装特性：安装Yarn，启用HA，启用QJM。

假设在5台机器上安装，安装后角色如下：

    hadoop1 NN，DN，RM，NM，ZK，JN，ZKFC
    hadoop2 NN，DN，NM，ZK，JN，ZKFC
    hadoop3 DN，NM，ZK，JN
    hadoop4 DN，NM，ZK，JN
    hadoop5 DN，NM，ZK，JN

    注：NN=NameNode，DN=DataNode，ZK=ZookeeperNode，JN=JournalNode，
        ZKFC=Zookeeper Failover Controller，RM=Resouse Manager，NM=NodeManager

安装之后的目录如下：
    
    tree -L 2
    .
    ├── apps                                         -- 程序目录
    │   ├── hadoop-2.0.0-cdh4.2.1
    │   ├── jdk1.7.0_25
    │   └── zookeeper-3.4.5-cdh4.2.1
    ├── data                                         -- 数据目录                 
    │   └── hadoop
    ├── download
    │   ├── hadoop-2.0.0-cdh4.2.1.tar.gz
    │   ├── jdk-7u25-linux-x64.gz
    │   └── zookeeper-3.4.5-cdh4.2.1.tar.gz
    ├── local                                        -- 程序目录
    │   ├── hadoop -> /home/zhaigy1/apps/hadoop-2.0.0-cdh4.2.1
    │   ├── jdk -> /home/zhaigy1/apps/jdk1.7.0_25
    │   └── zookeeper -> /home/zhaigy1/apps/zookeeper-3.4.5-cdh4.2.1
    ├── meta                                         -- 元数据目录
    │   ├── hadoop
    │   └── journal
    ├── temp                                            
    │   └── hadoop_temp
    └── yarn                                         -- yarn本地目录和日志目录
        ├── local-dir
        └── log-dir

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

>Tar包安装，相关Tar包在[http://archive.cloudera.com/cdh4/cdh/4]()页面查找下载，注意是tar.gz后缀。

>注意：如未特殊说明，操作都是在主机**hadoop1**进行


### 准备

假设你在5台机器上安装，安装用户为hadoop。
    
*   **干净的用户**
    
    用户最好是新建的，以减少环境影响。多台机器上应该建立相同的用户名并且有相同的密码（纯粹为了方便管理）。
    
*   **主机域名解析文件（/etc/hosts）**
    
    必须的一步，假设我们的域名定义如下（修改需要root权限）：  
  
        192.168.10.1    hadoop1
        192.168.10.2    hadoop2
        192.168.10.3    hadoop3
        192.168.10.4    hadoop4
        192.168.10.5    hadoop5
    
    *注：需要在全部机器上统一修改*

*   **SSH免密码登陆**
    
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
        scp -P 22 -r ~/.ssh hadoop2:$HOME
        ssh -p 22 hadoop2 'rm -f ~/.ssh/known_hosts'
        # 常用SSH配置会检查Host，配置后，需要手动登陆一下各机器验证一下。
    
    或者使用标准的方式，拷贝到其它机器
    
        sh-copy-id -i ~/.ssh/id_rsa.pub "-p 22 user@server"

    *注：此方法让所有机器共用一个密钥文件*  
    **注意：有2个NN，在我们的示例中是hadoop1和hadoop2**

*  时间同步

    在root用户的定时任务中增加：

        */30 * * * * /usr/sbin/ntpdate cn.pool.ntp.org>/dev/null \
            2>&1;/sbin/hwclock -w >/dev/null 2>&1
        注：每30分钟更新一下时间，并修改硬件时钟
        # 时间服务器可以换成其它的

*   必要的目录

        # 在hadoop1机器上执行
        mkdir -p ~/download ~/local ~/apps
        # 在其它机器上执行
        ssh -p 22 hadoop2 "mkdir -p ~/local ~/apps"

### 安装JDK

常规安装方式，下载，解压，安装，配置。

下载需要选择适合自己系统的版本。下载[JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html)，选择tar包版本，例如：[jdk-7u25-linux-x64.tar.gz](http://download.oracle.com/otn-pub/java/jdk/7u25-b15/jdk-7u25-linux-x64.tar.gz)

    # 在hadoop1上
    # 下载
    cd ~/download
    # 由于其页面需要同意一个协议，如果你下载不了，可以先从浏览器下载
    wget "http://download.oracle.com/otn-pub/java/jdk/7u25-b15/\
        jdk-7u25-linux-x64.tar.gz" -O jdk-7u25-linux-x64.tar.gz
    # 解压
    tar -xzvf jdk*.gz
    # 安装
    rm -f jdk1.7.0_25/src.zip
    mv jdk1.7.0_25 ~/apps/
    ln -sf $HOME/apps/jdk* $HOME/local/jdk
    # 配置
    vi ~/.bash_profile  # 增加如下内容
        # Java environment
        export JAVA_HOME=$HOME/local/jdk
        export CLASSPATH=$JAVA_HOME/lib
        export PATH=$JAVA_HOME/bin:$PATH
    # 重载环境
    source ~/.bash_profile
    # 检查
    java -version # 应该显示最新安装的版本

    # 在其它机器上做相同的操作，或者把安装好的软件拷贝过去，注意软链不能直接拷贝。
    # 示例操作如下：
    cd ~/apps
    ssh -p 22 hadoop2 "mkdir -p ~/apps"
    scp -P 22 -r jdk1.7.0_25 hadoop2:~/apps
    ssh -p 22 hadoop2 "ln -sf \$HOME/apps/jdk* \$HOME/local/jdk"
    scp -P 22 ~/.bash_profile hadoop2:~/
    ssh -p 22 hadoop2 "source ~/.bash_profile;java -version"

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
    rm -rf zookeeper-3.4.5-cdh4.2.1/docs
    mv zookeeper-3.4.5-cdh4.2.1 ~/apps
    ln -sf $HOME/apps/zookeeper* $HOME/local/zookeeper
    # 配置
    cd ~/local/zookeeper/conf
    vi zoo.cfg

        # 编辑文件内容如下，注意配置行后别多出空格
        tickTime=2000
        initLimit=10
        syncLimit=2
        # 可选客户端连接端口
        clientPort=50181
        maxClientCnxns=200
        # 可选数据存储目录
        dataDir=/home/hadoop/local/zookeeper/data
        # 可选数据日志（类似binlog）存储目录
        dataLogDir=/home/hadoop/local/zookeeper/datalog
        # 机器编号和域名:leader端口:选举端口
        server.1=hadoop1:50288:50388
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
    # 示例操作如下：
    cd ~/apps
    ssh -p 22 hadoop2 "mkdir -p ~/apps"
    scp -P 22 -r zookeeper-3.4.5-cdh4.2.1 hadoop2:~/apps 
    ssh -p 22 hadoop2 "ln -sf \$HOME/apps/zookeeper* \$HOME/local/zookeeper"
    ssh -p 22 hadoop2 "cd ~/local/zookeeper;sh bin/zkServer-initialize.sh \
        --myid=2 --force"
    scp -P 22 ~/.bash_profile hadoop2:~/

启动

    # 在每台机器上执行
    cd ~/local/zookeeper
    bin/zkServer.sh start       #启动
    bin/zkServer.sh status      #查看状态
    # 如果启动失败，请查看当前目录下的zookeeper.out日志文件
    
    # 在其它机器启动
    ssh -p 22 hadoop2 "cd ~/local/zookeeper;bin/zkServer.sh start"
    ssh -p 22 hadoop2 "cd ~/local/zookeeper;bin/zkServer.sh status"

    注意：最初单台启动时，查看状态可能并不正常，要等全部启动后，再查看状态。

### 安装Hadoop

    以安装4.2.1版本系列为例。

#### 安装

    在hadoop1上
    # 下载
    cd ~/download
    wget http://archive.cloudera.com/cdh4/cdh/4/hadoop-2.0.0-cdh4.2.1.tar.gz
    # 解压
    tar -xzvf hadoop-2.0.0-cdh4.2.1.tar.gz
    # 安装
    mv hadoop-2.0.0-cdh4.2.1 ~/apps
    ln -sf $HOME/apps/hadoop-2.0.0-cdh4.2.1 $HOME/local/hadoop

同步到其它机器

    cd ~/apps
    for h in hadoop2 hadoop3 hadoop4 hadoop5; do
      scp -P 22 -r ./hadoop-2.0.0-cdh4.2.1 $h:~/apps;
      ssh -p 22 $h 'ln -sf $HOME/apps/hadoop-2.0.0-cdh4.2.1 $HOME/local/hadoop';
    done

#### 配置

*   环境变量
        
        # 在hadoop1上
        vi ~/.bash_profile
            # 编辑文件，增加如下内容
            export HADOOP_HOME=$HOME/local/hadoop
            export HADOOP_PREFIX=$HADOOP_HOME
            export HADOOP_BIN=$HADOOP_HOME/bin
            export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
            export PATH=$HADOOP_BIN:$PATH
        
     同步到其它机器
     
        for h in hadoop2 hadoop3 hadoop4 hadoop5; do
          scp -P 22 ~/.bash_profile $h:~/;
        done

在`~/local/hadoop/etc/hadoop`目录。

    cd ~/local/hadoop/etc/hadoop

*   配置hadoop-env.sh  
        
        # 在最前位置增加
        shopt -s expand_aliases
        . $HOME/.bash_profile
        
        # 注释掉原有的JAVA_HOME
        # export JAVA_HOME=

        # 如果你的SSH端口不是标准的22，可以增加或修改这个
        export HADOOP_SSH_OPTS="-p 22 -o StrictHostKeyChecking=false"

*   配置core-site.xml

    先拷贝一个默认值文件
        
        cp $HOME/local/hadoop/src/hadoop-common-project/hadoop-common/src/\
            main/resources/core-default.xml .
    
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
            <value>hadoop1:50181,hadoop2:50181,hadoop3:50181,\
                hadoop4:50181,hadoop5:50181</value>
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

    创建对应的目录

        mkdir -p ~/temp/hadoop_temp

*   配置hdfs-site.xml
    
    先拷贝默认配置文件

        cp $HOME/local/hadoop/src/hadoop-hdfs-project/hadoop-hdfs/src\
            /main/resources/hdfs-default.xml .

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
            <value>qjournal://hadoop1:50485;hadoop2:50485;hadoop3:50485;\
            hadoop4:50485;hadoop5:50485/mycluster</value>
          </property>
         
          <!-- journal , QJM -->
          <property>
            <name>dfs.namenode.edits.journal-plugin.qjournal</name>
            <value>org.apache.hadoop.hdfs.qjournal.client.QuorumJournalManager</value>
          </property>
          
          <property>
            <name>dfs.journalnode.edits.dir</name>
            <value>${user.home}/meta/journal/edits</value>
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
            <value>org.apache.hadoop.hdfs.server.namenode.ha.\
                ConfiguredFailoverProxyProvider</value>
          </property>
        
          <property>
            <name>dfs.ha.fencing.methods</name>
            <!-- 注意这里的端口设置 -->
            <value>sshfence(${user.name}:22)</value>
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
            <value>${user.home}/local/hadoop/etc/hadoop/dfs.include</value>
          </property>
        
          <property>
            <name>dfs.hosts.exclude</name>
            <value>${user.home}/local/hadoop/etc/hadoop/dfs.exclude</value>
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

    创建对应的目录

        mkdir -p ~/meta/journal/edits ~/meta/hadoop/name ~/data/hadoop
        touch dfs.include dfs.exclude

*   配置yarn-site.xml

    先拷贝默认文件

        cp $HOME/local/hadoop/src/hadoop-yarn-project/hadoop-yarn/\
            hadoop-yarn-common/src/main/resources/yarn-default.xml .

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
                <value>/user</value>
            </property>
            
            <property>
                <description>hdfs path</description>
                <name>yarn.nodemanager.remote-app-log-dir-suffix</name>
                <value>logs</value>
            </property>

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
                <value>org.apache.hadoop.yarn.server.resourcemanager.\
                    scheduler.capacity.CapacityScheduler</value>
                <!-- <value>org.apache.hadoop.yarn.server.\
                    resourcemanager.scheduler.fair.FairScheduler</value>  -->
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
                <name>yarn.resourcemanager.am.max-retries</name>
                <value>4</value>
            </property>

            <property>
                <name>yarn.app.mapreduce.am.resource.mb</name>
                <value>640</value>
            </property>
        
            <property>
                <name>yarn.resourcemanager.nodes.include-path</name>
                <value>${user.home}/local/hadoop/etc/hadoop/yarn.include</value>
            </property>
            
            <property>
                <name>yarn.resourcemanager.nodes.exclude-path</name>
                <value>${user.home}/local/hadoop/etc/hadoop/yarn.exclude</value>
            </property>
        
            <property>
                <name>yarn.nodemanager.resource.memory-mb</name>
                <value>4096</value>
            </property>

            <property>
                <name>yarn.log-aggregation.retain-seconds</name>
                <value>604800</value>
            </property>

        </configuration>

    创建对应的目录
    
        mkdir -p ~/yarn/local-dir ~/yarn/log-dir
        touch yarn.include yarn.exclude

*   配置mapred-site.xml

    先拷贝默认配置文件

        cp $HOME/local/hadoop/src/hadoop-mapreduce-project/\
            hadoop-mapreduce-client/hadoop-mapreduce-client-core/\
            src/main/resources/mapred-default.xml .

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
                <name>mapreduce.map.java.opts</name>
                <value>-Xmx512m -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC \
                    -XX:+UseCMSCompactAtFullCollection \
                    -XX:+CMSClassUnloadingEnabled \
                    -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 \
                    -Duser.language=zh -Duser.region=CN</value>
            </property>

            <property>
                <name>mapreduce.reduce.java.opts</name>
                <value>-Xmx512m -XX:+DisableExplicitGC -XX:+UseConcMarkSweepGC \
                    -XX:+UseCMSCompactAtFullCollection \
                    -XX:+CMSClassUnloadingEnabled \
                    -Dclient.encoding.override=UTF-8 -Dfile.encoding=UTF-8 \
                    -Duser.language=zh -Duser.region=CN</value>
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
                <name>mapreduce.job.reduce.slowstart.completedmaps</name>
                <value>0.85</value>
            </property>

            <property>
                <name>mapreduce.reduce.shuffle.input.buffer.percent</name>
                <value>0.60</value>
            </property>
        
            <property>
                <name>yarn.app.mapreduce.am.resource.mb</name>
                <value>640</value>
            </property>
            
            <property>
                <name>yarn.app.mapreduce.am.command-opts</name>
                <value>-Xmx500m</value>
            </property>

            <property>
                <name>mapreduce.task.io.sort.factor</name>
                <value>20</value>
            </property>

            <property>
                <name>mapreduce.task.io.sort.mb</name>
                <value>200</value>
            </property>
            
            <property>
                <name>mapreduce.map.memory.mb</name>
                <value>640</value>
            </property>
            
            <property>
                <name>mapreduce.reduce.memory.mb</name>
                <value>640</value>
            </property>
           
            <property>
                <name>mapreduce.job.ubertask.enable</name>
                <value>false</value>
            </property>
            
            <property>
                <name>mapreduce.jobhistory.cleaner.enable</name>
                <value>true</value>
            </property>

        </configuration>

*   编辑slaves文件

        vi slaves
            haoop1
            haoop2
            haoop3
            haoop4
            haoop5    

*   同步配置到其它机器

        cd ~/local/hadoop/etc
        for h in hadoop2 hadoop3 hadoop4 hadoop5; do
            ssh -p 22 $h "rm -rf ~/local/hadoop/etc/hadoop"; 
            scp -P 22 -r ./hadoop $h:~/local/hadoop/etc/; 
            ssh -p 22 $h "mkdir -p ~/temp/hadoop_temp ~/meta/journal/edits \
                ~/meta/hadoop/name ~/data/hadoop ~/yarn/local-dir \
                ~/yarn/log-dir";
        done

#### 初始化

    # 在hadoop1执行
    # 启动journal nodes
    cd ~/local/hadoop
    sbin/hadoop-daemons.sh --hostnames "hadoop1 hadoop2 hadoop3 hadoop4 \
        hadoop5" start journalnode
    
    hadoop namenode -format
    
    # 从hadoop1拷贝元数据到hadoop2，保持元数据一致
    scp -P 22 -r ~/meta/hadoop/name hadoop2:$HOME/meta/hadoop
    或者，在启动了hadoop1上的NameNode的情况下：
    hdfs namenode -bootstrap

    # 检查确保zookeeper成功启动
    hdfs zkfc -formatZK

#### 启动

    # 在hadoop1执行
    cd ~/local/hadoop
    sbin/start-dfs.sh
    
    # 此时，如果成功，可以通过浏览器访问http://hadoop1:50070来看HDFS系统
    # http://hadoop2:50070可以看到备机

    # 在HDFS上创建YARN必要的目录
    hdfs dfs -mkdir /tmp
    hdfs dfs -chmod -R 1777 /tmp
    hdfs dfs -mkdir /user
    hdfs dfs -chmod -R 1777 /user
    hdfs dfs -mkdir /var/jobhistory/intermediate-done-dir
    hdfs dfs -mkdir /var/jobhistory/done
    hdfs dfs -chmod -R 1777 /var

    sbin/start-yarn.sh
    # 此时，如果成功，可以通过浏览器访问http://hadoop1:50088来看Yarn系统

## 测试

    准备一些文本数据，写入文件 test.txt

    hdfs dfs -mkdir /user/$USER/test/test-mr
    hdfs dfs -put text.txt /user/$USER/test/test-mr/

    hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.0-cdh4.2.1.jar wordcount /user/$USER/test/test-mr/ /user/$USER/test/test-mr-out    

# 为其它用户创建软连接和环境

上面安装的环境仅对hadoop用户有效，为了让其它用户也使用到，可以：

1. 为其它用户也安装hadoop，过程是上述过程的简略版，
2. 或者，用创建软链共用上述安装的Hadoop

        chmod a+x /home/hadoop
        
        ln -s -t /usr/local/bin /home/hadoop/local/hadoop/bin/*
        ln -s -t /usr/local/libexec /home/hadoop/local/hadoop/libexec/*
        ln -s -t /usr/local/etc /home/hadoop/local/hadoop/etc/hadoop
        
        vi .bash_profeil
            export JAVA_HOME=/home/hadoop/local/java
            export HADOOP_HOME=/home/hadoop/local/hadoop
            export HADOOP_PREFIX=$HADOOP_HOME

			
## 通过WEB方式访问Hdfs

	curl -X DELETE "http://hadoop1:50070/webhdfs/v1/mnt/hdfs/ssp/user/2?op=DELETE&user.name=hadoop&recursive=true"
	
### 补充说明

现在CDH4版本正在快速开发中，版本之间的变动稍大，上面示例并不完全适合CDH4.0~4.3的全部版本，但大同小异。
