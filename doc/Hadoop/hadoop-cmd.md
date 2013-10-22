# Hadoop Command

Hadoop的命令在如下目录：

    $HADOOP_PREFIX/sbin
    $HADOOP_PREFIX/bin

## hdfs

    用法: hdfs [--config 配置目录] 命令
           命令如下:
      dfs                  运行文件系统命令，例如ls，rm等。（重要，经常使用）
      namenode -format     格式化DFS文件系统。（初装时使用）
      secondarynamenode    运行DFS SNN。（建议不用SNN）
      namenode             运行DFS NN。（不用）
      journalnode          运行DFS journalnode。（不用）
      zkfc                 运行故障转移控制服务。（不用）
      datanode             运行一个DFS datanode。（不用）
      dfsadmin             运行一个DFS admin client。（使用）
      haadmin              运行一个DFS HA admin client。（使用）
      fsck                 运行DFS文件系统检测工具。（文件系统故障时使用，重要）
      balancer             运行一个机器均衡工具。（通常不这样用）
      jmxget               从NN或者DN获取JMX输出值。（高级功能，用于调试）
      oiv                  应用离线查看器查看一个fsimage。（高级功能，用于调试）
      oev                  应用离线查看器查看一个edits文件。（高级功能，用于调试）
      fetchdt              获取一个代理令牌从NN。
      getconf              获取配置值从配置文件。（编写shell脚本时可能用到）
      groups               获取用户属于哪个group组。

### hdfs dfs

这些命令和我们使用的linux命令非常类似。

    用法: hadoop fs [generic options]
    	[-cat [-ignoreCrc] <src> ...]
    	[-chgrp [-R] GROUP PATH...]
    	[-chmod [-R] <MODE[,MODE]... | OCTALMODE> PATH...]
    	[-chown [-R] [OWNER][:[GROUP]] PATH...]
    	[-copyFromLocal <localsrc> ... <dst>]
    	[-copyToLocal [-ignoreCrc] [-crc] <src> ... <localdst>]
    	[-count [-q] <path> ...]
    	[-cp <src> ... <dst>]
    	[-df [-h] [<path> ...]]
    	[-du [-s] [-h] <path> ...]
    	[-expunge]
    	[-get [-ignoreCrc] [-crc] <src> ... <localdst>]
    	[-getmerge [-nl] <src> <localdst>]
    	[-help [cmd ...]]
    	[-ls [-d] [-h] [-R] [<path> ...]]
    	[-mkdir [-p] <path> ...]
    	[-moveFromLocal <localsrc> ... <dst>]
    	[-moveToLocal <src> <localdst>]
    	[-mv <src> ... <dst>]
    	[-put <localsrc> ... <dst>]
    	[-rm [-f] [-r|-R] [-skipTrash] <src> ...]
    	[-rmdir [--ignore-fail-on-non-empty] <dir> ...]
    	[-setrep [-R] [-w] <rep> <path/file> ...]
    	[-stat [format] <path> ...]
    	[-tail [-f] <file>]
    	[-test -[ezd] <path>]
    	[-text [-ignoreCrc] <src> ...]
    	[-touchz <path> ...]
    	[-usage [cmd ...]]

不同的命令解释如下：

    -help           可以看到每个命令的详细介绍
    -copyFromLocal  把文件从本地上传到HDFS
    -put            同上
    -expunge        用于垃圾回收，把运行命令的这个用户的HDFS上的垃圾箱垃圾回收。
    -copyToLocal    拷贝HDFS文件到本地
    -get            同上
    -count          统计指定路径下的目录、文件数。-q是显示quota（配额）
    -getmerge       把指定路径下文件合并并排序成一个文件，放到本地目录。
                        -nl是在每个文件后添加换行符。
    -setrep         设置副本数。
    -stat           以指定格式打印文档统计信息。
    -test           测试一个文件是否存在(e)，是否文档(d)，是否为空(z)
    -touchz         类似touch，主要用于创建空文件，如果文件已经存在，会报错。


### hdfs dfsadmin

用于管理HDFS

    用法: hdfs dfsadmin
        [-report]
            报告集群信息
    
        [-safemode enter | leave | get | wait]
            安全模式进入、离开、获取状态。
    
        [-saveNamespace]
            要求把内存中数据刷到fsimage中，editlog置空。
    
        [-rollEdits]
            滚动editlog，生成一个新的editlog。
    
        [-restoreFailedStorage true|false|check]
            尝试恢复失败的存储器。（比如磁盘坏了，又修好了，可以选择用这个）
    
        [-refreshNodes]
            刷新节点列表，主要用于退役和恢复退役节点。
    
        [-finalizeUpgrade]
            终止升级过程。
    
        [-upgradeProgress status | details | force]
            显示升级状态。
    
        [-metasave filename]
            把元数据保存到指定文件。就是导出完整fsimage文件。
    
        [-refreshServiceAcl]        
        [-refreshUserToGroupsMappings]
        [-refreshSuperUserGroupsConfiguration]
    
        [-printTopology]
            打印集群机器机架部署。
    
        [-refreshNamenodes datanodehost:port]
            要求指定DN刷新NN（2.0版本中，DN可以对应多组NN），如果配置新的NN或者关闭它，需要此命令。
    
        [-deleteBlockPool datanode-host:port blockpoolId [force]]
    
        [-setQuota <quota> <dirname>...<dirname>]
            设置Quota（文件数配额）
    
        [-clrQuota <dirname>...<dirname>]
            清理设置的Quota
    
        [-setSpaceQuota <quota> <dirname>...<dirname>]
            设置Quota（磁盘配额）
    
        [-clrSpaceQuota <dirname>...<dirname>]
            清理设置的Quota
    
        [-setBalancerBandwidth <bandwidth in bytes per second>]
            平衡器可以使用的带宽
    
        [-fetchImage <local directory>]
            下载最新使用的fsimage并保存到指定目录
    
        [-help [cmd]]

###  hdfs haadmin

HA功能管理

    用法: hdfs haadmin [-ns <nameserviceId>]
        [-transitionToActive <serviceId>]
            把指定机器转为Active状态。（我们开启了自动转移，基本不用这个）

        [-transitionToStandby <serviceId>]
            把指定机器转为Standby状态。（同上）

        [-failover [--forcefence] [--forceactive] <serviceId> <serviceId>]
            同上

        [-getServiceState <serviceId>]
            获取状态（Active|Standby)
    
        [-checkHealth <serviceId>]
        [-help <command>]

## hdfs fsck

数据故障后，做数据检查

    用法: DFSck <path> [-list-corruptfileblocks | [-move | -delete | -openforwrite] [-files [-blocks [-locations | -racks]]]]
    	<path>	从这个路径开始检查，递归检查下属文档
    	-move	把故障文件移到/lost+found目录。在/user/$USER目录下
    	-delete	删除故障文件
    	-files	打印正在检查的文件
    	-openforwrite              打印正在写的文件
    	-list-corruptfileblocks    打印出丢失了的数据块列表和其对应的文件
    	-blocks                    打印块报告
    	-locations                 打印每个块的位置
    	-racks                     打印机架
    	
    默认fsck忽略正在写的文件，使用-openforwrite可以打印。
    在检查报告的最后会打印CORRUPT或者HEALTHY。

常用方式

    hdfs fsck -list-corruptfileblocks /
    hdfs fsck -files -blocks -locations /path/to/file
    hdfs fsck -delete /path/to/file


## yarn

    用法: yarn [--config confdir] COMMAND
    COMMAND包括:
      resourcemanager      运行RM（不用）
      nodemanager          运行NM（不用）
      rmadmin              管理工具
      version              打印版本
      jar <jar>            运行一个Jar文件。（我们运行Job就是通过这个）
      application          管理App，打印其状态，Kill它之类的（重要）
      node                 打印节点报告
      logs                 获取Container日志
      classpath            打印class path
      daemonlog            获取/设置服务程序日志的level
     or
      CLASSNAME            运行指定类CLASSNAME

### yarn rmadmin
    
    用法：yarn rmadmin
       [-refreshQueues]
            刷新作业队列，一般在你更改了队列配置后，运行此命令促使队列生效

       [-refreshNodes]
            刷新节点，一般在你退役或者回复退役节点时，运行此命令

       [-refreshUserToGroupsMappings]
       [-refreshSuperUserGroupsConfiguration]
       [-refreshAdminAcls]
       [-refreshServiceAcl]
       [-getGroups [username]]
       [-help [cmd]]

---

yarn中，可能最常用的就是application命令了：
    
    yarn application -kill applications_1370098878799_9879

---

补充：上面所有的命令，都支持**通用选项**：

    -conf <configuration file>     指定配置文件
    -D <property=value>            指定给定配置值
    -fs <local|namenode:port>      指定NN
    -jt <local|jobtracker:port>    指定JT
    -files <comma separated list of files>    文件列表，会拷贝到集群
    -libjars <comma separated list of jars>   指定java库，会加入classpath
    -archives <comma separated list of archives>    指定压缩文档，类似文件，可以支持解压

例如，上传文件时，想要使用指定的块大小，可以：

    hdfs -D dfs.block.size=$[4*1024*1024] dfs -put "localfilepath" /user/user/test

---

## 其它

其它命令是更高级的命令，是更常用的集群管理工具

*   **start-dfs.sh/stop-dfs.sh**

    启动/关闭文件系统，包括：NM，DN，ZKFC，JournalNode

*   **mr-jobhistory-daemon.sh**
    
    启动/关闭historyserver服务
    用法：

        mr-jobhistory-daemon.sh (start|stop) historyserver

*   **start-balancer.sh/stop-balancer.sh**

    启动负载均衡，后台程序。可以使用参数-threshold来指定衡量均衡的阀值
    例如：

        start-balancer.sh -threshold 5
        # 此值表示最多和最小容量与平均容量之差应小于5%，注意：最多核最小之差此时是10%。
        # 默认值是10

*   **hadoop-daemon.sh**

    以daemon的方式启动指定程序。

        hadoop-daemon.sh (start|stop) command
        其中，command可以是：
        namenode，datanode，journalnode，zkfc，balancer等
        以hdfs命令能run的，都可以写在这里

*   **hadoop-daemons.sh**

    功能同上，不过，它可以在多台机器上运行。这样你就不用一台一台的运行命令了。默认在slave节点上运行。注意，本机到其它机器需要开启免密码登陆。
    用法：
    
        hadoop-daemons.sh [--hosts hostlistfile] [start|stop] command args
        #注意，上面可以指定hosts，如果不指定，默认是集群全部机器

*   **distribute-exclude.sh**

    分发exclude文件到2台NN，仅hdfs的，不包括yarn（或者你配置两者使用相同的文件）

*   **refresh-namenodes.sh**

    刷新2台NN。
    这个命令和上个命令是连起来使用的。
    先调用分发，再调用刷新。

*   **start-yarn.sh/stop-yarn.sh**

    启动/停止YARN

*   **yarn-daemon.sh**

    用法：

        yarn-daemon.sh (start|stop) command

    后台启动/关闭YARN服务，可以启动/关闭的服务包括：resourcemanager, nodemanager
    以yarn程序可以运行的程序都可以启动。

*   **yarn-daemons.sh**

    同上，在多台机器上执行。默认在slave节点上运行。注意，本机到其它机器需要开启免密码登陆。
    示例：

        yarn-daemons.sh start nodemanager