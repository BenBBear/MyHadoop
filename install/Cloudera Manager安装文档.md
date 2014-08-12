# 安装文档 #

### 1. 环境准备 ###

机器：   5台

系统：   centos6.3
   
- 在每台主机上配置/etc/hosts为：

        hadoop1: 192.168.30.101
        hadoop2: 192.168.30.102
        hadoop3: 192.168.30.103
        hadoop4: 192.168.30.104
        hadoop5: 192.168.30.105

    > 注意主机名不能配置错误

- 关闭selinux
    
        setenforce 0
        #或者
        /etc/selinux/config：SELINUX=disabled    
- 关闭防火墙
    
        /etc/init.d/iptables stop

- 安装需要root用户登录或作为其他具有无需密码的伪权限的用户登录，以成为根root用户

- 如果使用root用户安装，确保root用户可以SSH登录

- 各机器时间需要进行同步

- 确保yum可以使用，可以通过yum安装软件

- 系统盘剩余空间不要少于8G

- 各机器能联外网

### 2. 安装过程 ###

#### 2.1 安装Cloudera Manager Server

1. 修改myhadoop_install.sh安装脚本

    修改sshport和root_password。

2. 修改hosts_name.txt
    
    在hosts_name.txt文件中写上所有需要安装的hostname，一个机名一行，不要留有空行。

3. 运行myhadoop_install.sh

    选择接受Lincese安装即可，安装完成后Cloudera Manager已启动。

5. 使用浏览器登录Cloudera Manager
    
    > 使用IE9以上、Firefox、Chrome或Opera打开Cloudera Manager。 地址：`http://92.168.30.101:7180`，登录用户名与密码都为`admin`。

#### 2.2 集群安装

- 选择标准版本，继续下一步。

- 为CDH集群安装指定主机
    
    以逗号分割输入所有加入集群的主机`IP`及`SSH`端口。搜索主机确认后继续下一步。

- 选择存储库
        
    默认将使用最新版本的CDH存储库地址为：`http://archive.cloudera.com/cdh4/parcels/latest/ `，可以使用其它版本的CDH。如使用CDH4.2.1，则填入存储库地址为：`http://archive.cloudera.com/cdh4/parcels/4.2.1/ `
- 提供SSH登录凭据

    输入root密码及ssh端口，继续下一步。
- 自动下载安装

    下载安装完后，继续下一步。
    > 因为是从外国网站下载安装会比较慢，有时会因为网络安装失败，只需点击重试失败的主机重新安装即可。

- 选择要在集群中安装的服务

    选择需要安装的CDH4服务（安装完成后，也可以断续增加安装的CDH4服务，也可以对服务进行修改），继续下一步。

- 数据库设置
    
    可以选择使用嵌入式数据库（Postgresql）这种方式最简单，也可以选择使用Mysql或Oracle, 选择外部数据库时，需要把对应的数据库JDBC驱动程序放入Cloudera Manager的lib包下，地址为：`/usr/share/cmf/lib/`，JDBC驱动程序放好后，填入数据库对应的IP及端口（对于Hive数据库需要先把数据库创建好，其它几个监控数据库使用的数据库会自行创建）测试连接成功后，继续下一步。或
    选择使用嵌入式数据库，测试连接成功后，继续下一步。

    > 在里配置的数据库是Hive及Cloudera Manager监控使用的数据库，对于Cloudera Manager自身使用的数据库SCM会使用嵌入式数据库(Postgresql)主要用于存放配置信息。
    > 如果Hive使用外部数据库如Mysql需要把Mysql JDBC驱动程序放入Hive的lib下，地址为：`/opt/cloudera/parcels/CDH/lib/hive/lib/`

- 审核配置更改

    更改你需要改变的配置内容，主要是数据存储目录及各服务角色对应的机器，确认各服务角色的分配是否符合目标要求，继续下一步。

    > 在CDH4.2.1中Zoonkeeper默认设置为不自动创建Zoonkeeper目录，在启动集群服务时需要手动创建（/var/lib/zookeeper）并更改它的所属为`zookeeper:zookeeper`或到管理界面把“启用数据目录的自动创建”设置为ture。

- 启动集群服务

    Cloudera Manager会自动启动配置的服务(服务启动前的预处理会自动完成,并把客户端配置部署好)。继续下一步完成安装。

- Cloudera Manager安装成功

#### 2.3 修改安装目录

> Cloudera Manager在安装时使用了默认安装路径（安装在系统目录中），安装过程中不能修改，对于系统盘一般较小，需要在安装完成后把安装程序及数据目录移动到其它硬盘目录，再做软链接指向原来的目录。

- 在管理界面停止CDH集群服务

- 在管理界面停止Cloudera Manager监控服务

- 停止Cloudera Management服务           
    
        /etc/init.d/cloudera-scm-server stop

- 停止Cloudera Management的内嵌数据库       

        /etc/init.d/cloudera-scm-server-db stop

- 在每台机器上停止Cloudera Manager agent        

        /etc/init.d/cloudera-scm-agent stop

- 移动Cloudera Management的内嵌数据库目录（这里移到/home/hadoop目录下）

        mv /var/lib/cloudera-scm-server-db/ /home/hadoop/

- 对Cloudera Management的内嵌数据库目录做软链接

        ln -s /home/hadoop/cloudera-scm-server-db/ /var/lib/

- 在所有机器上移动CDH安装程序（这里移到/home/hadoop目录下）

        mv /opt/cloudera/ /home/hadoop/

- 在所有机器上对CDH安装目录做软链接

        ln -s /home/hadoop/cloudera/ /opt/

- 启动Cloudera Management 内嵌数据库

        /etc/init.d/cloudera-scm-server-db start

- 启动Cloudera Management服务

        /etc/init.d/etc/init.d/cloudera-scm-server start

- 在所有机器上启动Cloudera Manager agent

        /etc/init.d/cloudera-scm-agent start

- 在管理界面启动Cloudera Manager监控服务

- 在管理界面启动CDH集群服务

#### 2.4 修改CDH服务配置

CDH的配置Cloudera Manager会设置默认值，大部分都可使用默认的，一般只需要修改各服务的端口地址、MapReduce调度器配置、JVM参数及数据目录、日志存放目录（更改目录后需要确保CDH服务对目录有权限写）。

### 3. 杂项 ###

- 添加服务 

    从CM界面上添加服务，需要注意的就是对于创建目录这些操作需要用户自己创建，而不是在添加服务时自行创建。
- 服务配置文件目录

    服务运行的配置文件是服务启动时Server从数据库中取出配置内容动态产生配置文件分发给Agnet使用。存放于`/var/run/cloudera-scm-agent/process/`目录下。

- 可以指定自己的parcels

	http://archive.cloudera.com/cdh4/parcels/latest/
	http://archive.cloudera.com/impala/parcels/latest/
	http://beta.cloudera.com/search/parcels/latest/
	http://archive.cloudera.com/gplextras/parcels/latest
	http://10.1.74.44:60001/cdh4/parcels/latest
	http://10.1.74.44:60001/cdh4/parcels/4.2.1
	http://10.1.74.44:60001/cm4/redhat/5/x86_64/cm/4.6.3/
	http://10.1.74.44:60001/cm4/redhat/5/x86_64/cm/4.6.3/
	http://10.1.74.44:60001/cm4/redhat/6/x86_64/cm/4.6.3
	http://10.1.74.44:60001/redhat/cdh/RPM-GPG-KEY-cloudera
	
	
	用nginx做静态代理， 配置如下
	
	server {
		listen       60001;
		server_name  _;
		#resolver 8.8.8.8;
		#charset koi8-r;

		#access_log  logs/host.access.log  main;
		# http://hadoop1:60001/archive.cloudera.com/cdh4/parcdls/latest
		root /usr/local/nginx/proxy_temp_dir;
		
		location / {
			#proxy_cache cache1;
			proxy_store on;
			proxy_temp_path  /usr/local/nginx/proxy_temp_dir;
			proxy_set_header Host "archive.cloudera.com";
			proxy_set_header X-Real-IP $remote_addr;
			proxy_set_header X-Forwarded-For $remote_addr;
			if ( $uri ~ /$ ) {
				proxy_pass http://archive.cloudera.com;
			}
			if ( !-e $request_filename ) {
				proxy_pass http://archive.cloudera.com;
			} 
		}
	}