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

- 关闭selinux
    
        setenforce 0
        #或者
        /etc/selinux/config：SELINUX=disabled    
- 关闭防火墙
    
        /etc/init.d/iptables stop
- 安装需要root用户登录或作为其他具有无需密码的伪权限的用户登录，以成为根root用户

### 2. 安装过程 ###

1. 准备安装文件cloudera-manager-installer.bin

    这是个二进制文件，运行于64位系统。下载地址：`http://archive.cloudera.com/cm4/installer/latest/cloudera-manager-installer.bin`。

2. 上传cloudera-manager-installer.bin到安装Cloudera Manager服务端的主机上
    
    如：这里使用Hadoop1为服务端并确保服务端机器能够联网，安装过程需要在线安装程序软件包。

3. 修改权限

        chmod u+x ./cloudera-manager-installer.bin

4. 运行cloudera-manager-installer.bin进行安装

    选择接受Lincese安装即可，安装完成后Cloudera Manager已启动。

5. 使用浏览器登录Cloudera Manager
    
    使用IE9以上、Firefox、Chrome或Opera打开Cloudera Manager。 地址：`http://92.168.30.101:7180`，登录用户名与密码都为`admin`。

- 选择标准版本，继续下一步。
- 为CDH集群安装指定主机
    
    以逗号分割输入所有加入集群的主机`IP`及`SSH`端口。搜索主机确认后继续下一步。
- 集群安装
    - 选择存储库
        
        默认即可，继续下一步。
    - 提供SSH登录凭据
    
        输入root密码，继续下一步。
    - 自动下载安装

        下载安装完后，继续下一步。
        > 因为是从外国网站下载安装会比较慢，有时会因为网络安装失败，只需点击重试失败的主机重新安装即可。
    - 选择要在集群中安装的服务

        选择需要安装的CDH4服务，继续下一步。

    - 数据库设置
        
        选择使用嵌入式数据库，测试连接成功后，继续下一步。

    - 审核配置更改

        更改你需要改变的配置内容，主要是数据存储目录，继续下一步。

    - 启动集群服务

        CM会自动启动配置的服务(服务启动前的预处理会自动完成,并把客户端配置部署好)。继续下一步完成安装。

    - Cloudera Manager安装成功


### 3. 杂项 ###

- 添加服务 

    从CM界面上添加服务，需要注意的就是对于创建目录这些操作需要用户自己创建，而不是在添加服务时自行创建。
- 服务配置文件目录

    服务运行的配置文件是服务启动时Server从数据库中取出配置内容动态产生配置文件分发给Agnet使用。存放于`/var/run/cloudera-scm-agent/process/`目录下。