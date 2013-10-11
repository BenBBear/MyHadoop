# rsyslog + omhdfs + hdfs

## rsyslog

    7.2.5
    rsyslog 是一个 syslogd 的多线程增强版

![](http://images.cnitblog.com/blog/493056/201303/10231124-47833e3a70d24191aeb6435264c4392f.png)

### 安装

安装依赖 libestr

    cd /tmp
    wget http://libestr.adiscon.com/files/download/libestr-0.1.2.tar.gz
    tar -xvf libestr-0.1.2.tar.gz 
    cd libestr-0.1.2
    ./configure --prefix=/usr
    make
    make install 

安装依赖 libee json-c

    cd /tmp
    wget http://www.libee.org/files/download/libee-0.4.1.tar.gz
    tar -xvf libee-0.4.1.tar.gz 
    cd libee-0.4.1
    ./configure --prefix=/usr
    make
    make install 
    
安装依赖 json-c

    wget https://github.com/downloads/json-c/json-c/json-c-0.10.tar.gz
    tar zxvf json-c-0.10.tar.gz
    cd json-c-0.10
    make
    make install
    cp json_object_iterator.h /usr/local/include/json/
    
安装 rsyslog + omhdfs

    export C_INCLUDE_PATH=$HOME/local/hadoop/src/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs
    
    export LIBRARY_PATH=$HOME/local/hadoop/src/hadoop-hdfs-project/hadoop-hdfs/target/native/target/usr/local/lib
    
    export LD_LIBRARY_PATH=$HOME/local/hadoop/src/hadoop-hdfs-project/hadoop-hdfs/target/native/target/usr/local/lib
    
    ./configure --prefix=$HOME/local/rsyslog --enable-omhdfs
    
    ./configure --prefix=$HOME/local/rsyslog --enable-omhdfs --enable-debug --  enable-rtinst

### 配置

配置文件 /etc/rsyslog.conf

测试配置文件是否正确

    rsyslogd -N1 -f file

修改配置后，重新加载

    /etc/init.d/rsyslog reload

### 启动

    rsyslogd -f /root/rsyslog_worker_dir/rsyslog.conf -i /root/rsyslog_worker_dir/rsyslog.pid

以debug方式启动

    rsyslogd -f /root/rsyslog_worker_dir/rsyslog.conf -i /root/rsyslog_worker_dir/rsyslog.pid -dn >debuglog

测试

    logger -it logger_test -p local5.info 'this is a test'


# 资料

[rsyslog](http://www.cnblogs.com/tobeseeker/archive/2013/03/10/2953250.html)  

[rsyslog](http://blog.oldzee.com/?tag=rsyslog)    

[omhdfs](http://www.rsyslog.com/doc/omhdfs.html)



日志级别:

    debug       –有调式信息的，日志信息最多
    info        –一般信息的日志，最常用
    notice      –最具有重要性的普通条件的信息
    warning     –警告级别
    err         –错误级别，阻止某个功能或者模块不能正常工作的信息
    crit        –严重级别，阻止整个系统或者整个软件不能正常工作的信息
    alert       –需要立刻修改的信息
    emerg       –内核崩溃等严重信息
    none        –什么都不记录