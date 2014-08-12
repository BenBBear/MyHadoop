# CDH LZO

主要参考：<http://www.cloudera.com/content/cloudera-content/cloudera-docs/CM4Ent/4.5.3/Cloudera-Manager-Enterprise-Edition-Installation-Guide/cmeeig_install_LZO_Compression.html>

    CDH的LZO是单独作为组件的，作为Parcel发布。

## 安装
Parcel是：<http://archive.cloudera.com/gplextras/parcels/latest/>

主要操作步骤：

1.  在管理界面配置上此Parcel
2.  下载、分发、激活 
3.  修改配置和重新启动需要LZO功能的服务

另外，需要：
    
    yum install lzo lzop -y

因为Parcel中提供的是hadoop-lzo，不包括lzo本身。
或者使用编译安装方式。
        
    wget http://www.oberhumer.com/opensource/lzo/download/lzo-2.06.tar.gz
    tar -zxvf lzo-2.06.tar.gz
    cd lzo-2.06
    ./configure
    make
    make install

这种方法安装的LZO是在/usr/lib中，你或许需要放到/usr/lib64目录中。

## 配置

1.  HDFS的core-site.xml中

    io.compression.codecs中增加

        com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec

## MR or YARN 启用

1.  在YARN environment safety valve（环境变量设置）中增加：（在cdh5中cm已经做了）

        HADOOP_CLASSPATH=/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/*
        JAVA_LIBRARY_PATH=/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/native

2.  在YARN Client environment safety valve（客户端的环境变量设置）中增加：（在cdh5中不需要）
    
        HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/*
        JAVA_LIBRARY_PATH=$JAVA_LIBRARY_PATH:/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/native

3.  在YARN的Gateway的YARN 客户端配置安全阀（yarn-site.xml）中增加：（在cdh5中不需要）

        <property>
          <name>yarn.application.classpath</name>
          <value>$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$YARN_HOME/*,$YARN_HOME/lib/*,/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/*,/opt/cloudera/parcels/CDH/lib/hbase/*,/opt/cloudera/parcels/CDH/lib/hbase/lib/*</value>
        </property>
    	<property>
    	  <name>mapreduce.admin.user.env</name>
    	  <value>LD_LIBRARY_PATH=$HADOOP_COMMON_HOME/lib/native:/opt/cloudera/parcels/HADOOP_LZO/lib/hadoop/lib/native</value>
    	</property>

4.  另外，你可能需要安装lzo，如果你的机器上没有的话。（因为Parcel中提供的是hadoop-lzo，不包括lzo本身）
        
        wget http://www.oberhumer.com/opensource/lzo/download/lzo-2.06.tar.gz
        tar -zxvf lzo-2.06.tar.gz
        cd lzo-2.06
        ./configure
        make
        make install

    这种方法安装的LZO是在/usr/lib中，你或许需要放到/usr/lib64目录中。

## HIVE

    不需要配置


## 测试

1.  创建一个本地文本文件

        seq -w 1 10000 > test.txt

2.  lzop压缩，没有lzop可以用（yum install lzop -y）安装

        lzop test.txt

3.  上传到hdfs，

        hdfs dfs -put test.txt.lzo /user/$USER/test/test-lzo/test.txt.lzo
      
4.  查看文件

        hdfs dfs -text /user/$USER/test/test-lzo/test.txt.lzo

5.  测试程序

        yarn org.apache.hadoop.examples.ExampleDriver wordcount /user/$USER/test/test-lzo/test.txt.lzo /user/$USER/test/test-lzo-out

5.  streaming程序的测试