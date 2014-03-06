# Hadoop本地化

    本地化代码需要本机编译。
    
    版本：CDH4*，Hadoop2*

## 本地库编译

### 前提条件

*   JDK 1.6+
*   Maven 3.0
*   ProtocolBuffer 2.4.1
*   CMake 2.6 or newer (if compiling native code)
*   联网

### 选项
        
    -Pnative            本地库
    -Drequire.snappy    
    -Dsnappy.prefix     指定头文件和库文件
    -Dsnappy.lib        指定库文件位置
    -DskipTests
        
示例

    # 在hadoop目录的src目录中、

    # 编译本地库，跳过测试。
    mvn package -Pnative -DskipTests    

    #编译本地库，发布文档，Tar包。
    mvn package -Pdist,native,docs,src -DskipTests -Dtar    

编译后的libhadoop.so和libhdfs.so使用find查找
    
    find . -name "libhadoop.so"
    
    $HADOOP_HOME/src/hadoop-hdfs-project/hadoop-hdfs/target/native/target/usr/local/lib/libhdfs.so
    $HADOOP_HOME/src/hadoop-hdfs-project/hadoop-hdfs/src/main/native/libhdfs
    
    $HADOOP_HOME/src/hadoop-common-project/hadoop-common/target/native/target/usr/local/lib/libhadoop.so

把对应的.so文件移动到hadoop的lib/native目录，重启即可

## Fuse-Dfs

### 主目录

    $HADOOP_PREFIX/src/hadoop-hdfs-project/hadoop-hdfs/src/main/native/fuse-dfs

编译过程

    需要 fuse 2.8.0以上版本
    编译本地库时，会编译它。

在一些机器上编译会说找不到fuse，但明明已经安装了的啊。原因不明。
可以用如下方式解决：
    
*   下载这个：[FindFUSE.cmake](https://github.com/julp/FindFUSE.cmake/blob/master/FindFUSE.cmake)，放到fuse-dfs的CMakeLists.txt同目录中。
*   修改CMakeLists.txt文件

        find_package(PkgConfig REQUIRED)
        set(CMAKE_MODULE_PATH "/home/hadoop/local/hadoop/src/hadoop-hdfs-project/hadoop-hdfs/src/main/native/fuse-dfs/")
        find_package(FUSE 2.8 REQUIRED)
        #pkg_check_modules(FUSE fuse)

重新编译就可以了

### 编译结果目录

    $HADOOP_PREFIX/src/hadoop-hdfs-project/hadoop-hdfs/target/native/main/native/fuse-dfs

    mvn <goals> -rf :hadoop-hdfs

    To see the full stack trace of the errors, re-run Maven with the -e switch.
    Re-run Maven using the -X switch to enable full debug logging.

1.  把对应的fuse-dfs程序和fuse_dfs_wrapper.sh文件(使用find查找它)移到你想要安装的位置，例如$HOME/local/fuse-dfs。

2.  修改swap.sh
    
    主要是
    *   LD_LIBRARY_PATH要配置libhdfs.so的正确位置，
    *   jvm的so的正确位置，
    *   CLASSPATH中包含Hadoop的配置文件目录
   
3.  为了方便，创业一个挂载脚本，例如叫：myfuse.sh

        mkdir -p $HOME/mnt/dfs
        DIR=$(cd $(dirname "$0"); pwd)
        sh $DIR/fuse_dfs_wrapper.sh \
        -oserver=hdfs://mycluster \
        -obig_writes -ousetrash -oprotected=/user:/tmp \
        $HOME/mnt/dfs

    特别的，如果觉得挂载后显示的文件大小和实际的不太对<http://kicklinux.com/hadoop-fuse-dfs-filesize/>,可以使用如下参数

        -odirect_io -oattribute_timeout=0 -oentry_timeout=0

4.  挂载

        sh myfuse.sh

    *如果提示你： allow_other_user 需要写入到 /etc/fuse.conf，照做*

    前提是需要：

        /etc/init.d/fuse start
        modprobe fuse
        另外，注意 fusermount 的权限问题

5.  卸载

        fusermount -u $HOME/mnt/dfs

6.  可选，磁盘启动挂载

        /home/hadoop/pkg/fuse-dfs/fuse_dfs_wrapper.3.sh#dfs://nameservice1 /home/hadoop/mnt/dfs fuse rw,usetrash,initchecks 0 0

## hadoop lzo

hadoop lzo是独立于hadoop的一个压缩组件。需要单独下载。

可以从这里： 对于Hadoop 2.0之后的版本
https://github.com/kambatla/hadoop-lzo

也可以从这里：对于Hadoop 0.2, 1.0版本
https://github.com/twitter/hadoop-lzo

对照ReadMe中的要求配置必要软件。

然后：
ant compile-native jar

如果你的lzo安装不是在默认的目录，编译时报找不到lzo库，你需要修改build.xml文件：
以我的安装示例来说是：

    <exec dir="${build.native}" executable="sh" failonerror="true">
       <env key="OS_NAME" value="${os.name}"/>
       <env key="OS_ARCH" value="${os.arch}"/>
       <!-- 下面两个根据实际情况修改 -->
       <env key="CFLAGS" value="-I${user.home}/local/lzo/include"/>
       <env key="LDFLAGS" value="-Wl,--no-as-needed -L${user.home}/local/lzo/lib"/>
       <env key="JVM_DATA_MODEL" value="${sun.arch.data.model}"/>
       <env key="NATIVE_SRCDIR" value="${native.src.dir}"/>
       <arg line="${native.src.dir}/configure"/>
    </exec>

编译成功后，把

    cp build/native/L*/lib/libgplcompression.so* ~/local/hadoop/lib/native/
    cp build/hadoop-lzo-0.4.15.jar ~/local/hadoop/share/hadoop/common/lib/

最后需要修改配置， core-site.xml：

    <property>
        <name>io.compression.codecs</name>
        <value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.SnappyCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec</value>
    </property>
    <property>
        <name>io.compression.codec.lzo.class</name>
        <value>com.hadoop.compression.lzo.LzoCodec</value>
    </property>


### 测试

1.  创建一个本地文本文件
2.  lzop压缩，没有lzop可以用（yum install lzop -y）安装
3.  上传到hdfs，hdfs dfs -put data.lzo /user/hadoop/test/lzo-test/data.lzo
4.  查看文件，hdfs dfs -text /user/hadoop/test/lzo-test/data.lzo

## open-ssl 安装

linux-generic64

