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

4.  挂载

        sh myfuse.sh

5.  卸载

        fusermount -u $HOME/mnt/dfs
    
