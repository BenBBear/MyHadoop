# MyHadoop 安装与配置

### 安装             
    MyHadoop基本Cloudera Manager和CDH, 为了解决安装CM与CDH需要从国外下载CM与CDH且这个东西都比较大且下载速度不是很稳定，通常在在安装过程中需要等待下载的时间比较长，为此，此工程将通过tar包安装CM和通过本地库安装CDH.

    tars目录用于存放下载的CM安装包
    parcels目录用于存放系统对应所需的parcels格式的CDH文件(CDH-4.2.1-1.cdh4.2.1.p0.5-el6.parcel)与包含CDH hash值的xxxxx.sha（CDH-4.2.1-1.cdh4.2.1.p0.5-el6.parcel.sha）文件

    关于参数的配置位于`cm_conf/confs.py` 文件中

    安装执行

            sh install.sh


### 配置        
    MyHadoop基本Cloudera Manager和CDH, 在安装了CDH服务后需要对配置进行更改，主要是对数据目录和JVM参数进行修改，在机器比较多时在页面中修改需要花费比较多的时间，为此本工程提供了通过配置文件的方式来更改配置，更改的方式是调用CM的REST API进行修改。

    关于参数的配置位于`cm_conf/confs.py` 文件中

    修改配置运行            

            python my_hadoop_conf.py