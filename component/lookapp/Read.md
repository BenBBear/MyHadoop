本程序是基础的，简单的对RM，进行采集并进行简单展示的程序。

look.sh是采集脚本，依赖环境变量${HADOOP_DIR_CONF}获取机器列表
修改look.sh的端口和RM地址，
请加入到crontab中，每分钟运行。
程序会在nodedata和data目录创建采集的数据。

webpython是通过简单的html展示采集的数据
需要py2.6

初次启动要运行以下命令:
    cd webpython ; sh install.sh

环境安装后，运行以下命令启动web 服务
    ./scriptrun.py

默认是5000端口




