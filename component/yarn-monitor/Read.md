本程序是针对YARN框架，通过restapi采集数据，并保存展示分析的程序。

version 0.1

需要python2.7
首次运行以下命令安装虚拟环境：
cd yarn-monitor ; sh install.sh

配置文件：lib\config.py
以下是几个重要的配置参数：
logpath:程序日志输出目录
sqlitepath:sqlite数据存放的目录，根据实际数据库保留适当的磁盘空间
rmhost,rmport:ResourceManager的host和端口
hshost,hsport:HistoryServer的host和端口
collect_interval：采集间隔600秒。如非必要不要更改。
hosts：集群中的NM列表

数据采集：
collect.py是采集脚本。
正式运行请添加以下命令到crontab
*/10 * * * * cd /home/XXX/yarn_monitor; flask/bin/python collect.py 
为了防止python程序因某些原因而积压，请加入检查脚本
*/10 * * * * sh /home/XXX/yarn_monitor/checkpython.sh >> xxx/checkpython.log

修改productrun.py中的端口,启动web界面：

cd /home/hadoop/yarn_monitor/lookapp/webpython
nohup flask/bin/python productrun.py  >>  /home/hadoop/yarn_monitor/logs/web.log  &

系统简介：
本系统分为4个部分：应用运行，集群历史状况，NM历史状况，应用查询。
应用运行：能够展示当前运行的应用，等待的应用。
集群历史状况：按照时间查询集群的应用运行数量，map数量，读写状况等
NM历史状况：按照时间和机器查询在这个机器上运行的应用数量，map数量，reduce数量等
应用查询：根据appid应用名称等查询完成的应用，并给出查询应用集合的综合描述。
使用Hadoop的Rest接口，每10分钟进行一次采集。




