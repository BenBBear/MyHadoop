#Hue #
=============
##1.安装##
- 在CM中安装
##2.介绍##
- 一个整合了多个服务的友好web界面

##3.功能使用##


###3.1.beeswax(hive ui)###
一个简易的hive客户端。可以通过界面方便地修改查询属性，拼写sql，保存sql，即时查询，保存结果，返回结果，查看日志等功能。

- 界面模块介绍
	- Query Editor:一个查询编辑器，可以保存或者执行sql语句。
	- My Queries：展示最近的保存的sql语句，和执行的sql查询。
	- Saved Queries：展示所有保存的sql语句。
	- History：查询所有的sql执行历史，查看结果。
	- Settings：方便地修改hive的配置（由于使用CM的配置，所以这里修改的配置不能使用）。
- beeswax至于hive类似于phpadmin至于mysql。可以方便地用于查询调试。
- 当前没有，从界面上终止操作的功能。

###3.2.impala###
不测试impala

###3.3.pig###
不测试pig

###3.4.filebrowser###
一个更加友好的hdfs文件浏览器，可以对文件或者文件夹进行新建、移动、权限修改和删除等操作，还可以方便地管理回收站，甚至，可以通过界面的上传和下载文件（注意：在HA的情况下，需要hdfs启用httpfs，才能使用filebrowser）。

###3.5.metastore###
一个方便的hive的metastore管理器。可以查看各个hive表的结构和创建hive表。
- 通过界面简单地导入数据表。（注意:如果是导入内部表，必须拥有对应的文件权限。）

###3.6.jobbrowser###
一个查看job运行情况的界面，对比起原生的RM的查看界面有所删减。

	在启动之前需要在hue的配置文件hue_safety_valve.ini中配置以下项
	[hadoop]
	[[yarn_clusters]]
	[[[default]]]
	resourcemanager_api_url=http://<RMHOST>:<RMPORT>
	proxy_api_url=http://<RMHOST>:<RMPORT>
	history_server_api_url=http://<HistoryServerHOST>:<HistoryServerPORT>
	node_manager_api_url=http://<HistoryServerHOST>:<HistoryServerPORT>

###3.7.jobdesigner###
一个用于方便生成Ooize的action的工具。这里描述下常用的类型。

- mapreduce Action
	- 一种用于可以不写主函数的简单的mapreduce的action
	- 只需要实现map和reduce的类，并且将该jar包上传到hdfs上。
	- 在界面上配置mapreduce运行的参数。
	- action的名字符合正则表达式 `'([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}' `
- java Action
	- 最常用的action。开发者写完一个完整的Hadoop的可运行jar包，并提交运行。类似于使用hadoop jar方式提交运行任务。
- fs Action
	- 用于操作hdfs目录的类型。可以用来删除或者创建目录。

Action的配置中的可以使用Ooize支持的EL函数进行参数化。
Ooize中的EL参数可参考["OoizeEL参数"](http://oozie.apache.org/docs/3.3.2/WorkflowFunctionalSpec.html#a4.1_Workflow_Job_Properties_or_Parameters )


###3.8.Oozie Editor###
一个创建和管理Workflow、Coordinator和bundle的界面。

- Workflow代表一组有执行顺序的action的。
	- action可以是job design中的一种，然后按照一定的顺序拼凑起来。
	- action界面上提示可以从job design中导入。但是笔者实际试用中，发现没有导入的按钮...

- Coordinator代表执行WorkFlow的特定启动条件
	- 可以定时启动Workflow。选择一个开始和结束的时段和启动频率，然后协调器会生成所有可运行的job。如果某些job已经过期，会自动生成运行。
	- 高级设定中还可以设置这个协调器启动的workflow的并行数量，超时时间和运行算法（FIFO,LIFO，LAST_ONLY）

Oozie使用建议：Ooize用作最简单的定时调度。在运行的java程序中完成更多的逻辑。

###3.9.shell ###
提供一个更加友好的hbase或者pig的shell命令行客户端。（用过hbase的shell的都知道有多难用）

###3.10.admin###
对于第一个登陆的用户，管理员用户，还可以创建和管理用户，可以连接smtp服务器发送邮件。

##4.外部数据库##
官方文档中提到可以使用外部的mysql。但是笔者在实际试验中发现，配置了外部数据库后，进行hive查询的时候，会报错
	
`Failed to retrieve server state of submitted query id xxxxx:`
`Could not connect to xxxx:32767 (code THRIFTTRANSPORT): `
`TTransportException(u'Could not connect to xxxx:32767',)`

然后，笔者遍寻配置，源码文件都找不到32767这个端口，所以，笔者并没有成功使用外部数据库。



##5.遇到问题##

- CDH升级到HA之后HUE不能使用。日志没有任何帮助。
尝试切换到31启动，还是失败,后自动好了。
	原因：初步估计是机器资源紧张导致的。

 	附带手动启动命令：

 		/opt/cloudera/parcels/CDH-4.3.0-1.cdh4.3.0.p0.22/share/hue/build/env/bin/hue runspawningserver
		/opt/cloudera/parcels/CDH-4.3.0-1.cdh4.3.0.p0.22/share/hue/build/env/bin/hue beeswax_server

- beeswax查询的时候报错:java.io.IOException: java.io.IOException: Not a file: hdfs://nameservice1/user/zouhc/test/out

	原因:外部表中出现目录，必须全部是文件

- hue有时候响应缓慢，请求timed out，过一会儿又好了。
	
	原因：每次想查看历史结果都会引起的响应缓慢。

- admin用户导入不了hive表。hue中通过导入hive的时候提示没有权限移动文件。即时源文件和目标目录都赋予777权限。提示 Permission denied by sticky bit setting。
	
	原因：导入文件的上层目录设置了sticky bit。解决方法hdfs dfs -chmod -t 上层目录。详情 `http://dongwei.iteye.com/blog/921961`

- 修改mysql数据库后，查询hive连接弹出 Failed to contact Server to check query status.
	
	原因：hue使用外部数据库的架构和使用本地数据库的架构不一致。请参考外部数据库的配置。

- Job提交的时候报错，需要符合正则表达式 `'([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}' `

	原因：虽然job designer和Ooize Editor中填写job名字的时候都没有错误提示。但是，名字必须符合正则表达式`'([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}' `的形式。

- 使用mapreduce类型的action的时候，遇到mapreduce.job.map.class is incompatible with map compatability mode.的报错
	
	原因：hadoop通过mapred.mapper.new-api设置，可以兼容旧版的api。job提交的时候会判断是否有不兼容的配置。Java程序中，使用API设置的，不会遇到这个问题。
	<table>
	<tr><th>参数</th><th>兼容模式</th><th>新API模式</th></tr>
	<tr><td>设置map</td><td>mapred.mapper.class</td>
	<td>mapreduce.job.map.class</td></tr>
	<tr><td>设置reduce</td><td>mapred.reducer.class</td>
	<td>mapreduce.job.reduce.class</td></tr>
	<tr><td>设置inputformat</td><td>mapred.input.format.class</td>
	<td>mapreduce.job.inputformat.class</td></tr>
	<tr><td>设置outputformat</td><td>mapred.output.format.class</td>
	<td>mapreduce.job.outputformat.class</td></tr>
	<tr><td>设置partitioner</td><td>mapred.partitioner.class</td>
	<td>mapreduce.job.partitioner.class</td></tr>
	</table>

- 提交workflow的时候提示，parameter [timezone] = [GMT+8] must be a valid TZ。

	原因：Hue的时区的下拉框中选择了GMT+8时区。但是，ooize不识别这个配置。Hue的bug。

- 如何使Workflow使用设置滚动路径，例如处理的路径是/tmp/年/月/日
	
	原因：由于Ooize支持的可拓展参数比较弱。推荐在java运行中完成这些所有的路径选取等工作。