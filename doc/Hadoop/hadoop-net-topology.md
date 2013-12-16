配置项：

net.topology.node.switch.mapping.impl

默认值：
org.apache.hadoop.net.ScriptBasedMapping
这个是通过一个脚本去获取机架感知

简单值:
org.apache.hadoop.net.TableMapping
指定配置文件(net.topology.table.file.name)即可
配置文件要求只有两列（使用空格或者\T分割），支持#开头的注释
映射文件第一列是ip地址，第二列是/rack1(必须要斜杠开头)



实际配置例子:
<property>
 <name>net.topology.node.switch.mapping.impl</name>
 <value>org.apache.hadoop.net.TableMapping</value>
</property>
<property>
 <name>net.topology.table.file.name</name>
 <value>/home/qiujw/hadoop/etc/hadoop/net-table</value>
</property>

net-table文件:
10.1.74.44 /r1
10.1.74.45 /r1
10.1.74.46 /r2
10.1.74.47 /r2
10.1.74.48 /r2