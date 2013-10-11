# 异常

    遇到的主要异常

## HDFS死亡节点

表现

*   死亡节点出现，在一段时间又恢复正常
*   最近死亡

判断死亡节点条件

    最后更新状态时间<now() - (2*心跳重新检查间隔5分钟+10*心跳间隔3秒）

DN每3秒发送1此心跳
NN每5分钟做一次心跳检查

数据块汇报间隔是6小时


## 数据写入超时

表现

*	在DN日志中，较多数据写超时，socket timeout
*	客户端和程序有时也报错

DataStorage	目录信息，存储信息

dataset	块信息

	DataBlockScanner	不是扫描全部Block，而是发现差异的Block和关闭提交的Block。
	DirectoryScanner	进行全盘块扫描，并和内存中比较。
	
	DataNode.transferBlock
		DataTransfer	对方接口 50900
	
	
两个比较典型的错误

	java.net.SocketTimeoutException: 180000 millis timeout while waiting for channel to be ready for read. ch : java.nio.channels.SocketChannel[connected local=/10.20.104.145:58026 remote=/10.20.104.155:50010]
		at org.apache.hadoop.net.SocketIOWithTimeout.doIO(SocketIOWithTimeout.java:165)
		at org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:156)
		at org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:129)
		at java.io.FilterInputStream.read(FilterInputStream.java:116)
		at java.io.BufferedInputStream.fill(BufferedInputStream.java:218)
		at java.io.BufferedInputStream.read(BufferedInputStream.java:237)
		at java.io.FilterInputStream.read(FilterInputStream.java:66)
		at org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.vintPrefixed(HdfsProtoUtil.java:169)
		at org.apache.hadoop.hdfs.server.datanode.DataXceiver.replaceBlock(DataXceiver.java:834)
		at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.opReplaceBlock(Receiver.java:137)
		at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.processOp(Receiver.java:70)
		at org.apache.hadoop.hdfs.server.datanode.DataXceiver.run(DataXceiver.java:221)
		at java.lang.Thread.run(Thread.java:662)
		
	发出拷贝请求，但是没有回应，回应超时	kpi46
	3分钟？连接超时？
		
	java.net.SocketTimeoutException: 185000 millis timeout while waiting for channel to be ready for read. ch : java.nio.channels.SocketChannel[connected local=/10.20.104.145:36798 remote=/10.20.104.151:50010]
		at org.apache.hadoop.net.SocketIOWithTimeout.doIO(SocketIOWithTimeout.java:165)
		at org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:156)
		at org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:129)
		at org.apache.hadoop.net.SocketInputStream.read(SocketInputStream.java:117)
		at java.io.FilterInputStream.read(FilterInputStream.java:66)
		at java.io.FilterInputStream.read(FilterInputStream.java:66)
		at org.apache.hadoop.hdfs.protocol.HdfsProtoUtil.vintPrefixed(HdfsProtoUtil.java:169)
		at org.apache.hadoop.hdfs.server.datanode.DataXceiver.writeBlock(DataXceiver.java:512)
		at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.opWriteBlock(Receiver.java:103)
		at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.processOp(Receiver.java:67)
		at org.apache.hadoop.hdfs.server.datanode.DataXceiver.run(DataXceiver.java:221)
		at java.lang.Thread.run(Thread.java:662)
		
	写块，客户端连接写入，pipeline，dn在向后发送数据前，先发送write命令，需要回应，但是无响应	kpi44
	3分另5秒？连接超时？
	timeoutValue，超时设置：3分钟超时+5秒扩展*targets数
		
	java.net.SocketTimeoutException: 480000 millis timeout while waiting for channel to be ready for write. ch : java.nio.channels.SocketChannel[connected local=/10.20.104.145:57999 remote=/10.20.101.131:50010]
		at org.apache.hadoop.net.SocketIOWithTimeout.waitForIO(SocketIOWithTimeout.java:247)
		at org.apache.hadoop.net.SocketOutputStream.waitForWritable(SocketOutputStream.java:166)
		at org.apache.hadoop.net.SocketOutputStream.transferToFully(SocketOutputStream.java:214)
		at org.apache.hadoop.hdfs.server.datanode.BlockSender.sendPacket(BlockSender.java:510)
		at org.apache.hadoop.hdfs.server.datanode.BlockSender.sendBlock(BlockSender.java:673)
		at org.apache.hadoop.hdfs.server.datanode.DataNode$DataTransfer.run(DataNode.java:1578)
		at java.lang.Thread.run(Thread.java:662)
		
	心跳包中的命令，执行数据传送，
	发送写writeBlock命令
	写数据，无法写入，写超时 kpi12
	8分钟超时
		
	java.net.SocketTimeoutException: 480000 millis timeout while waiting for channel to be ready for write. ch : java.nio.channels.SocketChannel[connected local=/10.20.104.145:50010 remote=/10.20.104.145:35850]
        at org.apache.hadoop.net.SocketIOWithTimeout.waitForIO(SocketIOWithTimeout.java:247)
        at org.apache.hadoop.net.SocketOutputStream.waitForWritable(SocketOutputStream.java:166)
        at org.apache.hadoop.net.SocketOutputStream.transferToFully(SocketOutputStream.java:214)
        at org.apache.hadoop.hdfs.server.datanode.BlockSender.sendPacket(BlockSender.java:510)
        at org.apache.hadoop.hdfs.server.datanode.BlockSender.sendBlock(BlockSender.java:673)
        at org.apache.hadoop.hdfs.server.datanode.DataXceiver.readBlock(DataXceiver.java:344)
        at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.opReadBlock(Receiver.java:92)
        at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.processOp(Receiver.java:64)
        at org.apache.hadoop.hdfs.server.datanode.DataXceiver.run(DataXceiver.java:221)
        at java.lang.Thread.run(Thread.java:662)
		
	客户端读取数据块，
	DN发送数据，头信息发送成功了，发送失败，失去连接 kpi41， 自己到自己
	8分钟，写超时
	-- 这个可能是客户端异常终止
		
	2013-10-09 10:18:08,064 ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: 
		DataNode{data=FSDataset{dirpath='[/home17/hadoop_data/current, /home18/hadoop_data/current, /home19/hadoop_data/current, /home20/hadoop_data/current, /home21/hadoop_data/current, /home22/hadoop_data/current, /home23/hadoop_data/current]'}, localName='kpi41:50010', storageID='DS-1054565424-10.20.104.145-50010-1328171506543', xmitsInProgress=0}:
		Exception transfering block BP-645555628-10.20.193.84-1372044206630:blk_-8252499837970475773_135289945 
			to mirror 10.20.104.143:50010: 
		java.net.SocketTimeoutException: 
			190000 millis timeout while waiting for channel to be ready for read. ch : 
			java.nio.channels.SocketChannel[connected local=/10.20.104.145:53284 remote=/10.20.104.143:50010]
		
错误较少，没有明显趋势
出现在多处
为何会自己向自己拷贝数据？ 【connected local=/10.20.104.145:50010 remote=/10.20.104.145:35850】

这些异常可以确定和DataSet无关。
1.	出现的地方不符
2.	有些连回应都无，根本不可能和DataSet相关

暂时认为是常见情况引发的异常：网络故障，客户端关闭等

replaceBlock	
	发送	copy命令
	接收数据块
	
		
	2013-10-08 02:28:47,361 ERROR org.apache.hadoop.hdfs.server.datanode.DataNode: kpi41:50010:DataXceiver error processing WRITE_BLOCK operation  src: /10.20.104.159:52413 dest: /10.20.104.145:50010
	java.io.IOException: Premature EOF from inputStream
			at org.apache.hadoop.io.IOUtils.readFully(IOUtils.java:194)
			at org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver.doReadFully(PacketReceiver.java:213)
			at org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver.doRead(PacketReceiver.java:134)
			at org.apache.hadoop.hdfs.protocol.datatransfer.PacketReceiver.receiveNextPacket(PacketReceiver.java:109)
			at org.apache.hadoop.hdfs.server.datanode.BlockReceiver.receivePacket(BlockReceiver.java:414)
			at org.apache.hadoop.hdfs.server.datanode.BlockReceiver.receiveBlock(BlockReceiver.java:635)
			at org.apache.hadoop.hdfs.server.datanode.DataXceiver.writeBlock(DataXceiver.java:570)
			at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.opWriteBlock(Receiver.java:103)
			at org.apache.hadoop.hdfs.protocol.datatransfer.Receiver.processOp(Receiver.java:67)
			at org.apache.hadoop.hdfs.server.datanode.DataXceiver.run(DataXceiver.java:221)
			at java.lang.Thread.run(Thread.java:662)
			
	？网络故障？读取不到数据了？
	写给下一个节点
	从源或者上一个节点读，写向下一个节点，使用xfer端口。
	从源读时出错！
	最初数据源是客户端写入的
	在一段时间内非常频繁的发生2-3分钟内
			
			
1. 数据扫描，随机时间？
2. 写超时比读超时时间短？ 写超时，默认是8分钟，读超时，默认1分钟
是源端发送失败
			
			
DataXceiverServer
	xferPort	dfs.datanode.address		50010	数据流地址	数据传输		
	infoPort	dfs.datanode.http.address	50075
	ipcPort		dfs.datanode.ipc.address	50020	命令
	
	
proxy socket ?
	连接xferPort，发送拷贝命令	在balance时有效
	

