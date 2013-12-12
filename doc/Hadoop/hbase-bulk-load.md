# Hbase Bulk Load

	快速入库，初次，后续入库都可以用。类似mysql的load data命令

## 过程

1.	写HFile
2.	Bulk Load

## 解析

过程解析：http://uestzengting.iteye.com/blog/1290557

## 示例
	
示例代码：https://github.com/Paschalis/HBase-Bulk-Load-Example/blob/master/src/cy/ac/ucy/paschalis/hbase/bulkimport/Driver.java


---

需要后续整理一下

	HFileOutputFormat
	LoadIncrementalHFiles		||	completebulkload tool

	HRegionInterface.bulkLoadHFiles
	HRegion.bulkLoadHFiles
	HStroe.assertBulkLoadHFileOk
	HFileScanner scanner = reader.getScanner	逐条验证
	store.bulkLoadHFile(path);
	StoreFile.rename(fs, srcPath, dstPath);

	hadoop jar   /usr/lib/hbase/hbase-0.94.6-cdh4.3.0-
	security.jar importtsv
	-Dimporttsv.separator=,
	-Dimporttsv.bulk.output=output
	-Dimporttsv.columns=HBASE_ROW_KEY,f:count wordcount word_count.csv



	Here’s a rundown of the different configuration elements:

	-Dimporttsv.separator=, specifies that the separator is a comma.
	-Dimporttsv.bulk.output=output is a relative path to where the HFiles will be written. Since your user on the VM is “cloudera” by default, it means the files will be in /user/cloudera/output. Skipping this option will make the job write directly to HBase.
	-Dimporttsv.columns=HBASE_ROW_KEY,f:count is a list of all the columns contained in this file. The row key needs to be identified using the all-caps HBASE_ROW_KEY string; otherwise it won’t start the job. (I decided to use the qualifier “count” but it could be anything else.)
	
	
	
	hadoop jar hbase-VERSION.jar completebulkload /user/todd/myoutput mytable
	
	
	
	hbase.hstore.bulkload.verify 一个开关，默认关闭，用于store步骤上的数据验证。