使用HiveInputFormat的话。会发现其是对其它InputFormat的包装，并且从缓存中获取的（getInputFormatFromCache），在多线程环境下，是要求被包装的InputFormat是多线程安全的。
但是，如果使用了Hadoop-Lzo的DeprecatedLzoTextInputFormat的话，它是非现场安全的，会引发问题，需要把它修改为线程安全的。

我的修改如下：Hadoop-Lzo 0.4.12

	/*
	 * This file is part of Hadoop-Gpl-Compression.
	 *
	 * Hadoop-Gpl-Compression is free software: you can redistribute it
	 * and/or modify it under the terms of the GNU General Public License
	 * as published by the Free Software Foundation, either version 3 of
	 * the License, or (at your option) any later version.
	 *
	 * Hadoop-Gpl-Compression is distributed in the hope that it will be
	 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty
	 * of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	 * GNU General Public License for more details.
	 *
	 * You should have received a copy of the GNU General Public License
	 * along with Hadoop-Gpl-Compression.  If not, see
	 * <http://www.gnu.org/licenses/>.
	 */

	package com.hadoop.mapred;

	import java.io.IOException;
	import java.util.ArrayList;
	import java.util.Arrays;
	import java.util.HashMap;
	import java.util.Iterator;
	import java.util.List;
	import java.util.Map;

	import org.apache.hadoop.fs.FileStatus;
	import org.apache.hadoop.fs.FileSystem;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapred.FileInputFormat;
	import org.apache.hadoop.mapred.FileSplit;
	import org.apache.hadoop.mapred.InputSplit;
	import org.apache.hadoop.mapred.JobConf;
	import org.apache.hadoop.mapred.RecordReader;
	import org.apache.hadoop.mapred.Reporter;

	import com.hadoop.compression.lzo.LzoIndex;
	import com.hadoop.compression.lzo.LzopCodec;

	/**
	 * This class conforms to the old (org.apache.hadoop.mapred.*) hadoop API style 
	 * which is deprecated but still required in places.  Streaming, for example, 
	 * does a check that the given input format is a descendant of 
	 * org.apache.hadoop.mapred.InputFormat, which any InputFormat-derived class
	 * from the new API fails.  In order for streaming to work, you must use
	 * com.hadoop.mapred.DeprecatedLzoTextInputFormat, not 
	 * com.hadoop.mapreduce.LzoTextInputFormat.  The classes attempt to be alike in
	 * every other respect.
	*/

	@SuppressWarnings("deprecation")
	public class ThreadSafeDeprecatedLzoTextInputFormat extends FileInputFormat<LongWritable, Text> {
	  public static final String LZO_INDEX_SUFFIX = ".index";
	  private final ThreadLocal<Map<Path,LzoIndex>> indexes=new ThreadLocal<Map<Path,LzoIndex>>(){
		@Override
		protected Map<Path, LzoIndex> initialValue() {
			return new HashMap<Path, LzoIndex>() ;
		}	  
	  };
	  
	  //private final Map<Path, LzoIndex> indexes = new HashMap<Path, LzoIndex>();

	  @Override
	  protected FileStatus[] listStatus(JobConf conf) throws IOException {
		List<FileStatus> files = new ArrayList<FileStatus>(Arrays.asList(super.listStatus(conf)));

		String fileExtension = new LzopCodec().getDefaultExtension();

		Iterator<FileStatus> it = files.iterator();
		while (it.hasNext()) {
		  FileStatus fileStatus = it.next();
		  Path file = fileStatus.getPath();

		  if (!file.toString().endsWith(fileExtension)) {
			// Get rid of non-LZO files.
			it.remove();
		  } else {
			FileSystem fs = file.getFileSystem(conf);
			LzoIndex index = LzoIndex.readIndex(fs, file);
			indexes.get().put(file, index);
		  }
		}

		return files.toArray(new FileStatus[] {});
	  }

	  @Override
	  protected boolean isSplitable(FileSystem fs, Path filename) {
		LzoIndex index = indexes.get().get(filename);
		return !index.isEmpty();
	  }

	  @Override
	  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
		FileSplit[] splits = (FileSplit[])super.getSplits(conf, numSplits);
		// Find new starts/ends of the filesplit that align with the LZO blocks.

		List<FileSplit> result = new ArrayList<FileSplit>();

		for (FileSplit fileSplit: splits) {
		  Path file = fileSplit.getPath();
		  FileSystem fs = file.getFileSystem(conf);
		  LzoIndex index = indexes.get().get(file);
		  if (index == null) {
			throw new IOException("Index not found for " + file);
		  }
		  if (index.isEmpty()) {
			// Empty index, keep it as is.
			result.add(fileSplit);
			continue;
		  }

		  long start = fileSplit.getStart();
		  long end = start + fileSplit.getLength();

		  long lzoStart = index.alignSliceStartToIndex(start, end);
		  long lzoEnd = index.alignSliceEndToIndex(end, fs.getFileStatus(file).getLen());

		  if (lzoStart != LzoIndex.NOT_FOUND  && lzoEnd != LzoIndex.NOT_FOUND) {
			result.add(new FileSplit(file, lzoStart, lzoEnd - lzoStart, fileSplit.getLocations()));
		  }
		}

		return result.toArray(new FileSplit[result.size()]);
	  }

	  @Override
	  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
		  JobConf conf, Reporter reporter) throws IOException {
		reporter.setStatus(split.toString());
		return new DeprecatedLzoLineRecordReader(conf, (FileSplit)split);
	  }

	}

最终的修改办法是：
仍按上述修改，但修改的是原类，没有新增类。仅在HiveServer机器更好对应的jar包。
这样，实际在HiveServer上运行的是支持多线程操作的，此时需要Split，而在Hadoop机器上，使用的仍是有旧的类代码。但是因为不需要对应的Split操作了，所以也不会有对应问题发生。
