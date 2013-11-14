package cn.uc.hadoop.mapreduce.lib.output;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import com.hadoop.compression.lzo.LzoIndex;

/** 
 * 改写自CDH4.2.1的TextOutputFormat.class
 * 需要Hadoop-Lzo的jar包
 * 使用此类会自动在job结束的时候，针对输出文件中带后缀的为lzo的文件建立分布式索引。
 *  
 */
public class LzoTextOutputFormat<K, V> extends FileOutputFormat<K, V> {
	private static final Log LOG = LogFactory.getLog(FileOutputFormat.class);
	
	public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";

	protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
		private static final String utf8 = "UTF-8";
		private static final byte[] newline;
		static {
			try {
				newline = "\n".getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		protected DataOutputStream out;
		protected Path file;
		protected FileSystem fs;
		private final byte[] keyValueSeparator;

		/**
		 * 修改的部分，添加了传入FileSystem和Path参数
		 * 用于在最后用来建立索引
		 */
		public LineRecordWriter(DataOutputStream out, String keyValueSeparator,
				FileSystem fs, Path file) {
			this.out = out;
			this.file = file;
			this.fs = fs;
			try {
				this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
			} catch (UnsupportedEncodingException uee) {
				throw new IllegalArgumentException("can't find " + utf8
						+ " encoding");
			}
		}

		/**
		 * Write the object to the byte stream, handling Text as a special case.
		 * 
		 * @param o
		 *            the object to print
		 * @throws IOException
		 *             if the write throws, we pass it on
		 */
		private void writeObject(Object o) throws IOException {
			if (o instanceof Text) {
				Text to = (Text) o;
				out.write(to.getBytes(), 0, to.getLength());
			} else {
				out.write(o.toString().getBytes(utf8));
			}
		}

		public synchronized void write(K key, V value) throws IOException {

			boolean nullKey = key == null || key instanceof NullWritable;
			boolean nullValue = value == null || value instanceof NullWritable;
			if (nullKey && nullValue) {
				return;
			}
			if (!nullKey) {
				writeObject(key);
			}
			if (!(nullKey || nullValue)) {
				out.write(keyValueSeparator);
			}
			if (!nullValue) {
				writeObject(value);
			}
			out.write(newline);
		}
		/**
		 * 重载了close函数，在关闭stream后建立索引
		 */
		public void close(TaskAttemptContext context) throws IOException{
			closeOutputStream(context);
			try {
				if (file != null) {
					LzoIndex.createIndex(fs, file);
				}
			} catch (IOException e) {
				LOG.error("Create indexing failed " + file, e);
			}
		}
		
		public synchronized void closeOutputStream(TaskAttemptContext context)
				throws IOException {
			out.close();
		}
	}

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = conf.get(SEPERATOR, "\t");
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(fileOut, keyValueSeparator, fs,
					file);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(new DataOutputStream(
					codec.createOutputStream(fileOut)), keyValueSeparator, fs,
					file);
		}
	}
}
