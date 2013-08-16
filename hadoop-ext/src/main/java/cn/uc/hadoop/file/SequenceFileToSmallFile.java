package cn.uc.hadoop.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SequenceFileToSmallFile {

	public static class WriteMapper extends Mapper<Text, Text, Text, Text> {
		Path dir = null;
		FileSystem fs;

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException {
			FSDataOutputStream out = null;
			try {
				String fileName = key.toString();
				fileName = fileName.replace(':', '_');
				Path file = new Path(dir, fileName);
				out = fs.create(file);
				out.write(value.getBytes(), 0, value.getLength());
			} finally {
				if (out != null) {
					out.close();
				}
			}
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			String d = context.getConfiguration().get("dir");
			dir = new Path(d);
			fs = FileSystem.get(conf);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser gop = new GenericOptionsParser(args);
		conf = gop.getConfiguration();
		String[] otherArgs = gop.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.out.println("SequenceFileToSmallFile from to");
			System.out
					.println("  you can use -D mapreduce.inputformat.class=*** to specify the input fromat");
			System.exit(1);
		}
		conf.set("dir", otherArgs[1]);

		Job job = new Job(conf, SequenceFileToSmallFile.class.getSimpleName());
		job.setJarByClass(SequenceFileToSmallFile.class);
		job.setMapperClass(WriteMapper.class);

		if (conf.get("mapreduce.inputformat.class") == null) {
			job.setInputFormatClass(KeyValueTextInputFormat.class);
		}
		job.setOutputFormatClass(NullOutputFormat.class);

		FileInputFormat.setInputPaths(job, otherArgs[0]);

		job.setNumReduceTasks(0);
		job.waitForCompletion(true);
	}
}
