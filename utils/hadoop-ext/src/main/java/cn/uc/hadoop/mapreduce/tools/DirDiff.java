package cn.uc.hadoop.mapreduce.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import cn.uc.hadoop.JobUtils;
import cn.uc.hadoop.mapreduce.lib.input.FilePathTextInputFormat;

public class DirDiff {
	static public class DirDiffMapper extends Mapper<Text, Text, Text, Text> {
		static public Logger LOG = Logger.getLogger(Mapper.class);
		private Text first = new Text("1");
		private Text second = new Text("2");
		private Path p1;
		private Path p2;
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			p1 = new Path(conf.get("mydir1")).makeQualified(fs);
			p2 = new Path(conf.get("mydir2")).makeQualified(fs);
//			LOG.info(p1.toString()+" "+p2.toString());
		}

		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			Path now = new Path(key.toString());
//			LOG.info(now+" "+now.getParent());
			if(now.getParent().equals(p1)) {
				context.write(value, first);
				context.getCounter("dirdiff", "p1-in").increment(1);
			}
			else if(now.getParent().equals(p2)) {
				context.write(value, second);
				context.getCounter("dirdiff", "p2-in").increment(1);
			}
			else{
				context.getCounter("dirdiff", "error-in").increment(1);
			}
			
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// NOTHING
		}
	}

	static public class DirDiffReducer extends Reducer<Text, Text, Text, Text> {
		private Text first = new Text("1");
		private Text second = new Text("2");
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

		}

		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			boolean p1=false,p2=false;
			for(Text now:value){
				if( now.equals(first) )p1=true;
				if( now.equals(second) )p2=true;
			}
			if( p1){
				if(p2){
					context.getCounter("dirdiff", "same").increment(1);
				}
				else{
					context.getCounter("dirdiff", "only-p1").increment(1);
					context.write(first, key);
				}
			}
			else{
				if(p2){
					context.getCounter("dirdiff", "only-p2").increment(1);
					context.write(second, key);
				}
				else{
					context.getCounter("dirdiff", "error-out").increment(1);
				}
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// NOTHING
		}
	}

	static public void main(String[] args) {
		if (args.length < 2) {
			System.out.println("there is no diff dir");
		}
		String dir1 = args[0];
		String dir2 = args[1];
		String output = args[2];
		try {
			Job job = Job.getInstance(new Configuration(), "diff dir");
			job.setJarByClass(DirDiff.class);
			job.setInputFormatClass(FilePathTextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setMapperClass(DirDiffMapper.class);
			job.setReducerClass(DirDiffReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			//add .lzo file
			addDir(job,dir1);
			addDir(job,dir2);
//			TextInputFormat.addInputPath(job,new Path(dir1));
//			TextInputFormat.addInputPath(job,new Path(dir2));
			job.getConfiguration().set("mydir1", dir1);
			job.getConfiguration().set("mydir2", dir2);
			TextOutputFormat.setOutputPath(job, new Path(output));
			JobUtils.setNumberOfReducers(job);
			job.waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	static public void addDir(Job job,String dir) throws IOException{
		Path temp = new Path(dir);
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] list = fs.listStatus(temp);
		for(FileStatus status:list){
			if(status.getPath().getName().endsWith(".lzo")){
				TextInputFormat.addInputPath(job, status.getPath());
			}
		}
	}
}
