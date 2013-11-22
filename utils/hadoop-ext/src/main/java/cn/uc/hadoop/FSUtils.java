package cn.uc.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/***
 *  see {@link cn.uc.hadoop.utils.FSUtils}
 */

final public class FSUtils {

	public static final class WriteMapper extends
			Mapper<Text, Text, Text, Text> {
		private FileSystem fs;

		// 传入文件名和行数
		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			// String name = key.toString();
			// 文件名
			String fileNames = value.toString();
			String[] fileNameArray = fileNames.split("<\\$>");
			Path[] ps = new Path[fileNameArray.length];
			try {
				for (int i = 0; i < ps.length; i++) {
					URI uri = new URI(fileNameArray[i]);
					Path p = new Path(uri);
					ps[i] = p;
				}
			} catch (URISyntaxException e) {
				throw new IOException(e);
			}
			mergeToFirst(fs, ps);
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			fs = FileSystem.get(context.getConfiguration());
		}

	}

	private static FileSystem defaultFs;

	/**
	 * 在HDFS内的文件数据拷贝，非分布式实现。
	 * 
	 * @param fs
	 *            文件系统
	 * @param srcFile
	 *            源文件
	 * @param decFile
	 *            目标文件
	 * @return
	 * @throws IOException
	 */
	public static boolean copyFile(final FileSystem fs, final Path srcFile,
			final Path decFile) throws IOException {
		return FileUtil.copy(fs, srcFile, fs, decFile, false, false,
				fs.getConf());
	}

	// 文件系统，每个文件的行数，文件数
	private static Path createControlFile(FileSystem fs,
			List<List<FileStatus>> groups) throws IOException {
		Path home = fs.getHomeDirectory();
		String appNameFlag = FSUtils.class.getSimpleName();
		String tempFlag = "temp";
		Path tempDir = new Path(home, appNameFlag + "_" + tempFlag);
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss_SSS");
		String input = df.format(new Date());
		Path dir = new Path(tempDir, input);
		// 清空输入输出，便于反复操作调试测试
		fs.delete(dir, true);

		int i = 0;
		String splitFlag = "<$>";
		for (List<FileStatus> group : groups) {
			i++;
			String name = "in_file_" + i;
			Path controlFile = new Path(dir, name);
			StringBuilder sb = new StringBuilder();
			for (FileStatus f : group) {
				Path p = f.getPath();
				URI uri = p.toUri();
				String uriStr = uri.toString();
				sb.append(uriStr);
				sb.append(splitFlag);
			}
			sb.setLength(sb.length() - splitFlag.length());
			SequenceFile.Writer writer = null;
			try {
				writer = SequenceFile.createWriter(fs, fs.getConf(),
						controlFile, Text.class, Text.class,
						CompressionType.NONE);
				writer.append(new Text("name"), new Text(sb.toString()));
			} catch (Exception e) {
				throw new IOException(e.getLocalizedMessage());
			} finally {
				if (writer != null) {
					writer.close();
				}
				writer = null;
			}
		}
		return dir;
	}

	public static FileSystem getDefaultFileSystem() throws IOException {
		if (defaultFs != null) {
			return defaultFs;
		} else {
			Configuration conf = new Configuration();
			defaultFs = FileSystem.get(conf);
			return defaultFs;
		}
	}

	/**
	 * 仅仅为了测试。
	 */
	@SuppressWarnings("unused")
	private static void main(String argv[]) throws IOException {
		Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);
		String dirs[] = { "/mnt", "/mnt/hdfs/ssp", "/user/kpi" };
		String file = "test_bash_934978134.txt";
		for (String dir : dirs) {
			Path path = new Path(new Path(dir), file);
			if (!fs.createNewFile(path)) {
				throw new RuntimeException("create file fail");
			}
			if (!safeDelete(fs, path)) {
				throw new RuntimeException("delete file fail");
			}
		}
	}

	/**
	 * <pre>
	 * 合并给定目录下的文件为单个文件，会删除原文件。不会递归遍历子目录。非分布式。
	 * 这不是一个有效率的实现，对于大数据，慎用。
	 * </pre>
	 * 
	 * @param fs
	 * @param dirPath
	 *            会过滤_和.开头的文件和目录
	 * @param name
	 *            合并后的文件名，在给定目录下，不能与原有文件重名。
	 * @return
	 * @throws IOException
	 */
	public static boolean mergeAllFiles(final FileSystem fs,
			final Path dirPath, final String name) throws IOException {
		if (!fs.getFileStatus(dirPath).isDir()) {
			return false;
		}

		Path decFile = new Path(dirPath, name);

		FileStatus[] files = fs.listStatus(dirPath, new PathFilter() {
			@Override
			public boolean accept(Path path) {
				try {
					if (!fs.isFile(path)) {
						return false;
					}
				} catch (IOException e) {
					return false;
				}
				String name = path.getName();
				if (name.startsWith("_") || name.startsWith(".")) {
					return false;
				}
				return true;
			}
		});

		if (files.length <= 0) {
			return false;
		}
		FileStatus maxLenFile = null;
		long maxLen = -1;
		for (FileStatus f : files) {
			long len = f.getLen();
			if (len > maxLen) {
				maxLen = f.getLen();
				maxLenFile = f;
			}
		}
		Path p = maxLenFile.getPath();
		if (!fs.rename(p, decFile)) {
			return false;
		}
		if (files.length <= 1) {
			return true;
		}
		FSDataOutputStream out = fs.append(decFile, 0x100000);

		for (FileStatus f : files) {
			if (f == maxLenFile) {
				continue;
			}
			FSDataInputStream in = fs.open(f.getPath(), 0x100000);
			for (int c = -1; (c = in.read()) >= 0;) {
				out.write(c);
			}
			out.flush();
			in.close();
			fs.delete(f.getPath(), false);
		}
		try {
			out.close();
		} catch (IOException e) {
		}
		return true;
	}

	/**
	 * minFileSize=1G
	 * 
	 * @see #mergeAllFilesMulti(FileSystem, Path, String)
	 */
	public static boolean mergeAllFilesMulti(final FileSystem fs,
			final Path dirPath, final String name) throws IOException {
		final long minFileSize = 1024 * 1024 * 1024;
		return mergeAllFilesMulti(fs, dirPath, name, minFileSize);
	}

	/**
	 * <pre>
	 * 合并给定目录下的文件为多个文件，会删除原文件。不会递归遍历子目录。
	 * 合并策略：
	 *     1.文件从大到小排序，
	 *     2.对于小于给定大小的文件，从文件队列尾部挑选文件合并，直到其大小达到给定值。
	 *     2.1.此时判断余下的文件的大小之和是否超过阀值，如果小于阀值，把余下的值全部合并。
	 *     3.否则继续操作直到队列完全结束。
	 * 合并方法：
	 *     1.本地多线程合并
	 *     2.MR分布式合并
	 *     
	 * 合并后的文件名为${name}-*，其中*是数字编号。
	 * 
	 * 注意：为了调用者处理方便，本程序会把底层的异常转换为IOException，调用者最好显式处理此异常。
	 * 出现异常表明数据发生了非预测的问题，会影响此数据的完整性。
	 * </pre>
	 * 
	 * @param fs
	 * @param dirPath
	 * @param name
	 *            合并后的文件名前缀，在给定目录下，不能与原有文件重名。
	 * @param minFileSize
	 *            最小文件大小。
	 * @return
	 * @throws IOException
	 */
	public static boolean mergeAllFilesMulti(final FileSystem fs,
			final Path dirPath, final String name, long minFileSize)
			throws IOException {
		try {
			if (!fs.getFileStatus(dirPath).isDir()) {
				return false;
			}
			// 选出待处理文件
			FileStatus[] files = fs.listStatus(dirPath, new PathFilter() {
				@Override
				public boolean accept(Path path) {
					try {
						if (!fs.isFile(path)) {
							return false;
						}
					} catch (IOException e) {
						return false;
					}
					String name = path.getName();
					if (name.startsWith("_") || name.startsWith(".")) {
						return false;
					}
					return true;
				}
			});

			if (files.length <= 0) {
				return false;
			}

			// 排序，从大到小
			Arrays.sort(files,
					Collections.reverseOrder(new Comparator<FileStatus>() {
						@Override
						public int compare(FileStatus f1, FileStatus f2) {
							long v = f1.getLen() - f2.getLen();
							if (v == 0L) {
								return 0;
							}
							return v > 0 ? 1 : -1;
						}
					}));

			long totalSize = 0L;
			for (FileStatus f : files) {
				totalSize += f.getLen();
			}

			// 分组
			List<List<FileStatus>> groups = new ArrayList<List<FileStatus>>();
			// 其它小文件合并在此文件上，并且此文件最后会更名
			List<FileStatus> okFiles = new ArrayList<FileStatus>(files.length);
			long remainSize = totalSize;
			Deque<FileStatus> deque = new LinkedList<FileStatus>();
			Collections.addAll(deque, files);
			for (; !deque.isEmpty();) {
				FileStatus f = deque.pollFirst();
				okFiles.add(f);// 组的第一个文件，以此命名
				remainSize -= f.getLen();
				if (f.getLen() >= minFileSize) {
					// 特殊处理，如果剩下的都是小文件，并且达不到阀值
					if (remainSize < minFileSize && !deque.isEmpty()) {
						List<FileStatus> group = new ArrayList<FileStatus>();
						group.add(f);
						group.addAll(deque);
						deque.clear();
						groups.add(group);
						break;
					}
					continue;
				}
				if (deque.isEmpty()) {
					continue;
				}
				List<FileStatus> group = new ArrayList<FileStatus>();
				group.add(f);
				long groupSize = f.getLen();
				// 小文件，从队列尾部找与它合并的文件
				for (; !deque.isEmpty();) {
					FileStatus tf = deque.pollLast();
					group.add(tf);
					groupSize += tf.getLen();
					remainSize -= tf.getLen();
					if (groupSize >= minFileSize) {
						break;
					}
				}
				if (remainSize < minFileSize) {
					group.addAll(deque);
					deque.clear();
				}
				groups.add(group);
			}
			// 这个值是根据测试得到的（以1G为阀值）
			if (groups.size() > 3) {// MR分布式
				Configuration conf = fs.getConf();
				Path input = createControlFile(fs, groups);
				Job job = new Job(conf, FSUtils.class.getSimpleName() + "_"
						+ "mergeAllFilesMulti");
				FileInputFormat.setInputPaths(job, input);
				job.setInputFormatClass(SequenceFileInputFormat.class);
				job.setJarByClass(FSUtils.class);

				job.setMapperClass(WriteMapper.class);
				job.setOutputFormatClass(NullOutputFormat.class);
				job.setNumReduceTasks(0);
				try {
					boolean succeed = job.waitForCompletion(false);
					if (!succeed) {
						throw new IOException("MR merge Job is failed!");
					}
				} catch (InterruptedException e) {
					throw new IOException(e);
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			} else if (groups.size() > 0) {// 本地多线程
				// 开始多线程处理
				final AtomicBoolean isOk = new AtomicBoolean(true);
				ExecutorService pool = Executors.newFixedThreadPool(10);
				for (List<FileStatus> group : groups) {
					final List<FileStatus> g = group;
					pool.execute(new Runnable() {
						@Override
						public void run() {
							try {
								mergeToFirst(fs, g);
							} catch (IOException e) {
								e.printStackTrace();
								isOk.set(false);
							}
						}
					});
				}
				pool.shutdown();
				// 等待结束
				for (; !pool.isTerminated();) {
					try {
						pool.awaitTermination(1, TimeUnit.SECONDS);
					} catch (InterruptedException e) {
						// noop
					}
					if (!isOk.get()) {
						pool.shutdownNow();
						throw new IOException("merge Job is failed!");
					}
				}
			}

			// 更名
			int index = 0;
			for (FileStatus f : okFiles) {
				index++;
				Path src = f.getPath();
				Path dst = new Path(f.getPath().getParent(),
						(name + "-" + index));
				if (fs.exists(dst)) {
					return false;
				}
				fs.rename(src, dst);
			}
			return true;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	/**
	 * 把给定的文件合并，合并到列表中的第一个文件上
	 * 
	 * @param fs
	 * 
	 * @param group
	 * @throws IOException
	 */
	static void mergeToFirst(FileSystem fs, List<FileStatus> group)
			throws IOException {
		if (group.size() <= 1) {
			return;
		}
		Path[] ps = new Path[group.size()];
		int i = 0;
		for (FileStatus f : group) {
			if (i >= ps.length) {
				break;
			}
			ps[i] = f.getPath();
			i++;
		}
		mergeToFirst(fs, ps);
	}

	static void mergeToFirst(FileSystem fs, Path[] ps) throws IOException {
		if (ps.length <= 1) {
			return;
		}
		Path first = ps[0];
		FSDataOutputStream out = fs.append(first, 0x100000);

		for (int i = 1; i < ps.length; i++) {
			Path f = ps[i];
			FSDataInputStream in = fs.open(f, 0x100000);
			for (int c = -1; (c = in.read()) >= 0;) {
				out.write(c);
			}
			out.flush();
			in.close();
			fs.delete(f, false);
		}
		try {
			out.close();
		} catch (IOException e) {
		}
	}

	/**
	 * 带垃圾回收工功能的删除。为了数据安全，不要调用系统的delete方法，调用此函数。
	 * 
	 * @return 是否成功删除
	 * @throws IOException
	 */
	public static boolean safeDelete(final FileSystem fs, final Path src)
			throws IOException {
		Trash trashTmp = new Trash(fs, fs.getConf());
		return trashTmp.moveToTrash(src);
	}
}
